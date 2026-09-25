using System.Diagnostics;
using System.Diagnostics.Metrics;
using System.Text.Json;
using Confluent.Kafka;
using Microsoft.Extensions.Options;
using Npgsql;
using Opticverge.Index.Contracts;

namespace Opticverge.Index.Publisher.Worker;

public sealed class Worker(
    IConfiguration configuration,
    IOptions<KafkaOptions> kafkaOptions,
    ILogger<Worker> logger) : BackgroundService
{
    // Pre-allocated command graph reused across every flush.
    // NpgsqlBatch.BatchCommands.Clear() releases the commands back to us without disposing them.
    private const int MaxIndexSlots = 64;

    private const string EnsureSchemaSql = """
                                           CREATE TABLE IF NOT EXISTS index_values (
                                               index_id                smallint    NOT NULL PRIMARY KEY,
                                               sequence                bigint      NOT NULL,
                                               value_e8                bigint      NOT NULL,
                                               level_e8                bigint      NOT NULL,
                                               constituent_count       int         NOT NULL,
                                               stale_constituent_count int         NOT NULL,
                                               timestamp_nanos         bigint      NOT NULL,
                                               updated_at              timestamptz NOT NULL DEFAULT now()
                                           )
                                           """;

    // Sequence guard in the WHERE clause prevents regressing to a stale value
    // if Kafka redelivers an older offset after a consumer restart.
    private const string UpsertSql = """
                                     INSERT INTO index_values
                                         (index_id, sequence, value_e8, level_e8, constituent_count, stale_constituent_count, timestamp_nanos, updated_at)
                                     VALUES ($1, $2, $3, $4, $5, $6, $7, now())
                                     ON CONFLICT (index_id) DO UPDATE SET
                                         sequence                = EXCLUDED.sequence,
                                         value_e8                = EXCLUDED.value_e8,
                                         level_e8                = EXCLUDED.level_e8,
                                         constituent_count       = EXCLUDED.constituent_count,
                                         stale_constituent_count = EXCLUDED.stale_constituent_count,
                                         timestamp_nanos         = EXCLUDED.timestamp_nanos,
                                         updated_at              = now()
                                     WHERE EXCLUDED.sequence > index_values.sequence
                                     """;

    private readonly IndexBatchSlot[] _batchSlots = CreateBatchSlots();

    private static IndexBatchSlot[] CreateBatchSlots()
    {
        var slots = new IndexBatchSlot[MaxIndexSlots];
        for (var i = 0; i < MaxIndexSlots; i++) slots[i] = new IndexBatchSlot();
        return slots;
    }

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        var kafka = kafkaOptions.Value;
        var connectionString = configuration.GetConnectionString("index-db");

        if (string.IsNullOrWhiteSpace(kafka.BootstrapServers))
        {
            logger.LogWarning("Kafka not configured. Publisher worker is idle.");
            await Task.Delay(Timeout.InfiniteTimeSpan, stoppingToken);
            return;
        }

        if (string.IsNullOrWhiteSpace(connectionString))
        {
            logger.LogWarning("Postgres connection string 'index-db' not configured. Publisher worker is idle.");
            await Task.Delay(Timeout.InfiniteTimeSpan, stoppingToken);
            return;
        }

        await using var dataSource = NpgsqlDataSource.Create(connectionString);
        await EnsureSchemaAsync(dataSource, stoppingToken);

        using var meter = new Meter("Opticverge.Index.Publisher", "1.0");
        long consumerLag = 0;
        meter.CreateObservableGauge("publisher.consumer.lag",
            () => Volatile.Read(ref consumerLag),
            "{messages}", "Consumer lag on index.deltas topic");
        var upsertCounter = meter.CreateCounter<long>(
            "publisher.upserts", "{rows}", "Index value rows upserted to Postgres");
        var flushCounter = meter.CreateCounter<long>(
            "publisher.flushes", "{flushes}", "Batch flush operations issued to Postgres");

        using var consumer = new ConsumerBuilder<Ignore, IndexValue>(new ConsumerConfig
            {
                BootstrapServers = kafka.BootstrapServers,
                GroupId = kafka.GroupId,
                AutoOffsetReset = AutoOffsetReset.Latest,
                EnableAutoCommit = true,
                StatisticsIntervalMs = 10_000
            })
            .SetStatisticsHandler((_, json) =>
            {
                try
                {
                    Volatile.Write(ref consumerLag, ParseConsumerLag(json));
                }
                catch
                {
                }
            })
            .SetValueDeserializer(IndexValueDeserializer.Instance)
            .Build();

        consumer.Subscribe(MarketTopics.IndexDeltas);

        // Buffer: latest value per index_id. Many deltas arrive per second but
        // only the most recent value matters for the UPSERT — earlier ones are discarded.
        var buffer = new Dictionary<int, IndexValue>();
        var flushInterval = TimeSpan.FromMilliseconds(kafka.FlushIntervalMs);
        var lastFlush = Stopwatch.GetTimestamp();

        logger.LogInformation("Publisher worker started. Flushing every {Interval}ms.", kafka.FlushIntervalMs);

        while (!stoppingToken.IsCancellationRequested)
            try
            {
                // Short-timeout poll so we flush on schedule even during quiet periods.
                var result = consumer.Consume(TimeSpan.FromMilliseconds(100));
                if (result is not null)
                {
                    var value = result.Message.Value;
                    buffer[value.IndexId.Value] = value;
                }

                if (Stopwatch.GetElapsedTime(lastFlush) >= flushInterval && buffer.Count > 0)
                {
                    var flushed = await FlushAsync(dataSource, buffer, stoppingToken);
                    upsertCounter.Add(flushed);
                    flushCounter.Add(1);
                    logger.LogDebug("Flushed {Count} index value(s) to Postgres", flushed);
                    buffer.Clear();
                    lastFlush = Stopwatch.GetTimestamp();
                }
            }
            catch (OperationCanceledException) when (stoppingToken.IsCancellationRequested)
            {
                break;
            }
            catch (ConsumeException ex) when (ex.Error.Code == ErrorCode.UnknownTopicOrPart)
            {
                logger.LogDebug("index.deltas topic not yet available, retrying...");
                await Task.Delay(TimeSpan.FromSeconds(2), stoppingToken);
            }
            catch (ConsumeException ex)
            {
                logger.LogWarning(ex, "Kafka consume error");
                await Task.Delay(TimeSpan.FromSeconds(1), stoppingToken);
            }
            catch (Exception ex)
            {
                logger.LogError(ex, "Postgres flush error");
                await Task.Delay(TimeSpan.FromSeconds(2), stoppingToken);
            }

        // Final flush on clean shutdown so Postgres holds the last-known state.
        if (buffer.Count > 0)
            try
            {
                await FlushAsync(dataSource, buffer, CancellationToken.None);
                logger.LogInformation("Final flush complete");
            }
            catch (Exception ex)
            {
                logger.LogWarning(ex, "Final flush failed");
            }
    }

    private async Task<int> FlushAsync(
        NpgsqlDataSource dataSource,
        Dictionary<int, IndexValue> buffer,
        CancellationToken ct)
    {
        await using var batch = dataSource.CreateBatch();

        var slot = 0;
        foreach (var (_, value) in buffer)
        {
            _batchSlots[slot].Fill(value);
            batch.BatchCommands.Add(_batchSlots[slot].Command);
            slot++;
        }

        await batch.ExecuteNonQueryAsync(ct);
        return buffer.Count;
    }

    private async Task EnsureSchemaAsync(NpgsqlDataSource dataSource, CancellationToken ct)
    {
        await using var cmd = dataSource.CreateCommand(EnsureSchemaSql);
        await cmd.ExecuteNonQueryAsync(ct);
        logger.LogInformation("Schema ready: index_values table ensured");
    }

    private static long ParseConsumerLag(string json)
    {
        using var doc = JsonDocument.Parse(json);
        if (!doc.RootElement.TryGetProperty("topics", out var topics)) return 0;
        var total = 0L;
        foreach (var topic in topics.EnumerateObject())
        {
            if (topic.Name != MarketTopics.IndexDeltas) continue;
            if (!topic.Value.TryGetProperty("partitions", out var partitions)) continue;
            foreach (var partition in partitions.EnumerateObject())
            {
                if (partition.Name == "-1") continue;
                if (partition.Value.TryGetProperty("consumer_lag", out var lag) && lag.GetInt64() >= 0)
                    total += lag.GetInt64();
            }
        }

        return total;
    }

    private sealed class IndexBatchSlot
    {
        public readonly NpgsqlBatchCommand Command;
        private readonly NpgsqlParameter<int> _constituentCount = new();
        private readonly NpgsqlParameter<short> _indexId = new();
        private readonly NpgsqlParameter<long> _levelE8 = new();
        private readonly NpgsqlParameter<long> _sequence = new();
        private readonly NpgsqlParameter<int> _staleConstituentCount = new();
        private readonly NpgsqlParameter<long> _timestampNanos = new();
        private readonly NpgsqlParameter<long> _valueE8 = new();

        public IndexBatchSlot()
        {
            Command = new NpgsqlBatchCommand(UpsertSql);
            Command.Parameters.Add(_indexId);
            Command.Parameters.Add(_sequence);
            Command.Parameters.Add(_valueE8);
            Command.Parameters.Add(_levelE8);
            Command.Parameters.Add(_constituentCount);
            Command.Parameters.Add(_staleConstituentCount);
            Command.Parameters.Add(_timestampNanos);
        }

        public void Fill(IndexValue value)
        {
            _indexId.TypedValue = value.IndexId.Value;
            _sequence.TypedValue = value.Sequence;
            _valueE8.TypedValue = value.ValueE8;
            _levelE8.TypedValue = value.LevelE8;
            _constituentCount.TypedValue = value.ConstituentCount;
            _staleConstituentCount.TypedValue = value.StaleConstituentCount;
            _timestampNanos.TypedValue = value.TimestampNanos;
        }
    }

    private sealed class IndexValueDeserializer : IDeserializer<IndexValue>
    {
        public static readonly IndexValueDeserializer Instance = new();

        public IndexValue Deserialize(ReadOnlySpan<byte> data, bool isNull, SerializationContext context)
        {
            IndexValueBinaryCodec.TryRead(data, out var value);
            return value;
        }
    }
}

public sealed class KafkaOptions
{
    public string? BootstrapServers { get; init; }
    public string GroupId { get; init; } = "publisher";
    public int FlushIntervalMs { get; init; } = 500;
}