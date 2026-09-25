using System.Text.Json;
using Confluent.Kafka;
using Microsoft.Extensions.Options;
using Opticverge.Index.Contracts;

namespace Opticverge.Index.Query.Api;

public sealed class LiveState
{
    private readonly Dictionary<int, IndexValue> _byId = new();
    private readonly object _lock = new();
    private IndexValue? _latest;

    // Swapped atomically under the lock; volatile so the read on All never needs a lock.
    private volatile IReadOnlyList<IndexValue> _sortedSnapshot = [];

    // Last value received (most recently updated index).
    public IndexValue? Latest
    {
        get
        {
            lock (_lock)
            {
                return _latest;
            }
        }
    }

    // All known index values sorted by IndexId, rebuilt on every Update().
    // The volatile reference ensures readers see a fully constructed array without locking.
    public IReadOnlyList<IndexValue> All => _sortedSnapshot;

    public void Update(IndexValue value)
    {
        lock (_lock)
        {
            _byId[value.IndexId.Value] = value;
            _latest = value;

            var sorted = new IndexValue[_byId.Count];
            var i = 0;
            foreach (var v in _byId.Values) sorted[i++] = v;
            Array.Sort(sorted, static (a, b) => a.IndexId.Value.CompareTo(b.IndexId.Value));
            _sortedSnapshot = sorted;
        }
    }
}

public sealed class IndexDeltaConsumerService(
    IOptions<KafkaOptions> kafkaOptions,
    LiveState liveState,
    ILogger<IndexDeltaConsumerService> logger) : BackgroundService
{
    protected override Task ExecuteAsync(CancellationToken stoppingToken)
    {
        var kafka = kafkaOptions.Value;
        if (string.IsNullOrWhiteSpace(kafka.BootstrapServers))
        {
            logger.LogWarning("Kafka not configured. Dashboard will show static data.");
            return Task.CompletedTask;
        }

        return Task.Run(() => Consume(kafka.BootstrapServers, stoppingToken), stoppingToken);
    }

    private async Task Consume(string bootstrapServers, CancellationToken stoppingToken)
    {
        using var consumer = new ConsumerBuilder<Ignore, IndexValue>(new ConsumerConfig
            {
                BootstrapServers = bootstrapServers,
                GroupId = "query-api",
                AutoOffsetReset = AutoOffsetReset.Latest,
                EnableAutoCommit = true
            })
            .SetValueDeserializer(IndexValueDeserializer.Instance)
            .Build();

        consumer.Subscribe(MarketTopics.IndexDeltas);

        while (!stoppingToken.IsCancellationRequested)
            try
            {
                var result = consumer.Consume(stoppingToken);
                var value = result.Message.Value;
                liveState.Update(value);
            }
            catch (ConsumeException ex) when (ex.Error.Code == ErrorCode.UnknownTopicOrPart)
            {
                logger.LogDebug("Topic not yet available, retrying...");
                await Task.Delay(TimeSpan.FromSeconds(2), stoppingToken);
            }
            catch (ConsumeException ex)
            {
                logger.LogWarning(ex, "Kafka consume error");
                await Task.Delay(TimeSpan.FromSeconds(1), stoppingToken);
            }
    }
}

public sealed class LiveMetricsState
{
    private readonly object _lock = new();
    private EngineMetricsDto? _latest;

    public EngineMetricsDto? Latest
    {
        get
        {
            lock (_lock)
            {
                return _latest;
            }
        }
    }

    public void Update(EngineMetricsDto value)
    {
        lock (_lock)
        {
            _latest = value;
        }
    }
}

internal sealed class IndexValueDeserializer : IDeserializer<IndexValue>
{
    public static readonly IndexValueDeserializer Instance = new();

    public IndexValue Deserialize(ReadOnlySpan<byte> data, bool isNull, SerializationContext context)
    {
        IndexValueBinaryCodec.TryRead(data, out var value);
        return value;
    }
}

public sealed class EngineMetricsConsumerService(
    IOptions<KafkaOptions> kafkaOptions,
    LiveMetricsState liveMetrics,
    ILogger<EngineMetricsConsumerService> logger) : BackgroundService
{
    protected override Task ExecuteAsync(CancellationToken stoppingToken)
    {
        var kafka = kafkaOptions.Value;
        if (string.IsNullOrWhiteSpace(kafka.BootstrapServers))
            return Task.CompletedTask;

        return Task.Run(() => Consume(kafka.BootstrapServers, stoppingToken), stoppingToken);
    }

    private async Task Consume(string bootstrapServers, CancellationToken stoppingToken)
    {
        using var consumer = new ConsumerBuilder<Ignore, string>(new ConsumerConfig
        {
            BootstrapServers = bootstrapServers,
            GroupId = "query-api-metrics",
            AutoOffsetReset = AutoOffsetReset.Latest,
            EnableAutoCommit = true
        }).Build();

        consumer.Subscribe(MarketTopics.EngineMetrics);

        while (!stoppingToken.IsCancellationRequested)
            try
            {
                var result = consumer.Consume(stoppingToken);
                var value = JsonSerializer.Deserialize(result.Message.Value, IndexJsonContext.Default.EngineMetricsDto);
                liveMetrics.Update(value!);
            }
            catch (ConsumeException ex) when (ex.Error.Code == ErrorCode.UnknownTopicOrPart)
            {
                logger.LogDebug("engine.metrics topic not yet available, retrying...");
                await Task.Delay(TimeSpan.FromSeconds(2), stoppingToken);
            }
            catch (ConsumeException ex)
            {
                logger.LogWarning(ex, "Kafka consume error");
                await Task.Delay(TimeSpan.FromSeconds(1), stoppingToken);
            }
    }
}

public sealed class KafkaOptions
{
    public string? BootstrapServers { get; init; }
}