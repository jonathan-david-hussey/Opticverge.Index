using System.Diagnostics;
using System.Text.Json;
using Confluent.Kafka;
using Confluent.Kafka.Admin;
using Microsoft.Extensions.Options;
using Opticverge.Index.Contracts;
using Opticverge.Index.Engine;

namespace Opticverge.Index.IndexEngine.Worker;

public sealed class Worker(
    IOptions<IndexEngineOptions> engineOptions,
    IOptions<KafkaOptions> kafkaOptions,
    ILogger<Worker> logger) : BackgroundService
{
    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        var kafka = kafkaOptions.Value;
        var options = engineOptions.Value;
        var catalog = IndexCatalog.CreateDemo();

        using var cachedTimestamp = options.UseCachedTimestamp
            ? new CachedTimestampSource(TimeSpan.FromMicroseconds(options.CachedTimestampMicros), "index-engine-clock")
            : null;

        ITimestampSource timestampSource = cachedTimestamp is null
            ? StopwatchTimestampSource.Instance
            : cachedTimestamp;

        using var pipeline = new DisruptorIndexPipeline(catalog.Indexes, new EngineOptions
        {
            PartitionCount = options.Partitions,
            RingBufferSize = options.RingBufferSize,
            WaitStrategy = options.WaitStrategy,
            UseBatchHandler = options.UseBatchHandler,
            ClearEventSlots = false,
            LatencySampleRate = options.LatencySampleRate,
            SequenceGapPolicy = options.SequenceGapPolicy,
            SequenceGapBufferSize = options.SequenceGapBufferSize,
            TimestampSource = timestampSource,
            UseDedicatedConsumerThread = options.UseDedicatedConsumerThread,
            DedicatedConsumerThreadPriority = options.DedicatedConsumerThreadPriority,
            DedicatedConsumerThreadAffinityMask = options.DedicatedConsumerThreadAffinityMask,
            DedicatedConsumerThreadIdealProcessor = options.DedicatedConsumerThreadIdealProcessor
        });

        if (string.IsNullOrWhiteSpace(kafka.BootstrapServers))
        {
            logger.LogWarning("Kafka bootstrap servers not configured. Index engine is healthy but idle.");
            await IdleWithMetricsAsync(pipeline, stoppingToken);
            return;
        }

        await EnsureSnapshotTopicAsync(kafka.BootstrapServers, stoppingToken);

        using var instrumentation = new PipelineInstrumentation(
            pipeline.Metrics,
            () => pipeline.Metrics.ConsumerLag,
            pipeline.Checkpoint);

        var partitionOffsets = new long[options.Partitions];
        Array.Fill(partitionOffsets, -1L);

        var indexCount = catalog.Indexes.Count;
        var deltaMessages = new Message<int, byte[]>[indexCount];
        var snapshotMessages = new Message<int, string>[indexCount];
        for (var i = 0; i < indexCount; i++)
        {
            var indexId = catalog.Indexes[i].IndexId.Value;
            deltaMessages[i] = new Message<int, byte[]>
            {
                Key = indexId,
                Value = new byte[IndexValueBinaryCodec.Size]
            };
            snapshotMessages[i] = new Message<int, string> { Key = indexId };
        }

        var metricsMessage = new Message<int, string> { Key = 0 };

        using var consumer = new ConsumerBuilder<Ignore, MarketTick>(new ConsumerConfig
            {
                BootstrapServers = kafka.BootstrapServers,
                GroupId = kafka.GroupId,
                AutoOffsetReset = AutoOffsetReset.Latest,
                EnableAutoCommit = true,
                FetchMinBytes = kafka.FetchMinBytes,
                QueuedMaxMessagesKbytes = kafka.QueuedMaxMessagesKbytes,
                StatisticsIntervalMs = 10_000
            })
            .SetValueDeserializer(TickDeserializer.Instance)
            .SetStatisticsHandler((_, json) =>
            {
                try
                {
                    pipeline.Metrics.UpdateConsumerLag(ParseConsumerLag(json));
                }
                catch
                {
                }
            })
            .Build();

        using var deltaProducer = new ProducerBuilder<int, byte[]>(new ProducerConfig
        {
            BootstrapServers = kafka.BootstrapServers,
            LingerMs = kafka.LingerMs,
            BatchNumMessages = kafka.BatchNumMessages,
            Acks = Acks.Leader
        }).Build();

        using var jsonProducer = new ProducerBuilder<int, string>(new ProducerConfig
        {
            BootstrapServers = kafka.BootstrapServers,
            LingerMs = kafka.LingerMs,
            BatchNumMessages = kafka.BatchNumMessages,
            Acks = Acks.Leader
        }).Build();

        var savedSnapshots = TryLoadSnapshots(kafka.BootstrapServers, logger);
        if (savedSnapshots.Count > 0)
        {
            pipeline.RestoreFromSnapshots(savedSnapshots);
            var primarySnapshot = savedSnapshots[0];
            var tpOffsets = Enumerable.Range(0, options.Partitions)
                .Select(i =>
                {
                    var replayOffset = ReplayOffsetForPartition(savedSnapshots, i);
                    var offset = replayOffset >= 0
                        ? new Offset(replayOffset + 1)
                        : Offset.Beginning;
                    return new TopicPartitionOffset(MarketTopics.RawTicks, i, offset);
                })
                .ToArray();
            consumer.Assign(tpOffsets);
            for (var i = 0; i < partitionOffsets.Length; i++)
                partitionOffsets[i] = ReplayOffsetForPartition(savedSnapshots, i);
            logger.LogInformation("Restored {Count} snapshot(s). Primary seq={Seq} value={ValueE8}",
                savedSnapshots.Count, primarySnapshot.Sequence, primarySnapshot.ValueE8);
        }
        else
        {
            consumer.Subscribe(MarketTopics.RawTicks);
        }

        _ = ConsumeCorporateActionsAsync(kafka, pipeline, stoppingToken);

        const int BatchCapacity = 4_096;
        var tickBatch = new MarketTick[BatchCapacity];
        var consumed = 0L;
        var lastDeltaPublish = 0L;

        while (!stoppingToken.IsCancellationRequested)
            try
            {
                // Blocking wait for the first message in this batch.
                var first = consumer.Consume(stoppingToken);
                if (first is null) continue;

                var batchSize = 0;
                var firstTick = first.Message.Value;
                if (firstTick.InstrumentId.Value == 0)
                {
                    pipeline.Metrics.MarkDropped();
                }
                else
                {
                    partitionOffsets[first.Partition.Value] = first.Offset.Value;
                    tickBatch[batchSize++] = firstTick;
                }

                // Non-blocking drain of librdkafka's internal buffer into the same batch.
                while (batchSize < BatchCapacity && !stoppingToken.IsCancellationRequested)
                {
                    ConsumeResult<Ignore, MarketTick>? next;
                    try
                    {
                        next = consumer.Consume(TimeSpan.Zero);
                    }
                    catch (ConsumeException)
                    {
                        break;
                    }

                    if (next is null) break;
                    var nextTick = next.Message.Value;
                    if (nextTick.InstrumentId.Value == 0)
                    {
                        pipeline.Metrics.MarkDropped();
                        continue;
                    }

                    partitionOffsets[next.Partition.Value] = next.Offset.Value;
                    tickBatch[batchSize++] = nextTick;
                }

                if (batchSize > 0)
                {
                    pipeline.TryPublishBatch(tickBatch.AsSpan(0, batchSize));
                    consumed += batchSize;

                    if (consumed - lastDeltaPublish >= options.DeltaPublishInterval)
                    {
                        lastDeltaPublish = consumed;
                        var pubStart = Stopwatch.GetTimestamp();
                        var nowNanos = MonotonicClock.TimestampNanos();
                        var indexes = pipeline.Indexes;
                        for (var i = 0; i < indexes.Count; i++)
                        {
                            var value = indexes[i];
                            IndexValueBinaryCodec.TryWrite(in value, deltaMessages[i].Value);
                            deltaProducer.Produce(MarketTopics.IndexDeltas, deltaMessages[i]);
                        }

                        metricsMessage.Value = JsonSerializer.Serialize(pipeline.Metrics.ToDto(), IndexJsonContext.Default.EngineMetricsDto);
                        jsonProducer.Produce(MarketTopics.EngineMetrics, metricsMessage);
                        var snapshots = pipeline.GetSnapshots(nowNanos, partitionOffsets);
                        for (var i = 0; i < snapshots.Length; i++)
                        {
                            snapshotMessages[i].Value = JsonSerializer.Serialize(snapshots[i], IndexJsonContext.Default.CalculatorSnapshot);
                            jsonProducer.Produce(MarketTopics.IndexSnapshots, snapshotMessages[i]);
                        }

                        var pubNanos = MonotonicClock.ToTimestampNanos(Stopwatch.GetTimestamp() - pubStart);
                        instrumentation.PublicationDurationNanos.Record(pubNanos);
                        pipeline.Metrics.RecordPublicationDuration(pubNanos);

                        logger.LogInformation("Consumed {Count:N0} ticks. Primary {IndexId}={ValueE8}",
                            consumed, indexes[0].IndexId.Value, indexes[0].ValueE8);
                    }
                }
            }
            catch (ConsumeException ex)
            {
                logger.LogWarning(ex, "Kafka consume failed");
            }

        // Write final snapshots on clean shutdown so the next start replays the smallest possible gap.
        try
        {
            var finalSnapshots = pipeline.GetSnapshots(MonotonicClock.TimestampNanos(), partitionOffsets);
            for (var i = 0; i < finalSnapshots.Length; i++)
            {
                snapshotMessages[i].Value = JsonSerializer.Serialize(finalSnapshots[i], IndexJsonContext.Default.CalculatorSnapshot);
                jsonProducer.Produce(MarketTopics.IndexSnapshots, snapshotMessages[i]);
            }

            deltaProducer.Flush(TimeSpan.FromSeconds(2));
            jsonProducer.Flush(TimeSpan.FromSeconds(2));
        }
        catch (Exception ex)
        {
            logger.LogWarning(ex, "Failed to write shutdown snapshots");
        }
    }

    private static async Task EnsureSnapshotTopicAsync(string bootstrapServers, CancellationToken cancellationToken)
    {
        using var admin = new AdminClientBuilder(new AdminClientConfig { BootstrapServers = bootstrapServers }).Build();
        try
        {
            await admin.CreateTopicsAsync([
                new TopicSpecification
                {
                    Name = MarketTopics.IndexSnapshots,
                    NumPartitions = 1,
                    ReplicationFactor = 1,
                    Configs = new Dictionary<string, string>
                    {
                        // Retain only the latest snapshot per key; older snapshots are compacted away.
                        ["cleanup.policy"] = "compact",
                        ["min.cleanable.dirty.ratio"] = "0.01",
                        ["segment.ms"] = "60000"
                    }
                }
            ]);
        }
        catch (CreateTopicsException ex) when (ex.Results.All(r => r.Error.Code == ErrorCode.TopicAlreadyExists))
        {
        }
    }

    private static Task ConsumeCorporateActionsAsync(KafkaOptions kafka, DisruptorIndexPipeline pipeline, CancellationToken stoppingToken)
    {
        return Task.Run(() =>
        {
            using var consumer = new ConsumerBuilder<Ignore, string>(new ConsumerConfig
            {
                BootstrapServers = kafka.BootstrapServers,
                GroupId = "index-engine-ca",
                AutoOffsetReset = AutoOffsetReset.Latest,
                EnableAutoCommit = true
            }).Build();

            consumer.Subscribe(MarketTopics.CorporateActions);

            while (!stoppingToken.IsCancellationRequested)
                try
                {
                    var result = consumer.Consume(stoppingToken);
                    var action = JsonSerializer.Deserialize(result.Message.Value, IndexJsonContext.Default.CorporateAction);
                    pipeline.EnqueueCorporateAction(action);
                }
                catch (ConsumeException)
                {
                }
                catch (OperationCanceledException)
                {
                    break;
                }
        }, stoppingToken);
    }

    private static IReadOnlyList<CalculatorSnapshot> TryLoadSnapshots(string bootstrapServers, ILogger logger)
    {
        try
        {
            using var consumer = new ConsumerBuilder<int, string>(new ConsumerConfig
            {
                BootstrapServers = bootstrapServers,
                GroupId = "index-engine-snapshot-loader-" + Guid.NewGuid().ToString("N")[..8],
                AutoOffsetReset = AutoOffsetReset.Latest,
                EnableAutoCommit = false
            }).Build();

            var tp = new TopicPartition(MarketTopics.IndexSnapshots, 0);
            var watermarks = consumer.QueryWatermarkOffsets(tp, TimeSpan.FromSeconds(3));
            if (watermarks.High.Value <= 0)
                return [];

            consumer.Assign([new TopicPartitionOffset(tp, Offset.Beginning)]);
            var snapshots = new Dictionary<int, CalculatorSnapshot>();
            var idlePolls = 0;

            while (idlePolls < 3)
            {
                var result = consumer.Consume(TimeSpan.FromMilliseconds(250));
                if (result is null)
                {
                    idlePolls++;
                    continue;
                }

                idlePolls = 0;
                if (result.Message?.Value is { } json)
                {
                    var snapshot = JsonSerializer.Deserialize(json, IndexJsonContext.Default.CalculatorSnapshot);
                    if (snapshot is not null) snapshots[snapshot.IndexId] = snapshot;
                }

                if (result.Offset >= watermarks.High - 1) break;
            }

            return [.. snapshots.Values.OrderBy(s => s.IndexId)];
        }
        catch (Exception ex)
        {
            logger.LogWarning(ex, "Could not load calculator snapshot; starting from initial state");
            return [];
        }
    }

    private static long ReplayOffsetForPartition(IReadOnlyList<CalculatorSnapshot> snapshots, int partition)
    {
        var offset = long.MaxValue;

        foreach (var snapshot in snapshots)
            if (partition < snapshot.PartitionOffsets.Length && snapshot.PartitionOffsets[partition] >= 0)
                offset = Math.Min(offset, snapshot.PartitionOffsets[partition]);

        return offset == long.MaxValue ? -1L : offset;
    }

    private static long ParseConsumerLag(string json)
    {
        using var doc = JsonDocument.Parse(json);
        if (!doc.RootElement.TryGetProperty("topics", out var topics)) return 0;
        var total = 0L;
        foreach (var topic in topics.EnumerateObject())
        {
            if (topic.Name != MarketTopics.RawTicks) continue;
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

    private static async Task IdleWithMetricsAsync(DisruptorIndexPipeline pipeline, CancellationToken stoppingToken)
    {
        while (!stoppingToken.IsCancellationRequested)
        {
            _ = pipeline.Metrics.ToDto();
            await Task.Delay(TimeSpan.FromSeconds(1), stoppingToken);
        }
    }

    // Decodes directly from the librdkafka-owned buffer into a MarketTick struct,
    // avoiding the intermediate byte[] allocation the default BytesDeserializer would produce.
    private sealed class TickDeserializer : IDeserializer<MarketTick>
    {
        public static readonly TickDeserializer Instance = new();

        public MarketTick Deserialize(ReadOnlySpan<byte> data, bool isNull, SerializationContext context)
        {
            TickBinaryCodec.TryRead(data, out var tick);
            return tick;
        }
    }
}

public sealed class IndexEngineOptions
{
    public int Partitions { get; init; } = 4;
    public int RingBufferSize { get; init; } = 1 << 16;
    public string WaitStrategy { get; init; } = "yielding";
    public bool UseBatchHandler { get; init; } = true;
    public int LatencySampleRate { get; init; } = 1_024;
    public SequenceGapPolicy SequenceGapPolicy { get; init; } = SequenceGapPolicy.BufferUntilRecovered;
    public int SequenceGapBufferSize { get; init; } = 8;
    public bool UseCachedTimestamp { get; init; }
    public int CachedTimestampMicros { get; init; } = 100;
    public bool UseDedicatedConsumerThread { get; init; }
    public ThreadPriority DedicatedConsumerThreadPriority { get; init; } = ThreadPriority.Normal;
    public long DedicatedConsumerThreadAffinityMask { get; init; }
    public int DedicatedConsumerThreadIdealProcessor { get; init; } = -1;
    public int DeltaPublishInterval { get; init; } = 16_384;
}

public sealed class KafkaOptions
{
    public string? BootstrapServers { get; init; }
    public string GroupId { get; init; } = "index-engine";
    public int LingerMs { get; init; } = 0;
    public int BatchNumMessages { get; init; } = 10_000;
    public int FetchMinBytes { get; init; } = 1;
    public int QueuedMaxMessagesKbytes { get; init; } = 64 * 1024;
}