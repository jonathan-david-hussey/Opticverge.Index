using System.Text.Json;
using BenchmarkDotNet.Attributes;
using BenchmarkDotNet.Engines;
using BenchmarkDotNet.Running;
using MemoryPack;
using Opticverge.Index.Contracts;
using Opticverge.Index.Engine;

using var gcLatency = RuntimeTuning.EnterSustainedLowLatencyGc();
BenchmarkSwitcher.FromAssembly(typeof(HotPathBenchmarks).Assembly).Run(args);

[MemoryDiagnoser]
[SimpleJob(warmupCount: 3, iterationCount: 5)]
public class HotPathBenchmarks
{
    private const int BatchSize = 1_024;
    private const int TickPoolSize = 1 << 16;
    private const int BatchPoolSize = 256;
    private readonly byte[] _batchBuffer = new byte[TickBinaryCodec.Size * BatchSize];
    private readonly MarketTick[][] _batchPool = new MarketTick[BatchPoolSize][];

    private readonly byte[] _buffer = new byte[TickBinaryCodec.Size];
    private readonly Consumer _consumer = new();
    private readonly long[] _instrumentSequences = new long[9];
    private readonly long[] _pricesE8 = new long[BatchSize];
    private readonly MarketTick[] _tickPool = new MarketTick[TickPoolSize];
    private readonly long[] _weightsE8 = new long[BatchSize];
    private MarketTick[] _batch = null!;
    private int _batchPoolIndex;
    private GcLatencyModeScope _benchmarkGcLatency;

    private WeightedIndexCalculator _calculator = null!;
    private long _exchangeTimestampNanos;
    private DisruptorIndexPipeline _pipeline = null!;
    private int _routeInstrumentId;
    private long _sequence;
    private WeightedIndexCalculator _spscCalculator = null!;
    private SpscMarketTickRingBuffer _spscRing = null!;
    private MarketTick _tick;
    private int _tickPoolIndex;

    [GlobalSetup]
    public void Setup()
    {
        _sequence = 0;
        Array.Clear(_instrumentSequences);
        _batchPoolIndex = 0;
        _routeInstrumentId = 0;
        _tickPoolIndex = 0;
        _benchmarkGcLatency = RuntimeTuning.EnterSustainedLowLatencyGc();
        var catalog = IndexCatalog.CreateDemo();
        _exchangeTimestampNanos = MonotonicClock.TimestampNanos();

        for (var i = 0; i < _tickPool.Length; i++) _tickPool[i] = CreateTick();

        CreateBatchPool(_exchangeTimestampNanos + TickPoolSize + 1_000_000);
        _batch = NextBatch();

        _tick = _tickPool[0];
        _calculator = new WeightedIndexCalculator(catalog.Indexes[0]);
        _spscCalculator = new WeightedIndexCalculator(catalog.Indexes[0]);
        _pipeline = new DisruptorIndexPipeline(
            catalog.Indexes[0],
            new EngineOptions
            {
                RingBufferSize = 1 << 20,
                WaitStrategy = "yielding",
                UseBatchHandler = true,
                AssumePrevalidatedTicks = true,
                LatencySampleRate = 0
            });
        _spscRing = new SpscMarketTickRingBuffer(1 << 20);

        for (var i = 0; i < _batch.Length; i++)
        {
            TickBinaryCodec.TryWrite(in _batch[i], _batchBuffer.AsSpan(i * TickBinaryCodec.Size, TickBinaryCodec.Size));
            _pricesE8[i] = 9_350_000_000 + i;
            _weightsE8[i] = PriceNormalizer.Scale;
        }

        var target = _pipeline.Metrics.MessagesOut + BatchSize;
        if (!_pipeline.TryPublishBatch(_batch)) throw new InvalidOperationException("Disruptor warm-up publish failed during benchmark setup.");

        if (!_pipeline.WaitForMessagesOut(target, TimeSpan.FromSeconds(10)))
            throw new InvalidOperationException(
                $"Disruptor consumer did not start during benchmark setup. In={_pipeline.Metrics.MessagesIn}, Out={_pipeline.Metrics.MessagesOut}, LatestSequence={_pipeline.Latest.Sequence}.");
    }

    [GlobalCleanup]
    public void Cleanup()
    {
        _pipeline.Dispose();
        _benchmarkGcLatency.Dispose();
    }

    [Benchmark]
    public MarketTick Create_Tick()
    {
        var tick = CreateTick();
        return tick;
    }

    [Benchmark]
    public bool Encode_Tick()
    {
        return TickBinaryCodec.TryWrite(in _tick, _buffer);
    }

    [Benchmark]
    public bool Decode_Tick()
    {
        TickBinaryCodec.TryWrite(in _tick, _buffer);
        return TickBinaryCodec.TryRead(_buffer, out _);
    }

    [Benchmark]
    public int RoutePartition()
    {
        var instrumentId = new InstrumentId((_routeInstrumentId++ & 8191) + 1);
        return PartitionRouter.ForInstrument(instrumentId, 8);
    }

    [Benchmark]
    public bool ApplyIndexMath()
    {
        ref readonly var tick = ref NextPooledTick();
        return _calculator.ApplyPrevalidated(in tick, out _);
    }

    [Benchmark]
    public bool ApplyIndexMathStateOnly()
    {
        ref readonly var tick = ref NextPooledTick();
        return _calculator.ApplyPrevalidated(in tick);
    }

    [Benchmark]
    public bool PublishToDisruptor()
    {
        ref readonly var tick = ref NextPooledTick();
        return _pipeline.TryPublish(in tick);
    }

    [Benchmark(OperationsPerInvoke = BatchSize)]
    public void EncodeTickBatch()
    {
        var offset = 0;
        for (var i = 0; i < _batch.Length; i++)
        {
            TickBinaryCodec.TryWrite(in _batch[i], _batchBuffer.AsSpan(offset, TickBinaryCodec.Size));
            offset += TickBinaryCodec.Size;
        }

        _consumer.Consume(_batchBuffer[0]);
    }

    [Benchmark(OperationsPerInvoke = BatchSize)]
    public void DecodeTickBatch()
    {
        var offset = 0;
        for (var i = 0; i < _batch.Length; i++)
        {
            TickBinaryCodec.TryRead(_batchBuffer.AsSpan(offset, TickBinaryCodec.Size), out var tick);
            _consumer.Consume(tick.Sequence);
            offset += TickBinaryCodec.Size;
        }
    }

    [Benchmark(OperationsPerInvoke = BatchSize)]
    public void ApplyIndexMathBatch()
    {
        for (var i = 0; i < _batch.Length; i++)
        {
            _calculator.ApplyPrevalidated(in _batch[i], out var value);
            _consumer.Consume(value.Sequence);
        }
    }

    [Benchmark(OperationsPerInvoke = BatchSize)]
    public void ApplyIndexMathBatchStateOnly()
    {
        for (var i = 0; i < _batch.Length; i++) _calculator.ApplyPrevalidated(in _batch[i]);
    }

    [Benchmark(OperationsPerInvoke = BatchSize)]
    public void PublishBatchToDisruptor()
    {
        _batch = NextBatch();
        _pipeline.TryPublishBatch(_batch);
    }

    [Benchmark(OperationsPerInvoke = BatchSize)]
    public void PublishBatchToDisruptorAndWait()
    {
        _batch = NextBatch();
        var target = _pipeline.Metrics.MessagesOut + BatchSize;
        _pipeline.TryPublishBatch(_batch);
        if (!_pipeline.WaitForMessagesOut(target, TimeSpan.FromSeconds(10)))
            throw new InvalidOperationException(
                $"Disruptor consumer did not drain the benchmark batch within the timeout. In={_pipeline.Metrics.MessagesIn}, Out={_pipeline.Metrics.MessagesOut}, Target={target}, LatestSequence={_pipeline.Latest.Sequence}.");
    }

    [Benchmark(OperationsPerInvoke = BatchSize)]
    public void SpscWriteDrainBatch()
    {
        var written = _spscRing.TryWriteBatch(_batch);
        if (written != BatchSize) throw new InvalidOperationException("SPSC benchmark ring buffer is full.");

        var drained = _spscRing.DrainTo(_spscCalculator, BatchSize, true);
        if (drained != BatchSize) throw new InvalidOperationException("SPSC benchmark ring buffer did not drain the batch.");
    }

    [Benchmark(OperationsPerInvoke = BatchSize)]
    public long RecomputeIndexScalar()
    {
        return IndexMath.RecomputeScalar(_pricesE8, _weightsE8);
    }

    [Benchmark(OperationsPerInvoke = BatchSize)]
    public long RecomputeIndexVectorizedProducts()
    {
        return IndexMath.RecomputeVectorizedProducts(_pricesE8, _weightsE8);
    }

    private MarketTick CreateTick()
    {
        var tickNumber = ++_sequence;
        var instrumentId = new InstrumentId((int)((tickNumber - 1) & 7) + 1);
        var sequence = ++_instrumentSequences[instrumentId.Value];
        var exchangeTimestampNanos = ++_exchangeTimestampNanos;
        return new MarketTick(
            sequence,
            instrumentId,
            new ExchangeId(1),
            9_350_000_000 + (tickNumber & 1023),
            100,
            exchangeTimestampNanos,
            exchangeTimestampNanos + 1_000,
            PartitionRouter.ForInstrument(instrumentId, 8),
            TickFlags.Trade);
    }

    private void CreateBatchPool(long firstTimestampNanos)
    {
        var sequences = new long[9];
        var timestamp = firstTimestampNanos;
        var priceOffset = 0L;

        for (var batchIndex = 0; batchIndex < _batchPool.Length; batchIndex++)
        {
            var batch = new MarketTick[BatchSize];
            for (var i = 0; i < batch.Length; i++)
            {
                var instrumentId = new InstrumentId((i & 7) + 1);
                var sequence = ++sequences[instrumentId.Value];
                var tickTimestamp = ++timestamp;
                batch[i] = new MarketTick(
                    sequence,
                    instrumentId,
                    new ExchangeId(1),
                    9_350_000_000 + ((priceOffset + i) & 1023),
                    100,
                    tickTimestamp,
                    tickTimestamp + 1_000,
                    PartitionRouter.ForInstrument(instrumentId, 8),
                    TickFlags.Trade);
            }

            _batchPool[batchIndex] = batch;
            priceOffset += BatchSize;
        }
    }

    private MarketTick[] NextBatch()
    {
        var index = _batchPoolIndex++ & (BatchPoolSize - 1);
        return _batchPool[index];
    }

    private ref readonly MarketTick NextPooledTick()
    {
        var index = _tickPoolIndex++ & (TickPoolSize - 1);
        return ref _tickPool[index];
    }
}

[MemoryDiagnoser]
[SimpleJob(warmupCount: 3, iterationCount: 5)]
public class ShardedPipelineBenchmarks
{
    private const int BatchSize = 1_024;
    private const int TickPoolSize = 1 << 16;

    private const int RingBufferSize = 1 << 16;

    // 64 pre-built batches per shard × 1024 ticks = 65536 unique ticks before wrapping.
    private const int BatchPoolSize = 64;

    private const int OneShardOperations = BatchSize;
    private const int TwoShardOperations = BatchSize * 2;
    private const int FourShardOperations = BatchSize * 4;
    private const int EightShardOperations = BatchSize * 8;

    // On HT machines, ProcessorCount is logical processors (2× physical).
    // BusySpinWaitStrategy needs one dedicated thread per *physical* core — two spinning
    // HT siblings share execution units and give no throughput gain over one.
    // ProcessorCount / 2 approximates physical cores on HT hardware; it is conservative
    // on non-HT hardware (halves the real count) but never unsafe.
    private static readonly int EffectiveShardCount =
        Math.Clamp(Environment.ProcessorCount / 2, 1, 32);

    private readonly Consumer _consumer = new();
    private int[] _batchPoolIndexes = null!;

    // Per-shard prebuilt pools — rotated cheaply inside the measured method.
    private MarketTick[][][] _batchPools = null!;
    private GcLatencyModeScope _benchmarkGcLatency;
    private EngineMemoryBudget _memoryBudget;

    private DisruptorIndexPipeline[] _pipelines = null!;
    private long[] _targets = null!;

    [GlobalSetup]
    public void Setup()
    {
        _benchmarkGcLatency = RuntimeTuning.EnterSustainedLowLatencyGc();
        var catalog = IndexCatalog.CreateDemo();
        _pipelines = new DisruptorIndexPipeline[EffectiveShardCount];
        _batchPools = new MarketTick[EffectiveShardCount][][];
        _batchPoolIndexes = new int[EffectiveShardCount];
        _targets = new long[EffectiveShardCount];

        for (var shard = 0; shard < EffectiveShardCount; shard++)
        {
            // Pin each consumer to logical processor shard*2 (the first HT of each physical core).
            // The HT sibling (shard*2+1) is left free for the producer and benchmark runner threads.
            // YieldingWaitStrategy rather than BusySpinWaitStrategy: on a laptop the consumers spin
            // continuously between iterations and drive all cores to 100%, causing thermal throttling
            // that skews later benchmark results. Yielding gives identical throughput for batch workloads
            // (the consumer yields only when the ring buffer is empty) without the heat.
            _pipelines[shard] = new DisruptorIndexPipeline(
                catalog.Indexes[0],
                new EngineOptions
                {
                    RingBufferSize = RingBufferSize,
                    WaitStrategy = "yielding",
                    UseBatchHandler = true,
                    AssumePrevalidatedTicks = true,
                    ClearEventSlots = false,
                    LatencySampleRate = 0,
                    UseDedicatedConsumerThread = true,
                    DedicatedConsumerThreadPriority = ThreadPriority.Normal,
                    DedicatedConsumerThreadIdealProcessor = shard * 2
                });

            // Build a pool of pre-generated batches so tick construction is not inside
            // the measured method. Each shard gets a disjoint sequence and timestamp space.
            var shardTimestamp = (shard + 1) * 100_000_000L;
            var shardSequences = new long[9];
            var pool = new MarketTick[BatchPoolSize][];
            for (var b = 0; b < BatchPoolSize; b++)
            {
                var batch = new MarketTick[BatchSize];
                for (var i = 0; i < batch.Length; i++)
                {
                    var instrumentId = new InstrumentId((i & 7) + 1);
                    var seq = ++shardSequences[instrumentId.Value];
                    var ts = ++shardTimestamp;
                    batch[i] = new MarketTick(seq, instrumentId, new ExchangeId(1),
                        9_350_000_000 + (seq & 1023), 100,
                        ts, ts + 1_000,
                        shard, TickFlags.Trade);
                }

                pool[b] = batch;
            }

            _batchPools[shard] = pool;
        }

        _memoryBudget = EngineMemoryBudgetEstimator.Estimate(
            EffectiveShardCount, RingBufferSize, TickPoolSize, BatchSize);

        PublishAndConsume(1);
    }

    [GlobalCleanup]
    public void Cleanup()
    {
        for (var i = 0; i < _pipelines.Length; i++)
            _pipelines[i].Dispose();

        _benchmarkGcLatency.Dispose();
    }

    [Benchmark(OperationsPerInvoke = OneShardOperations)]
    public void PublishAndConsume_1Shard()
    {
        PublishAndConsume(Math.Min(1, EffectiveShardCount));
    }

    [Benchmark(OperationsPerInvoke = TwoShardOperations)]
    public void PublishAndConsume_2Shards()
    {
        PublishAndConsume(Math.Min(2, EffectiveShardCount));
    }

    [Benchmark(OperationsPerInvoke = FourShardOperations)]
    public void PublishAndConsume_4Shards()
    {
        PublishAndConsume(Math.Min(4, EffectiveShardCount));
    }

    [Benchmark(OperationsPerInvoke = EightShardOperations)]
    public void PublishAndConsume_8Shards()
    {
        PublishAndConsume(Math.Min(8, EffectiveShardCount));
    }

    // Reports ns per invocation (not per tick) — divide by EffectiveShardCount * BatchSize for ns/tick.
    [Benchmark]
    public void PublishAndConsume_MaxShards()
    {
        PublishAndConsume(EffectiveShardCount);
    }

    [Benchmark]
    public long EstimatedPreallocatedHotPathBytes()
    {
        return _memoryBudget.TotalHotPathBytes;
    }

    private void PublishAndConsume(int shardCount)
    {
        for (var shard = 0; shard < shardCount; shard++)
            _targets[shard] = _pipelines[shard].Metrics.MessagesOut + BatchSize;

        for (var shard = 0; shard < shardCount; shard++)
        {
            var batch = _batchPools[shard][_batchPoolIndexes[shard]++ & (BatchPoolSize - 1)];
            if (!_pipelines[shard].TryPublishBatch(batch))
                throw new InvalidOperationException($"Shard {shard} publish failed.");
        }

        for (var shard = 0; shard < shardCount; shard++)
            if (!_pipelines[shard].WaitForMessagesOut(_targets[shard], TimeSpan.FromSeconds(10)))
                throw new InvalidOperationException(
                    $"Shard {shard} did not drain. In={_pipelines[shard].Metrics.MessagesIn}, Out={_pipelines[shard].Metrics.MessagesOut}, Target={_targets[shard]}.");

        _consumer.Consume(_pipelines[0].Latest.Sequence);
    }
}

[MemoryDiagnoser]
[SimpleJob(warmupCount: 3, iterationCount: 5)]
public class TimestampBenchmarks
{
    private CachedTimestampSource _cachedTimestamp = null!;

    [GlobalSetup]
    public void Setup()
    {
        _cachedTimestamp = new CachedTimestampSource(TimeSpan.FromMicroseconds(100), "benchmark-clock");
    }

    [GlobalCleanup]
    public void Cleanup()
    {
        _cachedTimestamp.Dispose();
    }

    [Benchmark]
    public long ExactTimestampNanos()
    {
        return MonotonicClock.TimestampNanos();
    }

    [Benchmark]
    public long CachedTimestampNanos()
    {
        return _cachedTimestamp.TimestampNanos();
    }
}

[MemoryDiagnoser]
[SimpleJob(warmupCount: 3, iterationCount: 5)]
public class IndexValueSerializationBenchmarks
{
    private readonly byte[] _binaryBuffer = new byte[IndexValueBinaryCodec.Size];

    private readonly MemoryPackIndexValue _memoryPackValue = new(
        100,
        123_456,
        987_654_321,
        123_456_789,
        1_234_567_890,
        8,
        1);

    private readonly IndexValue _value = new(
        new IndexId(100),
        123_456,
        987_654_321,
        123_456_789,
        1_234_567_890,
        8,
        1);

    private string _json = null!;
    private byte[] _memoryPackBytes = null!;

    [GlobalSetup]
    public void Setup()
    {
        _json = JsonSerializer.Serialize(_value, IndexJsonContext.Default.IndexValue);
        IndexValueBinaryCodec.TryWrite(in _value, _binaryBuffer);
        _memoryPackBytes = MemoryPackSerializer.Serialize(_memoryPackValue);
    }

    [Benchmark]
    public string SerializeJson()
    {
        return JsonSerializer.Serialize(_value, IndexJsonContext.Default.IndexValue);
    }

    [Benchmark]
    public IndexValue DeserializeJson()
    {
        return JsonSerializer.Deserialize(_json, IndexJsonContext.Default.IndexValue);
    }

    [Benchmark]
    public bool SerializeBinary()
    {
        return IndexValueBinaryCodec.TryWrite(in _value, _binaryBuffer);
    }

    [Benchmark]
    public IndexValue DeserializeBinary()
    {
        IndexValueBinaryCodec.TryRead(_binaryBuffer, out var value);
        return value;
    }

    [Benchmark]
    public byte[] SerializeMemoryPack()
    {
        return MemoryPackSerializer.Serialize(_memoryPackValue);
    }

    [Benchmark]
    public MemoryPackIndexValue? DeserializeMemoryPack()
    {
        return MemoryPackSerializer.Deserialize<MemoryPackIndexValue>(_memoryPackBytes);
    }
}

[MemoryPackable]
public readonly partial record struct MemoryPackIndexValue(
    short IndexId,
    long Sequence,
    long ValueE8,
    long LevelE8,
    long TimestampNanos,
    int ConstituentCount,
    int StaleConstituentCount);

[MemoryDiagnoser]
[SimpleJob(warmupCount: 3, iterationCount: 5)]
public class MetricsSnapshotBenchmarks
{
    private const int SampleCount = 16_384;
    private GcLatencyModeScope _benchmarkGcLatency;

    private LatencyHistogram _histogram = null!;
    private EngineMetrics _metrics = null!;
    private long[] _partitionOffsets = null!;
    private DisruptorIndexPipeline _pipeline = null!;

    [GlobalSetup]
    public void Setup()
    {
        _benchmarkGcLatency = RuntimeTuning.EnterSustainedLowLatencyGc();

        // Fill to capacity so Snapshot() exercises the full sort path on every call.
        _histogram = new LatencyHistogram();
        for (var i = 0; i < SampleCount; i++)
            _histogram.Record((i + 1) * 100L);

        // Populate all four histograms so ToDto() hits the full Snapshot() path four times.
        _metrics = new EngineMetrics();
        _metrics.MarkIn(SampleCount);
        for (var i = 0; i < SampleCount; i++)
        {
            _metrics.RecordLatency((i + 1) * 100L);
            _metrics.RecordEndToEndLatency((i + 1) * 100L);
            _metrics.RecordCalcDuration((i + 1) * 100L);
            _metrics.RecordPublicationDuration((i + 1) * 100L);
        }

        _metrics.MarkOut(SampleCount);

        // Multi-index pipeline matching the production configuration.
        var catalog = IndexCatalog.CreateDemo();
        _pipeline = new DisruptorIndexPipeline(catalog.Indexes, new EngineOptions
        {
            RingBufferSize = 1 << 16,
            WaitStrategy = "yielding",
            UseBatchHandler = true,
            AssumePrevalidatedTicks = true,
            LatencySampleRate = 0
        });

        // Prime all three calculators with one tick per constituent (instruments 1-8).
        var batch = new MarketTick[8];
        for (var i = 0; i < batch.Length; i++)
        {
            var seq = i + 1L;
            var id = new InstrumentId(i + 1);
            batch[i] = new MarketTick(seq, id, new ExchangeId(1),
                9_350_000_000 + i, 100,
                seq * 1_000, seq * 1_000 + 500,
                PartitionRouter.ForInstrument(id, 4), TickFlags.Trade);
        }

        var target = _pipeline.Metrics.MessagesOut + batch.Length;
        if (!_pipeline.TryPublishBatch(batch))
            throw new InvalidOperationException("Warm-up publish failed.");
        if (!_pipeline.WaitForMessagesOut(target, TimeSpan.FromSeconds(10)))
            throw new InvalidOperationException("Warm-up drain timed out.");

        _partitionOffsets = new long[4];
        Array.Fill(_partitionOffsets, 0L);
    }

    [GlobalCleanup]
    public void Cleanup()
    {
        _pipeline.Dispose();
        _benchmarkGcLatency.Dispose();
    }

    [Benchmark]
    public void LatencyHistogram_Record()
    {
        _histogram.Record(1_234_567L);
    }

    [Benchmark]
    public LatencySnapshot LatencyHistogram_Snapshot()
    {
        return _histogram.Snapshot();
    }

    [Benchmark]
    public EngineMetricsDto EngineMetrics_ToDto()
    {
        return _metrics.ToDto();
    }

    [Benchmark]
    public CalculatorSnapshot[] Pipeline_GetSnapshots()
    {
        return _pipeline.GetSnapshots(MonotonicClock.TimestampNanos(), _partitionOffsets);
    }
}

[MemoryDiagnoser]
[SimpleJob(warmupCount: 3, iterationCount: 5)]
public class MultiIndexBenchmarks
{
    private const int BatchSize = 1_024;
    private const int TickPoolSize = 1 << 16;
    private GcLatencyModeScope _benchmarkGcLatency;

    private WeightedIndexCalculator[] _calculators = null!;
    private EngineMetrics _metrics = null!;
    private MarketTick[] _tickPool = null!;
    private int _tickPoolIndex;
    private CorporateAction _weightChangeAction;

    [GlobalSetup]
    public void Setup()
    {
        _benchmarkGcLatency = RuntimeTuning.EnterSustainedLowLatencyGc();
        var catalog = IndexCatalog.CreateDemo();

        _calculators = new WeightedIndexCalculator[catalog.Indexes.Count];
        for (var i = 0; i < catalog.Indexes.Count; i++)
            _calculators[i] = new WeightedIndexCalculator(catalog.Indexes[i]);

        _metrics = new EngineMetrics();
        _tickPool = new MarketTick[TickPoolSize];
        var ts = MonotonicClock.TimestampNanos();
        for (var i = 0; i < _tickPool.Length; i++)
        {
            var seq = i + 1L;
            var id = new InstrumentId((i & 7) + 1);
            _tickPool[i] = new MarketTick(seq, id, new ExchangeId(1),
                9_350_000_000 + (i & 1023), 100,
                ts + seq, ts + seq + 500,
                PartitionRouter.ForInstrument(id, 4), TickFlags.Trade);
        }

        // Prime all calculators with one full round of ticks.
        for (var i = 0; i < 8; i++)
        {
            ref readonly var tick = ref _tickPool[i];
            foreach (var calc in _calculators)
                calc.ApplyPrevalidated(in tick);
        }

        _weightChangeAction = new CorporateAction(
            _calculators[0].IndexId,
            1,
            CorporateActionType.WeightChange,
            PriceNormalizer.Scale);

        _metrics.MarkIn(BatchSize);
    }

    [GlobalCleanup]
    public void Cleanup()
    {
        _benchmarkGcLatency.Dispose();
    }

    // 3-calculator fan-out: mirrors what OnBatch does per tick across all indexes.
    [Benchmark(OperationsPerInvoke = BatchSize)]
    public void MultiIndex_ApplyPrevalidatedBatch()
    {
        for (var i = 0; i < BatchSize; i++)
        {
            ref readonly var tick = ref _tickPool[_tickPoolIndex++ & (TickPoolSize - 1)];
            foreach (var calc in _calculators)
                calc.ApplyPrevalidated(in tick);
        }
    }

    // Snapshot read at batch boundary — fixed-point double division.
    [Benchmark]
    public IndexValue Calculator_Latest()
    {
        return _calculators[0].Latest;
    }

    // Batch-end counter bump — must be zero allocation.
    [Benchmark(OperationsPerInvoke = BatchSize)]
    public void EngineMetrics_MarkOut()
    {
        _metrics.MarkOut(BatchSize);
    }

    // Per-sample latency record — must be zero allocation.
    [Benchmark]
    public void EngineMetrics_RecordLatency()
    {
        _metrics.RecordLatency(1_234_567L);
    }

    // Corporate action on the hot calculator — must be zero allocation.
    [Benchmark]
    public bool CorporateAction_WeightChange()
    {
        return _calculators[0].ApplyCorporateAction(_weightChangeAction);
    }
}

[MemoryDiagnoser]
[SimpleJob(warmupCount: 3, iterationCount: 5)]
public class ManyIndexRoutingBenchmarks
{
    private const int BatchSize = 1_024;
    private const int TickPoolSize = 1 << 16;
    private const int IndexCount = 64;
    private const int InstrumentCount = 256;
    private const int ConstituentsPerIndex = 8;
    private const int BatchPoolSize = 64;
    private readonly Consumer _consumer = new();
    private GcLatencyModeScope _benchmarkGcLatency;

    private WeightedIndexCalculator[] _naiveCalculators = null!;

    private DisruptorIndexPipeline _pipeline = null!;

    // Pre-built pool used inside PipelineArrayRouted_64Indexes — tick generation excluded from measurement.
    private MarketTick[][] _pipelineBatchPool = null!;
    private int _pipelineBatchPoolIndex;
    private WeightedIndexCalculator[] _routedCalculators = null!;
    private int[][] _routesByInstrumentId = null!;
    private MarketTick[] _tickPool = null!;
    private int _tickPoolIndex;

    [GlobalSetup]
    public void Setup()
    {
        _benchmarkGcLatency = RuntimeTuning.EnterSustainedLowLatencyGc();
        var definitions = CreateDefinitions();
        _naiveCalculators = [.. definitions.Select(d => new WeightedIndexCalculator(d))];
        _routedCalculators = [.. definitions.Select(d => new WeightedIndexCalculator(d))];
        _routesByInstrumentId = BuildRoutes(definitions, InstrumentCount);
        _pipeline = new DisruptorIndexPipeline(
            definitions,
            new EngineOptions
            {
                RingBufferSize = 1 << 20,
                WaitStrategy = "yielding",
                UseBatchHandler = true,
                AssumePrevalidatedTicks = true,
                LatencySampleRate = 0
            });

        _tickPool = new MarketTick[TickPoolSize];
        var timestamp = MonotonicClock.TimestampNanos();
        var sequences = new long[InstrumentCount + 1];
        for (var i = 0; i < _tickPool.Length; i++)
        {
            var instrumentId = new InstrumentId(i % InstrumentCount + 1);
            var sequence = ++sequences[instrumentId.Value];
            _tickPool[i] = new MarketTick(
                sequence,
                instrumentId,
                new ExchangeId(1),
                9_350_000_000 + (i & 1023),
                100,
                timestamp + sequence,
                timestamp + sequence + 500,
                PartitionRouter.ForInstrument(instrumentId, 4),
                TickFlags.Trade);
        }

        // Build a pre-generated pool for the pipeline benchmark — tick construction
        // stays out of the measured method.
        var poolTimestamp = timestamp + TickPoolSize + 1_000_000;
        var poolSequences = new long[InstrumentCount + 1];
        _pipelineBatchPool = new MarketTick[BatchPoolSize][];
        for (var b = 0; b < BatchPoolSize; b++)
        {
            var batch = new MarketTick[BatchSize];
            for (var i = 0; i < batch.Length; i++)
            {
                var instrumentId = new InstrumentId(i % InstrumentCount + 1);
                var seq = ++poolSequences[instrumentId.Value];
                var ts = ++poolTimestamp;
                batch[i] = new MarketTick(seq, instrumentId, new ExchangeId(1),
                    9_350_000_000 + (seq & 1023), 100,
                    ts, ts + 500,
                    PartitionRouter.ForInstrument(instrumentId, 4), TickFlags.Trade);
            }

            _pipelineBatchPool[b] = batch;
        }

        PublishAndWait();
    }

    [GlobalCleanup]
    public void Cleanup()
    {
        _pipeline.Dispose();
        _benchmarkGcLatency.Dispose();
    }

    [Benchmark(OperationsPerInvoke = BatchSize)]
    public void NaiveFanOut_64Indexes()
    {
        for (var i = 0; i < BatchSize; i++)
        {
            ref readonly var tick = ref _tickPool[_tickPoolIndex++ & (TickPoolSize - 1)];
            foreach (var calc in _naiveCalculators)
                calc.ApplyPrevalidated(in tick);
        }
    }

    [Benchmark(OperationsPerInvoke = BatchSize)]
    public void ArrayRouted_64Indexes()
    {
        for (var i = 0; i < BatchSize; i++)
        {
            ref readonly var tick = ref _tickPool[_tickPoolIndex++ & (TickPoolSize - 1)];
            foreach (var calcIndex in _routesByInstrumentId[tick.InstrumentId.Value])
                _routedCalculators[calcIndex].ApplyPrevalidated(in tick);
        }
    }

    [Benchmark(OperationsPerInvoke = BatchSize)]
    public void PipelineArrayRouted_64Indexes()
    {
        PublishAndWait();
    }

    private void PublishAndWait()
    {
        var batch = _pipelineBatchPool[_pipelineBatchPoolIndex++ & (BatchPoolSize - 1)];
        var target = _pipeline.Metrics.MessagesOut + BatchSize;
        if (!_pipeline.TryPublishBatch(batch)) throw new InvalidOperationException("Pipeline publish failed.");

        if (!_pipeline.WaitForMessagesOut(target, TimeSpan.FromSeconds(10)))
            throw new InvalidOperationException(
                $"Pipeline did not drain. In={_pipeline.Metrics.MessagesIn}, Out={_pipeline.Metrics.MessagesOut}, Target={target}.");

        _consumer.Consume(_pipeline.Latest.Sequence);
    }

    private static IndexDefinition[] CreateDefinitions()
    {
        var definitions = new IndexDefinition[IndexCount];
        for (var index = 0; index < definitions.Length; index++)
        {
            var constituents = new IndexConstituentDefinition[ConstituentsPerIndex];
            for (var c = 0; c < constituents.Length; c++)
            {
                var instrumentId = (index * (ConstituentsPerIndex / 2) + c) % InstrumentCount + 1;
                constituents[c] = new IndexConstituentDefinition(
                    new InstrumentId(instrumentId),
                    PriceNormalizer.Scale / ConstituentsPerIndex,
                    9_350_000_000 + instrumentId);
            }

            definitions[index] = new IndexDefinition(
                new IndexId((short)(1_000 + index)),
                $"OVX-{index}",
                "GBP",
                constituents);
        }

        return definitions;
    }

    private static int[][] BuildRoutes(IndexDefinition[] definitions, int instrumentCount)
    {
        var builders = new List<int>?[instrumentCount + 1];
        for (var index = 0; index < definitions.Length; index++)
            foreach (var constituent in definitions[index].Constituents)
                (builders[constituent.InstrumentId.Value] ??= new List<int>()).Add(index);

        var routes = new int[instrumentCount + 1][];
        for (var i = 0; i < builders.Length; i++) routes[i] = builders[i] is { } builder ? [.. builder] : [];

        return routes;
    }
}