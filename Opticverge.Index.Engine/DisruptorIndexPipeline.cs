using System.Collections.Concurrent;
using System.Diagnostics;
using System.Runtime.CompilerServices;
using Disruptor;
using Disruptor.Dsl;
using Opticverge.Index.Contracts;

namespace Opticverge.Index.Engine;

public sealed class DisruptorIndexPipeline : IDisposable
{
    private readonly DedicatedThreadTaskScheduler? _consumerScheduler;
    private readonly ValueDisruptor<TickRingEvent> _disruptor;
    private readonly IEngineHandler _handler;
    private readonly EngineOptions _options;
    private readonly ValueRingBuffer<TickRingEvent> _ringBuffer;
    private bool _disposed;

    // Single-index convenience overload (preserves existing call sites and tests).
    public DisruptorIndexPipeline(IndexDefinition definition, EngineOptions? options = null)
        : this([definition], options)
    {
    }

    public DisruptorIndexPipeline(IReadOnlyList<IndexDefinition> definitions, EngineOptions? options = null)
    {
        _options = options ?? new EngineOptions();
        var waitStrategy = CreateWaitStrategy(_options.WaitStrategy);
        _consumerScheduler = _options.UseDedicatedConsumerThread
            ? new DedicatedThreadTaskScheduler(
                "index-engine-consumer",
                _options.DedicatedConsumerThreadPriority,
                (nint)_options.DedicatedConsumerThreadAffinityMask,
                _options.DedicatedConsumerThreadIdealProcessor)
            : null;

        _disruptor = new ValueDisruptor<TickRingEvent>(
            () => default,
            _options.RingBufferSize,
            _consumerScheduler ?? TaskScheduler.Default,
            ProducerType.Single,
            waitStrategy);

        if (_options.UseBatchHandler)
        {
            var handler = new EngineBatchEventHandler(
                definitions,
                _options.AssumePrevalidatedTicks,
                _options.ClearEventSlots,
                _options.LatencySampleRate,
                _options.SequenceGapPolicy,
                _options.SequenceGapBufferSize,
                _options.TimestampSource,
                _options.PartitionCount);
            _handler = handler;
            _disruptor.HandleEventsWith(handler);
        }
        else
        {
            var handler = new EngineEventHandler(
                definitions,
                _options.AssumePrevalidatedTicks,
                _options.ClearEventSlots,
                _options.LatencySampleRate,
                _options.SequenceGapPolicy,
                _options.SequenceGapBufferSize,
                _options.TimestampSource,
                _options.PartitionCount);
            _handler = handler;
            _disruptor.HandleEventsWith(handler);
        }

        _ringBuffer = _disruptor.Start();
    }

    public EngineMetrics Metrics => _handler.Metrics;

    // Tracks checkpoint write / restore lifecycle for observability (slide 21: checkpoint age, restore duration).
    public CheckpointTracker Checkpoint { get; } = new();

    // First index value — kept for backward compatibility with single-index callers.
    public IndexValue Latest => _handler.Indexes[0];

    // All index values — one entry per IndexDefinition passed at construction.
    public IReadOnlyList<IndexValue> Indexes => _handler.Indexes;

    public void Dispose()
    {
        if (_disposed) return;

        _disposed = true;
        _disruptor.Shutdown(TimeSpan.FromSeconds(2));
        _consumerScheduler?.Dispose();
    }

    public void EnqueueCorporateAction(CorporateAction action)
    {
        _handler.EnqueueCorporateAction(action);
    }

    public void RestoreFromSnapshot(CalculatorSnapshot snapshot)
    {
        var start = Stopwatch.GetTimestamp();
        _handler.RestoreFromSnapshots([snapshot]);
        Checkpoint.MarkRestored(MonotonicClock.ToTimestampNanos(Stopwatch.GetTimestamp() - start));
    }

    public void RestoreFromSnapshots(IReadOnlyList<CalculatorSnapshot> snapshots)
    {
        var start = Stopwatch.GetTimestamp();
        _handler.RestoreFromSnapshots(snapshots);
        Checkpoint.MarkRestored(MonotonicClock.ToTimestampNanos(Stopwatch.GetTimestamp() - start));
    }

    public CalculatorSnapshot GetSnapshot(long timestampNanos, long[] partitionOffsets)
    {
        var result = _handler.GetSnapshots(timestampNanos, partitionOffsets);
        Checkpoint.MarkWritten();
        return result[0];
    }

    public CalculatorSnapshot[] GetSnapshots(long timestampNanos, long[] partitionOffsets)
    {
        var result = _handler.GetSnapshots(timestampNanos, partitionOffsets);
        Checkpoint.MarkWritten();
        return result;
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public bool TryPublish(in MarketTick tick)
    {
        if (_disposed) return false;

        Metrics.MarkIn();
        using var scope = _ringBuffer.PublishEvent();
        ref var evt = ref scope.Event();
        evt.CopyFrom(in tick);
        return true;
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public bool TryPublishBatch(ReadOnlySpan<MarketTick> ticks)
    {
        if (_disposed || ticks.IsEmpty) return false;

        Metrics.MarkIn(ticks.Length);
        using var scope = _ringBuffer.PublishEvents(ticks.Length);
        for (var i = 0; i < ticks.Length; i++)
        {
            ref var evt = ref scope.Event(i);
            evt.CopyFrom(in ticks[i]);
        }

        return true;
    }

    public bool WaitForMessagesOut(long target, TimeSpan timeout)
    {
        var startedAt = Stopwatch.GetTimestamp();
        var spin = new SpinWait();

        while (Metrics.MessagesOut < target)
        {
            if (Stopwatch.GetElapsedTime(startedAt) > timeout) return false;

            spin.SpinOnce(-1);
        }

        return true;
    }

    private static IWaitStrategy CreateWaitStrategy(string value)
    {
        return value.Equals("busyspin", StringComparison.OrdinalIgnoreCase)
            ? new BusySpinWaitStrategy()
            : new YieldingWaitStrategy();
    }

    private interface IEngineHandler
    {
        EngineMetrics Metrics { get; }
        IReadOnlyList<IndexValue> Indexes { get; }
        void EnqueueCorporateAction(CorporateAction action);
        void RestoreFromSnapshots(IReadOnlyList<CalculatorSnapshot> snapshots);
        CalculatorSnapshot[] GetSnapshots(long timestampNanos, long[] partitionOffsets);
    }

    private sealed class EngineEventHandler(
        IReadOnlyList<IndexDefinition> definitions,
        bool assumePrevalidatedTicks,
        bool clearEventSlots,
        int latencySampleRate,
        SequenceGapPolicy sequenceGapPolicy,
        int sequenceGapBufferSize,
        ITimestampSource timestampSource,
        int partitionCount) : IValueEventHandler<TickRingEvent>, IEngineHandler
    {
        private readonly bool _assumePrevalidatedTicks = assumePrevalidatedTicks;
        private readonly CalculatorRoutingTable _calculatorRoutes = CalculatorRoutingTable.Create(definitions);

        private readonly WeightedIndexCalculator[] _calculators =
            [.. definitions.Select(d => new WeightedIndexCalculator(d))];

        private readonly bool _clearEventSlots = clearEventSlots;
        private readonly SequenceTracker _lastSequence = SequenceTracker.Create(definitions, sequenceGapBufferSize);
        private readonly int _latencySampleRate = latencySampleRate;
        private readonly IndexValue[] _latestValues = new IndexValue[definitions.Count];
        private readonly ConcurrentQueue<CorporateAction> _pendingActions = new();
        private readonly SequenceGapPolicy _sequenceGapPolicy = sequenceGapPolicy;
        private readonly CalculatorSnapshot[] _snapshotBuffer = CreateSnapshotBuffer(definitions, partitionCount);
        private readonly ITimestampSource _timestampSource = timestampSource;
        private readonly int[] _touchedCalculatorIndexes = new int[definitions.Count];
        private readonly bool[] _touchedCalculators = new bool[definitions.Count];

        public EngineMetrics Metrics { get; } = new();

        public IReadOnlyList<IndexValue> Indexes => _latestValues;

        public void EnqueueCorporateAction(CorporateAction action)
        {
            _pendingActions.Enqueue(action);
        }

        public void RestoreFromSnapshots(IReadOnlyList<CalculatorSnapshot> snapshots)
        {
            foreach (var snap in snapshots)
                for (var i = 0; i < _calculators.Length; i++)
                    if (_calculators[i].IndexId == snap.IndexId)
                    {
                        _calculators[i].RestoreFromSnapshot(snap);
                        _latestValues[i] = _calculators[i].Latest;
                    }

            _lastSequence.RestoreFromSnapshots(snapshots);
        }

        public CalculatorSnapshot[] GetSnapshots(long timestampNanos, long[] partitionOffsets)
        {
            for (var i = 0; i < _calculators.Length; i++)
            {
                _calculators[i].FillSnapshot(_snapshotBuffer[i], timestampNanos, partitionOffsets);
                _lastSequence.FillSnapshot(_snapshotBuffer[i]);
            }

            return _snapshotBuffer;
        }

        public void OnEvent(ref TickRingEvent data, long sequence, bool endOfBatch)
        {
            while (_pendingActions.TryDequeue(out var action))
                for (var c = 0; c < _calculators.Length; c++)
                    if (_calculators[c].IndexId == action.IndexId && _calculators[c].ApplyCorporateAction(action))
                        _latestValues[c] = _calculators[c].Latest;

            var instrumentId = data.InstrumentId;
            var sequenceStatus = _lastSequence.Accept(
                instrumentId,
                data.Sequence,
                _sequenceGapPolicy,
                in data);
            if (sequenceStatus == SequenceStatus.Duplicate)
            {
                Metrics.MarkDuplicate();
                if (_clearEventSlots) data.Clear();
                return;
            }

            if (sequenceStatus is SequenceStatus.BufferedGap or SequenceStatus.GapBufferFull)
            {
                Metrics.MarkSequenceGap();
                if (sequenceStatus == SequenceStatus.GapBufferFull)
                    Metrics.MarkCalculationFailure();
                if (_clearEventSlots) data.Clear();
                return;
            }

            if (sequenceStatus == SequenceStatus.BufferedDuplicate)
            {
                Metrics.MarkDuplicate();
                if (_clearEventSlots) data.Clear();
                return;
            }

            if (sequenceStatus == SequenceStatus.Gap)
            {
                Metrics.MarkSequenceGap();
                if (_sequenceGapPolicy == SequenceGapPolicy.DropUntilRecovered)
                {
                    if (_clearEventSlots) data.Clear();
                    return;
                }
            }

            var shouldSample = ShouldSampleLatency(sequence, _latencySampleRate);
            if (shouldSample)
            {
                var now = _timestampSource.TimestampNanos();
                Metrics.RecordLatency(now - data.ReceiveTimestampNanos);
                if (data.ExchangeTimestampNanos > 0)
                    Metrics.RecordEndToEndLatency(now - data.ExchangeTimestampNanos);
            }

            var calcStart = shouldSample ? Stopwatch.GetTimestamp() : 0L;
            var receiveNanos = data.ReceiveTimestampNanos;
            var applied = ApplyAcceptedTick(in data, receiveNanos);
            if (shouldSample)
                Metrics.RecordCalcDuration(MonotonicClock.ToTimestampNanos(Stopwatch.GetTimestamp() - calcStart));

            if (applied)
            {
                Metrics.MarkOut();
                DrainBufferedTicks(instrumentId);
            }
            else if ((data.Flags & TickFlags.Stale) != 0)
            {
                Metrics.MarkStale();
            }
            else
            {
                Metrics.MarkCalculationFailure();
            }

            if (_clearEventSlots) data.Clear();
        }

        public void OnBatchStart(long batchSize)
        {
        }

        public void OnStart()
        {
        }

        public void OnShutdown()
        {
        }

        public void OnTimeout(long sequence)
        {
        }

        private static CalculatorSnapshot[] CreateSnapshotBuffer(
            IReadOnlyList<IndexDefinition> definitions, int partitionCount)
        {
            var buffer = new CalculatorSnapshot[definitions.Count];
            for (var i = 0; i < definitions.Count; i++)
                buffer[i] = new CalculatorSnapshot
                {
                    InstrumentIds = new int[definitions[i].Constituents.Count],
                    LatestPricesE8 = new long[definitions[i].Constituents.Count],
                    WeightsE8 = new long[definitions[i].Constituents.Count],
                    LastTimestampNanos = new long[definitions[i].Constituents.Count],
                    LastSequences = new long[definitions[i].Constituents.Count],
                    PartitionOffsets = new long[partitionCount]
                };
            return buffer;
        }

        private bool ApplyAcceptedTick(in TickRingEvent data, long receiveNanos)
        {
            var applied = false;
            var touchedCount = 0;
            var routes = _calculatorRoutes.GetCalculatorIndexes(data.InstrumentId);

            // Record fan-out factor: number of indexes this instrument event is routed to.
            // This is the dependency-routing stage metric (slide 19 stage 5, slide 21 throughput).
            Metrics.RecordFanOutFactor(routes.Length);

            foreach (var calcIndex in routes)
            {
                var calc = _calculators[calcIndex];
                if (_assumePrevalidatedTicks ? calc.ApplyPrevalidated(in data) : calc.Apply(in data))
                {
                    applied = true;
                    touchedCount = MarkTouched(calcIndex, touchedCount, _touchedCalculatorIndexes, _touchedCalculators);
                }
            }

            for (var i = 0; i < touchedCount; i++)
            {
                var calcIndex = _touchedCalculatorIndexes[i];
                _touchedCalculators[calcIndex] = false;
                _calculators[calcIndex].SetReceiveTimestamp(receiveNanos);
                _latestValues[calcIndex] = _calculators[calcIndex].Latest;
            }

            return applied;
        }

        private void DrainBufferedTicks(int instrumentId)
        {
            while (_lastSequence.TryTakeNextBuffered(instrumentId, out var buffered))
                if (ApplyAcceptedTick(in buffered, buffered.ReceiveTimestampNanos))
                    Metrics.MarkOut();
                else if ((buffered.Flags & TickFlags.Stale) != 0)
                    Metrics.MarkStale();
                else
                    Metrics.MarkCalculationFailure();
        }

        private static bool ShouldSampleLatency(long sequence, int sampleRate)
        {
            return sampleRate > 0 && (sampleRate == 1 || sequence % sampleRate == 0);
        }

        private static int MarkTouched(int calcIndex, int count, int[] touchedIndexes, bool[] touchedFlags)
        {
            if (touchedFlags[calcIndex]) return count;

            touchedFlags[calcIndex] = true;
            touchedIndexes[count] = calcIndex;
            return count + 1;
        }
    }

    private sealed class EngineBatchEventHandler(
        IReadOnlyList<IndexDefinition> definitions,
        bool assumePrevalidatedTicks,
        bool clearEventSlots,
        int latencySampleRate,
        SequenceGapPolicy sequenceGapPolicy,
        int sequenceGapBufferSize,
        ITimestampSource timestampSource,
        int partitionCount) : IValueEventHandler<TickRingEvent>, IEngineHandler
    {
        private readonly bool _assumePrevalidatedTicks = assumePrevalidatedTicks;
        private readonly CalculatorRoutingTable _calculatorRoutes = CalculatorRoutingTable.Create(definitions);

        private readonly WeightedIndexCalculator[] _calculators =
            [.. definitions.Select(d => new WeightedIndexCalculator(d))];

        private readonly bool _clearEventSlots = clearEventSlots;
        private readonly SequenceTracker _lastSequence = SequenceTracker.Create(definitions, sequenceGapBufferSize);
        private readonly int _latencySampleRate = latencySampleRate;
        private readonly IndexValue[] _latestValues = new IndexValue[definitions.Count];
        private readonly ConcurrentQueue<CorporateAction> _pendingActions = new();
        private readonly SequenceGapPolicy _sequenceGapPolicy = sequenceGapPolicy;
        private readonly CalculatorSnapshot[] _snapshotBuffer = CreateSnapshotBuffer(definitions, partitionCount);
        private readonly ITimestampSource _timestampSource = timestampSource;
        private readonly int[] _touchedCalculatorIndexes = new int[definitions.Count];
        private readonly bool[] _touchedCalculators = new bool[definitions.Count];
        private readonly long[] _touchedReceiveNanos = new long[definitions.Count];
        private int _batchFailures;
        private bool _batchFirstEvent;

        private int _batchProcessed;

        // Per-batch accumulators — reset in OnBatchStart.
        private bool _batchShouldSample;
        private int _batchStale;
        private int _batchTouchedCount;
        private long _sampleCounter;

        public EngineMetrics Metrics { get; } = new();

        public IReadOnlyList<IndexValue> Indexes => _latestValues;

        public void EnqueueCorporateAction(CorporateAction action)
        {
            _pendingActions.Enqueue(action);
        }

        public void RestoreFromSnapshots(IReadOnlyList<CalculatorSnapshot> snapshots)
        {
            foreach (var snap in snapshots)
                for (var i = 0; i < _calculators.Length; i++)
                    if (_calculators[i].IndexId == snap.IndexId)
                    {
                        _calculators[i].RestoreFromSnapshot(snap);
                        _latestValues[i] = _calculators[i].Latest;
                    }

            _lastSequence.RestoreFromSnapshots(snapshots);
        }

        public CalculatorSnapshot[] GetSnapshots(long timestampNanos, long[] partitionOffsets)
        {
            for (var i = 0; i < _calculators.Length; i++)
            {
                _calculators[i].FillSnapshot(_snapshotBuffer[i], timestampNanos, partitionOffsets);
                _lastSequence.FillSnapshot(_snapshotBuffer[i]);
            }

            return _snapshotBuffer;
        }

        public void OnBatchStart(long batchSize)
        {
            while (_pendingActions.TryDequeue(out var action))
                for (var c = 0; c < _calculators.Length; c++)
                    if (_calculators[c].IndexId == action.IndexId && _calculators[c].ApplyCorporateAction(action))
                        _latestValues[c] = _calculators[c].Latest;

            // Decide once per batch whether to sample — avoids integer-divide per tick.
            _sampleCounter += batchSize;
            _batchShouldSample = _latencySampleRate > 0 && _sampleCounter >= _latencySampleRate;
            if (_batchShouldSample) _sampleCounter = 0;

            _batchFirstEvent = true;
            _batchProcessed = 0;
            _batchStale = 0;
            _batchFailures = 0;
            _batchTouchedCount = 0;
        }

        public void OnEvent(ref TickRingEvent data, long sequence, bool endOfBatch)
        {
            var instrumentId = data.InstrumentId;
            var sequenceStatus = _lastSequence.Accept(
                instrumentId,
                data.Sequence,
                _sequenceGapPolicy,
                in data);
            if (sequenceStatus == SequenceStatus.Duplicate)
            {
                Metrics.MarkDuplicate();
                if (_clearEventSlots) data.Clear();
                if (endOfBatch) CompleteBatch();
                return;
            }

            if (sequenceStatus is SequenceStatus.BufferedGap or SequenceStatus.GapBufferFull)
            {
                Metrics.MarkSequenceGap();
                if (sequenceStatus == SequenceStatus.GapBufferFull)
                    _batchFailures++;
                if (_clearEventSlots) data.Clear();
                if (endOfBatch) CompleteBatch();
                return;
            }

            if (sequenceStatus == SequenceStatus.BufferedDuplicate)
            {
                Metrics.MarkDuplicate();
                if (_clearEventSlots) data.Clear();
                if (endOfBatch) CompleteBatch();
                return;
            }

            if (sequenceStatus == SequenceStatus.Gap)
            {
                Metrics.MarkSequenceGap();
                if (_sequenceGapPolicy == SequenceGapPolicy.DropUntilRecovered)
                {
                    if (_clearEventSlots) data.Clear();
                    if (endOfBatch) CompleteBatch();
                    return;
                }
            }

            var sampleThis = _batchShouldSample && _batchFirstEvent;
            _batchFirstEvent = false;

            if (sampleThis)
            {
                var now = _timestampSource.TimestampNanos();
                Metrics.RecordLatency(now - data.ReceiveTimestampNanos);
                if (data.ExchangeTimestampNanos > 0)
                    Metrics.RecordEndToEndLatency(now - data.ExchangeTimestampNanos);
            }

            var calcStart = sampleThis ? Stopwatch.GetTimestamp() : 0L;
            ApplyAcceptedTick(in data);
            DrainBufferedTicks(instrumentId);
            if (sampleThis)
                Metrics.RecordCalcDuration(MonotonicClock.ToTimestampNanos(Stopwatch.GetTimestamp() - calcStart));

            if (_clearEventSlots) data.Clear();

            if (endOfBatch)
                CompleteBatch();
        }

        public void OnStart()
        {
        }

        public void OnShutdown()
        {
        }

        public void OnTimeout(long sequence)
        {
        }

        private static CalculatorSnapshot[] CreateSnapshotBuffer(
            IReadOnlyList<IndexDefinition> definitions, int partitionCount)
        {
            var buffer = new CalculatorSnapshot[definitions.Count];
            for (var i = 0; i < definitions.Count; i++)
                buffer[i] = new CalculatorSnapshot
                {
                    InstrumentIds = new int[definitions[i].Constituents.Count],
                    LatestPricesE8 = new long[definitions[i].Constituents.Count],
                    WeightsE8 = new long[definitions[i].Constituents.Count],
                    LastTimestampNanos = new long[definitions[i].Constituents.Count],
                    LastSequences = new long[definitions[i].Constituents.Count],
                    PartitionOffsets = new long[partitionCount]
                };
            return buffer;
        }

        private int MarkTouched(int calcIndex, int count)
        {
            if (_touchedCalculators[calcIndex]) return count;

            _touchedCalculators[calcIndex] = true;
            _touchedCalculatorIndexes[count] = calcIndex;
            return count + 1;
        }

        private void ApplyAcceptedTick(in TickRingEvent data)
        {
            var applied = false;
            var routes = _calculatorRoutes.GetCalculatorIndexes(data.InstrumentId);

            // Record fan-out factor: number of indexes this instrument event is routed to (slide 19 stage 5).
            Metrics.RecordFanOutFactor(routes.Length);

            foreach (var calcIndex in routes)
            {
                var calc = _calculators[calcIndex];
                if (_assumePrevalidatedTicks ? calc.ApplyPrevalidated(in data) : calc.Apply(in data))
                {
                    applied = true;
                    _batchTouchedCount = MarkTouched(calcIndex, _batchTouchedCount);
                    _touchedReceiveNanos[calcIndex] = data.ReceiveTimestampNanos;
                }
            }

            if (applied)
                _batchProcessed++;
            else if ((data.Flags & TickFlags.Stale) != 0) _batchStale++;
            else _batchFailures++;
        }

        private void DrainBufferedTicks(int instrumentId)
        {
            while (_lastSequence.TryTakeNextBuffered(instrumentId, out var buffered)) ApplyAcceptedTick(in buffered);
        }

        private void CompleteBatch()
        {
            for (var i = 0; i < _batchTouchedCount; i++)
            {
                var calcIndex = _touchedCalculatorIndexes[i];
                _touchedCalculators[calcIndex] = false;
                _calculators[calcIndex].SetReceiveTimestamp(_touchedReceiveNanos[calcIndex]);
                _latestValues[calcIndex] = _calculators[calcIndex].Latest;
            }

            Metrics.MarkOut(_batchProcessed);
            if (_batchStale != 0) Metrics.MarkStale(_batchStale);
            if (_batchFailures != 0) Metrics.MarkCalculationFailures(_batchFailures);
        }
    }

    private enum SequenceStatus
    {
        Accepted,
        Duplicate,
        Gap,
        BufferedGap,
        BufferedDuplicate,
        GapBufferFull,
        Untracked
    }

    private sealed class CalculatorRoutingTable
    {
        private static readonly int[] Empty = [];

        private readonly int[][] _routesByInstrumentId;

        private CalculatorRoutingTable(int[][] routesByInstrumentId)
        {
            _routesByInstrumentId = routesByInstrumentId;
        }

        public static CalculatorRoutingTable Create(IReadOnlyList<IndexDefinition> definitions)
        {
            var maxInstrumentId = 0;
            foreach (var definition in definitions)
            foreach (var constituent in definition.Constituents)
                maxInstrumentId = Math.Max(maxInstrumentId, constituent.InstrumentId.Value);

            var routeBuilders = new List<int>?[maxInstrumentId + 1];
            for (var calcIndex = 0; calcIndex < definitions.Count; calcIndex++)
                foreach (var constituent in definitions[calcIndex].Constituents)
                    (routeBuilders[constituent.InstrumentId.Value] ??= new List<int>()).Add(calcIndex);

            var routes = new int[maxInstrumentId + 1][];
            for (var instrumentId = 0; instrumentId < routeBuilders.Length; instrumentId++)
                if (routeBuilders[instrumentId] is { } calculatorIndexes)
                    routes[instrumentId] = [.. calculatorIndexes];

            return new CalculatorRoutingTable(routes);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public ReadOnlySpan<int> GetCalculatorIndexes(int instrumentId)
        {
            return (uint)instrumentId < (uint)_routesByInstrumentId.Length
                ? _routesByInstrumentId[instrumentId] ?? Empty
                : Empty;
        }
    }

    private sealed class SequenceTracker
    {
        private readonly int _gapBufferSize;
        private readonly PendingTickBuffer?[] _pendingByInstrumentId;
        private readonly long[] _sequencesByInstrumentId;

        private SequenceTracker(long[] sequencesByInstrumentId, int gapBufferSize)
        {
            _sequencesByInstrumentId = sequencesByInstrumentId;
            _pendingByInstrumentId = new PendingTickBuffer?[sequencesByInstrumentId.Length];
            _gapBufferSize = Math.Max(0, gapBufferSize);
        }

        public static SequenceTracker Create(IReadOnlyList<IndexDefinition> definitions, int gapBufferSize)
        {
            var maxInstrumentId = 0;
            foreach (var definition in definitions)
            foreach (var constituent in definition.Constituents)
                maxInstrumentId = Math.Max(maxInstrumentId, constituent.InstrumentId.Value);

            var sequences = new long[maxInstrumentId + 1];
            Array.Fill(sequences, -1L);
            return new SequenceTracker(sequences, gapBufferSize);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public SequenceStatus Accept(
            int instrumentId,
            long sequence,
            SequenceGapPolicy policy,
            in TickRingEvent tick)
        {
            if ((uint)instrumentId >= (uint)_sequencesByInstrumentId.Length) return SequenceStatus.Untracked;

            ref var lastSequence = ref _sequencesByInstrumentId[instrumentId];
            if (sequence > 0 && lastSequence >= 0 && sequence <= lastSequence) return SequenceStatus.Duplicate;

            if (sequence > 0 && lastSequence >= 0 && sequence > lastSequence + 1)
            {
                if (policy == SequenceGapPolicy.ProcessAndReport)
                {
                    lastSequence = sequence;
                    RemoveBufferedAtOrBefore(instrumentId, sequence);
                    return SequenceStatus.Gap;
                }

                if (policy == SequenceGapPolicy.BufferUntilRecovered && _gapBufferSize > 0) return BufferGap(instrumentId, sequence, in tick);

                return SequenceStatus.Gap;
            }

            if (sequence > lastSequence) lastSequence = sequence;

            return SequenceStatus.Accepted;
        }

        public bool TryTakeNextBuffered(int instrumentId, out TickRingEvent tick)
        {
            if ((uint)instrumentId >= (uint)_pendingByInstrumentId.Length)
            {
                tick = default;
                return false;
            }

            var nextSequence = _sequencesByInstrumentId[instrumentId] + 1;
            if (_pendingByInstrumentId[instrumentId] is not { } pending ||
                !pending.TryTake(nextSequence, out tick))
            {
                tick = default;
                return false;
            }

            _sequencesByInstrumentId[instrumentId] = nextSequence;
            return true;
        }

        public void FillSnapshot(CalculatorSnapshot snapshot)
        {
            var count = Math.Min(snapshot.InstrumentIds.Length, snapshot.LastSequences.Length);
            for (var i = 0; i < count; i++)
            {
                var instrumentId = snapshot.InstrumentIds[i];
                snapshot.LastSequences[i] = (uint)instrumentId < (uint)_sequencesByInstrumentId.Length
                    ? _sequencesByInstrumentId[instrumentId]
                    : -1L;
            }
        }

        public void RestoreFromSnapshots(IReadOnlyList<CalculatorSnapshot> snapshots)
        {
            Array.Fill(_sequencesByInstrumentId, -1L);
            Array.Clear(_pendingByInstrumentId);

            foreach (var snapshot in snapshots)
            {
                var count = Math.Min(snapshot.InstrumentIds.Length, snapshot.LastSequences.Length);
                for (var i = 0; i < count; i++)
                {
                    var instrumentId = snapshot.InstrumentIds[i];
                    if ((uint)instrumentId >= (uint)_sequencesByInstrumentId.Length) continue;

                    var sequence = snapshot.LastSequences[i];
                    if (sequence > _sequencesByInstrumentId[instrumentId]) _sequencesByInstrumentId[instrumentId] = sequence;
                }
            }
        }

        private SequenceStatus BufferGap(int instrumentId, long sequence, in TickRingEvent tick)
        {
            var pending = _pendingByInstrumentId[instrumentId];
            if (pending is null)
            {
                pending = new PendingTickBuffer(_gapBufferSize);
                _pendingByInstrumentId[instrumentId] = pending;
            }

            return pending.TryAdd(sequence, in tick) switch
            {
                PendingTickAddResult.Added => SequenceStatus.BufferedGap,
                PendingTickAddResult.Duplicate => SequenceStatus.BufferedDuplicate,
                _ => SequenceStatus.GapBufferFull
            };
        }

        private void RemoveBufferedAtOrBefore(int instrumentId, long sequence)
        {
            if ((uint)instrumentId < (uint)_pendingByInstrumentId.Length &&
                _pendingByInstrumentId[instrumentId] is { } pending)
                pending.RemoveAtOrBefore(sequence);
        }
    }

    private sealed class PendingTickBuffer(int capacity)
    {
        private readonly bool[] _occupied = new bool[capacity];
        private readonly TickRingEvent[] _ticks = new TickRingEvent[capacity];
        private int _count;

        public PendingTickAddResult TryAdd(long sequence, in TickRingEvent tick)
        {
            var emptySlot = -1;
            for (var i = 0; i < _ticks.Length; i++)
            {
                if (_occupied[i])
                {
                    if (_ticks[i].Sequence == sequence) return PendingTickAddResult.Duplicate;
                    continue;
                }

                emptySlot = i;
            }

            if (emptySlot < 0 || _count >= _ticks.Length) return PendingTickAddResult.Full;

            _ticks[emptySlot] = tick;
            _occupied[emptySlot] = true;
            _count++;
            return PendingTickAddResult.Added;
        }

        public bool TryTake(long sequence, out TickRingEvent tick)
        {
            for (var i = 0; i < _ticks.Length; i++)
            {
                if (!_occupied[i] || _ticks[i].Sequence != sequence) continue;

                tick = _ticks[i];
                _ticks[i].Clear();
                _occupied[i] = false;
                _count--;
                return true;
            }

            tick = default;
            return false;
        }

        public void RemoveAtOrBefore(long sequence)
        {
            for (var i = 0; i < _ticks.Length; i++)
            {
                if (!_occupied[i] || _ticks[i].Sequence > sequence) continue;

                _ticks[i].Clear();
                _occupied[i] = false;
                _count--;
            }
        }
    }

    private enum PendingTickAddResult
    {
        Added,
        Duplicate,
        Full
    }
}