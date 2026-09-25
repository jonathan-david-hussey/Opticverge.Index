using System.Diagnostics;
using System.Diagnostics.Metrics;
using System.Runtime.CompilerServices;
using Opticverge.Index.Contracts;

namespace Opticverge.Index.Engine;

public sealed class EngineMetrics
{
    private readonly LatencyHistogram _calcDuration = new();
    private readonly LatencyHistogram _endToEndLatency = new();

    // Fan-out factor: how many index calculators each instrument tick is routed to.
    // P50/P99 from this histogram describe the dependency-routing stage (slide 19, stage 5).
    private readonly LatencyHistogram _fanOutFactor = new(4_096);
    private readonly LatencyHistogram _latency = new();
    private readonly LatencyHistogram _publicationDuration = new();
    internal volatile Histogram<long>? OtelCalcDuration;
    internal volatile Histogram<long>? OtelEndToEndLatency;

    // Set by PipelineInstrumentation before the consume loop starts; volatile for safe cross-thread publish.
    internal volatile Histogram<long>? OtelProcessingLatency;

    private long _calculationFailures;
    private long _consumerLag;
    private long _dlqEvents;
    private long _droppedMessages;
    private long _duplicateEvents;
    private long _lastLatencyNanos;
    private long _messagesIn;
    private long _messagesOut;
    private long _sequenceGaps;
    private long _staleTicks;
    private long _startedAt;

    public long MessagesIn => Volatile.Read(ref _messagesIn);
    public long MessagesOut => Volatile.Read(ref _messagesOut);
    public long DroppedMessages => Volatile.Read(ref _droppedMessages);
    public long StaleTicks => Volatile.Read(ref _staleTicks);
    public long DuplicateEvents => Volatile.Read(ref _duplicateEvents);
    public long SequenceGaps => Volatile.Read(ref _sequenceGaps);
    public long CalculationFailures => Volatile.Read(ref _calculationFailures);
    public long ConsumerLag => Volatile.Read(ref _consumerLag);

    // Events quarantined because they could not be processed: stale (out-of-order by event time)
    // or dropped (zero InstrumentId / deserialization failure).  Maps to "DLQ depth" in slide 21.
    public long DlqEvents => Volatile.Read(ref _dlqEvents);

    public double MessagesPerSecond
    {
        get
        {
            var startedAt = Volatile.Read(ref _startedAt);
            if (startedAt == 0) return 0;
            var elapsed = Stopwatch.GetElapsedTime(startedAt).TotalSeconds;
            var messagesIn = Volatile.Read(ref _messagesIn);
            return elapsed <= 0 ? 0 : messagesIn / elapsed;
        }
    }

    public void UpdateConsumerLag(long lag)
    {
        Volatile.Write(ref _consumerLag, lag);
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public void MarkIn()
    {
        var next = _messagesIn + 1;
        if (next == 1) Volatile.Write(ref _startedAt, Stopwatch.GetTimestamp());
        Volatile.Write(ref _messagesIn, next);
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public void MarkIn(int count)
    {
        var next = _messagesIn + count;
        if (next == count) Volatile.Write(ref _startedAt, Stopwatch.GetTimestamp());
        Volatile.Write(ref _messagesIn, next);
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public void MarkOut()
    {
        var next = _messagesOut + 1;
        Volatile.Write(ref _messagesOut, next);
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public void MarkOut(int count)
    {
        var next = _messagesOut + count;
        Volatile.Write(ref _messagesOut, next);
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public void MarkDropped()
    {
        var next = _droppedMessages + 1;
        Volatile.Write(ref _droppedMessages, next);
        MarkDlq();
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public void MarkStale()
    {
        var next = _staleTicks + 1;
        Volatile.Write(ref _staleTicks, next);
        MarkDlq();
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public void MarkStale(int count)
    {
        var next = _staleTicks + count;
        Volatile.Write(ref _staleTicks, next);
        MarkDlq(count);
    }

    // Increments the DLQ depth counter. Called by MarkStale and MarkDropped so all
    // quarantined events are visible in the single "index.dlq.events" observable.
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public void MarkDlq()
    {
        var next = _dlqEvents + 1;
        Volatile.Write(ref _dlqEvents, next);
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public void MarkDlq(int count)
    {
        var next = _dlqEvents + count;
        Volatile.Write(ref _dlqEvents, next);
    }

    // Records the fan-out factor for one tick: the number of index calculators it was routed to.
    // Called from ApplyAcceptedTick in the event handlers.
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public void RecordFanOutFactor(int indexCount)
    {
        _fanOutFactor.Record(indexCount);
    }

    public (long P50, long P99) FanOutFactorPercentiles()
    {
        var snap = _fanOutFactor.Snapshot();
        return (snap.P50Nanos, snap.P99Nanos);
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public void MarkDuplicate()
    {
        var next = _duplicateEvents + 1;
        Volatile.Write(ref _duplicateEvents, next);
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public void MarkSequenceGap()
    {
        var next = _sequenceGaps + 1;
        Volatile.Write(ref _sequenceGaps, next);
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public void MarkCalculationFailure()
    {
        var next = _calculationFailures + 1;
        Volatile.Write(ref _calculationFailures, next);
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public void MarkCalculationFailures(int count)
    {
        var next = _calculationFailures + count;
        Volatile.Write(ref _calculationFailures, next);
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public void RecordLatency(long nanos)
    {
        Volatile.Write(ref _lastLatencyNanos, nanos);
        _latency.Record(nanos);
        OtelProcessingLatency?.Record(nanos);
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public void RecordEndToEndLatency(long nanos)
    {
        _endToEndLatency.Record(nanos);
        OtelEndToEndLatency?.Record(nanos);
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public void RecordCalcDuration(long nanos)
    {
        _calcDuration.Record(nanos);
        OtelCalcDuration?.Record(nanos);
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public void RecordPublicationDuration(long nanos)
    {
        _publicationDuration.Record(nanos);
    }

    public EngineMetricsDto ToDto()
    {
        var startedAt = Volatile.Read(ref _startedAt);
        var elapsed = startedAt == 0 ? 0.0 : Stopwatch.GetElapsedTime(startedAt).TotalSeconds;
        var messagesIn = Volatile.Read(ref _messagesIn);
        var messagesOut = Volatile.Read(ref _messagesOut);
        var snapshot = _latency.Snapshot();
        var e2eSnapshot = _endToEndLatency.Snapshot();
        var calcSnapshot = _calcDuration.Snapshot();
        var pubSnapshot = _publicationDuration.Snapshot();

        var fanOut = FanOutFactorPercentiles();
        return new EngineMetricsDto(
            messagesIn,
            messagesOut,
            Volatile.Read(ref _droppedMessages),
            Volatile.Read(ref _staleTicks),
            elapsed <= 0 ? 0 : messagesIn / elapsed,
            Volatile.Read(ref _lastLatencyNanos),
            snapshot.P50Nanos,
            snapshot.P95Nanos,
            snapshot.P99Nanos,
            snapshot.P999Nanos,
            GC.CollectionCount(0),
            GC.CollectionCount(1),
            GC.CollectionCount(2),
            Volatile.Read(ref _duplicateEvents),
            Volatile.Read(ref _sequenceGaps),
            Volatile.Read(ref _calculationFailures),
            messagesIn - messagesOut,
            Volatile.Read(ref _consumerLag),
            e2eSnapshot.P50Nanos,
            e2eSnapshot.P95Nanos,
            e2eSnapshot.P99Nanos,
            calcSnapshot.P99Nanos,
            pubSnapshot.P99Nanos,
            Volatile.Read(ref _dlqEvents),
            fanOut.P50,
            fanOut.P99);
    }
}