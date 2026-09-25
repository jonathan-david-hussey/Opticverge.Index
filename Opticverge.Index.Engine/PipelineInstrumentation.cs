using System.Diagnostics.Metrics;

namespace Opticverge.Index.Engine;

public sealed class PipelineInstrumentation : IDisposable
{
    public const string MeterName = "Opticverge.Index.Pipeline";

    public readonly Histogram<long> PublicationDurationNanos;

    private readonly Meter _meter;

    // CheckpointTracker is optional for backward compatibility with callers that have not
    // yet wired it up; when null the checkpoint-age gauge always reports 0.
    public PipelineInstrumentation(
        EngineMetrics metrics,
        Func<long> consumerLagProvider,
        CheckpointTracker? checkpoint = null)
    {
        _meter = new Meter(MeterName, "1.0");

        // ── Stage latency histograms (slide 21: event-to-ingest, ingest-to-calculate, calc-to-publish) ──

        metrics.OtelProcessingLatency = _meter.CreateHistogram<long>(
            "index.processing.latency", "ns",
            "Ingest-to-calculate latency: receive timestamp to WeightedIndexCalculator.Apply start");
        metrics.OtelEndToEndLatency = _meter.CreateHistogram<long>(
            "index.end_to_end.latency", "ns",
            "End-to-end latency: exchange timestamp to index calculation start (sampled once per batch)");
        metrics.OtelCalcDuration = _meter.CreateHistogram<long>(
            "index.calculation.duration", "ns",
            "WeightedIndexCalculator.Apply duration (stateful-calculator stage, slide 19 stage 7)");
        PublicationDurationNanos = _meter.CreateHistogram<long>(
            "index.publication.duration", "ns",
            "Kafka produce call duration for index.deltas (publication-log stage, slide 19 stage 8)");

        // ── Throughput counters (slide 21: events/sec by producer and consumer stage) ──

        _meter.CreateObservableCounter("index.messages.in",
            () => metrics.MessagesIn, "{ticks}", "Ticks received by index engine from ticks.raw");
        _meter.CreateObservableCounter("index.messages.out",
            () => metrics.MessagesOut, "{ticks}", "Index calculations emitted to index.deltas");
        _meter.CreateObservableCounter("index.messages.dropped",
            () => metrics.DroppedMessages, "{ticks}", "Ticks dropped (zero/null InstrumentId)");
        _meter.CreateObservableCounter("index.ticks.stale",
            () => metrics.StaleTicks, "{ticks}", "Out-of-order ticks rejected by event-time ordering");
        _meter.CreateObservableCounter("index.events.duplicate",
            () => metrics.DuplicateEvents, "{events}", "Duplicate sequence events detected");
        _meter.CreateObservableCounter("index.events.sequence_gap",
            () => metrics.SequenceGaps, "{gaps}", "Sequence number gaps detected (slide 21: correctness)");
        _meter.CreateObservableCounter("index.calculation.failures",
            () => metrics.CalculationFailures, "{failures}", "Rejected non-stale ticks");

        // ── DLQ depth (slide 21: data quality — DLQ depth) ──

        _meter.CreateObservableCounter("index.dlq.events",
            () => metrics.DlqEvents, "{events}",
            "Total events quarantined (stale + dropped); maps to DLQ depth in the observability checklist");

        // ── Fan-out factor (slide 19 stage 5: dependency routing) ──

        _meter.CreateObservableGauge("index.fanout.factor.p50",
            () => metrics.FanOutFactorPercentiles().P50,
            "{indexes}", "P50 fan-out factor: indexes a tick is routed to per instrument event");
        _meter.CreateObservableGauge("index.fanout.factor.p99",
            () => metrics.FanOutFactorPercentiles().P99,
            "{indexes}", "P99 fan-out factor: peak routing fan-out across instruments");

        // ── Backlog gauges (slide 21: consumer lag in records and wall-clock age) ──

        _meter.CreateObservableGauge("index.ring_buffer.depth",
            () => metrics.MessagesIn - metrics.MessagesOut,
            "{slots}", "Pending events in the Disruptor ring buffer");
        _meter.CreateObservableGauge("index.processing.rate",
            () => metrics.MessagesPerSecond, "{ticks/s}", "Ticks processed per second (cumulative average)");
        _meter.CreateObservableGauge("index.consumer.lag",
            consumerLagProvider, "{messages}", "Total consumer lag on ticks.raw (slide 21: backlog)");

        // ── State / checkpoint metrics (slide 21: checkpoint age, restore duration) ──

        if (checkpoint is not null)
        {
            _meter.CreateObservableGauge("index.checkpoint.age",
                () => checkpoint.AgeNanos == long.MaxValue ? 0L : checkpoint.AgeNanos,
                "ns", "Wall-clock age of the most recently written calculator snapshot");
            _meter.CreateObservableGauge("index.checkpoint.restore_duration",
                () => checkpoint.RestoreDurationNanos,
                "ns", "Duration of the most recent state restore from a saved snapshot (RTO proxy)");
            _meter.CreateObservableGauge("index.checkpoint.state_size",
                () => checkpoint.StateSizeBytes,
                "{bytes}", "Approximate serialised size of the last checkpoint");
        }
    }

    public void Dispose()
    {
        _meter.Dispose();
    }
}