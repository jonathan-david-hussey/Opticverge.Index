using Opticverge.Index.Contracts;
using Opticverge.Index.Engine;

namespace Opticverge.Index.Tests;

public sealed class TopicInstrumentationTests
{
    // ── topic coverage ───────────────────────────────────────────────────

    [Fact]
    public void MarketTopics_All_ContainsExpectedTopicCount()
    {
        Assert.Equal(8, MarketTopics.All.Length);
    }

    [Fact]
    public void MarketTopics_All_ContainsDeadLetterQueue()
    {
        Assert.Contains(MarketTopics.DeadLetterQueue, MarketTopics.All);
        Assert.Equal("dlq.events", MarketTopics.DeadLetterQueue);
    }

    [Fact]
    public void PipelineStage_DefinesTenStages()
    {
        var stages = new[]
        {
            PipelineStage.FeedIngress,
            PipelineStage.DurableRawStream,
            PipelineStage.Normalization,
            PipelineStage.InstrumentState,
            PipelineStage.DependencyRouting,
            PipelineStage.CalculationStream,
            PipelineStage.StatefulCalculator,
            PipelineStage.PublicationLog,
            PipelineStage.Distribution,
            PipelineStage.AuditReplay
        };

        Assert.Equal(10, stages.Length);
        Assert.All(stages, s => Assert.False(string.IsNullOrEmpty(s)));
        Assert.Equal(stages.Distinct().Count(), stages.Length);
    }

    // ── DLQ metrics ─────────────────────────────────────────────────────

    [Fact]
    public void EngineMetrics_MarkStale_IncrementsDlqAndStaleTicks()
    {
        var metrics = new EngineMetrics();
        metrics.MarkStale();

        Assert.Equal(1, metrics.StaleTicks);
        Assert.Equal(1, metrics.DlqEvents);
    }

    [Fact]
    public void EngineMetrics_MarkDropped_IncrementsDlqAndDroppedMessages()
    {
        var metrics = new EngineMetrics();
        metrics.MarkDropped();

        Assert.Equal(1, metrics.DroppedMessages);
        Assert.Equal(1, metrics.DlqEvents);
    }

    [Fact]
    public void EngineMetrics_MarkStaleBatch_AccumulatesCorrectly()
    {
        var metrics = new EngineMetrics();
        metrics.MarkStale(5);

        Assert.Equal(5, metrics.StaleTicks);
        Assert.Equal(5, metrics.DlqEvents);
    }

    [Fact]
    public void EngineMetrics_DlqEvents_ReflectedInToDto()
    {
        var metrics = new EngineMetrics();
        metrics.MarkStale();
        metrics.MarkDropped();

        var dto = metrics.ToDto();

        Assert.Equal(2, dto.DlqEvents);
    }

    // ── fan-out factor instrumentation ──────────────────────────────────

    [Fact]
    public void EngineMetrics_FanOutFactorPercentiles_ZeroBeforeAnyRecording()
    {
        var metrics = new EngineMetrics();
        var (p50, p99) = metrics.FanOutFactorPercentiles();

        Assert.Equal(0, p50);
        Assert.Equal(0, p99);
    }

    [Fact]
    public void EngineMetrics_FanOutPercentiles_ExposedInDto()
    {
        var metrics = new EngineMetrics();
        for (var i = 0; i < 100; i++) metrics.RecordFanOutFactor(2);

        var dto = metrics.ToDto();

        Assert.Equal(2, dto.FanOutFactorP50);
        Assert.Equal(2, dto.FanOutFactorP99);
    }

    // ── CheckpointTracker ────────────────────────────────────────────────

    [Fact]
    public void CheckpointTracker_AgeNanos_ReturnsMaxValueBeforeFirstWrite()
    {
        var tracker = new CheckpointTracker();

        Assert.Equal(long.MaxValue, tracker.AgeNanos);
    }

    [Fact]
    public void CheckpointTracker_MarkWritten_AgeNanosDropsBelowMaxValue()
    {
        var tracker = new CheckpointTracker();
        tracker.MarkWritten();

        Assert.True(tracker.AgeNanos < long.MaxValue);
        Assert.True(tracker.AgeNanos >= 0);
    }

    [Fact]
    public void CheckpointTracker_MarkRestored_PersistsDuration()
    {
        var tracker = new CheckpointTracker();
        tracker.MarkRestored(500_000L);

        Assert.Equal(500_000L, tracker.RestoreDurationNanos);
        Assert.True(tracker.LastRestoredNanos > 0);
    }

    [Fact]
    public void CheckpointTracker_MarkWritten_WithStateSize_PersistsStateSize()
    {
        var tracker = new CheckpointTracker();
        tracker.MarkWritten(1_024_000);

        Assert.Equal(1_024_000, tracker.StateSizeBytes);
    }

    // ── PipelineInstrumentation wires up checkpoint metrics ──────────────

    [Fact]
    public void PipelineInstrumentation_WithCheckpoint_DoesNotThrow()
    {
        var metrics = new EngineMetrics();
        var tracker = new CheckpointTracker();

        using var instrumentation = new PipelineInstrumentation(metrics, () => 0L, tracker);

        tracker.MarkWritten(4096);
        tracker.MarkRestored(1_000_000);

        // Verify the CheckpointTracker state is readable after wiring.
        Assert.True(tracker.AgeNanos < long.MaxValue);
        Assert.Equal(1_000_000L, tracker.RestoreDurationNanos);
    }

    [Fact]
    public void PipelineInstrumentation_WithoutCheckpoint_DoesNotThrow()
    {
        var metrics = new EngineMetrics();

        // checkpoint = null (default) — no checkpoint gauges registered, no exception.
        using var instrumentation = new PipelineInstrumentation(metrics, () => 0L);
    }
}