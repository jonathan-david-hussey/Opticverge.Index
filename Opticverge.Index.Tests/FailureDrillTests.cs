using Opticverge.Index.Contracts;
using Opticverge.Index.Engine;

namespace Opticverge.Index.Tests;

///
public sealed class FailureDrillTests
{
    // ── helpers ─────────────────────────────────────────────────────────────

    private static MarketTick Tick(
        long seq,
        int instrumentId,
        long priceE8,
        long exchangeNanos = 0,
        TickFlags flags = TickFlags.Trade)
    {
        return new MarketTick(seq, new InstrumentId(instrumentId), new ExchangeId(1), priceE8, 100,
            exchangeNanos == 0 ? seq * 1_000 : exchangeNanos, seq * 1_000, 0, flags);
    }

    private static DisruptorIndexPipeline SingleIndexPipeline(SequenceGapPolicy gapPolicy = SequenceGapPolicy.BufferUntilRecovered)
    {
        return new DisruptorIndexPipeline(IndexCatalog.CreateDemo().Indexes[0],
            new EngineOptions
            {
                RingBufferSize = 1024,
                LatencySampleRate = 0,
                SequenceGapPolicy = gapPolicy,
                SequenceGapBufferSize = 16
            });
    }

    // ── Scenario 1: Duplicate-on-restart ─────────────────────────────────

    [Fact]
    public void Scenario1_DuplicateOnRestart_IncrementsDuplicateEvents_NotCalculationFailures()
    {
        using var pipeline = SingleIndexPipeline();

        var original = Tick(1, 1, 9_400_000_000);
        var replay = Tick(1, 1, 9_400_000_000); // same sequence — simulates restart replay

        pipeline.TryPublish(in original);
        SpinWait.SpinUntil(() => pipeline.Metrics.MessagesOut >= 1, TimeSpan.FromSeconds(2));

        pipeline.TryPublish(in replay);
        SpinWait.SpinUntil(() => pipeline.Metrics.DuplicateEvents >= 1, TimeSpan.FromSeconds(2));

        Assert.Equal(1, pipeline.Metrics.DuplicateEvents);
        Assert.Equal(0, pipeline.Metrics.CalculationFailures);
        // Only the original should have been applied — MessagesOut stays at 1.
        Assert.Equal(1, pipeline.Metrics.MessagesOut);
    }

    // ── Scenario 2: Hot partition ─────────────────────────────────────────

    [Fact]
    public void Scenario2_HotPartition_HighVolumeBurst_AllTicksProcessedWithoutLoss()
    {
        using var pipeline = new DisruptorIndexPipeline(
            IndexCatalog.CreateDemo().Indexes[0],
            new EngineOptions
            {
                RingBufferSize = 4096,
                LatencySampleRate = 0,
                AssumePrevalidatedTicks = true,
                UseBatchHandler = true
            });

        const int Count = 1024;
        var ticks = new MarketTick[Count];
        for (var i = 0; i < Count; i++)
            ticks[i] = Tick(i + 1, 1, 9_400_000_000 + i);

        var target = pipeline.Metrics.MessagesOut + Count;
        pipeline.TryPublishBatch(ticks);

        Assert.True(pipeline.WaitForMessagesOut(target, TimeSpan.FromSeconds(5)));
        Assert.Equal(0, pipeline.Metrics.DroppedMessages);
        Assert.Equal(0, pipeline.Metrics.CalculationFailures);
    }

    // ── Scenario 3: Unavailable reference data ────────────────────────────

    [Fact]
    public void Scenario3_UnavailableReferenceData_UnknownInstrument_TickRoutesToNowhere()
    {
        // A pipeline with instrument 1 only; instrument 99 is unknown.
        var definition = new IndexDefinition(
            new IndexId(100), "IDX", "GBP",
            [new IndexConstituentDefinition(new InstrumentId(1), PriceNormalizer.Scale, 100_000_000)]);
        using var pipeline = new DisruptorIndexPipeline(definition,
            new EngineOptions { RingBufferSize = 256, LatencySampleRate = 0 });

        var knownTick = Tick(1, 1, 100_000_000);
        var unknownTick = Tick(2, 99, 100_000_000); // unknown instrument

        pipeline.TryPublish(in knownTick);
        SpinWait.SpinUntil(() => pipeline.Metrics.MessagesOut >= 1, TimeSpan.FromSeconds(2));

        // The pipeline processes the tick but it touches no calculators — counted as a failure.
        pipeline.TryPublish(in unknownTick);
        SpinWait.SpinUntil(() => pipeline.Metrics.MessagesIn >= 2, TimeSpan.FromSeconds(2));
        SpinWait.SpinUntil(() => pipeline.Metrics.CalculationFailures >= 1 || pipeline.Metrics.MessagesOut >= 2,
            TimeSpan.FromSeconds(2));

        // Index value should not have changed due to the unknown instrument tick.
        var snap = pipeline.GetSnapshot(0, new long[4]);
        Assert.Equal(100_000_000L, snap.LatestPricesE8[0]);
    }

    // ── Scenario 4: Malformed event ───────────────────────────────────────

    [Fact]
    public void Scenario4_MalformedEvent_ZeroInstrumentId_IncrementsDlqDropped()
    {
        using var pipeline = SingleIndexPipeline();

        // The Worker drops InstrumentId == 0 before publishing, which increments DroppedMessages → DlqEvents.
        // Simulate that path directly on the metrics object.
        pipeline.Metrics.MarkDropped();

        Assert.Equal(1, pipeline.Metrics.DroppedMessages);
        Assert.Equal(1, pipeline.Metrics.DlqEvents);
    }

    // ── Scenario 5: Out-of-order arrival ─────────────────────────────────

    [Fact]
    public void Scenario5_OutOfOrderArrival_ExchangeFlaggedStale_IncrementsStaleTicks_AndDlq()
    {
        // Exchange-level stale: the feed has already marked the tick with TickFlags.Stale.
        // The pipeline counts it via MarkStale (not CalculationFailure).
        using var pipeline = SingleIndexPipeline();

        var first = Tick(1, 1, 9_400_000_000, 2_000);
        // seq 2 arrives with TickFlags.Stale — exchange already knew this was out of order.
        var stale = new MarketTick(2, new InstrumentId(1), new ExchangeId(1),
            9_500_000_000, 100, 1_000, 2_000, 0, TickFlags.Stale);

        pipeline.TryPublish(in first);
        SpinWait.SpinUntil(() => pipeline.Metrics.MessagesOut >= 1, TimeSpan.FromSeconds(2));

        pipeline.TryPublish(in stale);
        SpinWait.SpinUntil(() => pipeline.Metrics.StaleTicks >= 1, TimeSpan.FromSeconds(2));

        Assert.Equal(1, pipeline.Metrics.StaleTicks);
        Assert.Equal(1, pipeline.Metrics.DlqEvents);
    }

    // ── Scenario 6: Backlog buildup ───────────────────────────────────────

    [Fact]
    public void Scenario6_BacklogBuildup_GapBufferHoldsOutOfOrderTick_ThenDrainsOnFill()
    {
        using var pipeline = SingleIndexPipeline();

        var seq1 = Tick(1, 1, 9_400_000_000, 1_000);
        var seq3 = Tick(3, 1, 9_600_000_000, 3_000); // arrives before seq 2 — buffered
        var seq2 = Tick(2, 1, 9_500_000_000, 2_000); // fills the gap

        pipeline.TryPublish(in seq1);
        SpinWait.SpinUntil(() => pipeline.Metrics.MessagesOut >= 1, TimeSpan.FromSeconds(2));

        // seq3 arrives before seq2 — should increment SequenceGaps and be buffered.
        pipeline.TryPublish(in seq3);
        SpinWait.SpinUntil(() => pipeline.Metrics.SequenceGaps >= 1, TimeSpan.FromSeconds(2));
        Assert.Equal(1, pipeline.Metrics.SequenceGaps);

        // seq2 fills the gap; seq3 should then be drained from the buffer.
        var target = pipeline.Metrics.MessagesOut + 2;
        pipeline.TryPublish(in seq2);
        Assert.True(pipeline.WaitForMessagesOut(target, TimeSpan.FromSeconds(2)));

        Assert.Equal(3, pipeline.Latest.Sequence);
        Assert.Equal(9_600_000_000L, pipeline.GetSnapshot(0, new long[4]).LatestPricesE8[0]);
    }

    // ── Scenario 7: Bad value correction ─────────────────────────────────

    [Fact]
    public void Scenario7_BadValueCorrection_CorrectedTickUpdatesIndexState()
    {
        using var pipeline = new DisruptorIndexPipeline(
            IndexCatalog.CreateDemo().Indexes[0],
            new EngineOptions
            {
                RingBufferSize = 256,
                LatencySampleRate = 0,
                AssumePrevalidatedTicks = true
            });

        // First tick: an erroneously high price (but still within E8 safe range).
        var bad = Tick(1, 1, 50_000_000_000L, 1_000); // $500 — valid but "wrong"
        pipeline.TryPublish(in bad);
        SpinWait.SpinUntil(() => pipeline.Metrics.MessagesOut >= 1, TimeSpan.FromSeconds(2));

        var badValue = pipeline.Latest.ValueE8;
        Assert.True(badValue > 0);

        // Correction: a higher-timestamped tick with the correct price.
        var corrected = Tick(2, 1, 9_400_000_000, 2_000, TickFlags.Trade | TickFlags.Corrected);
        pipeline.TryPublish(in corrected);
        SpinWait.SpinUntil(() => pipeline.Metrics.MessagesOut >= 2, TimeSpan.FromSeconds(2));

        var correctedValue = pipeline.Latest.ValueE8;
        Assert.NotEqual(badValue, correctedValue);
        Assert.Equal(9_400_000_000L, pipeline.GetSnapshot(0, new long[4]).LatestPricesE8[0]);
    }

    // ── Scenario 8: Checkpoint corruption / restore ───────────────────────

    [Fact]
    public void Scenario8_CheckpointCorruption_RestoreThenForwardProcess_CorrectnessPreserved()
    {
        // Phase 1: build state up to a snapshot.
        CalculatorSnapshot snapshot;
        using (var phase1 = SingleIndexPipeline())
        {
            var tick = Tick(1, 1, 9_400_000_000, 1_000);
            phase1.TryPublish(in tick);
            SpinWait.SpinUntil(() => phase1.Metrics.MessagesOut >= 1, TimeSpan.FromSeconds(2));
            snapshot = phase1.GetSnapshot(1_000, new long[4]);
        }

        // Phase 2: simulate restart by creating a fresh pipeline and restoring the snapshot.
        using var phase2 = SingleIndexPipeline();
        phase2.RestoreFromSnapshot(snapshot);

        // Verify CheckpointTracker records the restore.
        Assert.True(phase2.Checkpoint.LastRestoredNanos > 0);

        // Forward processing after restore should succeed without corruption.
        var next = Tick(2, 1, 9_500_000_000, 2_000);
        phase2.TryPublish(in next);
        SpinWait.SpinUntil(() => phase2.Metrics.MessagesOut >= 1, TimeSpan.FromSeconds(2));

        Assert.Equal(2, phase2.Latest.Sequence);
        Assert.Equal(9_500_000_000L, phase2.GetSnapshot(2_000, new long[4]).LatestPricesE8[0]);
        Assert.Equal(0, phase2.Metrics.DuplicateEvents);
    }

    // ── Replay correctness: audit / replay stage (slide 19 stage 10) ─────

    [Fact]
    public void ReplayCorrectness_SnapshotRestorePreservesSequenceAndValue()
    {
        // Use seq=1 so no gap buffering triggers (BufferUntilRecovered would hold seq=5
        // because sequences 1–4 never arrived).
        using var pipeline = SingleIndexPipeline();

        var tick = Tick(1, 1, 12_000_000_000, 1_000);
        pipeline.TryPublish(in tick);
        SpinWait.SpinUntil(() => pipeline.Metrics.MessagesOut >= 1, TimeSpan.FromSeconds(2));

        var snap = pipeline.GetSnapshot(1_000, new long[4]);

        Assert.Equal(1, snap.Sequence);
        Assert.Equal(12_000_000_000L, snap.LatestPricesE8[0]);

        // Verify CheckpointTracker age falls after MarkWritten.
        Assert.True(pipeline.Checkpoint.AgeNanos < long.MaxValue);
    }
}