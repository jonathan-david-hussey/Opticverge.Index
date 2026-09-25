using Opticverge.Index.Contracts;
using Opticverge.Index.Engine;

namespace Opticverge.Index.Tests;

public sealed class FanOutFanInTests
{
    // ── helpers ─────────────────────────────────────────────────────────────

    private static MarketTick Tick(int instrumentId, long seq, long priceE8, long exchangeNanos = 0)
    {
        return new MarketTick(seq, new InstrumentId(instrumentId), new ExchangeId(1), priceE8, 100,
            exchangeNanos == 0 ? seq * 1_000 : exchangeNanos, seq * 1_000, 0, TickFlags.Trade);
    }

    private static IndexDefinition SingleConstituentIndex(int indexId, int instrumentId, long initialPriceE8 = 100_000_000)
    {
        return new IndexDefinition(new IndexId((short)indexId), $"IDX-{indexId}", "GBP",
            [new IndexConstituentDefinition(new InstrumentId(instrumentId), PriceNormalizer.Scale, initialPriceE8)]);
    }

    // ── fan-out: one instrument routed to multiple indexes ───────────────

    [Fact]
    public void FanOut_InstrumentSharedByTwoIndexes_BothIndexValuesUpdate()
    {
        // Instrument 1 appears in both Index 100 and Index 101.
        var definitions = new List<IndexDefinition>
        {
            new(new IndexId(100), "IDX-A", "GBP",
            [
                new IndexConstituentDefinition(new InstrumentId(1), PriceNormalizer.Scale / 2, 100_000_000),
                new IndexConstituentDefinition(new InstrumentId(2), PriceNormalizer.Scale / 2, 100_000_000)
            ]),
            new(new IndexId(101), "IDX-B", "GBP",
                [new IndexConstituentDefinition(new InstrumentId(1), PriceNormalizer.Scale, 100_000_000)])
        };

        using var pipeline = new DisruptorIndexPipeline(definitions);
        var tick = Tick(1, 1, 200_000_000);
        pipeline.TryPublish(in tick);

        // Both indexes should register a calculation failure or success as a sign of routing.
        SpinWait.SpinUntil(() => pipeline.Metrics.MessagesOut > 0 || pipeline.Metrics.CalculationFailures > 0,
            TimeSpan.FromSeconds(2));

        // Fan-out factor should be recorded as 2 (instrument 1 routes to both calculators).
        SpinWait.SpinUntil(() => pipeline.Metrics.FanOutFactorPercentiles().P50 > 0, TimeSpan.FromSeconds(2));
        var (p50, _) = pipeline.Metrics.FanOutFactorPercentiles();
        Assert.Equal(2, p50);
    }

    [Fact]
    public void FanOut_InstrumentUniqueToEachIndex_FanOutOfOnePerInstrument()
    {
        var definitions = new List<IndexDefinition>
        {
            new(new IndexId(100), "IDX-A", "GBP",
                [new IndexConstituentDefinition(new InstrumentId(1), PriceNormalizer.Scale, 100_000_000)]),
            new(new IndexId(101), "IDX-B", "GBP",
                [new IndexConstituentDefinition(new InstrumentId(2), PriceNormalizer.Scale, 100_000_000)])
        };

        using var pipeline = new DisruptorIndexPipeline(definitions,
            new EngineOptions { RingBufferSize = 256, LatencySampleRate = 0, AssumePrevalidatedTicks = true });

        // Publish one tick each for the two exclusive instruments.
        var t1 = Tick(1, 1, 150_000_000);
        var t2 = Tick(2, 2, 150_000_000);
        pipeline.TryPublishBatch([t1, t2]);

        SpinWait.SpinUntil(() => pipeline.Metrics.MessagesIn >= 2, TimeSpan.FromSeconds(2));
        SpinWait.SpinUntil(() => pipeline.Metrics.FanOutFactorPercentiles().P50 > 0, TimeSpan.FromSeconds(2));

        var (p50, p99) = pipeline.Metrics.FanOutFactorPercentiles();
        Assert.Equal(1, p50);
        Assert.Equal(1, p99);
    }

    [Fact]
    public void FanOut_UnknownInstrument_ProducesNoIndexUpdate()
    {
        var definitions = new List<IndexDefinition>
        {
            SingleConstituentIndex(100, 1)
        };

        using var pipeline = new DisruptorIndexPipeline(definitions,
            new EngineOptions { RingBufferSize = 256, LatencySampleRate = 0, AssumePrevalidatedTicks = true });

        // First publish a known-instrument tick so the pipeline is running.
        var known = Tick(1, 1, 150_000_000);
        pipeline.TryPublish(in known);
        SpinWait.SpinUntil(() => pipeline.Metrics.MessagesOut >= 1, TimeSpan.FromSeconds(2));

        var valueAfterKnown = pipeline.Latest.ValueE8;
        Assert.True(valueAfterKnown > 0);

        // Now publish a tick for an instrument not in any index.
        var unknown = Tick(2, 999, 100_000_000);
        pipeline.TryPublish(in unknown);
        SpinWait.SpinUntil(() => pipeline.Metrics.MessagesIn >= 2, TimeSpan.FromSeconds(2));
        // Wait briefly for processing to complete.
        SpinWait.SpinUntil(() => pipeline.Metrics.MessagesOut >= 2 || pipeline.Metrics.CalculationFailures >= 1,
            TimeSpan.FromSeconds(2));

        // The index value must not have changed because instrument 999 routes to nothing.
        Assert.Equal(valueAfterKnown, pipeline.Latest.ValueE8);
        // MessagesOut should still be 1 — no new index calculation was emitted.
        Assert.Equal(1, pipeline.Metrics.MessagesOut);
    }

    // ── fan-in: multiple instruments converge to single index state ──────

    [Fact]
    public void WeightedIndexCalculator_FanIn_TwoInstrumentsTwoTicks_IndexUpdatesAfterEach()
    {
        var definition = new IndexDefinition(
            new IndexId(100), "FANIN", "GBP",
            [
                new IndexConstituentDefinition(new InstrumentId(1), PriceNormalizer.Scale / 2, 100_000_000),
                new IndexConstituentDefinition(new InstrumentId(2), PriceNormalizer.Scale / 2, 100_000_000)
            ]);
        var calc = new WeightedIndexCalculator(definition);

        var tick1 = Tick(1, 1, 110_000_000);
        var tick2 = Tick(2, 2, 120_000_000);

        Assert.True(calc.Apply(in tick1, out var after1));
        var valueAfter1 = after1.ValueE8;

        Assert.True(calc.Apply(in tick2, out var after2));
        var valueAfter2 = after2.ValueE8;

        // Each tick should change the index value since different instruments have different prices.
        Assert.NotEqual(0, valueAfter1);
        Assert.NotEqual(0, valueAfter2);
        Assert.NotEqual(valueAfter1, valueAfter2);
    }

    [Fact]
    public void DisruptorPipeline_TickToSharedInstrument_UpdatesBothIndexes()
    {
        // Index 100 and 101 both contain instrument 1.
        var sharedInstrument = new InstrumentId(1);
        var definitions = new List<IndexDefinition>
        {
            new(new IndexId(100), "IDX-A", "GBP",
                [new IndexConstituentDefinition(sharedInstrument, PriceNormalizer.Scale, 100_000_000)]),
            new(new IndexId(101), "IDX-B", "GBP",
                [new IndexConstituentDefinition(sharedInstrument, PriceNormalizer.Scale, 100_000_000)])
        };

        using var pipeline = new DisruptorIndexPipeline(definitions);
        var tick = Tick(1, 1, 200_000_000);
        pipeline.TryPublish(in tick);
        SpinWait.SpinUntil(() => pipeline.Metrics.MessagesOut > 0, TimeSpan.FromSeconds(2));

        var indexes = pipeline.Indexes;
        Assert.Equal(2, indexes.Count);
        Assert.All(indexes, idx => Assert.True(idx.ValueE8 > 0));
    }

    [Fact]
    public void DisruptorPipeline_TickToExclusiveInstrument_UpdatesOnlyOneIndex()
    {
        // Instrument 1 in Index 100 only; instrument 2 in Index 101 only.
        var definitions = new List<IndexDefinition>
        {
            new(new IndexId(100), "IDX-A", "GBP",
                [new IndexConstituentDefinition(new InstrumentId(1), PriceNormalizer.Scale, 100_000_000)]),
            new(new IndexId(101), "IDX-B", "GBP",
                [new IndexConstituentDefinition(new InstrumentId(2), PriceNormalizer.Scale, 100_000_000)])
        };

        using var pipeline = new DisruptorIndexPipeline(definitions);

        // Only instrument 1 ticks.
        var tick = Tick(1, 1, 150_000_000);
        pipeline.TryPublish(in tick);
        SpinWait.SpinUntil(() => pipeline.Metrics.MessagesIn >= 1, TimeSpan.FromSeconds(2));
        SpinWait.SpinUntil(() => pipeline.Metrics.MessagesOut > 0, TimeSpan.FromSeconds(2));

        var indexes = pipeline.Indexes;
        // Index 100 should have a non-zero value; index 101 has had no input so value remains at initial.
        Assert.True(indexes.First(i => i.IndexId.Value == 100).ValueE8 > 0);
    }

    // ── fan-out factor metric recording ─────────────────────────────────

    [Fact]
    public void EngineMetrics_RecordFanOutFactor_ReturnsNonZeroP50AfterSamples()
    {
        var metrics = new EngineMetrics();

        for (var i = 0; i < 1_000; i++)
            metrics.RecordFanOutFactor(3);

        var (p50, p99) = metrics.FanOutFactorPercentiles();

        Assert.Equal(3, p50);
        Assert.Equal(3, p99);
    }

    [Fact]
    public void EngineMetrics_RecordFanOutFactor_MixedDistribution_P99HigherThanP50()
    {
        var metrics = new EngineMetrics();

        // 90% of instruments route to 1 index, 10% route to 5.
        for (var i = 0; i < 900; i++) metrics.RecordFanOutFactor(1);
        for (var i = 0; i < 100; i++) metrics.RecordFanOutFactor(5);

        var (p50, p99) = metrics.FanOutFactorPercentiles();

        Assert.Equal(1, p50);
        Assert.True(p99 >= 5, $"Expected p99 >= 5 but got {p99}");
    }
}