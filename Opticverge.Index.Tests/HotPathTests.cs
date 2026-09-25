using System.Diagnostics;
using System.Runtime.CompilerServices;
using Opticverge.Index.Contracts;
using Opticverge.Index.Engine;
using Projects;

namespace Opticverge.Index.Tests;

public sealed class HotPathTests
{
    [Fact]
    public void TickBinaryCodec_RoundTripsFixedWidthPayload()
    {
        var tick = CreateTick(42, 3, 123_456_789);
        Span<byte> buffer = stackalloc byte[TickBinaryCodec.Size];

        Assert.True(TickBinaryCodec.TryWrite(in tick, buffer));
        Assert.True(TickBinaryCodec.TryRead(buffer, out var decoded));
        Assert.Equal(tick, decoded);
    }

    [Fact]
    public void IndexValueBinaryCodec_RoundTripsFixedWidthPayload()
    {
        var value = new IndexValue(new IndexId(100), 42, 123_456_789, 987_654_321, 12_345, 8, 1);
        Span<byte> buffer = stackalloc byte[IndexValueBinaryCodec.Size];

        Assert.True(IndexValueBinaryCodec.TryWrite(in value, buffer));
        Assert.True(IndexValueBinaryCodec.TryRead(buffer, out var decoded));

        Assert.Equal(value, decoded);
    }

    [Fact]
    public void PartitionRouter_UsesStableShard()
    {
        var first = PartitionRouter.ForInstrument(new InstrumentId(123), 8);
        var second = PartitionRouter.ForInstrument(new InstrumentId(123), 8);

        Assert.Equal(first, second);
        Assert.InRange(first, 0, 7);
    }

    [Fact]
    public void PriceNormalizer_UsesEightDecimalFixedPoint()
    {
        var scaled = PriceNormalizer.ToScaledPrice(123.45678901m);

        Assert.Equal(12_345_678_901, scaled);
        Assert.Equal(123.45678901m, PriceNormalizer.ToDecimal(scaled));
    }

    [Fact]
    public void ReferenceDataValidator_AcceptsVersionedDemoCatalog()
    {
        var snapshot = IndexCatalog.CreateDemoSnapshot();

        var errors = ReferenceDataValidator.Validate(snapshot);

        Assert.Empty(errors);
        Assert.Equal(1, snapshot.Version);
        Assert.All(snapshot.Indexes, index =>
        {
            Assert.Equal(snapshot.Version, index.ReferenceDataVersion);
            Assert.Equal(IndexMethodology.WeightedPrice, index.Methodology);
        });
    }

    [Fact]
    public void ReferenceDataValidator_RejectsUnknownConstituentInstrument()
    {
        var snapshot = new ReferenceDataSnapshot(
            1,
            1,
            [new InstrumentDefinition(new InstrumentId(1), "ABC", new ExchangeId(1), "GBP")],
            [
                new IndexDefinition(
                    new IndexId(100),
                    "BAD",
                    "GBP",
                    [new IndexConstituentDefinition(new InstrumentId(2), PriceNormalizer.Scale, 100_000_000)])
            ],
            []);

        var errors = ReferenceDataValidator.Validate(snapshot);

        Assert.Contains(errors, error => error.Contains("unknown instrument id 2", StringComparison.OrdinalIgnoreCase));
    }

    [Fact]
    public void ReferenceDataValidator_RejectsSparseExternalInstrumentIds()
    {
        var snapshot = new ReferenceDataSnapshot(
            1,
            1,
            [new InstrumentDefinition(new InstrumentId(1_000_000), "EXT", new ExchangeId(1), "GBP")],
            [
                new IndexDefinition(
                    new IndexId(100),
                    "SPARSE",
                    "GBP",
                    [new IndexConstituentDefinition(new InstrumentId(1_000_000), PriceNormalizer.Scale, 100_000_000)])
            ],
            []);

        var errors = ReferenceDataValidator.Validate(snapshot);

        Assert.Contains(errors, error => error.Contains("dense internal ids", StringComparison.OrdinalIgnoreCase));
    }

    [Fact]
    public void ReferenceDataValidator_RejectsWeightedPriceWeightsThatDoNotSumToScale()
    {
        var snapshot = new ReferenceDataSnapshot(
            1,
            1,
            [new InstrumentDefinition(new InstrumentId(1), "ABC", new ExchangeId(1), "GBP")],
            [
                new IndexDefinition(
                    new IndexId(100),
                    "BAD-WEIGHT",
                    "GBP",
                    [new IndexConstituentDefinition(new InstrumentId(1), PriceNormalizer.Scale / 2, 100_000_000)])
            ],
            []);

        var errors = ReferenceDataValidator.Validate(snapshot);

        Assert.Contains(errors, error => error.Contains("must sum to", StringComparison.OrdinalIgnoreCase));
    }

    [Fact]
    public void ReferenceDataValidator_RequiresFxForCrossCurrencyConstituent()
    {
        var snapshot = new ReferenceDataSnapshot(
            1,
            1,
            [new InstrumentDefinition(new InstrumentId(1), "ABC", new ExchangeId(1), "USD")],
            [
                new IndexDefinition(
                    new IndexId(100),
                    "GBP-IDX",
                    "GBP",
                    [new IndexConstituentDefinition(new InstrumentId(1), PriceNormalizer.Scale, 100_000_000)])
            ],
            []);

        var errors = ReferenceDataValidator.Validate(snapshot);

        Assert.Contains(errors, error => error.Contains("requires FX USD->GBP", StringComparison.OrdinalIgnoreCase));
    }

    [Fact]
    public void ReferenceDataValidator_AcceptsCrossCurrencyConstituentWithFx()
    {
        var snapshot = new ReferenceDataSnapshot(
            1,
            1,
            [new InstrumentDefinition(new InstrumentId(1), "ABC", new ExchangeId(1), "USD")],
            [
                new IndexDefinition(
                    new IndexId(100),
                    "GBP-IDX",
                    "GBP",
                    [new IndexConstituentDefinition(new InstrumentId(1), PriceNormalizer.Scale, 100_000_000)])
            ],
            [new FxRateDefinition("USD", "GBP", 78_000_000, 1)]);

        var errors = ReferenceDataValidator.Validate(snapshot);

        Assert.Empty(errors);
    }

    [Fact]
    public void ReferenceDataValidator_RequiresSharesForMarketCapMethodology()
    {
        var snapshot = new ReferenceDataSnapshot(
            1,
            1,
            [new InstrumentDefinition(new InstrumentId(1), "ABC", new ExchangeId(1), "GBP")],
            [
                new IndexDefinition(
                    new IndexId(100),
                    "MCAP",
                    "GBP",
                    [new IndexConstituentDefinition(new InstrumentId(1), 0, 100_000_000)])
                {
                    Methodology = IndexMethodology.FloatAdjustedMarketCap
                }
            ],
            []);

        var errors = ReferenceDataValidator.Validate(snapshot);

        Assert.Contains(errors, error => error.Contains("must have positive shares", StringComparison.OrdinalIgnoreCase));
    }

    [Fact]
    public void WeightedIndexCalculator_UpdatesIndexValue()
    {
        var calculator = new WeightedIndexCalculator(IndexCatalog.CreateDemo().Indexes[0]);

        var tick = CreateTick(1, 1, 9_400_000_000);

        Assert.True(calculator.Apply(in tick, out var value));

        Assert.Equal(new IndexId(100), value.IndexId);
        Assert.Equal(1, value.Sequence);
        Assert.True(value.ValueE8 > 0);
        Assert.Equal(8, value.ConstituentCount);
    }

    [Fact]
    public void WeightedIndexCalculator_HandlesSparseInstrumentIds()
    {
        var definition = new IndexDefinition(
            new IndexId(200),
            "SPARSE",
            "GBP",
            [new IndexConstituentDefinition(new InstrumentId(1_000_000), PriceNormalizer.Scale, 100_000_000)]);
        var calculator = new WeightedIndexCalculator(definition);
        var tick = CreateTick(1, 1_000_000, 101_000_000);

        Assert.True(calculator.Apply(in tick, out var value));
        Assert.Equal(new IndexId(200), value.IndexId);
        Assert.Equal(1, value.Sequence);
    }

    [Fact]
    public void WeightedIndexCalculator_RejectsOutOfOrderTicks()
    {
        var calculator = new WeightedIndexCalculator(IndexCatalog.CreateDemo().Indexes[0]);
        var first = CreateTick(exchangeTimestampNanos: 2_000);
        var stale = CreateTick(2, exchangeTimestampNanos: 1_999);

        Assert.True(calculator.Apply(in first, out _));
        Assert.False(calculator.Apply(in stale, out _));
    }

    [Fact]
    public void WeightedIndexCalculator_PrevalidatedPath_AllowsPooledTickReuse()
    {
        var calculator = new WeightedIndexCalculator(IndexCatalog.CreateDemo().Indexes[0]);
        var tick = CreateTick(exchangeTimestampNanos: 2_000);

        Assert.True(calculator.ApplyPrevalidated(in tick));
        Assert.True(calculator.ApplyPrevalidated(in tick));
    }

    [Fact]
    public void HotPathIndexApply_DoesNotAllocate()
    {
        var calculator = new WeightedIndexCalculator(IndexCatalog.CreateDemo().Indexes[0]);
        var warmup = CreateTick();
        var measured = CreateTick(2, exchangeTimestampNanos: 2_000);

        calculator.Apply(in warmup, out _);

        var before = GC.GetAllocatedBytesForCurrentThread();
        calculator.Apply(in measured, out _);
        var after = GC.GetAllocatedBytesForCurrentThread();

        Assert.Equal(before, after);
    }

    [Fact]
    public void SpscRingBuffer_WritesAndDrainsWithoutAllocation()
    {
        var ring = new SpscMarketTickRingBuffer(1024);
        var calculator = new WeightedIndexCalculator(IndexCatalog.CreateDemo().Indexes[0]);
        var ticks = new MarketTick[128];

        for (var i = 0; i < ticks.Length; i++)
            ticks[i] = CreateTick(
                i / 8 + 1,
                (i & 7) + 1,
                exchangeTimestampNanos: i + 1);

        var before = GC.GetAllocatedBytesForCurrentThread();
        var written = ring.TryWriteBatch(ticks);
        var drained = ring.DrainTo(calculator, ticks.Length);
        var after = GC.GetAllocatedBytesForCurrentThread();

        Assert.Equal(ticks.Length, written);
        Assert.Equal(ticks.Length, drained);
        Assert.Equal(before, after);
    }

    [Fact]
    public void VectorizedRecompute_MatchesScalarForSafeWholeWeights()
    {
        Span<long> prices = stackalloc long[16];
        Span<long> weights = stackalloc long[16];

        for (var i = 0; i < prices.Length; i++)
        {
            prices[i] = 9_350_000_000 + i;
            weights[i] = PriceNormalizer.Scale;
        }

        Assert.Equal(
            IndexMath.RecomputeScalar(prices, weights),
            IndexMath.RecomputeVectorizedProducts(prices, weights));
    }

    [Fact]
    public void DisruptorPipeline_AcceptsTicks()
    {
        using var pipeline = new DisruptorIndexPipeline(
            IndexCatalog.CreateDemo().Indexes[0],
            new EngineOptions { RingBufferSize = 1024 });

        var tick = CreateTick();

        Assert.True(pipeline.TryPublish(in tick));
    }

    [Fact]
    public void DisruptorPipeline_DropsDuplicateSequence_BeforeIncrementalCalculation()
    {
        using var pipeline = new DisruptorIndexPipeline(
            IndexCatalog.CreateDemo().Indexes[0],
            new EngineOptions
            {
                RingBufferSize = 1024,
                LatencySampleRate = 0
            });

        var first = CreateTick(1, 1, 9_400_000_000);
        var target = pipeline.Metrics.MessagesOut + 1;

        Assert.True(pipeline.TryPublish(in first));
        Assert.True(pipeline.WaitForMessagesOut(target, TimeSpan.FromSeconds(1)));

        var afterFirst = pipeline.Latest;
        var duplicate = CreateTick(1, 1, 10_000_000_000, 2_000);

        Assert.True(pipeline.TryPublish(in duplicate));
        Assert.True(WaitUntil(() => pipeline.Metrics.DuplicateEvents == 1, TimeSpan.FromSeconds(1)));

        Assert.Equal(afterFirst.Sequence, pipeline.Latest.Sequence);
        Assert.Equal(afterFirst.ValueE8, pipeline.Latest.ValueE8);
        Assert.Equal(target, pipeline.Metrics.MessagesOut);
        Assert.Equal(0, pipeline.Metrics.SequenceGaps);
    }

    [Fact]
    public void DisruptorPipeline_BuffersGapSequence_AndAppliesAfterMissingSequence()
    {
        using var pipeline = new DisruptorIndexPipeline(
            IndexCatalog.CreateDemo().Indexes[0],
            new EngineOptions
            {
                RingBufferSize = 1024,
                LatencySampleRate = 0
            });

        var first = CreateTick(1, 1, 9_400_000_000);
        var firstTarget = pipeline.Metrics.MessagesOut + 1;

        Assert.True(pipeline.TryPublish(in first));
        Assert.True(pipeline.WaitForMessagesOut(firstTarget, TimeSpan.FromSeconds(1)));

        var afterFirst = pipeline.Latest;
        var gapped = CreateTick(3, 1, 10_000_000_000, 3_000);

        Assert.True(pipeline.TryPublish(in gapped));
        Assert.True(WaitUntil(() => pipeline.Metrics.SequenceGaps == 1, TimeSpan.FromSeconds(1)));

        Assert.Equal(afterFirst.Sequence, pipeline.Latest.Sequence);
        Assert.Equal(afterFirst.ValueE8, pipeline.Latest.ValueE8);
        Assert.Equal(firstTarget, pipeline.Metrics.MessagesOut);

        var missing = CreateTick(2, 1, 9_500_000_000, 2_000);
        var secondTarget = pipeline.Metrics.MessagesOut + 2;

        Assert.True(pipeline.TryPublish(in missing));
        Assert.True(pipeline.WaitForMessagesOut(secondTarget, TimeSpan.FromSeconds(1)));

        Assert.Equal(afterFirst.Sequence + 2, pipeline.Latest.Sequence);
        Assert.Equal(10_000_000_000, pipeline.GetSnapshot(0, new long[4]).LatestPricesE8[0]);
        Assert.Equal(1, pipeline.Metrics.SequenceGaps);
    }

    [Fact]
    public void DisruptorPipeline_BatchHandlerDrainsPublishedTicks()
    {
        using var pipeline = new DisruptorIndexPipeline(
            IndexCatalog.CreateDemo().Indexes[0],
            new EngineOptions
            {
                RingBufferSize = 1024,
                UseBatchHandler = true,
                AssumePrevalidatedTicks = true,
                LatencySampleRate = 0
            });

        var ticks = new MarketTick[128];
        for (var i = 0; i < ticks.Length; i++)
            ticks[i] = CreateTick(
                i / 8 + 1,
                (i & 7) + 1,
                exchangeTimestampNanos: i + 1);

        var target = pipeline.Metrics.MessagesOut + ticks.Length;

        Assert.True(pipeline.TryPublishBatch(ticks));
        Assert.True(pipeline.WaitForMessagesOut(target, TimeSpan.FromSeconds(1)));
    }

    [Fact]
    public void DisruptorPipeline_BatchHandler_DropsFinalDuplicate_AndCompletesBatch()
    {
        using var pipeline = new DisruptorIndexPipeline(
            IndexCatalog.CreateDemo().Indexes[0],
            new EngineOptions
            {
                RingBufferSize = 1024,
                UseBatchHandler = true,
                AssumePrevalidatedTicks = true,
                LatencySampleRate = 0
            });

        var ticks = new[]
        {
            CreateTick(1, 1, 9_400_000_000),
            CreateTick(1, 1, 10_000_000_000, 2_000)
        };
        var target = pipeline.Metrics.MessagesOut + 1;

        Assert.True(pipeline.TryPublishBatch(ticks));
        Assert.True(pipeline.WaitForMessagesOut(target, TimeSpan.FromSeconds(1)));
        Assert.True(WaitUntil(() => pipeline.Metrics.DuplicateEvents == 1, TimeSpan.FromSeconds(1)));

        Assert.Equal(1, pipeline.Latest.Sequence);
        Assert.Equal(target, pipeline.Metrics.MessagesOut);
        Assert.Equal(0, pipeline.Metrics.SequenceGaps);
        Assert.NotEqual(10_000_000_000, pipeline.GetSnapshot(0, new long[4]).LatestPricesE8[0]);
    }

    [Fact]
    public void DisruptorPipeline_BatchHandler_DropsFinalGap_AndCompletesBatch()
    {
        using var pipeline = new DisruptorIndexPipeline(
            IndexCatalog.CreateDemo().Indexes[0],
            new EngineOptions
            {
                RingBufferSize = 1024,
                UseBatchHandler = true,
                AssumePrevalidatedTicks = true,
                LatencySampleRate = 0
            });

        var ticks = new[]
        {
            CreateTick(1, 1, 9_400_000_000),
            CreateTick(3, 1, 10_000_000_000, 3_000)
        };
        var target = pipeline.Metrics.MessagesOut + 1;

        Assert.True(pipeline.TryPublishBatch(ticks));
        Assert.True(pipeline.WaitForMessagesOut(target, TimeSpan.FromSeconds(1)));
        Assert.True(WaitUntil(() => pipeline.Metrics.SequenceGaps == 1, TimeSpan.FromSeconds(1)));

        Assert.Equal(1, pipeline.Latest.Sequence);
        Assert.Equal(target, pipeline.Metrics.MessagesOut);
        Assert.NotEqual(10_000_000_000, pipeline.GetSnapshot(0, new long[4]).LatestPricesE8[0]);
    }

    [Fact]
    public void DisruptorPipeline_BatchHandler_BuffersAndReplaysGapWithinBatch()
    {
        using var pipeline = new DisruptorIndexPipeline(
            IndexCatalog.CreateDemo().Indexes[0],
            new EngineOptions
            {
                RingBufferSize = 1024,
                UseBatchHandler = true,
                AssumePrevalidatedTicks = true,
                LatencySampleRate = 0
            });

        var ticks = new[]
        {
            CreateTick(1, 1, 9_400_000_000),
            CreateTick(3, 1, 10_000_000_000, 3_000),
            CreateTick(2, 1, 9_500_000_000, 2_000)
        };
        var target = pipeline.Metrics.MessagesOut + ticks.Length;

        Assert.True(pipeline.TryPublishBatch(ticks));
        Assert.True(pipeline.WaitForMessagesOut(target, TimeSpan.FromSeconds(1)));

        Assert.Equal(3, pipeline.Latest.Sequence);
        Assert.Equal(1, pipeline.Metrics.SequenceGaps);
        Assert.Equal(10_000_000_000, pipeline.GetSnapshot(0, new long[4]).LatestPricesE8[0]);
    }

    [Fact]
    public void DisruptorPipeline_DropUntilRecoveredPolicy_DropsGapSequence()
    {
        using var pipeline = new DisruptorIndexPipeline(
            IndexCatalog.CreateDemo().Indexes[0],
            new EngineOptions
            {
                RingBufferSize = 1024,
                LatencySampleRate = 0,
                SequenceGapPolicy = SequenceGapPolicy.DropUntilRecovered
            });

        var first = CreateTick(1, 1, 9_400_000_000);
        var gapped = CreateTick(3, 1, 10_000_000_000, 3_000);
        var target = pipeline.Metrics.MessagesOut + 1;

        Assert.True(pipeline.TryPublish(in first));
        Assert.True(pipeline.TryPublish(in gapped));
        Assert.True(pipeline.WaitForMessagesOut(target, TimeSpan.FromSeconds(1)));
        Assert.True(WaitUntil(() => pipeline.Metrics.SequenceGaps == 1, TimeSpan.FromSeconds(1)));

        Assert.Equal(1, pipeline.Latest.Sequence);
        Assert.NotEqual(10_000_000_000, pipeline.GetSnapshot(0, new long[4]).LatestPricesE8[0]);
    }

    [Fact]
    public void DisruptorPipeline_ProcessAndReportGapPolicy_AppliesGapSequence()
    {
        using var pipeline = new DisruptorIndexPipeline(
            IndexCatalog.CreateDemo().Indexes[0],
            new EngineOptions
            {
                RingBufferSize = 1024,
                LatencySampleRate = 0,
                SequenceGapPolicy = SequenceGapPolicy.ProcessAndReport
            });

        var ticks = new[]
        {
            CreateTick(1, 1, 9_400_000_000),
            CreateTick(3, 1, 10_000_000_000, 3_000)
        };
        var target = pipeline.Metrics.MessagesOut + ticks.Length;

        Assert.True(pipeline.TryPublishBatch(ticks));
        Assert.True(pipeline.WaitForMessagesOut(target, TimeSpan.FromSeconds(1)));

        Assert.Equal(2, pipeline.Latest.Sequence);
        Assert.Equal(1, pipeline.Metrics.SequenceGaps);
        Assert.Equal(10_000_000_000, pipeline.GetSnapshot(0, new long[4]).LatestPricesE8[0]);
    }

    [Fact]
    public void DisruptorPipeline_RestoreFromSnapshot_PreservesPerInstrumentSequenceState()
    {
        var definition = IndexCatalog.CreateDemo().Indexes[0];
        CalculatorSnapshot snapshot;

        using (var source = new DisruptorIndexPipeline(
                   definition,
                   new EngineOptions
                   {
                       RingBufferSize = 1024,
                       UseBatchHandler = true,
                       AssumePrevalidatedTicks = true,
                       LatencySampleRate = 0
                   }))
        {
            var ticks = new[]
            {
                CreateTick(1, 1, 9_400_000_000),
                CreateTick(2, 1, 9_500_000_000, 2_000)
            };
            var target = source.Metrics.MessagesOut + ticks.Length;

            Assert.True(source.TryPublishBatch(ticks));
            Assert.True(source.WaitForMessagesOut(target, TimeSpan.FromSeconds(1)));

            snapshot = source.GetSnapshot(123_456, new long[4]);
        }

        Assert.Equal(1, snapshot.InstrumentIds[0]);
        Assert.Equal(2, snapshot.LastSequences[0]);

        using var restored = new DisruptorIndexPipeline(
            definition,
            new EngineOptions
            {
                RingBufferSize = 1024,
                UseBatchHandler = true,
                AssumePrevalidatedTicks = true,
                LatencySampleRate = 0
            });

        restored.RestoreFromSnapshot(snapshot);
        var afterRestore = restored.Latest;

        Assert.Equal(snapshot.Sequence, afterRestore.Sequence);
        Assert.Equal(snapshot.ValueE8, afterRestore.ValueE8);
        Assert.Equal(snapshot.TimestampNanos, afterRestore.TimestampNanos);

        var duplicate = CreateTick(2, 1, 10_000_000_000, 3_000);
        Assert.True(restored.TryPublish(in duplicate));
        Assert.True(WaitUntil(() => restored.Metrics.DuplicateEvents == 1, TimeSpan.FromSeconds(1)));
        Assert.Equal(afterRestore.Sequence, restored.Latest.Sequence);
        Assert.Equal(afterRestore.ValueE8, restored.Latest.ValueE8);

        var buffered = CreateTick(4, 1, 10_100_000_000, 4_000);
        Assert.True(restored.TryPublish(in buffered));
        Assert.True(WaitUntil(() => restored.Metrics.SequenceGaps == 1, TimeSpan.FromSeconds(1)));
        Assert.Equal(afterRestore.Sequence, restored.Latest.Sequence);

        var missing = CreateTick(3, 1, 9_600_000_000, 3_500);
        var recoveryTarget = restored.Metrics.MessagesOut + 2;
        Assert.True(restored.TryPublish(in missing));
        Assert.True(restored.WaitForMessagesOut(recoveryTarget, TimeSpan.FromSeconds(1)));

        Assert.Equal(afterRestore.Sequence + 2, restored.Latest.Sequence);
        Assert.Equal(10_100_000_000, restored.GetSnapshot(0, new long[4]).LatestPricesE8[0]);
    }

    [Fact]
    public void WeightedIndexCalculator_DeterministicReplay_MatchesCheckpoint()
    {
        var definition = IndexCatalog.CreateDemo().Indexes[0];
        var calc = new WeightedIndexCalculator(definition);

        // Warm up — one tick per constituent
        for (var i = 1; i <= 8; i++)
        {
            var t = CreateTick(i, i,
                9_350_000_000 + i * 1_000_000, i * 1_000);
            calc.Apply(in t, out _);
        }

        var snapshot = calc.GetSnapshot(MonotonicClock.TimestampNanos(), new long[4]);
        var levelAtCheckpoint = calc.Snapshot(0).LevelE8;

        // Apply post-checkpoint ticks
        for (var i = 9; i <= 16; i++)
        {
            var t = CreateTick(i, (i - 1) % 8 + 1,
                9_350_000_000 + i * 1_000_000, i * 1_000);
            calc.Apply(in t, out _);
        }

        // Restore and replay the same post-checkpoint ticks
        var replayed = new WeightedIndexCalculator(definition);
        replayed.RestoreFromSnapshot(snapshot);

        Assert.Equal(levelAtCheckpoint, replayed.Snapshot(0).LevelE8);

        for (var i = 9; i <= 16; i++)
        {
            var t = CreateTick(i, (i - 1) % 8 + 1,
                9_350_000_000 + i * 1_000_000, i * 1_000);
            replayed.Apply(in t, out _);
        }

        Assert.Equal(calc.Snapshot(0).LevelE8, replayed.Snapshot(0).LevelE8);
        Assert.Equal(calc.Snapshot(0).ValueE8, replayed.Snapshot(0).ValueE8);
        Assert.Equal(calc.Snapshot(0).Sequence, replayed.Snapshot(0).Sequence);
    }

    [Fact]
    public void WeightedIndexCalculator_ConstituentRemoval_PreservesLevel()
    {
        var calc = new WeightedIndexCalculator(IndexCatalog.CreateDemo().Indexes[0]);

        var tick = CreateTick(1, 1, 9_400_000_000);
        calc.Apply(in tick, out _);

        var levelBefore = calc.Snapshot(0).LevelE8;
        var countBefore = calc.Snapshot(0).ConstituentCount;

        Assert.True(calc.ApplyCorporateAction(
            new CorporateAction(100, 1, CorporateActionType.ConstituentRemoval, 0)));

        var after = calc.Snapshot(0);

        // Level must be continuous within a 1-unit rounding tolerance.
        Assert.True(Math.Abs(after.LevelE8 - levelBefore) <= 1,
            $"Level discontinuity: before={levelBefore} after={after.LevelE8}");
        Assert.Equal(countBefore - 1, after.ConstituentCount);

        // Subsequent ticks for the removed constituent must be rejected.
        var followUp = CreateTick(2, 1,
            9_500_000_000, 9_000);
        Assert.False(calc.Apply(in followUp, out _));
    }

    [Fact]
    public void DisruptorPipeline_MultiIndex_RoutesTicks_ToCorrectCalculators()
    {
        var catalog = IndexCatalog.CreateDemo();
        // All three indexes in one pipeline — one ring buffer, one consumer thread.
        using var pipeline = new DisruptorIndexPipeline(
            catalog.Indexes,
            new EngineOptions
            {
                RingBufferSize = 1024,
                UseBatchHandler = true,
                AssumePrevalidatedTicks = true,
                LatencySampleRate = 0
            });

        // Instrument 1 belongs to OVX-RT8 (100) and OVX-LC4 (101) but NOT OVX-SC4 (102).
        // Instrument 5 belongs to OVX-RT8 (100) and OVX-SC4 (102) but NOT OVX-LC4 (101).
        var tickInstr1 = CreateTick();
        var tickInstr5 = CreateTick(2, 5, exchangeTimestampNanos: 2_000);
        var target = pipeline.Metrics.MessagesOut + 2;

        Assert.True(pipeline.TryPublish(in tickInstr1));
        Assert.True(pipeline.TryPublish(in tickInstr5));
        Assert.True(pipeline.WaitForMessagesOut(target, TimeSpan.FromSeconds(2)));

        var indexes = pipeline.Indexes;
        Assert.Equal(3, indexes.Count);

        // OVX-RT8 (index 0) contains instruments 1-8 — both ticks should advance its sequence.
        Assert.True(indexes[0].Sequence >= 2, $"OVX-RT8 sequence={indexes[0].Sequence}");

        // OVX-LC4 (index 1) contains instruments 1-4 — only tick for instrument 1 should hit it.
        Assert.True(indexes[1].Sequence >= 1, $"OVX-LC4 sequence={indexes[1].Sequence}");

        // OVX-SC4 (index 2) contains instruments 5-8 — only tick for instrument 5 should hit it.
        Assert.True(indexes[2].Sequence >= 1, $"OVX-SC4 sequence={indexes[2].Sequence}");

        // Each sub-index has its own level, both starting near 1000.
        Assert.True(indexes[1].LevelE8 > 0);
        Assert.True(indexes[2].LevelE8 > 0);
    }

    [Fact]
    public void DisruptorPipeline_BatchHandler_UsesPerCalculatorReceiveTimestamp()
    {
        var catalog = IndexCatalog.CreateDemo();
        using var pipeline = new DisruptorIndexPipeline(
            catalog.Indexes,
            new EngineOptions
            {
                RingBufferSize = 1024,
                UseBatchHandler = true,
                AssumePrevalidatedTicks = true,
                LatencySampleRate = 0
            });

        var tickInstr1 = CreateTick();
        var tickInstr5 = CreateTick(1, 5, exchangeTimestampNanos: 2_000);
        var ticks = new[] { tickInstr1, tickInstr5 };
        var target = pipeline.Metrics.MessagesOut + ticks.Length;

        Assert.True(pipeline.TryPublishBatch(ticks));
        Assert.True(pipeline.WaitForMessagesOut(target, TimeSpan.FromSeconds(2)));

        var indexes = pipeline.Indexes;
        Assert.Equal(tickInstr5.ReceiveTimestampNanos, indexes[0].TimestampNanos);
        Assert.Equal(tickInstr1.ReceiveTimestampNanos, indexes[1].TimestampNanos);
        Assert.Equal(tickInstr5.ReceiveTimestampNanos, indexes[2].TimestampNanos);
    }

    [Fact]
    public void WeightedIndexCalculator_ConstituentAddition_PreservesLevel()
    {
        // Start with an index that has instrument 1 removed (weight=0 via removal action).
        var calc = new WeightedIndexCalculator(IndexCatalog.CreateDemo().Indexes[0]);
        var tick = CreateTick(1, 1, 9_400_000_000);
        calc.Apply(in tick, out _);

        var removalAction = new CorporateAction(100, 1, CorporateActionType.ConstituentRemoval, 0);
        Assert.True(calc.ApplyCorporateAction(removalAction));

        var levelAfterRemoval = calc.Snapshot(0).LevelE8;
        var countAfterRemoval = calc.Snapshot(0).ConstituentCount;

        // Re-add with the original weight — divisor must absorb the change and preserve level.
        var additionAction = new CorporateAction(100, 1, CorporateActionType.ConstituentAddition, 18_000_000);
        Assert.True(calc.ApplyCorporateAction(additionAction));

        var after = calc.Snapshot(0);
        Assert.True(Math.Abs(after.LevelE8 - levelAfterRemoval) <= 1,
            $"Level should be continuous: before={levelAfterRemoval} after={after.LevelE8}");
        Assert.Equal(countAfterRemoval + 1, after.ConstituentCount);

        // Subsequent ticks for the re-added instrument must now be accepted.
        var followUp = CreateTick(2, 1,
            9_500_000_000, 9_000);
        Assert.True(calc.Apply(in followUp, out _));
    }

    [Fact(Skip = "Requires Docker/Redpanda. Run explicitly when validating the Aspire AppHost.")]
    public async Task AspireAppHost_CanStartDistributedApplication()
    {
        var appHost = await DistributedApplicationTestingBuilder
            .CreateAsync<Opticverge_Index_AppHost>([], CancellationToken.None);

        await using var app = await appHost.BuildAsync(CancellationToken.None);
        await app.StartAsync(CancellationToken.None);
    }

    private static MarketTick CreateTick(
        long sequence = 1,
        int instrumentId = 1,
        long priceE8 = 9_350_000_000,
        long exchangeTimestampNanos = 1_000)
    {
        return new MarketTick(
            sequence,
            new InstrumentId(instrumentId),
            new ExchangeId(1),
            priceE8,
            100,
            exchangeTimestampNanos,
            exchangeTimestampNanos + 1_000,
            PartitionRouter.ForInstrument(new InstrumentId(instrumentId), 4),
            TickFlags.Trade);
    }

    private static bool WaitUntil(Func<bool> condition, TimeSpan timeout)
    {
        var startedAt = Stopwatch.GetTimestamp();
        var spin = new SpinWait();

        while (!condition())
        {
            if (Stopwatch.GetElapsedTime(startedAt) > timeout) return false;

            spin.SpinOnce(-1);
        }

        return true;
    }
}

public static class TestConfiguration
{
    [ModuleInitializer]
    public static void DisableAspireReloadOnChange()
    {
        AppContext.SetSwitch("Microsoft.Extensions.Configuration.DisableFileSystemWatcher", true);
    }
}