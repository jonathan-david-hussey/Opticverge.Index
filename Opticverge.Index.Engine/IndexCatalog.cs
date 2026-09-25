using Opticverge.Index.Contracts;

namespace Opticverge.Index.Engine;

public static class IndexCatalog
{
    private const long DemoReferenceDataVersion = 1;
    private const long DemoEffectiveFromNanos = 1;

    public static DemoCatalog CreateDemo()
    {
        var snapshot = CreateDemoSnapshot();
        ReferenceDataValidator.ThrowIfInvalid(snapshot);
        return new DemoCatalog(snapshot.Instruments, snapshot.Indexes)
        {
            ReferenceDataVersion = snapshot.Version,
            EffectiveFromNanos = snapshot.EffectiveFromNanos,
            FxRates = snapshot.FxRates
        };
    }

    public static ReferenceDataSnapshot CreateDemoSnapshot()
    {
        var instruments = new[]
        {
            new InstrumentDefinition(new InstrumentId(1), "LSEG", new ExchangeId(1), "GBP") { VenueMic = "XLON" },
            new InstrumentDefinition(new InstrumentId(2), "VOD", new ExchangeId(1), "GBP") { VenueMic = "XLON" },
            new InstrumentDefinition(new InstrumentId(3), "AZN", new ExchangeId(1), "GBP") { VenueMic = "XLON" },
            new InstrumentDefinition(new InstrumentId(4), "HSBA", new ExchangeId(1), "GBP") { VenueMic = "XLON" },
            new InstrumentDefinition(new InstrumentId(5), "BP", new ExchangeId(1), "GBP") { VenueMic = "XLON" },
            new InstrumentDefinition(new InstrumentId(6), "SHEL", new ExchangeId(1), "GBP") { VenueMic = "XLON" },
            new InstrumentDefinition(new InstrumentId(7), "ULVR", new ExchangeId(1), "GBP") { VenueMic = "XLON" },
            new InstrumentDefinition(new InstrumentId(8), "GSK", new ExchangeId(1), "GBP") { VenueMic = "XLON" }
        };

        var index = new IndexDefinition(
            new IndexId(100),
            "OVX-RT8",
            "GBP",
            [
                new IndexConstituentDefinition(new InstrumentId(1), 18_000_000, 9_300_000_000),
                new IndexConstituentDefinition(new InstrumentId(2), 7_500_000, 78_000_000),
                new IndexConstituentDefinition(new InstrumentId(3), 19_000_000, 12_400_000_000),
                new IndexConstituentDefinition(new InstrumentId(4), 11_500_000, 790_000_000),
                new IndexConstituentDefinition(new InstrumentId(5), 10_500_000, 510_000_000),
                new IndexConstituentDefinition(new InstrumentId(6), 14_000_000, 2_850_000_000),
                new IndexConstituentDefinition(new InstrumentId(7), 11_000_000, 4_640_000_000),
                new IndexConstituentDefinition(new InstrumentId(8), 8_500_000, 1_635_000_000)
            ])
        {
            ReferenceDataVersion = DemoReferenceDataVersion,
            EffectiveFromNanos = DemoEffectiveFromNanos,
            Methodology = IndexMethodology.WeightedPrice
        };

        // Sub-indexes derived from the same instrument universe — all fed from the single tick stream.
        var largeCap = new IndexDefinition(
            new IndexId(101),
            "OVX-LC4",
            "GBP",
            [
                new IndexConstituentDefinition(new InstrumentId(1), 25_000_000, 9_300_000_000),
                new IndexConstituentDefinition(new InstrumentId(2), 15_000_000, 78_000_000),
                new IndexConstituentDefinition(new InstrumentId(3), 35_000_000, 12_400_000_000),
                new IndexConstituentDefinition(new InstrumentId(4), 25_000_000, 790_000_000)
            ])
        {
            ReferenceDataVersion = DemoReferenceDataVersion,
            EffectiveFromNanos = DemoEffectiveFromNanos,
            Methodology = IndexMethodology.WeightedPrice
        };

        var smallCap = new IndexDefinition(
            new IndexId(102),
            "OVX-SC4",
            "GBP",
            [
                new IndexConstituentDefinition(new InstrumentId(5), 20_000_000, 510_000_000),
                new IndexConstituentDefinition(new InstrumentId(6), 30_000_000, 2_850_000_000),
                new IndexConstituentDefinition(new InstrumentId(7), 25_000_000, 4_640_000_000),
                new IndexConstituentDefinition(new InstrumentId(8), 25_000_000, 1_635_000_000)
            ])
        {
            ReferenceDataVersion = DemoReferenceDataVersion,
            EffectiveFromNanos = DemoEffectiveFromNanos,
            Methodology = IndexMethodology.WeightedPrice
        };

        return new ReferenceDataSnapshot(
            DemoReferenceDataVersion,
            DemoEffectiveFromNanos,
            instruments,
            [index, largeCap, smallCap],
            []);
    }
}

public sealed record DemoCatalog(
    IReadOnlyList<InstrumentDefinition> Instruments,
    IReadOnlyList<IndexDefinition> Indexes)
{
    public long ReferenceDataVersion { get; init; } = 1;
    public long EffectiveFromNanos { get; init; }
    public IReadOnlyList<FxRateDefinition> FxRates { get; init; } = [];
}