using System.Buffers.Binary;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using System.Text.Json.Serialization;

namespace Opticverge.Index.Contracts;

public static class MarketTopics
{
    public const string RawTicks = "ticks.raw";
    public const string NormalizedTicks = "ticks.normalized";
    public const string IndexSnapshots = "index.snapshots";
    public const string IndexDeltas = "index.deltas";
    public const string IndexAudit = "index.audit";
    public const string EngineMetrics = "engine.metrics";
    public const string CorporateActions = "corporate.actions";

    // Dead-letter queue for events that cannot be processed (malformed, stale, unknown instrument).
    // Depth of this topic is a key data-quality signal from slide 21 of the observability checklist.
    public const string DeadLetterQueue = "dlq.events";

    public static readonly string[] All =
    [
        RawTicks,
        NormalizedTicks,
        IndexSnapshots,
        IndexDeltas,
        IndexAudit,
        EngineMetrics,
        CorporateActions,
        DeadLetterQueue
    ];
}

public static class PipelineStage
{
    public const string FeedIngress = "feed.ingress";
    public const string DurableRawStream = "durable.raw.stream";
    public const string Normalization = "normalization";
    public const string InstrumentState = "instrument.state";
    public const string DependencyRouting = "dependency.routing";
    public const string CalculationStream = "calculation.stream";
    public const string StatefulCalculator = "stateful.calculator";
    public const string PublicationLog = "publication.log";
    public const string Distribution = "distribution";
    public const string AuditReplay = "audit.replay";
}

public enum CorporateActionType
{
    WeightChange,
    ConstituentRemoval,
    ConstituentAddition
}

public enum IndexMethodology
{
    WeightedPrice,
    FloatAdjustedMarketCap
}

public sealed class CalculatorSnapshot
{
    public int IndexId { get; set; }
    public long Sequence { get; set; }
    public long ValueE8 { get; set; }
    public double Divisor { get; set; }
    public int[] InstrumentIds { get; set; } = [];
    public long[] LatestPricesE8 { get; set; } = [];
    public long[] WeightsE8 { get; set; } = [];
    public long[] LastTimestampNanos { get; set; } = [];
    public long[] LastSequences { get; set; } = [];
    public long TimestampNanos { get; set; }
    public long[] PartitionOffsets { get; set; } = [];
}

public readonly record struct CorporateAction(
    int IndexId,
    int InstrumentId,
    CorporateActionType Type,
    long NewWeightE8);

public readonly record struct InstrumentId(int Value)
{
    public override string ToString()
    {
        return Value.ToString();
    }
}

public readonly record struct ExchangeId(short Value)
{
    public override string ToString()
    {
        return Value.ToString();
    }
}

public readonly record struct IndexId(short Value)
{
    public override string ToString()
    {
        return Value.ToString();
    }
}

[Flags]
public enum TickFlags : byte
{
    None = 0,
    Trade = 1,
    Bid = 2,
    Ask = 4,
    Synthetic = 8,
    Stale = 16,
    Corrected = 32
}

public readonly record struct MarketTick(
    long Sequence,
    InstrumentId InstrumentId,
    ExchangeId ExchangeId,
    long PriceE8,
    int Quantity,
    long ExchangeTimestampNanos,
    long ReceiveTimestampNanos,
    int Partition,
    TickFlags Flags);

public readonly record struct IndexValue(
    IndexId IndexId,
    long Sequence,
    long ValueE8,
    long LevelE8,
    long TimestampNanos,
    int ConstituentCount,
    int StaleConstituentCount);

public sealed record InstrumentDefinition(
    InstrumentId InstrumentId,
    string Symbol,
    ExchangeId PrimaryExchangeId,
    string Currency)
{
    public string VenueMic { get; init; } = "";
    public bool IsActive { get; init; } = true;
}

public sealed record IndexConstituentDefinition(
    InstrumentId InstrumentId,
    long WeightE8,
    long InitialPriceE8,
    bool CorporateActionPlaceholder = false)
{
    private const long PriceScale = 100_000_000L;
    public long Shares { get; init; }
    public long FreeFloatFactorE8 { get; init; } = PriceScale;
    public long FxRateE8 { get; init; } = PriceScale;
}

public sealed record IndexDefinition(
    IndexId IndexId,
    string Code,
    string Currency,
    IReadOnlyList<IndexConstituentDefinition> Constituents)
{
    public long ReferenceDataVersion { get; init; } = 1;
    public long EffectiveFromNanos { get; init; }
    public IndexMethodology Methodology { get; init; } = IndexMethodology.WeightedPrice;
}

public sealed record FxRateDefinition(
    string FromCurrency,
    string ToCurrency,
    long RateE8,
    long TimestampNanos);

public sealed record ReferenceDataSnapshot(
    long Version,
    long EffectiveFromNanos,
    IReadOnlyList<InstrumentDefinition> Instruments,
    IReadOnlyList<IndexDefinition> Indexes,
    IReadOnlyList<FxRateDefinition> FxRates);

public readonly record struct EngineMetricsDto(
    long MessagesIn,
    long MessagesOut,
    long DroppedMessages,
    long StaleTicks,
    double MessagesPerSecond,
    long LastLatencyNanos,
    long P50LatencyNanos,
    long P95LatencyNanos,
    long P99LatencyNanos,
    long P999LatencyNanos,
    int Gen0Collections,
    int Gen1Collections,
    int Gen2Collections,
    long DuplicateEvents,
    long SequenceGaps,
    long CalculationFailures,
    long RingBufferDepth,
    long ConsumerLag,
    long EndToEndP50Nanos,
    long EndToEndP95Nanos,
    long EndToEndP99Nanos,
    long CalcDurationP99Nanos,
    long PublicationDurationP99Nanos,
    // Observability additions (slide 21): DLQ depth and fan-out factor distribution.
    long DlqEvents,
    long FanOutFactorP50,
    long FanOutFactorP99);

public sealed record DashboardStateDto(
    DateTimeOffset Timestamp,
    IReadOnlyList<IndexValue> Indexes,
    EngineMetricsDto Metrics,
    IReadOnlyList<string> Topics,
    IReadOnlyList<string> OptimizationNotes);

[JsonSerializable(typeof(MarketTick))]
[JsonSerializable(typeof(IndexValue))]
[JsonSerializable(typeof(InstrumentDefinition))]
[JsonSerializable(typeof(IndexConstituentDefinition))]
[JsonSerializable(typeof(IndexDefinition))]
[JsonSerializable(typeof(FxRateDefinition))]
[JsonSerializable(typeof(ReferenceDataSnapshot))]
[JsonSerializable(typeof(EngineMetricsDto))]
[JsonSerializable(typeof(DashboardStateDto))]
[JsonSerializable(typeof(CorporateAction))]
[JsonSerializable(typeof(CalculatorSnapshot))]
[JsonSerializable(typeof(string[]))]
[JsonSourceGenerationOptions(PropertyNamingPolicy = JsonKnownNamingPolicy.CamelCase)]
public sealed partial class IndexJsonContext : JsonSerializerContext;

public static class TickBinaryCodec
{
    public const int Size = 47;

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static bool TryWrite(in MarketTick tick, Span<byte> destination)
    {
        if (destination.Length < Size) return false;

        if (BitConverter.IsLittleEndian)
        {
            ref var target = ref MemoryMarshal.GetReference(destination);
            Unsafe.WriteUnaligned(ref target, tick.Sequence);
            Unsafe.WriteUnaligned(ref Unsafe.Add(ref target, 8), tick.InstrumentId.Value);
            Unsafe.WriteUnaligned(ref Unsafe.Add(ref target, 12), tick.ExchangeId.Value);
            Unsafe.WriteUnaligned(ref Unsafe.Add(ref target, 14), tick.PriceE8);
            Unsafe.WriteUnaligned(ref Unsafe.Add(ref target, 22), tick.Quantity);
            Unsafe.WriteUnaligned(ref Unsafe.Add(ref target, 26), tick.ExchangeTimestampNanos);
            Unsafe.WriteUnaligned(ref Unsafe.Add(ref target, 34), tick.ReceiveTimestampNanos);
            Unsafe.WriteUnaligned(ref Unsafe.Add(ref target, 42), tick.Partition);
            Unsafe.Add(ref target, 46) = (byte)tick.Flags;
            return true;
        }

        BinaryPrimitives.WriteInt64LittleEndian(destination[..8], tick.Sequence);
        BinaryPrimitives.WriteInt32LittleEndian(destination.Slice(8, 4), tick.InstrumentId.Value);
        BinaryPrimitives.WriteInt16LittleEndian(destination.Slice(12, 2), tick.ExchangeId.Value);
        BinaryPrimitives.WriteInt64LittleEndian(destination.Slice(14, 8), tick.PriceE8);
        BinaryPrimitives.WriteInt32LittleEndian(destination.Slice(22, 4), tick.Quantity);
        BinaryPrimitives.WriteInt64LittleEndian(destination.Slice(26, 8), tick.ExchangeTimestampNanos);
        BinaryPrimitives.WriteInt64LittleEndian(destination.Slice(34, 8), tick.ReceiveTimestampNanos);
        BinaryPrimitives.WriteInt32LittleEndian(destination.Slice(42, 4), tick.Partition);
        destination[46] = (byte)tick.Flags;
        return true;
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static bool TryRead(ReadOnlySpan<byte> source, out MarketTick tick)
    {
        if (source.Length < Size)
        {
            tick = default;
            return false;
        }

        if (BitConverter.IsLittleEndian)
        {
            ref var sourceRef = ref Unsafe.AsRef(in MemoryMarshal.GetReference(source));
            tick = new MarketTick(
                Unsafe.ReadUnaligned<long>(ref sourceRef),
                new InstrumentId(Unsafe.ReadUnaligned<int>(ref Unsafe.Add(ref sourceRef, 8))),
                new ExchangeId(Unsafe.ReadUnaligned<short>(ref Unsafe.Add(ref sourceRef, 12))),
                Unsafe.ReadUnaligned<long>(ref Unsafe.Add(ref sourceRef, 14)),
                Unsafe.ReadUnaligned<int>(ref Unsafe.Add(ref sourceRef, 22)),
                Unsafe.ReadUnaligned<long>(ref Unsafe.Add(ref sourceRef, 26)),
                Unsafe.ReadUnaligned<long>(ref Unsafe.Add(ref sourceRef, 34)),
                Unsafe.ReadUnaligned<int>(ref Unsafe.Add(ref sourceRef, 42)),
                (TickFlags)Unsafe.Add(ref sourceRef, 46));

            return true;
        }

        tick = new MarketTick(
            BinaryPrimitives.ReadInt64LittleEndian(source[..8]),
            new InstrumentId(BinaryPrimitives.ReadInt32LittleEndian(source.Slice(8, 4))),
            new ExchangeId(BinaryPrimitives.ReadInt16LittleEndian(source.Slice(12, 2))),
            BinaryPrimitives.ReadInt64LittleEndian(source.Slice(14, 8)),
            BinaryPrimitives.ReadInt32LittleEndian(source.Slice(22, 4)),
            BinaryPrimitives.ReadInt64LittleEndian(source.Slice(26, 8)),
            BinaryPrimitives.ReadInt64LittleEndian(source.Slice(34, 8)),
            BinaryPrimitives.ReadInt32LittleEndian(source.Slice(42, 4)),
            (TickFlags)source[46]);

        return true;
    }
}

public static class IndexValueBinaryCodec
{
    public const int Size = 42;

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static bool TryWrite(in IndexValue value, Span<byte> destination)
    {
        if (destination.Length < Size) return false;

        if (BitConverter.IsLittleEndian)
        {
            ref var target = ref MemoryMarshal.GetReference(destination);
            Unsafe.WriteUnaligned(ref target, value.IndexId.Value);
            Unsafe.WriteUnaligned(ref Unsafe.Add(ref target, 2), value.Sequence);
            Unsafe.WriteUnaligned(ref Unsafe.Add(ref target, 10), value.ValueE8);
            Unsafe.WriteUnaligned(ref Unsafe.Add(ref target, 18), value.LevelE8);
            Unsafe.WriteUnaligned(ref Unsafe.Add(ref target, 26), value.TimestampNanos);
            Unsafe.WriteUnaligned(ref Unsafe.Add(ref target, 34), value.ConstituentCount);
            Unsafe.WriteUnaligned(ref Unsafe.Add(ref target, 38), value.StaleConstituentCount);
            return true;
        }

        BinaryPrimitives.WriteInt16LittleEndian(destination[..2], value.IndexId.Value);
        BinaryPrimitives.WriteInt64LittleEndian(destination.Slice(2, 8), value.Sequence);
        BinaryPrimitives.WriteInt64LittleEndian(destination.Slice(10, 8), value.ValueE8);
        BinaryPrimitives.WriteInt64LittleEndian(destination.Slice(18, 8), value.LevelE8);
        BinaryPrimitives.WriteInt64LittleEndian(destination.Slice(26, 8), value.TimestampNanos);
        BinaryPrimitives.WriteInt32LittleEndian(destination.Slice(34, 4), value.ConstituentCount);
        BinaryPrimitives.WriteInt32LittleEndian(destination.Slice(38, 4), value.StaleConstituentCount);
        return true;
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static bool TryRead(ReadOnlySpan<byte> source, out IndexValue value)
    {
        if (source.Length < Size)
        {
            value = default;
            return false;
        }

        if (BitConverter.IsLittleEndian)
        {
            ref var sourceRef = ref Unsafe.AsRef(in MemoryMarshal.GetReference(source));
            value = new IndexValue(
                new IndexId(Unsafe.ReadUnaligned<short>(ref sourceRef)),
                Unsafe.ReadUnaligned<long>(ref Unsafe.Add(ref sourceRef, 2)),
                Unsafe.ReadUnaligned<long>(ref Unsafe.Add(ref sourceRef, 10)),
                Unsafe.ReadUnaligned<long>(ref Unsafe.Add(ref sourceRef, 18)),
                Unsafe.ReadUnaligned<long>(ref Unsafe.Add(ref sourceRef, 26)),
                Unsafe.ReadUnaligned<int>(ref Unsafe.Add(ref sourceRef, 34)),
                Unsafe.ReadUnaligned<int>(ref Unsafe.Add(ref sourceRef, 38)));
            return true;
        }

        value = new IndexValue(
            new IndexId(BinaryPrimitives.ReadInt16LittleEndian(source[..2])),
            BinaryPrimitives.ReadInt64LittleEndian(source.Slice(2, 8)),
            BinaryPrimitives.ReadInt64LittleEndian(source.Slice(10, 8)),
            BinaryPrimitives.ReadInt64LittleEndian(source.Slice(18, 8)),
            BinaryPrimitives.ReadInt64LittleEndian(source.Slice(26, 8)),
            BinaryPrimitives.ReadInt32LittleEndian(source.Slice(34, 4)),
            BinaryPrimitives.ReadInt32LittleEndian(source.Slice(38, 4)));
        return true;
    }
}