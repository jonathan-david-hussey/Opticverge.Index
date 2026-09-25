using System.Runtime.CompilerServices;
using Opticverge.Index.Contracts;

namespace Opticverge.Index.Engine;

public struct TickRingEvent
{
    public long Sequence;
    public int InstrumentId;
    public short ExchangeId;
    public long PriceE8;
    public int Quantity;
    public long ExchangeTimestampNanos;
    public long ReceiveTimestampNanos;
    public int Partition;
    public TickFlags Flags;

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public void CopyFrom(in MarketTick tick)
    {
        Sequence = tick.Sequence;
        InstrumentId = tick.InstrumentId.Value;
        ExchangeId = tick.ExchangeId.Value;
        PriceE8 = tick.PriceE8;
        Quantity = tick.Quantity;
        ExchangeTimestampNanos = tick.ExchangeTimestampNanos;
        ReceiveTimestampNanos = tick.ReceiveTimestampNanos;
        Partition = tick.Partition;
        Flags = tick.Flags;
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public readonly MarketTick ToMarketTick()
    {
        return new MarketTick(
            Sequence,
            new InstrumentId(InstrumentId),
            new ExchangeId(ExchangeId),
            PriceE8,
            Quantity,
            ExchangeTimestampNanos,
            ReceiveTimestampNanos,
            Partition,
            Flags);
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public void Clear()
    {
        this = default;
    }
}