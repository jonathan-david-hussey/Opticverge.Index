using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using Opticverge.Index.Contracts;

namespace Opticverge.Index.Engine;

public sealed class SpscMarketTickRingBuffer
{
    private readonly MarketTick[] _buffer;
    private readonly int _mask;
    private PaddedLong _readSequence;
    private PaddedLong _writeSequence;

    public SpscMarketTickRingBuffer(int capacity)
    {
        if (capacity < 2 || (capacity & (capacity - 1)) != 0) throw new ArgumentOutOfRangeException(nameof(capacity), "Capacity must be a power of two.");

        _buffer = new MarketTick[capacity];
        _mask = capacity - 1;
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public bool TryWrite(in MarketTick tick)
    {
        var writeSequence = _writeSequence.Value;
        var wrapPoint = writeSequence - _buffer.Length;
        var readSequence = Volatile.Read(ref _readSequence.Value);
        if (wrapPoint >= readSequence) return false;

        _buffer[(int)writeSequence & _mask] = tick;
        Volatile.Write(ref _writeSequence.Value, writeSequence + 1);
        return true;
    }

    public int TryWriteBatch(ReadOnlySpan<MarketTick> ticks)
    {
        var writeSequence = _writeSequence.Value;
        var readSequence = Volatile.Read(ref _readSequence.Value);
        var available = _buffer.Length - (int)(writeSequence - readSequence);
        var count = Math.Min(available, ticks.Length);

        for (var i = 0; i < count; i++) _buffer[(int)(writeSequence + i) & _mask] = ticks[i];

        Volatile.Write(ref _writeSequence.Value, writeSequence + count);
        return count;
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public bool TryRead(out MarketTick tick)
    {
        var readSequence = _readSequence.Value;
        var writeSequence = Volatile.Read(ref _writeSequence.Value);
        if (readSequence >= writeSequence)
        {
            tick = default;
            return false;
        }

        tick = _buffer[(int)readSequence & _mask];
        Volatile.Write(ref _readSequence.Value, readSequence + 1);
        return true;
    }

    public int DrainTo(WeightedIndexCalculator calculator, int maxMessages)
    {
        return DrainTo(calculator, maxMessages, false);
    }

    public int DrainTo(WeightedIndexCalculator calculator, int maxMessages, bool assumePrevalidatedTicks)
    {
        var readSequence = _readSequence.Value;
        var writeSequence = Volatile.Read(ref _writeSequence.Value);
        var available = (int)Math.Min(maxMessages, writeSequence - readSequence);

        for (var i = 0; i < available; i++)
        {
            ref readonly var tick = ref _buffer[(int)(readSequence + i) & _mask];
            if (assumePrevalidatedTicks)
                calculator.ApplyPrevalidated(in tick);
            else
                calculator.Apply(in tick);
        }

        Volatile.Write(ref _readSequence.Value, readSequence + available);
        return available;
    }
}

[StructLayout(LayoutKind.Explicit, Size = 128)]
public struct PaddedLong
{
    [FieldOffset(64)] public long Value;
}