using System.Runtime.CompilerServices;
using Opticverge.Index.Contracts;

namespace Opticverge.Index.Engine;

public static class PartitionRouter
{
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static int ForInstrument(InstrumentId instrumentId, int partitionCount)
    {
        if (partitionCount <= 1) return 0;

        var hash = (uint)instrumentId.Value * 2_654_435_761u;
        return IsPowerOfTwo(partitionCount)
            ? (int)(hash & (uint)(partitionCount - 1))
            : (int)(hash % (uint)partitionCount);
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static bool IsPowerOfTwo(int value)
    {
        return (value & (value - 1)) == 0;
    }
}