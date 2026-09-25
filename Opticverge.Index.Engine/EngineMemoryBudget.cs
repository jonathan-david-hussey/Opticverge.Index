using System.Runtime.CompilerServices;
using Opticverge.Index.Contracts;

namespace Opticverge.Index.Engine;

public readonly record struct EngineMemoryBudget(
    int Shards,
    int RingBufferSize,
    int TickPoolSize,
    int BatchSize,
    long MarketTickPoolBytes,
    long BatchBytes,
    long BinaryBatchBufferBytes,
    long DisruptorSlotPayloadBytes,
    long SpscRingBytes)
{
    public long TotalHotPathBytes =>
        MarketTickPoolBytes + BatchBytes + BinaryBatchBufferBytes + DisruptorSlotPayloadBytes + SpscRingBytes;
}

public static class EngineMemoryBudgetEstimator
{
    private const int ApproximateTickRingEventPayloadBytes = 64;

    public static EngineMemoryBudget Estimate(
        int shards,
        int ringBufferSize,
        int tickPoolSize,
        int batchSize,
        bool includeSpscComparisonBuffer = false)
    {
        ArgumentOutOfRangeException.ThrowIfNegativeOrZero(shards);
        ArgumentOutOfRangeException.ThrowIfNegativeOrZero(ringBufferSize);
        ArgumentOutOfRangeException.ThrowIfNegativeOrZero(tickPoolSize);
        ArgumentOutOfRangeException.ThrowIfNegativeOrZero(batchSize);

        var marketTickBytes = Unsafe.SizeOf<MarketTick>();
        var marketTickPoolBytes = (long)tickPoolSize * marketTickBytes;
        var batchBytes = (long)shards * batchSize * marketTickBytes;
        var binaryBatchBufferBytes = (long)shards * batchSize * TickBinaryCodec.Size;
        var disruptorSlotPayloadBytes = (long)shards * ringBufferSize * ApproximateTickRingEventPayloadBytes;
        var spscRingBytes = includeSpscComparisonBuffer
            ? (long)shards * ringBufferSize * marketTickBytes
            : 0;

        return new EngineMemoryBudget(
            shards,
            ringBufferSize,
            tickPoolSize,
            batchSize,
            marketTickPoolBytes,
            batchBytes,
            binaryBatchBufferBytes,
            disruptorSlotPayloadBytes,
            spscRingBytes);
    }
}