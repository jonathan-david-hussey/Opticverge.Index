namespace Opticverge.Index.Engine;

public enum SequenceGapPolicy
{
    BufferUntilRecovered,
    DropUntilRecovered,
    ProcessAndReport
}

public sealed class EngineOptions
{
    public int PartitionCount { get; init; } = 4;
    public int RingBufferSize { get; init; } = 1 << 16;
    public string WaitStrategy { get; init; } = "yielding";
    public bool UseBatchHandler { get; init; } = true;
    public bool AssumePrevalidatedTicks { get; init; }
    public bool ClearEventSlots { get; init; }
    public int LatencySampleRate { get; init; } = 1_024;
    public SequenceGapPolicy SequenceGapPolicy { get; init; } = SequenceGapPolicy.BufferUntilRecovered;
    public int SequenceGapBufferSize { get; init; } = 8;
    public ITimestampSource TimestampSource { get; init; } = StopwatchTimestampSource.Instance;
    public bool UseDedicatedConsumerThread { get; init; }
    public ThreadPriority DedicatedConsumerThreadPriority { get; init; } = ThreadPriority.Normal;
    public long DedicatedConsumerThreadAffinityMask { get; init; }
    public int DedicatedConsumerThreadIdealProcessor { get; init; } = -1;
}