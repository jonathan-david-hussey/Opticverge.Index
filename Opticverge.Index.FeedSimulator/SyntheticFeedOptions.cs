namespace Opticverge.Index.FeedSimulator;

public sealed class SyntheticFeedOptions
{
    public int Instruments { get; init; } = 8;
    public int Exchanges { get; init; } = 4;
    public int Partitions { get; init; } = 4;
    public int MessagesPerSecond { get; init; } = 100_000;
    public int BurstSize { get; init; } = 1_024;
    public long Seed { get; init; } = 42;
}