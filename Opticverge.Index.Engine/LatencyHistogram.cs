namespace Opticverge.Index.Engine;

public sealed class LatencyHistogram
{
    private readonly long[] _samples;
    private readonly long[] _sortBuffer;
    private int _count;
    private int _next;

    public LatencyHistogram(int sampleCount = 16_384)
    {
        _samples = new long[sampleCount];
        _sortBuffer = new long[sampleCount];
    }

    public void Record(long nanos)
    {
        _samples[_next] = nanos < 0 ? 0 : nanos;
        _next++;

        if (_next == _samples.Length) _next = 0;

        if (_count < _samples.Length) _count++;
    }

    public LatencySnapshot Snapshot()
    {
        if (_count == 0) return default;

        Array.Copy(_samples, _sortBuffer, _count);
        Array.Sort(_sortBuffer, 0, _count);

        return new LatencySnapshot(
            Percentile(_sortBuffer, _count, 0.50),
            Percentile(_sortBuffer, _count, 0.95),
            Percentile(_sortBuffer, _count, 0.99),
            Percentile(_sortBuffer, _count, 0.999));
    }

    private static long Percentile(long[] sorted, int count, double percentile)
    {
        var index = (int)Math.Ceiling(percentile * count) - 1;
        return sorted[Math.Clamp(index, 0, count - 1)];
    }
}

public readonly record struct LatencySnapshot(
    long P50Nanos,
    long P95Nanos,
    long P99Nanos,
    long P999Nanos);