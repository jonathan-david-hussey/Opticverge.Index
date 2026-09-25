using System.Runtime;

namespace Opticverge.Index.Engine;

public static class RuntimeTuning
{
    public static GcLatencyModeScope EnterSustainedLowLatencyGc()
    {
        return new GcLatencyModeScope(GCLatencyMode.SustainedLowLatency);
    }

    public static NoGcRegionScope TryEnterNoGcRegion(long totalSizeBytes, long lohSizeBytes = 0)
    {
        var started = lohSizeBytes > 0
            ? GC.TryStartNoGCRegion(totalSizeBytes, lohSizeBytes, true)
            : GC.TryStartNoGCRegion(totalSizeBytes, true);

        return new NoGcRegionScope(started);
    }
}

public readonly struct GcLatencyModeScope : IDisposable
{
    private readonly GCLatencyMode _previous;

    public GcLatencyModeScope(GCLatencyMode latencyMode)
    {
        _previous = GCSettings.LatencyMode;
        GCSettings.LatencyMode = latencyMode;
    }

    public void Dispose()
    {
        GCSettings.LatencyMode = _previous;
    }
}

public readonly struct NoGcRegionScope : IDisposable
{
    public bool Started { get; }

    public NoGcRegionScope(bool started)
    {
        Started = started;
    }

    public void Dispose()
    {
        if (!Started || GCSettings.LatencyMode != GCLatencyMode.NoGCRegion) return;

        GC.EndNoGCRegion();
    }
}