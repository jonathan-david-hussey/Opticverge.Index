namespace Opticverge.Index.Engine;

// Tracks the lifecycle of calculator snapshots (checkpoints) for observability.
// Corresponds to the "State" dimension of the observability checklist (slide 21):
//   checkpoint age, restore duration, state size per partition.
public sealed class CheckpointTracker
{
    private long _lastRestoredNanos;
    private long _lastWrittenNanos;
    private long _restoreDurationNanos;
    private long _stateSizeBytes;

    public long LastWrittenNanos => Volatile.Read(ref _lastWrittenNanos);
    public long LastRestoredNanos => Volatile.Read(ref _lastRestoredNanos);
    public long RestoreDurationNanos => Volatile.Read(ref _restoreDurationNanos);
    public long StateSizeBytes => Volatile.Read(ref _stateSizeBytes);

    // Wall-clock age of the most recent checkpoint in nanoseconds.
    // Returns long.MaxValue if no checkpoint has ever been written.
    public long AgeNanos
    {
        get
        {
            var last = LastWrittenNanos;
            return last == 0 ? long.MaxValue : MonotonicClock.TimestampNanos() - last;
        }
    }

    // Called when GetSnapshots() writes a checkpoint to the snapshot topic.
    public void MarkWritten(long approximateStateSizeBytes = 0)
    {
        Volatile.Write(ref _lastWrittenNanos, MonotonicClock.TimestampNanos());
        if (approximateStateSizeBytes > 0)
            Volatile.Write(ref _stateSizeBytes, approximateStateSizeBytes);
    }

    // Called when RestoreFromSnapshots() finishes loading a saved checkpoint.
    public void MarkRestored(long durationNanos)
    {
        Volatile.Write(ref _lastRestoredNanos, MonotonicClock.TimestampNanos());
        Volatile.Write(ref _restoreDurationNanos, durationNanos);
    }
}