using System.Diagnostics;
using System.Runtime.CompilerServices;

namespace Opticverge.Index.Engine;

public static class MonotonicClock
{
    private const long NanosPerSecond = 1_000_000_000L;

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static long TimestampNanos()
    {
        return ToTimestampNanos(Stopwatch.GetTimestamp());
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static long ToTimestampNanos(long stopwatchTimestamp)
    {
        // Compute seconds and sub-second remainder separately to avoid intermediate
        // long overflow: (timestamp * 1e9) overflows after ~15 min at 10 MHz.
        var seconds = stopwatchTimestamp / Stopwatch.Frequency;
        var remainder = stopwatchTimestamp % Stopwatch.Frequency;
        return seconds * NanosPerSecond + remainder * NanosPerSecond / Stopwatch.Frequency;
    }
}

public interface ITimestampSource
{
    long TimestampNanos();
}

public sealed class StopwatchTimestampSource : ITimestampSource
{
    public static readonly StopwatchTimestampSource Instance = new();

    private StopwatchTimestampSource()
    {
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public long TimestampNanos()
    {
        return MonotonicClock.TimestampNanos();
    }
}

public sealed class CachedTimestampSource : ITimestampSource, IDisposable
{
    private readonly Thread _thread;
    private readonly long _updateIntervalTicks;
    private int _running = 1;
    private long _timestampNanos;

    public CachedTimestampSource(TimeSpan updateInterval, string threadName = "index-clock")
    {
        if (updateInterval <= TimeSpan.Zero) throw new ArgumentOutOfRangeException(nameof(updateInterval), "Cached timestamp interval must be positive.");

        _timestampNanos = MonotonicClock.TimestampNanos();
        _updateIntervalTicks = Math.Max(1, (long)(updateInterval.TotalSeconds * Stopwatch.Frequency));
        _thread = new Thread(Run)
        {
            IsBackground = true,
            Name = threadName
        };
        _thread.Start();
    }

    public void Dispose()
    {
        if (Interlocked.Exchange(ref _running, 0) == 0) return;

        _thread.Join(TimeSpan.FromSeconds(1));
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public long TimestampNanos()
    {
        return Volatile.Read(ref _timestampNanos);
    }

    private void Run()
    {
        Thread.BeginThreadAffinity();
        try
        {
            while (Volatile.Read(ref _running) != 0)
            {
                var timestamp = Stopwatch.GetTimestamp();
                Volatile.Write(ref _timestampNanos, MonotonicClock.ToTimestampNanos(timestamp));

                var nextUpdate = timestamp + _updateIntervalTicks;
                while (Volatile.Read(ref _running) != 0 && Stopwatch.GetTimestamp() < nextUpdate) Thread.SpinWait(32);
            }
        }
        finally
        {
            Thread.EndThreadAffinity();
        }
    }
}