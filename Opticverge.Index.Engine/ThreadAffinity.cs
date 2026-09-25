using System.Runtime.InteropServices;

namespace Opticverge.Index.Engine;

public static class ThreadAffinity
{
    public static bool TryApplyCurrentThread(nint processorAffinityMask, int idealProcessor)
    {
        if (!OperatingSystem.IsWindows()) return false;

        var applied = false;
        var currentThread = GetCurrentThread();

        if (processorAffinityMask != 0)
        {
            var previous = SetThreadAffinityMask(currentThread, (nuint)processorAffinityMask);
            if (previous == 0) return false;

            applied = true;
        }

        if (idealProcessor >= 0)
        {
            var previous = SetThreadIdealProcessor(currentThread, (uint)idealProcessor);
            if (previous == uint.MaxValue) return false;

            applied = true;
        }

        return applied;
    }

    [DllImport("kernel32.dll")]
    private static extern nint GetCurrentThread();

    [DllImport("kernel32.dll", SetLastError = true)]
    private static extern nuint SetThreadAffinityMask(nint hThread, nuint dwThreadAffinityMask);

    [DllImport("kernel32.dll", SetLastError = true)]
    private static extern uint SetThreadIdealProcessor(nint hThread, uint dwIdealProcessor);
}