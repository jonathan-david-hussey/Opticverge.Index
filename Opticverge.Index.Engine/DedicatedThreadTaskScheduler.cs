using System.Collections.Concurrent;

namespace Opticverge.Index.Engine;

public sealed class DedicatedThreadTaskScheduler : TaskScheduler, IDisposable
{
    private readonly BlockingCollection<Task> _tasks = [];
    private readonly Thread _thread;
    private bool _disposed;

    public DedicatedThreadTaskScheduler(
        string threadName,
        ThreadPriority priority = ThreadPriority.Normal,
        nint processorAffinityMask = 0,
        int idealProcessor = -1)
    {
        _thread = new Thread(() => Run(processorAffinityMask, idealProcessor))
        {
            IsBackground = true,
            Name = threadName,
            Priority = priority
        };
        _thread.Start();
    }

    public void Dispose()
    {
        if (_disposed) return;

        _disposed = true;
        _tasks.CompleteAdding();
        _thread.Join(TimeSpan.FromSeconds(2));
        _tasks.Dispose();
    }

    protected override IEnumerable<Task>? GetScheduledTasks()
    {
        return _tasks.ToArray();
    }

    protected override void QueueTask(Task task)
    {
        ObjectDisposedException.ThrowIf(_disposed, this);
        _tasks.Add(task);
    }

    protected override bool TryExecuteTaskInline(Task task, bool taskWasPreviouslyQueued)
    {
        return false;
    }

    private void Run(nint processorAffinityMask, int idealProcessor)
    {
        Thread.BeginThreadAffinity();
        try
        {
            ThreadAffinity.TryApplyCurrentThread(processorAffinityMask, idealProcessor);

            foreach (var task in _tasks.GetConsumingEnumerable()) TryExecuteTask(task);
        }
        finally
        {
            Thread.EndThreadAffinity();
        }
    }
}