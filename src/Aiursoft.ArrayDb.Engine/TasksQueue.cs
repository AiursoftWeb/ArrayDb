using System.Collections.Concurrent;

namespace Aiursoft.ArrayDb.Engine;

/// <summary>
/// Implements a task queue that can be used to add tasks to a queue and execute them one by one.
/// </summary>
public class TasksQueue
{
    private readonly ConcurrentQueue<Action> _pendingTaskFactories = new();
    private readonly object _loc = new();

    public Task Engine { get; private set; } = Task.CompletedTask;
    
    public int PendingTasksCount => _pendingTaskFactories.Count;
    
    /// <summary>
    /// Adds a new task to the queue.
    /// </summary>
    /// <param name="taskFactory">A factory method that creates the task to be added to the queue.</param>
    public void QueueNew(Action taskFactory)
    {
        _pendingTaskFactories.Enqueue(taskFactory);

        lock (_loc)
        {
            if (!Engine.IsCompleted)
            {
                return;
            }

            Engine = Task.Run(RunTasks);
        }
    }

    /// <summary>
    /// Executes the tasks in the queue one by one.
    ///
    /// Never call this method directly. Because this class is used to protect the methods to be executed in a single thread.
    /// </summary>
    /// <returns>A task that represents the completion of all the tasks in the queue.</returns>
    private void RunTasks()
    {
        while (_pendingTaskFactories.TryDequeue(out var taskFactory))
        {
            taskFactory();
        }
    }
}