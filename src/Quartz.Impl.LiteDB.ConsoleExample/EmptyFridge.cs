using System;
using System.Threading.Tasks;

namespace Quartz.Impl.LiteDB.ConsoleExample
{
    [PersistJobDataAfterExecution]
    public class EmptyFridge : IJob
    {
        Task IJob.Execute(IJobExecutionContext context)
        {
            Console.WriteLine("Emptying the fridge...");
            return Task.CompletedTask;
        }
    }
}