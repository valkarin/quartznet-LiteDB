using System;
using System.Threading.Tasks;

namespace Quartz.Impl.LiteDB.ConsoleExample
{
    [PersistJobDataAfterExecution]
    public class CheckAlive : IJob
    {
        Task IJob.Execute(IJobExecutionContext context)
        {
            Console.WriteLine("Verifying site is up...");
            return Task.CompletedTask;
        }
    }
}