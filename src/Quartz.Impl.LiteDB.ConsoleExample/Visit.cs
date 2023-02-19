using System;
using System.Threading.Tasks;

namespace Quartz.Impl.LiteDB.ConsoleExample
{
    [PersistJobDataAfterExecution]
    public class Visit : IJob
    {
        Task IJob.Execute(IJobExecutionContext context)
        {
            Console.WriteLine("Visiting the office, once :)");
            return Task.CompletedTask;
        }
    }
}