using System;
using System.Threading.Tasks;

namespace Quartz.Impl.LiteDB.ConsoleExample
{
    [PersistJobDataAfterExecution]
    public class TurnOffLights : IJob
    {
        Task IJob.Execute(IJobExecutionContext context)
        {
            Console.WriteLine("Turning lights off...");
            return Task.CompletedTask;
        }
    }
}