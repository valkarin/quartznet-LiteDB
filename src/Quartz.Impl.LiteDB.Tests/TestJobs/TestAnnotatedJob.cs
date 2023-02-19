using System.Threading.Tasks;

namespace Quartz.Impl.LiteDB.Tests.TestJobs
{
    [DisallowConcurrentExecution]
    [PersistJobDataAfterExecution]
    public class TestAnnotatedJob : IJob
    {
        public Task Execute(IJobExecutionContext context)
        {
            return Task.CompletedTask;
        }
    }
}