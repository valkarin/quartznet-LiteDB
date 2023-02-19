using System.Threading.Tasks;

namespace Quartz.Impl.LiteDB.Tests.TestJobs
{
    public class TestJob : IJob
    {
        public Task Execute(IJobExecutionContext context)
        {
            return Task.CompletedTask;
        }
    }
}