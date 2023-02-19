using System.Threading.Tasks;

namespace Quartz.Impl.LiteDB.Tests.TestJobs
{
    internal class NoOpJob : IJob
    {
        public Task Execute(IJobExecutionContext context)
        {
            // NoOp
            return Task.CompletedTask;
        }
    }
}