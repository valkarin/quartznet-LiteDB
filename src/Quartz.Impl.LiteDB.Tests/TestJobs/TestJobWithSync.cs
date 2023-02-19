using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using Xunit;

namespace Quartz.Impl.LiteDB.Tests.TestJobs
{
    public class TestJobWithSync : IJob
    {
        public Task  Execute(IJobExecutionContext context)
        {
            try
            {
                var jobExecTimestamps = (List<DateTime>) context.Scheduler.Context.Get(TestConsts.DateStamps);
                var barrier = (Barrier) context.Scheduler.Context.Get(TestConsts.Barrier);

                jobExecTimestamps.Add(DateTime.UtcNow);

                barrier.SignalAndWait(TestConsts.TestTimeout);
            }
            catch (Exception e)
            {
                Assert.Fail("Await on barrier was interrupted: " + e);
            }

            return Task.CompletedTask;
        }
    }
}