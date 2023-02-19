using System;
using System.Collections.Specialized;
using System.Diagnostics;
using System.Threading.Tasks;
using FluentAssertions;
using Quartz.Impl.LiteDB.Tests.TestJobs;
using Xunit;

namespace Quartz.Impl.LiteDB.Tests
{
    public class LiteDbJobStoreIssueNo5 : IClassFixture<TestCleanUp>
    {
        private readonly JobListener _listener;

        private readonly ITrigger _trigger;

        private readonly IJobDetail _job;
        
        private readonly string _filename;

        public LiteDbJobStoreIssueNo5()
        {
            _filename = TestConsts.GenerateFilename;
            _trigger = TriggerBuilder.Create()
                .WithIdentity("trigger", "g1")
                .StartAt(DateTime.Now - TimeSpan.FromHours(1))
                .WithSimpleSchedule(s => s.WithMisfireHandlingInstructionFireNow())
                .Build();
            _job = JobBuilder.Create<TestJob>()
                .WithIdentity("job", "g1")
                .Build();
            _listener = new JobListener();
        }

        private async Task ScheduleTestJobAndWaitForExecution(IScheduler scheduler)
        {
            scheduler.ListenerManager.AddJobListener(_listener);
            await scheduler.Start();
            await scheduler.ScheduleJob(_job, _trigger);
            var sp = Stopwatch.StartNew();
            while (!await scheduler.CheckExists(_job.Key) || _listener.WasExecuted == false)
            {
                if (sp.ElapsedMilliseconds > 5000)
                    break;
                await Task.Delay(100);
            }
            await scheduler.Shutdown(true);
        }
        
        [Fact]
        public async Task InMemory()
        {
            var scheduler = await StdSchedulerFactory.GetDefaultScheduler();

            await ScheduleTestJobAndWaitForExecution(scheduler);

            _listener.WasExecuted.Should().BeTrue();
        }
        
        [Fact]
        public async Task InLiteDb()
        {
            var properties = new NameValueCollection
            {
                // Normal scheduler properties
                ["quartz.scheduler.instanceName"] = "TestScheduler",
                ["quartz.scheduler.instanceId"] = "instance_one",
                ["quartz.serializer.type"] = "binary",
                // LiteDB JobStore property
                ["quartz.jobStore.type"] = "Quartz.Impl.LiteDB.LiteDbJobStore, Quartz.Impl.LiteDB",
                ["quartz.jobStore.connectionString"] = $"Filename={_filename};",
            };

            ISchedulerFactory sf = new StdSchedulerFactory(properties);
            var scheduler = await sf.GetScheduler();

            await ScheduleTestJobAndWaitForExecution(scheduler);

            _listener.WasExecuted.Should().BeTrue();
        }
    }
}