using System;
using System.Collections.Generic;
using System.Collections.Specialized;
using System.Globalization;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using FluentAssertions;
using Quartz.Impl.LiteDB.Tests.TestJobs;
using Quartz.Impl.Matchers;
using Xunit;

namespace Quartz.Impl.LiteDB.Tests
{
    public class LiteDbJobStoreIntegrationTests : IClassFixture<TestCleanUp>
    {
        private readonly string _filename;

        public LiteDbJobStoreIntegrationTests()
        {
            _filename = $"test_{DateTimeOffset.UtcNow.Ticks}.db";
        }

        private async Task<IScheduler> CreateScheduler(string name, int threadCount)
        {
            var properties = new NameValueCollection
            {
                // Setting some scheduler properties
                ["quartz.scheduler.instanceName"] = name + "Scheduler",
                ["quartz.scheduler.instanceId"] = "AUTO",
                ["quartz.threadPool.threadCount"] = threadCount.ToString(CultureInfo.InvariantCulture),
                ["quartz.threadPool.type"] = "Quartz.Simpl.SimpleThreadPool, Quartz",
                ["quartz.serializer.type"] = "binary",
                
                // Setting LiteDb as the persisted JobStore
                ["quartz.jobStore.type"] = "Quartz.Impl.LiteDB.LiteDbJobStore, Quartz.Impl.LiteDB",
                ["quartz.jobStore.connectionString"] = $"Filename={_filename};",
            };

            var stdSchedulerFactory = new StdSchedulerFactory(properties);
            return await stdSchedulerFactory.GetScheduler();
        }
        
        [Fact]
        public async Task TestBasicStorageFunctions()
        {
            var sched = await CreateScheduler("TestBasicStorageFunctions", 2);
            await sched.Start(CancellationToken.None);
            await sched.Standby(CancellationToken.None);

            // test basic storage functions of scheduler...
            var job = JobBuilder.Create()
                .OfType<TestJob>()
                .WithIdentity("j1")
                .StoreDurably()
                .Build();

            await sched.Awaiting(s => s.CheckExists(new JobKey("j1"))).Should().NotThrowAsync()
                .WithResult(false, "Unexpected existence of job named 'j1'.");

            await sched.AddJob(job, false);

            await sched.Awaiting(s => s.CheckExists(new JobKey("j1"))).Should().NotThrowAsync()
                .WithResult(true, "Expected existence of job named 'j1' but checkExists return false.");

            job = await sched.GetJobDetail(new JobKey("j1"));

            job.Should().NotBeNull("Stored job not found!");

            await sched.DeleteJob(new JobKey("j1"));

            var trigger = TriggerBuilder.Create()
                .WithIdentity("t1")
                .ForJob(job!)
                .StartNow()
                .WithSimpleSchedule(x => x.RepeatForever().WithIntervalInSeconds(5))
                .Build();

            await sched.Awaiting(s => s.CheckExists(new TriggerKey("t1"))).Should().NotThrowAsync()
                .WithResult(false, "Unexpected existence of trigger named '11'.");

            await sched.ScheduleJob(job, trigger);

            await sched.Awaiting(s => s.CheckExists(new TriggerKey("t1"))).Should().NotThrowAsync()
                .WithResult(true, "Expected existence of trigger named 't1' but checkExists return false.");

            job = await sched.GetJobDetail(new JobKey("j1"));

            job.Should().NotBeNull("Stored job not found!");

            trigger = await sched.GetTrigger(new TriggerKey("t1"));

            trigger.Should().NotBeNull("Stored trigger not found!");

            job = JobBuilder.Create()
                .OfType<TestJob>()
                .WithIdentity("j2", "g1")
                .Build();

            trigger = TriggerBuilder.Create()
                .WithIdentity("t2", "g1")
                .ForJob(job)
                .StartNow()
                .WithSimpleSchedule(x => x.RepeatForever().WithIntervalInSeconds(5))
                .Build();

            await sched.ScheduleJob(job, trigger);

            job = JobBuilder.Create()
                .OfType<TestJob>()
                .WithIdentity("j3", "g1")
                .Build();

            trigger = TriggerBuilder.Create()
                .WithIdentity("t3", "g1")
                .ForJob(job)
                .StartNow()
                .WithSimpleSchedule(x => x.RepeatForever().WithIntervalInSeconds(5))
                .Build();

            await sched.ScheduleJob(job, trigger);

            IList<string> jobGroups = (await sched.GetJobGroupNames()).ToList();
            IList<string> triggerGroups = (await sched.GetTriggerGroupNames()).ToList();

            jobGroups.Count.Should().Be(2, "Job group list size expected to be = 2 + 1 Default");

            triggerGroups.Count.Should().Be(2, "Trigger group list size expected to be = 2");

            ISet<JobKey> jobKeys = (await sched.GetJobKeys(GroupMatcher<JobKey>.GroupEquals(JobKey.DefaultGroup)))
                .ToHashSet();
            ISet<TriggerKey> triggerKeys =
                (await sched.GetTriggerKeys(GroupMatcher<TriggerKey>.GroupEquals(TriggerKey.DefaultGroup))).ToHashSet();

            jobKeys.Count.Should().Be(1, "Number of jobs expected in default group was 1 ");

            triggerKeys.Count.Should().Be(1, "Number of triggers expected in default group was 1 ");

            jobKeys = (await sched.GetJobKeys(GroupMatcher<JobKey>.GroupEquals("g1"))).ToHashSet();
            triggerKeys = (await sched.GetTriggerKeys(GroupMatcher<TriggerKey>.GroupEquals("g1"))).ToHashSet();

            jobKeys.Count.Should().Be(2, "Number of jobs expected in 'g1' group was 2 ");

            triggerKeys.Count.Should().Be(2, "Number of triggers expected in 'g1' group was 2 ");

            var s = await sched.GetTriggerState(new TriggerKey("t2", "g1"));

            s.Should().Be(TriggerState.Normal, "State of trigger t2 expected to be NORMAL ");

            await sched.PauseTrigger(new TriggerKey("t2", "g1"));
            s = await sched.GetTriggerState(new TriggerKey("t2", "g1"));

            s.Should().Be(TriggerState.Paused, "State of trigger t2 expected to be PAUSED ");

            await sched.ResumeTrigger(new TriggerKey("t2", "g1"));
            s = await sched.GetTriggerState(new TriggerKey("t2", "g1"));

            s.Should().Be(TriggerState.Normal, "State of trigger t2 expected to be NORMAL ");

            ISet<string> pausedGroups = (await sched.GetPausedTriggerGroups()).ToHashSet();

            pausedGroups.Count.Should().Be(0, "Size of paused trigger groups list expected to be 0 ");

            await sched.PauseTriggers(GroupMatcher<TriggerKey>.GroupEquals("g1"));

            // test that adding a trigger to a paused group causes the new trigger to be paused also... 
            job = JobBuilder.Create()
                .OfType<TestJob>()
                .WithIdentity("j4", "g1")
                .Build();

            trigger = TriggerBuilder.Create()
                .WithIdentity("t4", "g1")
                .ForJob(job)
                .StartNow()
                .WithSimpleSchedule(x => x.RepeatForever().WithIntervalInSeconds(5))
                .Build();

            await sched.ScheduleJob(job, trigger);

            pausedGroups = (await sched.GetPausedTriggerGroups()).ToHashSet();

            pausedGroups.Count.Should().Be(1, "Size of paused trigger groups list expected to be 1 ");

            s = await sched.GetTriggerState(new TriggerKey("t2", "g1"));

            s.Should().Be(TriggerState.Paused, "State of trigger t2 expected to be PAUSED ");

            s = await sched.GetTriggerState(new TriggerKey("t4", "g1"));

            s.Should().Be(TriggerState.Paused, "State of trigger t4 expected to be PAUSED");

            await sched.ResumeTriggers(GroupMatcher<TriggerKey>.GroupEquals("g1"));

            s = await sched.GetTriggerState(new TriggerKey("t2", "g1"));

            s.Should().Be(TriggerState.Normal, "State of trigger t2 expected to be NORMAL ");

            s = await sched.GetTriggerState(new TriggerKey("t4", "g1"));

            s.Should().Be(TriggerState.Normal, "State of trigger t2 expected to be NORMAL ");

            pausedGroups = (await sched.GetPausedTriggerGroups()).ToHashSet();

            pausedGroups.Count.Should().Be(0, "Size of paused trigger groups list expected to be 0 ");


            await sched.Awaiting(scheduler => scheduler.UnscheduleJob(new TriggerKey("foasldfksajdflk"))).Should()
                .NotThrowAsync()
                .WithResult(false,
                    "Scheduler should have returned 'false' from attempt to unschedule non-existing trigger.");

            await sched.Awaiting(scheduler => scheduler.UnscheduleJob(new TriggerKey("t3", "g1"))).Should()
                .NotThrowAsync()
                .WithResult(true,
                    "Scheduler should have returned 'true' from attempt to unschedule existing trigger. ");

            jobKeys = (await sched.GetJobKeys(GroupMatcher<JobKey>.GroupEquals("g1"))).ToHashSet();
            triggerKeys = (await sched.GetTriggerKeys(GroupMatcher<TriggerKey>.GroupEquals("g1"))).ToHashSet();

            jobKeys.Count.Should()
                .Be(2,
                    "Number of jobs expected in 'g1' group was 2 "); // job should have been deleted also, because it is non-durable

            triggerKeys.Count.Should().Be(2, "Number of triggers expected in 'g1' group was 2 ");

            await sched.Awaiting(scheduler => scheduler.UnscheduleJob(new TriggerKey("t1"))).Should().NotThrowAsync()
                .WithResult(true,
                    "Scheduler should have returned 'true' from attempt to unschedule existing trigger. ");

            jobKeys = (await sched.GetJobKeys(GroupMatcher<JobKey>.GroupEquals(JobKey.DefaultGroup))).ToHashSet();
            triggerKeys = (await sched.GetTriggerKeys(GroupMatcher<TriggerKey>.GroupEquals(TriggerKey.DefaultGroup)))
                .ToHashSet();

            jobKeys.Count.Should()
                .Be(1,
                    "Number of jobs expected in default group was 1 "); // job should have been left in place, because it is non-durable

            triggerKeys.Count.Should().Be(0, "Number of triggers expected in default group was 0 ");

            await sched.Shutdown();
        }
        
        [Fact]
        public async Task TestAbilityToFireImmediatelyWhenStartedBefore()
        {
            var jobExecTimestamps = new List<DateTime>();
            var barrier = new Barrier(2);

            var sched = await CreateScheduler("testAbilityToFireImmediatelyWhenStartedBefore", 5);
            sched.Context.Put(TestConsts.Barrier, barrier);
            sched.Context.Put(TestConsts.DateStamps, jobExecTimestamps);
            await sched.Start();


            var job1 = JobBuilder.Create<TestJobWithSync>()
                .WithIdentity("job1")
                .Build();

            var trigger1 = TriggerBuilder.Create()
                .ForJob(job1)
                .Build();

            var sTime = DateTime.UtcNow;

            await sched.ScheduleJob(job1, trigger1);

            barrier.SignalAndWait(TestConsts.TestTimeout);

            await sched.Shutdown(false);

            var fTime = jobExecTimestamps[0];

            TimeSpan.FromMilliseconds(7000).Should().BeGreaterThan(TimeSpan.FromTicks(fTime.Subtract(sTime).Ticks),
                "Immediate trigger did not fire within a reasonable amount of time.");
        }
        
        [Fact]
        public async Task TestAbilityToFireImmediatelyWhenStartedBeforeWithTriggerJob()
        {
            var jobExecTimestamps = new List<DateTime>();
            var barrier = new Barrier(2);

            var sched = await CreateScheduler("testAbilityToFireImmediatelyWhenStartedBeforeWithTriggerJob", 5);
            await sched.Clear();

            sched.Context.Put(TestConsts.Barrier, barrier);
            sched.Context.Put(TestConsts.DateStamps, jobExecTimestamps);

            await sched.Start();

            Thread.Yield();

            var job1 = JobBuilder.Create<TestJobWithSync>()
                .WithIdentity("job1").StoreDurably().Build();
            await sched.AddJob(job1, false);

            var sTime = DateTime.UtcNow;

            await sched.TriggerJob(job1.Key);

            barrier.SignalAndWait(TestConsts.TestTimeout);

            await sched.Shutdown(false);

            var fTime = jobExecTimestamps[0];

            // This is dangerously subjective!  but what else to do?
            TimeSpan.FromMilliseconds(7000).Should().BeGreaterThan(TimeSpan.FromTicks(fTime.Subtract(sTime).Ticks),
                "Immediate trigger did not fire within a reasonable amount of time.");
        }
        
        [Fact]
        public async Task TestAbilityToFireImmediatelyWhenStartedAfter()
        {
            var jobExecTimestamps = new List<DateTime>();

            var barrier = new Barrier(2);

            var sched = await CreateScheduler("testAbilityToFireImmediatelyWhenStartedAfter", 5);

            sched.Context.Put(TestConsts.Barrier, barrier);
            sched.Context.Put(TestConsts.DateStamps, jobExecTimestamps);

            var job1 = JobBuilder.Create<TestJobWithSync>().WithIdentity("job1").Build();
            var trigger1 = TriggerBuilder.Create().ForJob(job1).Build();

            var sTime = DateTime.UtcNow;

            await sched.Start();
            await sched.ScheduleJob(job1, trigger1);

            barrier.SignalAndWait(TestConsts.TestTimeout);

            await sched.Shutdown(false);

            var fTime = jobExecTimestamps[0];

            // This is dangerously subjective!  but what else to do?
            TimeSpan.FromMilliseconds(7000).Should().BeGreaterThan(TimeSpan.FromTicks(fTime.Subtract(sTime).Ticks),
                "Immediate trigger did not fire within a reasonable amount of time.");
        }

        [Fact]
        public async Task TestScheduleMultipleTriggersForAJob()
        {
            var job = JobBuilder.Create<TestJob>().WithIdentity("job1", "group1").Build();
            var trigger1 = TriggerBuilder.Create()
                .WithIdentity("trigger1", "group1")
                .StartNow()
                .WithSimpleSchedule(x => x.WithIntervalInSeconds(1).RepeatForever())
                .Build();
            var trigger2 = TriggerBuilder.Create()
                .WithIdentity("trigger2", "group1")
                .StartNow()
                .WithSimpleSchedule(x => x.WithIntervalInSeconds(1).RepeatForever())
                .Build();

            ISet<ITrigger> triggersForJob = new HashSet<ITrigger>();
            triggersForJob.Add(trigger1);
            triggersForJob.Add(trigger2);

            var sched = await CreateScheduler("testScheduleMultipleTriggersForAJob", 5);
            await sched.Start();

            await sched.ScheduleJob(job, triggersForJob.ToList(), true);

            IList<ITrigger> triggersOfJob = (await sched.GetTriggersOfJob(job.Key)).ToList();

            triggersForJob.Count.Should().Be(2);
            triggersOfJob.Should().Contain(trigger1).And.Contain(trigger2);

            await sched.Shutdown(false);
        }
        
        [Fact]
        public async Task TestDurableStorageFunctions()
        {
            var sched = await CreateScheduler("testDurableStorageFunctions", 2);
            await sched.Clear();

            // test basic storage functions of scheduler...

            var job = JobBuilder.Create<TestJob>()
                .WithIdentity("j1")
                .StoreDurably()
                .Build();

            await sched.Awaiting(s => s.CheckExists(new JobKey("j1"))).Should()
                .NotThrowAsync().WithResult(false, "Unexpected existence of job named 'j1'.");

            await sched.AddJob(job, false);

            await sched.Awaiting(s => s.CheckExists(new JobKey("j1"))).Should()
                .NotThrowAsync().WithResult(true, "Unexpected non-existence of job named 'j1'.");

            var nonDurableJob = JobBuilder.Create<TestJob>()
                .WithIdentity("j2")
                .Build();

            await sched.Awaiting(s => s.AddJob(nonDurableJob, false)).Should()
                .ThrowAsync<SchedulerException>("Storage of non-durable job should not have succeeded.");

            await sched.Awaiting(s => s.CheckExists(new JobKey("j2"))).Should()
                .NotThrowAsync().WithResult(false, "Unexpected existence of job named 'j2'.");

            await sched.AddJob(nonDurableJob, false, true);

            await sched.Awaiting(s => s.CheckExists(new JobKey("j2"))).Should()
                .NotThrowAsync().WithResult(true, "Unexpected existence of job named 'j2'.");
        }

        [Fact]
        public async Task TestShutdownWithoutWaitIsUnclean()
        {
            var jobExecTimestamps = new List<DateTime>();
            var barrier = new Barrier(2);
            var scheduler = await CreateScheduler("testShutdownWithoutWaitIsUnclean", 8);
            try
            {
                scheduler.Context.Put(TestConsts.Barrier, barrier);
                scheduler.Context.Put(TestConsts.DateStamps, jobExecTimestamps);
                await scheduler.Start();
                var jobName = Guid.NewGuid().ToString();
                await scheduler.AddJob(
                    JobBuilder.Create<TestJobWithSync>().WithIdentity(jobName).StoreDurably().Build(), false);
                await scheduler.ScheduleJob(TriggerBuilder.Create().ForJob(jobName).StartNow().Build());
                while ((await scheduler.GetCurrentlyExecutingJobs()).Count == 0) Thread.Sleep(50);
            }
            finally
            {
                await scheduler.Shutdown(false);
            }

            barrier.SignalAndWait(TestConsts.TestTimeout);
        }

        [Fact]
        public async Task TestShutdownWithWaitIsClean()
        {
            var jobExecTimestamps = new List<DateTime>();
            var barrier = new Barrier(2);
            var scheduler = await CreateScheduler("testShutdownWithoutWaitIsUnclean", 8);
            try
            {
                scheduler.Context.Put(TestConsts.Barrier, barrier);
                scheduler.Context.Put(TestConsts.DateStamps, jobExecTimestamps);
                await scheduler.Start();
                var jobName = Guid.NewGuid().ToString();
                await scheduler.AddJob(
                    JobBuilder.Create<TestJobWithSync>().WithIdentity(jobName).StoreDurably().Build(), false);
                await scheduler.ScheduleJob(TriggerBuilder.Create().ForJob(jobName).StartNow().Build());
                while ((await scheduler.GetCurrentlyExecutingJobs()).Count == 0) Thread.Sleep(50);
            }
            finally
            {
                Action shutdownAction = () => scheduler.Shutdown(true);

                shutdownAction.ExecutionTime()
                    .Should()
                    .BeLessThanOrEqualTo(TimeSpan.FromMilliseconds(500));

                barrier.SignalAndWait(TestConsts.TestTimeout);

                shutdownAction.ExecutionTime()
                    .Should()
                    .BeLessThanOrEqualTo(TimeSpan.FromMilliseconds(5000));
            }
        }
    }
}