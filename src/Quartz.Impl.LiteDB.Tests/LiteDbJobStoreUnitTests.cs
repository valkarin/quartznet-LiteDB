using System;
using System.Collections.Generic;
using System.Diagnostics.CodeAnalysis;
using System.Linq;
using System.Threading.Tasks;
using FluentAssertions;
using Quartz.Impl.Calendar;
using Quartz.Impl.LiteDB.Tests.TestJobs;
using Quartz.Impl.Matchers;
using Quartz.Impl.Triggers;
using Quartz.Simpl;
using Quartz.Spi;
using Xunit;
using Xunit.Abstractions;

namespace Quartz.Impl.LiteDB.Tests
{
    public class LiteDbJobStoreUnitTests : IAsyncLifetime, IClassFixture<TestCleanUp>
    {
        private readonly ITestOutputHelper _output;
        private LiteDbJobStore _fJobStore;
        private JobDetailImpl _fJobDetail;
        private SampleSignaler _fSignaler;
        private string _filename = "";

        public LiteDbJobStoreUnitTests(ITestOutputHelper output)
        {
            _output = output;
        }

        public async Task InitializeAsync()
        {
            _filename = TestConsts.GenerateFilename;
            _fJobStore = new LiteDbJobStore
            {
                ConnectionString = $"Filename={_filename};Mode=Shared;"
            };

            _fSignaler = new SampleSignaler();
            await _fJobStore.Initialize(new SimpleTypeLoadHelper(), _fSignaler);
            await _fJobStore.SchedulerStarted();

            _fJobDetail = new JobDetailImpl("job1", "jobGroup1", typeof(NoOpJob)) { Durable = true };
            await _fJobStore.StoreJob(_fJobDetail, true);
        }

        public Task DisposeAsync()
        {
            return Task.CompletedTask;
        }

        [Fact]
        public async Task TestAcquireNextTrigger()
        {
            var d = DateBuilder.EvenMinuteDateAfterNow();
            IOperableTrigger trigger1 = new SimpleTriggerImpl("trigger1", "triggerGroup1", _fJobDetail.Name,
                _fJobDetail.Group, d.AddSeconds(200), d.AddSeconds(200), 2, TimeSpan.FromSeconds(2));
            IOperableTrigger trigger2 = new SimpleTriggerImpl("trigger2", "triggerGroup1", _fJobDetail.Name,
                _fJobDetail.Group, d.AddSeconds(50), d.AddSeconds(200), 2, TimeSpan.FromSeconds(2));
            IOperableTrigger trigger3 = new SimpleTriggerImpl("trigger1", "triggerGroup2", _fJobDetail.Name,
                _fJobDetail.Group, d.AddSeconds(100), d.AddSeconds(200), 2, TimeSpan.FromSeconds(2));

            trigger1.ComputeFirstFireTimeUtc(null);
            trigger2.ComputeFirstFireTimeUtc(null);
            trigger3.ComputeFirstFireTimeUtc(null);
            await _fJobStore.StoreTrigger(trigger1, false);
            await _fJobStore.StoreTrigger(trigger2, false);
            await _fJobStore.StoreTrigger(trigger3, false);

            trigger1.GetNextFireTimeUtc().Should().NotBeNull();

            // ReSharper disable once PossibleInvalidOperationException
            var firstFireTime = trigger1.GetNextFireTimeUtc().Value;

            (await _fJobStore.Awaiting(store =>
                    store.AcquireNextTriggers(
                        d.AddMilliseconds(10),
                        1,
                        TimeSpan.Zero)).Should().NotThrowAsync())
                .Which
                .Count
                .Should()
                .Be(0);

            (await _fJobStore.Awaiting(store =>
                    store.AcquireNextTriggers(
                        firstFireTime.AddSeconds(10),
                        1,
                        TimeSpan.Zero)).Should().NotThrowAsync())
                .Which
                .Should()
                .SatisfyRespectively(first =>
                {
                    first.Should().BeEquivalentTo(trigger2, options => options.Excluding(t => t.FireInstanceId));
                });

            (await _fJobStore.Awaiting(store =>
                    store.AcquireNextTriggers(
                        firstFireTime.AddSeconds(10),
                        1,
                        TimeSpan.Zero)).Should().NotThrowAsync())
                .Which
                .Should()
                .SatisfyRespectively(first =>
                {
                    first.Should().BeEquivalentTo(trigger3, options => options.Excluding(t => t.FireInstanceId));
                });

            (await _fJobStore.Awaiting(store =>
                    store.AcquireNextTriggers(
                        firstFireTime.AddSeconds(10),
                        1,
                        TimeSpan.Zero)).Should().NotThrowAsync())
                .Which
                .Should()
                .SatisfyRespectively(first =>
                {
                    first.Should().BeEquivalentTo(trigger1, options => options.Excluding(t => t.FireInstanceId));
                });

            (await _fJobStore.Awaiting(store =>
                    store.AcquireNextTriggers(
                        firstFireTime.AddSeconds(10),
                        1,
                        TimeSpan.Zero)).Should().NotThrowAsync())
                .Which
                .Count
                .Should()
                .Be(0);


            // release trigger3
            await _fJobStore.ReleaseAcquiredTrigger(trigger3);

            (await _fJobStore.Awaiting(store =>
                    store.AcquireNextTriggers(
                        firstFireTime.AddSeconds(10),
                        1,
                        TimeSpan.FromMilliseconds(1))).Should().NotThrowAsync())
                .Which
                .Should()
                .SatisfyRespectively(first =>
                {
                    first.Should().BeEquivalentTo(trigger3, options => options.Excluding(t => t.FireInstanceId));
                });
        }

        [Fact]
        public async Task TestAcquireNextTriggerBatch()
        {
            var d = DateBuilder.EvenMinuteDateAfterNow();

            IOperableTrigger early = new SimpleTriggerImpl("early", "triggerGroup1", _fJobDetail.Name,
                _fJobDetail.Group,
                d, d.AddMilliseconds(5), 2, TimeSpan.FromSeconds(2));
            IOperableTrigger trigger1 = new SimpleTriggerImpl("trigger1", "triggerGroup1", _fJobDetail.Name,
                _fJobDetail.Group, d.AddMilliseconds(200000), d.AddMilliseconds(200005), 2, TimeSpan.FromSeconds(2));
            IOperableTrigger trigger2 = new SimpleTriggerImpl("trigger2", "triggerGroup1", _fJobDetail.Name,
                _fJobDetail.Group, d.AddMilliseconds(200100), d.AddMilliseconds(200105), 2, TimeSpan.FromSeconds(2));
            IOperableTrigger trigger3 = new SimpleTriggerImpl("trigger3", "triggerGroup1", _fJobDetail.Name,
                _fJobDetail.Group, d.AddMilliseconds(200200), d.AddMilliseconds(200205), 2, TimeSpan.FromSeconds(2));
            IOperableTrigger trigger4 = new SimpleTriggerImpl("trigger4", "triggerGroup1", _fJobDetail.Name,
                _fJobDetail.Group, d.AddMilliseconds(200300), d.AddMilliseconds(200305), 2, TimeSpan.FromSeconds(2));
            IOperableTrigger trigger10 = new SimpleTriggerImpl("trigger10", "triggerGroup2", _fJobDetail.Name,
                _fJobDetail.Group, d.AddMilliseconds(500000), d.AddMilliseconds(700000), 2, TimeSpan.FromSeconds(2));

            early.ComputeFirstFireTimeUtc(null);
            trigger1.ComputeFirstFireTimeUtc(null);
            trigger2.ComputeFirstFireTimeUtc(null);
            trigger3.ComputeFirstFireTimeUtc(null);
            trigger4.ComputeFirstFireTimeUtc(null);
            trigger10.ComputeFirstFireTimeUtc(null);
            await _fJobStore.StoreTrigger(early, false);
            await _fJobStore.StoreTrigger(trigger1, false);
            await _fJobStore.StoreTrigger(trigger2, false);
            await _fJobStore.StoreTrigger(trigger3, false);
            await _fJobStore.StoreTrigger(trigger4, false);
            await _fJobStore.StoreTrigger(trigger10, false);

            trigger1.GetNextFireTimeUtc().Should().NotBeNull();

            // ReSharper disable once PossibleInvalidOperationException
            var firstFireTime = trigger1.GetNextFireTimeUtc().Value;

            var acquiredTriggers =
                (await _fJobStore.AcquireNextTriggers(firstFireTime.AddSeconds(10), 4, TimeSpan.FromSeconds(1)))
                .ToList();
            acquiredTriggers.Count.Should().Be(4);
            acquiredTriggers.Should().SatisfyRespectively(
                first => { first.Key.Should().BeEquivalentTo(early.Key); },
                second => { second.Key.Should().BeEquivalentTo(trigger1.Key); },
                third => { third.Key.Should().BeEquivalentTo(trigger2.Key); },
                fourth => { fourth.Key.Should().BeEquivalentTo(trigger3.Key); });

            await _fJobStore.ReleaseAcquiredTrigger(early);
            await _fJobStore.ReleaseAcquiredTrigger(trigger1);
            await _fJobStore.ReleaseAcquiredTrigger(trigger2);
            await _fJobStore.ReleaseAcquiredTrigger(trigger3);

            acquiredTriggers =
                (await _fJobStore.AcquireNextTriggers(firstFireTime.AddSeconds(10), 5, TimeSpan.FromMilliseconds(1000)))
                .ToList();

            acquiredTriggers.Count.Should().Be(5);
            acquiredTriggers.Should().SatisfyRespectively(
                first => { first.Key.Should().BeEquivalentTo(early.Key); },
                second => { second.Key.Should().BeEquivalentTo(trigger1.Key); },
                third => { third.Key.Should().BeEquivalentTo(trigger2.Key); },
                fourth => { fourth.Key.Should().BeEquivalentTo(trigger3.Key); },
                fifth => { fifth.Key.Should().BeEquivalentTo(trigger4.Key); });

            await _fJobStore.ReleaseAcquiredTrigger(early);
            await _fJobStore.ReleaseAcquiredTrigger(trigger1);
            await _fJobStore.ReleaseAcquiredTrigger(trigger2);
            await _fJobStore.ReleaseAcquiredTrigger(trigger3);
            await _fJobStore.ReleaseAcquiredTrigger(trigger4);

            acquiredTriggers =
                (await _fJobStore.AcquireNextTriggers(firstFireTime.AddSeconds(10), 6, TimeSpan.FromSeconds(1)))
                .ToList();

            acquiredTriggers.Count.Should().Be(5);
            acquiredTriggers.Should().SatisfyRespectively(
                first => { first.Key.Should().BeEquivalentTo(early.Key); },
                second => { second.Key.Should().BeEquivalentTo(trigger1.Key); },
                third => { third.Key.Should().BeEquivalentTo(trigger2.Key); },
                fourth => { fourth.Key.Should().BeEquivalentTo(trigger3.Key); },
                fifth => { fifth.Key.Should().BeEquivalentTo(trigger4.Key); });

            await _fJobStore.ReleaseAcquiredTrigger(early);
            await _fJobStore.ReleaseAcquiredTrigger(trigger1);
            await _fJobStore.ReleaseAcquiredTrigger(trigger2);
            await _fJobStore.ReleaseAcquiredTrigger(trigger3);
            await _fJobStore.ReleaseAcquiredTrigger(trigger4);

            acquiredTriggers =
                (await _fJobStore.AcquireNextTriggers(firstFireTime.AddMilliseconds(1), 5, TimeSpan.Zero))
                .ToList();

            acquiredTriggers.Count.Should().Be(2);

            await _fJobStore.ReleaseAcquiredTrigger(early);
            await _fJobStore.ReleaseAcquiredTrigger(trigger1);

            acquiredTriggers =
                (await _fJobStore.AcquireNextTriggers(firstFireTime.AddMilliseconds(250), 5,
                    TimeSpan.FromMilliseconds(199))).ToList();

            acquiredTriggers.Count().Should().Be(5);

            await _fJobStore.ReleaseAcquiredTrigger(early);
            await _fJobStore.ReleaseAcquiredTrigger(trigger1);
            await _fJobStore.ReleaseAcquiredTrigger(trigger2);
            await _fJobStore.ReleaseAcquiredTrigger(trigger3);
            await _fJobStore.ReleaseAcquiredTrigger(trigger4);

            acquiredTriggers =
                (await _fJobStore.AcquireNextTriggers(firstFireTime.AddMilliseconds(150), 5,
                    TimeSpan.FromMilliseconds(50L))).ToList();

            acquiredTriggers.Count().Should().Be(4);

            await _fJobStore.ReleaseAcquiredTrigger(early);
            await _fJobStore.ReleaseAcquiredTrigger(trigger1);
            await _fJobStore.ReleaseAcquiredTrigger(trigger2);
            await _fJobStore.ReleaseAcquiredTrigger(trigger3);
        }

        [Fact]
        [SuppressMessage("ReSharper", "AccessToModifiedClosure")]
        [SuppressMessage("ReSharper", "PossibleInvalidOperationException")]
        public async Task TestTriggerStates()
        {
            IOperableTrigger trigger = new SimpleTriggerImpl("trigger1", "triggerGroup1",
                _fJobDetail.Name,
                _fJobDetail.Group,
                DateTimeOffset.Now.AddSeconds(100),
                DateTimeOffset.Now.AddSeconds(200),
                2,
                TimeSpan.FromSeconds(2));

            trigger.ComputeFirstFireTimeUtc(null);

            await _fJobStore.Awaiting(store => store.GetTriggerState(trigger.Key)).Should().NotThrowAsync()
                .WithResult(TriggerState.None);

            await _fJobStore.StoreTrigger(trigger, false);

            await _fJobStore.Awaiting(store => store.GetTriggerState(trigger.Key)).Should().NotThrowAsync()
                .WithResult(TriggerState.Normal);

            await _fJobStore.PauseTrigger(trigger.Key);
            await _fJobStore.Awaiting(store => store.GetTriggerState(trigger.Key)).Should().NotThrowAsync()
                .WithResult(TriggerState.Paused);

            await _fJobStore.ResumeTrigger(trigger.Key);
            await _fJobStore.Awaiting(store => store.GetTriggerState(trigger.Key)).Should().NotThrowAsync()
                .WithResult(TriggerState.Normal);

            trigger.GetNextFireTimeUtc().Should().NotBeNull();

            trigger = (await _fJobStore.AcquireNextTriggers(trigger.GetNextFireTimeUtc().Value.AddSeconds(10), 1,
                TimeSpan.FromMilliseconds(1))).ToArray()[0];
            trigger.Should().NotBeNull();

            await _fJobStore.ReleaseAcquiredTrigger(trigger);

            trigger = (await _fJobStore.AcquireNextTriggers(trigger.GetNextFireTimeUtc().Value.AddSeconds(10), 1,
                TimeSpan.FromMilliseconds(1))).ToArray()[0];

            trigger.Should().NotBeNull();

            (await _fJobStore.Awaiting(store => store.AcquireNextTriggers(
                trigger.GetNextFireTimeUtc().Value.AddSeconds(10),
                1, TimeSpan.FromMilliseconds(1))).Should().NotThrowAsync()).Which.Should().HaveCount(0);
        }

        [Fact]
        public async Task TestRemoveCalendarWhenTriggersPresent()
        {
            // QRTZNET-29
            const string calenderName = "cal";
            
            IOperableTrigger trigger = new SimpleTriggerImpl("trigger1", "triggerGroup1", _fJobDetail.Name,
                _fJobDetail.Group, DateTimeOffset.Now.AddSeconds(100), DateTimeOffset.Now.AddSeconds(200), 2,
                TimeSpan.FromSeconds(2));
            trigger.ComputeFirstFireTimeUtc(null);
            ICalendar cal = new MonthlyCalendar();
            await _fJobStore.Awaiting(store => store.StoreTrigger(trigger, false))
                .Should().NotThrowAsync();

            await _fJobStore.Awaiting(store => store.StoreCalendar(calenderName, cal, false, true))
                .Should().NotThrowAsync();

            (await _fJobStore.Awaiting(store => store.GetCalendarNames()).Should().NotThrowAsync())
                .Which.Should().HaveCount(1).And.SatisfyRespectively(
                    first => first.Should().BeEquivalentTo(calenderName));

            await _fJobStore.Awaiting(store => store.GetNumberOfCalendars()).Should().NotThrowAsync()
                .WithResult(1);

            await _fJobStore.Awaiting(store => store.RemoveCalendar("cal"))
                .Should().NotThrowAsync();
        }

        [Fact]
        public async Task TestStoreTriggerReplacesTrigger()
        {
            const string jobName = "StoreTriggerReplacesTrigger";
            const string jobGroup = "StoreTriggerReplacesTriggerGroup";
            var detail = new JobDetailImpl(jobName, jobGroup, typeof(NoOpJob));
            await _fJobStore.StoreJob(detail, false);

            const string trName = "StoreTriggerReplacesTrigger";
            const string trGroup = "StoreTriggerReplacesTriggerGroup";
            IOperableTrigger tr = new SimpleTriggerImpl(trName, trGroup, DateTimeOffset.Now);
            tr.JobKey = new JobKey(jobName, jobGroup);
            tr.CalendarName = null;

            await _fJobStore.StoreTrigger(tr, false);

            (await _fJobStore.Awaiting(store => store.RetrieveTrigger(new TriggerKey(trName, trGroup)))
                .Should().NotThrowAsync()).Which.Should().BeEquivalentTo(tr);

            tr.CalendarName = "NonExistingCalendar";
            await _fJobStore.StoreTrigger(tr, true);

            (await _fJobStore.Awaiting(store => store.RetrieveTrigger(new TriggerKey(trName, trGroup)))
                .Should().NotThrowAsync()).Which.Should().BeEquivalentTo(tr);

            (await _fJobStore.Awaiting(store => store.RetrieveTrigger(new TriggerKey(trName, trGroup)))
                    .Should().NotThrowAsync()).Which.CalendarName.Should().NotBeNull().And
                .BeEquivalentTo(tr.CalendarName, "StoreJob doesn't replace triggers");

            await _fJobStore.Awaiting(store => store.StoreTrigger(tr, false))
                .Should().ThrowAsync<ObjectAlreadyExistsException>("an attempt to store duplicate trigger succeeded");
        }

        [Fact]
        public async Task PauseJobGroupPausesNewJob()
        {
            const string jobName1 = "PauseJobGroupPausesNewJob";
            const string jobName2 = "PauseJobGroupPausesNewJob2";
            const string jobGroup = "PauseJobGroupPausesNewJobGroup";
            var detail = new JobDetailImpl(jobName1, jobGroup, typeof(NoOpJob))
            {
                Durable = true
            };
            await _fJobStore.StoreJob(detail, false);
            await _fJobStore.PauseJobs(GroupMatcher<JobKey>.GroupEquals(jobGroup));

            detail = new JobDetailImpl(jobName2, jobGroup, typeof(NoOpJob))
            {
                Durable = true
            };

            await _fJobStore.StoreJob(detail, false);

            const string trName = "PauseJobGroupPausesNewJobTrigger";
            const string trGroup = "PauseJobGroupPausesNewJobTriggerGroup";
            IOperableTrigger tr = new SimpleTriggerImpl(trName, trGroup, DateTimeOffset.UtcNow);
            tr.JobKey = new JobKey(jobName2, jobGroup);
            await _fJobStore.StoreTrigger(tr, false);
            await _fJobStore.Awaiting(store => store.GetTriggerState(tr.Key)).Should()
                .NotThrowAsync().WithResult(TriggerState.Paused);
        }

        [Fact]
        public async Task TestRetrieveJob_NoJobFound()
        {
            var job = await _fJobStore.RetrieveJob(new JobKey("not", "existing"));
            job.Should().BeNull();
        }

        [Fact]
        public async Task TestRetrieveTrigger_NoTriggerFound()
        {
            var trigger = await _fJobStore.RetrieveTrigger(new TriggerKey("not", "existing"));
            trigger.Should().BeNull();
        }

        [Fact]
        public async Task TestStoreAndRetrieveJobGroups()
        {
            // Store jobs.
            for (var i = 0; i < 10; i++)
            {
                var job = JobBuilder.Create<NoOpJob>().WithIdentity("job" + i).Build();
                await _fJobStore.StoreJob(job, false);
            }

            var groups = await _fJobStore.GetJobGroupNames();

            groups.Count.Should().Be(2, "1 stored + 1 default group");
        }

        [Fact]
        public async Task TestStoreAndRetrieveTriggerGroups()
        {
            // Store jobs and triggers.
            for (var i = 0; i < 10; i++)
            {
                var job = JobBuilder.Create<NoOpJob>().WithIdentity("job" + i).Build();
                await _fJobStore.StoreJob(job, true);
                var schedule = SimpleScheduleBuilder.Create();
                var trigger = TriggerBuilder.Create().WithIdentity("trigger" + i).WithSchedule(schedule).ForJob(job)
                    .Build();
                await _fJobStore.StoreTrigger((IOperableTrigger)trigger, true);
            }

            var groups = await _fJobStore.GetTriggerGroupNames();

            groups.Count.Should().Be(1);
        }

        [Fact]
        public async Task TestStoreAndRetrieveTriggers()
        {
            // Store jobs and triggers.
            for (var i = 0; i < 10; i++)
            {
                var job = JobBuilder.Create<NoOpJob>().WithIdentity("job" + i).Build();
                await _fJobStore.StoreJob(job, true);
                var schedule = SimpleScheduleBuilder.Create();
                var trigger = TriggerBuilder.Create().WithIdentity("trigger" + i).WithSchedule(schedule).ForJob(job)
                    .Build();
                await _fJobStore.StoreTrigger((IOperableTrigger)trigger, true);
            }

            // Retrieve job and trigger.
            for (var i = 0; i < 10; i++)
            {
                var jobKey = JobKey.Create("job" + i);
                var storedJob = await _fJobStore.RetrieveJob(jobKey);
                storedJob.Key.Should().BeEquivalentTo(jobKey);

                var triggerKey = new TriggerKey("trigger" + i);
                ITrigger storedTrigger = await _fJobStore.RetrieveTrigger(triggerKey);
                storedTrigger.Key.Should().BeEquivalentTo(triggerKey);
            }
        }

        [Fact]
        public async Task TestStoreAndRetrieveJobs()
        {
            // Store jobs.
            for (var i = 0; i < 10; i++)
            {
                var job = JobBuilder.Create<NoOpJob>().WithIdentity("job" + i).Build();
                await _fJobStore.StoreJob(job, false);
            }

            // Retrieve jobs.
            for (var i = 0; i < 10; i++)
            {
                var jobKey = JobKey.Create("job" + i);
                var storedJob = await _fJobStore.RetrieveJob(jobKey);
                jobKey.Should().BeEquivalentTo(storedJob.Key);
            }
        }

        [Fact]
        public async Task TestAcquireTriggers()
        {
            // Setup: Store jobs and triggers.
            var startTime0 = DateTime.UtcNow.AddMinutes(1).ToUniversalTime(); // a min from now.
            for (var i = 0; i < 10; i++)
            {
                var startTime = startTime0.AddMinutes(i * 1); // a min apart
                var job = JobBuilder.Create<NoOpJob>().WithIdentity("job" + i).Build();
                var schedule = SimpleScheduleBuilder.RepeatMinutelyForever(2);
                var trigger = (IOperableTrigger)TriggerBuilder.Create().WithIdentity("trigger" + i)
                    .WithSchedule(schedule).ForJob(job).StartAt(startTime).Build();

                // Manually trigger the first fire time computation that scheduler would do. Otherwise 
                // the store.acquireNextTriggers() will not work properly.
                var fireTime = trigger.ComputeFirstFireTimeUtc(null);
                fireTime.Should().NotBeNull();

                await _fJobStore.StoreJobAndTrigger(job, trigger);
            }

            // Test acquire one trigger at a time
            for (var i = 0; i < 10; i++)
            {
                DateTimeOffset noLaterThan = startTime0.AddMinutes(i);
                const int maxCount = 1;
                var timeWindow = TimeSpan.Zero;
                var triggers = (await _fJobStore.AcquireNextTriggers(noLaterThan, maxCount, timeWindow)).ToList();
                triggers.Should().ContainSingle();
                triggers[0].Key.Name.Should().Be($"trigger{i}");

                // Let's remove the trigger now.
                await _fJobStore.RemoveJob(triggers[0].JobKey);
            }
        }

        [Fact]
        public async Task TestAcquireTriggersInBatch()
        {
            // Setup: Store jobs and triggers.
            var startTime0 = DateTimeOffset.UtcNow.AddMinutes(1); // a min from now.
            for (var i = 0; i < 10; i++)
            {
                var startTime = startTime0.AddMinutes(i); // a min apart
                var job = JobBuilder.Create<NoOpJob>().WithIdentity("job" + i).Build();
                var schedule = SimpleScheduleBuilder.RepeatMinutelyForever(2);
                var trigger = (IOperableTrigger)TriggerBuilder.Create().WithIdentity("trigger" + i)
                    .WithSchedule(schedule).ForJob(job).StartAt(startTime).Build();

                // Manually trigger the first fire time computation that scheduler would do. Otherwise 
                // the store.acquireNextTriggers() will not work properly.
                var fireTime = trigger.ComputeFirstFireTimeUtc(null);
                fireTime.Should().NotBeNull();

                await _fJobStore.StoreJobAndTrigger(job, trigger);
            }

            // Test acquire batch of triggers at a time
            var noLaterThan = startTime0.AddMinutes(10);
            const int maxCount = 7;
            var timeWindow = TimeSpan.FromMinutes(8);
            IList<IOperableTrigger> triggers =
                (await _fJobStore.AcquireNextTriggers(noLaterThan, maxCount, timeWindow)).ToList();

            triggers.Count.Should().Be(7);

            for (var i = 0; i < 7; i++) triggers[i].Key.Name.Should().BeEquivalentTo($"trigger{i}");
        }

        [Fact]
        public async Task TestResetErrorTrigger()
        {
            var baseFireTimeDate = DateBuilder.EvenMinuteDateAfterNow();

            IOperableTrigger trigger1 = new SimpleTriggerImpl(
                "trigger1",
                "triggerGroup1",
                _fJobDetail.Name,
                _fJobDetail.Group,
                baseFireTimeDate.AddMilliseconds(200000),
                baseFireTimeDate.AddMilliseconds(200000),
                2,
                TimeSpan.FromMilliseconds(2000));

            trigger1.ComputeFirstFireTimeUtc(null);

            await _fJobStore.StoreTrigger(trigger1, false);

            trigger1.GetNextFireTimeUtc().Should().NotBeNull();

            // ReSharper disable once PossibleInvalidOperationException
            var firstFireTime = trigger1.GetNextFireTimeUtc().Value;

            // pretend to fire it
            var aqTs = await _fJobStore.AcquireNextTriggers(
                firstFireTime.AddMilliseconds(10000),
                1,
                TimeSpan.Zero);

            aqTs.Should().HaveCountGreaterThan(0);

            aqTs.Should().SatisfyRespectively(first => { first.Key.Should().BeEquivalentTo(trigger1.Key); });

            var fTs = await _fJobStore.TriggersFired(aqTs);
            fTs.Should().HaveCountGreaterThan(0);

            var ft = fTs.First();

            ft.TriggerFiredBundle.Should().NotBeNull();

            // get the trigger into error state
            await _fJobStore.TriggeredJobComplete(ft.TriggerFiredBundle?.Trigger,
                ft.TriggerFiredBundle?.JobDetail,
                SchedulerInstruction.SetTriggerError);

            await _fJobStore.Awaiting(store => store.GetTriggerState(trigger1.Key)).Should().NotThrowAsync()
                .WithResult(TriggerState.Error);

            // test reset
            await _fJobStore.ResetTriggerFromErrorState(trigger1.Key);

            await _fJobStore.Awaiting(store => store.GetTriggerState(trigger1.Key)).Should().NotThrowAsync()
                .WithResult(TriggerState.Normal);
        }

        [Fact]
        public async Task TestJobDeleteReturnValue()
        {
            var job = JobBuilder.Create<NoOpJob>()
                .WithIdentity("job0")
                .StoreDurably()
                .Build();

            await _fJobStore.StoreJob(job, false);

            await _fJobStore.Awaiting(store => store.RemoveJob(new JobKey("job0")))
                .Should().NotThrowAsync()
                .WithResult(true, "Expected RemoveJob to return True when deleting an existing job");

            await _fJobStore.Awaiting(store => store.RemoveJob(new JobKey("job0")))
                .Should().NotThrowAsync()
                .WithResult(false, "Expected RemoveJob to return False when deleting an non-existing job");
        }

        [Fact]
        public async Task TestPauseResumeTriggersAndJobs()
        {
            const string groupName = "DEFAULT";
            // Store jobs and triggers.
            for (var i = 0; i < 10; i++)
            {
                var job = JobBuilder.Create<NoOpJob>().WithIdentity("job" + i, groupName).Build();
                await _fJobStore.StoreJob(job, true);
                var schedule = SimpleScheduleBuilder.Create();
                var trigger = TriggerBuilder.Create().WithIdentity("trigger" + i, groupName).WithSchedule(schedule)
                    .ForJob(job)
                    .Build();
                await _fJobStore.StoreTrigger((IOperableTrigger)trigger, true);
            }

            var triggerKeys = await _fJobStore
                .GetTriggerKeys(GroupMatcher<TriggerKey>.GroupEquals(groupName));

            async void CheckState(TriggerKey triggerKey, TriggerState state)
            {
                await _fJobStore.Awaiting(store => store.GetTriggerState(triggerKey))
                    .Should()
                    .NotThrowAsync()
                    .WithResult(state);
            }

            triggerKeys.Should().AllSatisfy(triggerKey => CheckState(triggerKey, TriggerState.Normal));

            await _fJobStore.PauseTriggers(GroupMatcher<TriggerKey>.GroupEquals(groupName));

            triggerKeys.Should().AllSatisfy(triggerKey => CheckState(triggerKey, TriggerState.Paused));

            await _fJobStore.Awaiting(store => store.IsTriggerGroupPaused(groupName)).Should()
                .NotThrowAsync().WithResult(true);

            await _fJobStore.ResumeTriggers(GroupMatcher<TriggerKey>.GroupEquals(groupName));

            triggerKeys.Should().AllSatisfy(triggerKey => CheckState(triggerKey, TriggerState.Normal));

            await _fJobStore.PauseAll();

            triggerKeys.Should().AllSatisfy(triggerKey => CheckState(triggerKey, TriggerState.Paused));

            await _fJobStore.ResumeAll();

            triggerKeys.Should().AllSatisfy(triggerKey => CheckState(triggerKey, TriggerState.Normal));

            await _fJobStore.PauseJobs(GroupMatcher<JobKey>.GroupEquals(groupName));

            await _fJobStore.Awaiting(store => store.IsJobGroupPaused(groupName)).Should()
                .NotThrowAsync().WithResult(true);

            triggerKeys.Should().AllSatisfy(triggerKey => CheckState(triggerKey, TriggerState.Paused));

            var jobKeys = await _fJobStore.GetJobKeys(GroupMatcher<JobKey>.GroupEquals(groupName));

            await _fJobStore.Awaiting(store => store.ResumeJob(jobKeys.First())).Should().NotThrowAsync();

            (await _fJobStore.Awaiting(store => store.GetTriggersForJob(jobKeys.First())).Should()
                    .NotThrowAsync()).Which.Should().HaveCount(1)
                .And.SatisfyRespectively(first => CheckState(first.Key, TriggerState.Normal));

            await _fJobStore.Awaiting(store => store.ResumeJobs(GroupMatcher<JobKey>.GroupEquals(groupName)))
                .Should().NotThrowAsync();
            
            triggerKeys.Should().AllSatisfy(triggerKey => CheckState(triggerKey, TriggerState.Normal));
        }

        [Fact]
        public async Task TestGetNumberOfJobsAndTriggers()
        {
            const string groupName = "DEFAULT";
            const int count = 10;
            // Store jobs and triggers.
            for (var i = 0; i < count; i++)
            {
                var job = JobBuilder.Create<NoOpJob>().WithIdentity("job" + i, groupName).Build();
                await _fJobStore.StoreJob(job, true);
                var schedule = SimpleScheduleBuilder.Create();
                var trigger = TriggerBuilder.Create().WithIdentity("trigger" + i, groupName).WithSchedule(schedule)
                    .ForJob(job)
                    .Build();
                await _fJobStore.StoreTrigger((IOperableTrigger)trigger, true);
            }

            await _fJobStore.Awaiting(store => store.GetNumberOfJobs()).Should()
                .NotThrowAsync().WithResult(count + 1); // +1 Default job.

            await _fJobStore.Awaiting(store => store.GetNumberOfTriggers()).Should()
                .NotThrowAsync().WithResult(count);

            var jobKeys = await _fJobStore.GetJobKeys(GroupMatcher<JobKey>.GroupEquals(groupName));

            await _fJobStore.Awaiting(store => store.RemoveJob(jobKeys.First())).Should().NotThrowAsync()
                .WithResult(true);

            await _fJobStore.Awaiting(store => store.GetNumberOfJobs()).Should()
                .NotThrowAsync().WithResult(count + 1 - 1); // +1 Default job. 1 Removed.

            // First one is already removed.
            await _fJobStore.Awaiting(store => store.RemoveJobs(jobKeys.Skip(1).ToList())).Should().NotThrowAsync()
                .WithResult(true);

            await _fJobStore.Awaiting(store => store.GetNumberOfJobs()).Should()
                .NotThrowAsync().WithResult(1); // 1 Default job.

            var triggerKeys = await _fJobStore
                .GetTriggerKeys(GroupMatcher<TriggerKey>.GroupEquals(groupName));

            await _fJobStore.Awaiting(store => store.RemoveTrigger(triggerKeys.First())).Should().NotThrowAsync()
                .WithResult(true);

            await _fJobStore.Awaiting(store => store.GetNumberOfTriggers()).Should()
                .NotThrowAsync().WithResult(count - 1); // 1 Removed

            await _fJobStore.Awaiting(store => store.RemoveTriggers(triggerKeys.Skip(1).ToList())).Should()
                .NotThrowAsync().WithResult(true);

            await _fJobStore.Awaiting(store => store.GetNumberOfTriggers()).Should()
                .NotThrowAsync().WithResult(0); // 1 Removed
        }
        
        [Fact]
        public async Task TestReplaceTrigger()
        {
            const string groupName = "DEFAULT";
            const string triggerName = "t1";
            const string triggerName2 = "t2";
            var job = JobBuilder.Create<NoOpJob>().WithIdentity("j1", groupName).Build();
            
            await _fJobStore.StoreJob(job, true);
            
            var schedule = SimpleScheduleBuilder.Create();
            var trigger = TriggerBuilder.Create().WithIdentity(triggerName, groupName).WithSchedule(schedule)
                .ForJob(job)
                .Build();
            
            await _fJobStore.StoreTrigger((IOperableTrigger)trigger, true);

            var trigger2 = TriggerBuilder.Create().WithIdentity(triggerName2, groupName).WithSchedule(schedule)
                .ForJob(job)
                .Build();

            await _fJobStore.Awaiting(store => store.CheckExists(trigger.Key)).Should().NotThrowAsync()
                .WithResult(true);

            await _fJobStore.Awaiting(store => store.ReplaceTrigger(trigger.Key, (IOperableTrigger)trigger2))
                .Should().NotThrowAsync().WithResult(true);
            
            await _fJobStore.Awaiting(store => store.CheckExists(trigger.Key)).Should().NotThrowAsync()
                .WithResult(false);
            
            await _fJobStore.Awaiting(store => store.CheckExists(trigger2.Key)).Should().NotThrowAsync()
                .WithResult(true);
        }
    }
}