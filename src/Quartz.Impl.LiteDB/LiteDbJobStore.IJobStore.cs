using System;
using System.Collections.Generic;
using System.Globalization;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using LiteDB;
using LiteDB.Async;
using Quartz.Impl.LiteDB.Domains;
using Quartz.Impl.LiteDB.Domains.Comparators;
using Quartz.Impl.LiteDB.Extensions;
using Quartz.Impl.Matchers;
using Quartz.Simpl;
using Quartz.Spi;

namespace Quartz.Impl.LiteDB
{
    public partial class LiteDbJobStore
    {
        public async Task Initialize(ITypeLoadHelper loadHelper, ISchedulerSignaler signaler,
            CancellationToken cancellationToken = new CancellationToken())
        {
            _signaler = signaler;
            loadHelper ??= new SimpleTypeLoadHelper();
            _typeLoadHelper = loadHelper;
            _typeLoadHelper.Initialize();
            RegisterCustomTypes();
            _connectionString ??= new ConnectionString(ConnectionString)
            {
                Connection = ConnectionType.Shared
            };
            if (!System.IO.File.Exists(_connectionString.Filename)) // Create initial database.
            {
                var db = new LiteDatabaseAsync(_connectionString);
                await db.GetCollectionNamesAsync();
                db.Dispose();
            }
        }

        public async Task SchedulerStarted(CancellationToken cancellationToken = new CancellationToken())
        {
            using var db = GetDatabase();
            var schedulerCollection = db.GetCollection<Scheduler>();

            await schedulerCollection.EnsureIndexAsync(s => s.InstanceName, true);

            var exists = await schedulerCollection.ExistsAsync(s =>
                s.InstanceName == InstanceName);

            if (!exists)
            {
                var scheduler = new Scheduler() { InstanceName = InstanceName };
                await schedulerCollection.InsertAsync(scheduler);
                await db.CommitAsync();
                return;
            }

            try
            {
                await RecoverSchedulerData(db);
            }
            catch (SchedulerException se)
            {
                throw new SchedulerConfigException("Failure occurred during job recovery.", se);
            }
        }

        public async Task SchedulerPaused(CancellationToken cancellationToken = new CancellationToken())
        {
            await SetSchedulerState(SchedulerState.Paused);
        }

        public async Task SchedulerResumed(CancellationToken cancellationToken = new CancellationToken())
        {
            await SetSchedulerState(SchedulerState.Resumed);
        }

        public async Task Shutdown(CancellationToken cancellationToken = new CancellationToken())
        {
            await SetSchedulerState(SchedulerState.Shutdown);
        }

        public async Task StoreJobAndTrigger(IJobDetail newJob, IOperableTrigger newTrigger,
            CancellationToken cancellationToken = new CancellationToken())
        {
            await StoreJob(newJob, true, cancellationToken);
            await StoreTrigger(newTrigger, true, cancellationToken);
        }

        public async Task<bool> IsJobGroupPaused(string groupName,
            CancellationToken cancellationToken = new CancellationToken())
        {
            using var db = GetDatabase(true);

            return await db.GetCollection<Scheduler>()
                .Include(s => s.PausedJobGroups)
                .Query()
                .Where(s =>
                    s.InstanceName == InstanceName &&
                    s.PausedJobGroups.Contains(groupName))
                .ExistsAsync();
        }

        public async Task<bool> IsTriggerGroupPaused(string groupName,
            CancellationToken cancellationToken = new CancellationToken())
        {
            return (await GetPausedTriggerGroups(cancellationToken).ConfigureAwait(false)).Contains(groupName);
        }

        public async Task StoreJob(IJobDetail newJob, bool replaceExisting,
            CancellationToken cancellationToken = new CancellationToken())
        {
            using var db = GetDatabase();
            var jobCollection = db.GetCollection<Job>();

            if (await jobCollection.ExistsAsync(j =>
                    j.Key == newJob.Key.GetDatabaseId()))
                if (!replaceExisting)
                    throw new ObjectAlreadyExistsException(newJob);

            var job = new Job(newJob, InstanceName);

            await jobCollection.InsertAsync(job);
            await db.CommitAsync();
        }

        public async Task StoreJobsAndTriggers(
            IReadOnlyDictionary<IJobDetail, IReadOnlyCollection<ITrigger>> triggersAndJobs, bool replace,
            CancellationToken cancellationToken = new CancellationToken())
        {
            using var db = GetDatabase();
            var schedulerCollection = db.GetCollection<Scheduler>();
            var jobCollection = db.GetCollection<Job>();
            var triggerCollection = db.GetCollection<Trigger>();

            var scheduler = await schedulerCollection
                .Include(s => s.PausedJobGroups)
                .Include(s => s.BlockedJobs)
                .FindOneAsync(s =>
                    s.InstanceName == InstanceName);

            foreach (var pair in triggersAndJobs)
            {
                // First store the current job
                await jobCollection.InsertAsync(new Job(pair.Key, InstanceName));

                // Storing all triggers for the current job
                foreach (var orig in pair.Value.OfType<IOperableTrigger>())
                {
                    var trigger = new Trigger(orig, InstanceName);

                    var isInPausedTriggerGroup = await triggerCollection
                        .Query()
                        .Where(t => (t.State == InternalTriggerState.Paused ||
                                     t.State == InternalTriggerState.PausedAndBlocked) &&
                                    t.Group == orig.Key.Group)
                        .ExistsAsync();

                    if (isInPausedTriggerGroup || scheduler.PausedJobGroups.Contains(orig.JobKey.Group))
                    {
                        trigger.State = InternalTriggerState.Paused;
                        if (scheduler.BlockedJobs.Contains(orig.GetDatabaseId()))
                            trigger.State = InternalTriggerState.PausedAndBlocked;
                    }
                    else if (scheduler.BlockedJobs.Contains(orig.GetDatabaseId()))
                    {
                        trigger.State = InternalTriggerState.Blocked;
                    }

                    await triggerCollection.InsertAsync(trigger);
                }
            }

            await db.CommitAsync();
        }

        public async Task<bool> RemoveJob(JobKey jobKey, CancellationToken cancellationToken = new CancellationToken())
        {
            using var db = GetDatabase();
            var jobCollection = db.GetCollection<Job>();

            if (!await jobCollection.ExistsAsync(j => j.Key == jobKey.GetDatabaseId()))
                return false;

            return await jobCollection.DeleteAsync(
                (await jobCollection.FindOneAsync(j => j.Key == jobKey.GetDatabaseId())).Id);
        }

        public async Task<bool> RemoveJobs(IReadOnlyCollection<JobKey> jobKeys,
            CancellationToken cancellationToken = new CancellationToken())
        {
            using var db = GetDatabase();
            var ids = jobKeys.Select(k => k.GetDatabaseId());
            var result = await db.GetCollection<Job>().DeleteManyAsync(j => ids.Contains(j.Key)) == jobKeys.Count;
            await db.CommitAsync();
            return result;
        }

        public async Task<IJobDetail> RetrieveJob(JobKey jobKey,
            CancellationToken cancellationToken = new CancellationToken())
        {
            using var db = GetDatabase(true);

            var job = await db.GetCollection<Job>().FindOneAsync(j => j.Key == jobKey.GetDatabaseId());

            return job?.Deserialize();
        }

        public async Task StoreTrigger(IOperableTrigger newTrigger, bool replaceExisting,
            CancellationToken cancellationToken = new CancellationToken())
        {
            using var db = GetDatabase();
            var triggerCollection = db.GetCollection<Trigger>();
            var jobCollection = db.GetCollection<Job>();
            var schedulerCollection = db.GetCollection<Scheduler>();

            var trig = await triggerCollection
                .FindOneAsync(t => t.Key == newTrigger.Key.GetDatabaseId());

            if (trig != null)
                if (!replaceExisting)
                    throw new ObjectAlreadyExistsException(newTrigger);

            if (!await jobCollection.ExistsAsync(job => job.Key == newTrigger.JobKey.GetDatabaseId()))
                throw new JobPersistenceException("The job (" + newTrigger.JobKey +
                                                  ") referenced by the trigger does not exist.");

            var trigger = new Trigger(newTrigger, InstanceName);
            if (trig != null)
                trigger.Id = trig.Id;
            var isTriggerGroupPaused = await triggerCollection
                .Include(t => t.Scheduler)
                .Query()
                .Where(t =>
                    t.Group == newTrigger.Key.Group &&
                    (t.State == InternalTriggerState.Paused ||
                     t.State == InternalTriggerState.PausedAndBlocked))
                .ExistsAsync();

            var scheduler = await schedulerCollection
                .Include(s => s.PausedJobGroups)
                .Include(s => s.BlockedJobs)
                .FindOneAsync(s => s.InstanceName == InstanceName);

            if (scheduler != null)
            {
                var isJobGroupPaused = scheduler.PausedJobGroups.Contains(newTrigger.JobKey.Group);

                // make sure trigger group is not paused and that job is not blocked
                if (isTriggerGroupPaused || isJobGroupPaused)
                {
                    trigger.State = InternalTriggerState.Paused;

                    if (scheduler.BlockedJobs.Contains(newTrigger.GetJobDatabaseId()))
                        trigger.State = InternalTriggerState.PausedAndBlocked;
                }
                else if (scheduler.BlockedJobs.Contains(newTrigger.GetJobDatabaseId()))
                {
                    trigger.State = InternalTriggerState.Blocked;
                }
            }

            // Overwrite if exists
            await triggerCollection.UpsertAsync(trigger);
            await db.CommitAsync();
        }

        public async Task<bool> RemoveTrigger(TriggerKey triggerKey,
            CancellationToken cancellationToken = new CancellationToken())
        {
            using var db = GetDatabase();
            var triggerCollection = db.GetCollection<Trigger>();
            var jobCollection = db.GetCollection<Job>();

            if (!await triggerCollection.ExistsAsync(t => t.Key == triggerKey.GetDatabaseId()))
                return false;

            // Request trigger and associated job
            var trigger = await triggerCollection
                .Include(t => t.JobKey)
                .FindOneAsync(t => t.Key == triggerKey.GetDatabaseId());

            // Get pre-loaded associated job
            var job = (await jobCollection.FindOneAsync(j => j.Key == trigger.JobKey))?.Deserialize();

            // Check for more triggers
            var hasMoreTriggers = job != null && await triggerCollection
                .Query()
                .Where(t =>
                    t.JobName == job.Key.Name &&
                    t.Group == job.Key.Group &&
                    t.Key != trigger.Key) // exclude our own since not yet deleted
                .ExistsAsync();

            // Remove the trigger's job if it is not associated with any other triggers
            if (job != null && !hasMoreTriggers && !job.Durable)
            {
                await jobCollection.DeleteAsync(
                    (await jobCollection.FindOneAsync(j => j.Key == job.Key.GetDatabaseId())).Id);
                await _signaler.NotifySchedulerListenersJobDeleted(job.Key, cancellationToken);
            }

            // Delete trigger
            await triggerCollection.DeleteAsync((await triggerCollection
                .FindOneAsync(t => t.Key == triggerKey.GetDatabaseId())).Id);

            await db.CommitAsync();

            return true;
        }

        public async Task<bool> RemoveTriggers(IReadOnlyCollection<TriggerKey> triggerKeys,
            CancellationToken cancellationToken = new CancellationToken())
        {
            using var db = GetDatabase();
            var col = db.GetCollection<Trigger>();

            var ids = triggerKeys.Select(t => t.GetDatabaseId());
            var result = await col.DeleteManyAsync(t => ids.Contains(t.Key)) == triggerKeys.Count;

            await db.CommitAsync();
            return result;
        }

        public async Task<bool> ReplaceTrigger(TriggerKey triggerKey, IOperableTrigger newTrigger,
            CancellationToken cancellationToken = new CancellationToken())
        {
            if (!await CheckExists(triggerKey, cancellationToken).ConfigureAwait(false))
                return false;

            await RemoveTriggerOnly(triggerKey).ConfigureAwait(false);
            await StoreTrigger(newTrigger, true, cancellationToken).ConfigureAwait(false);

            return true;
        }

        public async Task<IOperableTrigger> RetrieveTrigger(TriggerKey triggerKey,
            CancellationToken cancellationToken = new CancellationToken())
        {
            using var db = GetDatabase(true);
            var col = db.GetCollection<Trigger>();

            var trigger = await col.FindOneAsync(t => t.Key == triggerKey.GetDatabaseId());

            return trigger?.Deserialize();
        }

        public async Task<bool> CalendarExists(string calName,
            CancellationToken cancellationToken = new CancellationToken())
        {
            using var db = GetDatabase(true);
            var col = db.GetCollection<Scheduler>();

            var scheduler = await col
                .Include(s => s.Calendars)
                .FindOneAsync(s => s.InstanceName == InstanceName);

            return scheduler?.Calendars is { } && scheduler.Calendars.ContainsKey(calName);
        }

        public async Task<bool> CheckExists(JobKey jobKey,
            CancellationToken cancellationToken = new CancellationToken())
        {
            using var db = GetDatabase(true);
            var col = db.GetCollection<Job>();
            return await col.ExistsAsync(j => j.Key == jobKey.GetDatabaseId());
        }

        public async Task<bool> CheckExists(TriggerKey triggerKey,
            CancellationToken cancellationToken = new CancellationToken())
        {
            using var db = GetDatabase(true);
            var col = db.GetCollection<Trigger>();

            return await col.ExistsAsync(t => t.Key == triggerKey.GetDatabaseId());
        }

        public async Task ClearAllSchedulingData(CancellationToken cancellationToken = new CancellationToken())
        {
            using var db = GetDatabase();
            var triggerCollection = db.GetCollection<Trigger>();
            var jobCollection = db.GetCollection<Job>();

            await jobCollection.DeleteManyAsync(j => j.Scheduler == InstanceName);
            await triggerCollection.DeleteManyAsync(t => t.Scheduler == InstanceName);
            await db.CommitAsync();
        }

        public async Task StoreCalendar(string name, ICalendar calendar, bool replaceExisting, bool updateTriggers,
            CancellationToken cancellationToken = new CancellationToken())
        {
            using var db = GetDatabase();
            var schedulerCollection = db.GetCollection<Scheduler>();
            var triggerCollection = db.GetCollection<Trigger>();

            var scheduler = await schedulerCollection
                .Include(s => s.Calendars)
                .FindOneAsync(s => s.InstanceName == InstanceName);

            if (scheduler?.Calendars is null)
                throw new NullReferenceException(string.Format(CultureInfo.InvariantCulture,
                    "Scheduler with instance name '{0}' is null", InstanceName));

            if (await CalendarExists(name, cancellationToken).ConfigureAwait(false) && !replaceExisting)
                throw new ObjectAlreadyExistsException(string.Format(CultureInfo.InvariantCulture,
                    "Calendar with name '{0}' already exists.", name));

            var calendarCopy = calendar.Clone();

            // add or replace calendar
            scheduler.Calendars[name] = calendarCopy;

            await schedulerCollection.UpdateAsync(scheduler);

            if (!updateTriggers)
            {
                await db.CommitAsync();
                return;
            }

            var triggersKeysToUpdate = await triggerCollection
                .Query()
                .Where(t => t.CalendarName == name)
                .Select(t => t.Key)
                .ToListAsync();

            if (!triggersKeysToUpdate.Any())
                return;

            foreach (var triggerKey in triggersKeysToUpdate)
            {
                var triggerToUpdate = await triggerCollection.FindOneAsync(t => t.Key == triggerKey);
                var trigger = triggerToUpdate.Deserialize();
                trigger.UpdateWithNewCalendar(calendarCopy, _misfireThreshold);
                triggerToUpdate.UpdateFireTimes(trigger);
                await triggerCollection.UpdateAsync(triggerToUpdate);
            }

            await db.CommitAsync();
        }

        public async Task<bool> RemoveCalendar(string calName,
            CancellationToken cancellationToken = new CancellationToken())
        {
            using var db = GetDatabase();
            var col = db.GetCollection<Scheduler>();

            if (await RetrieveCalendar(calName, cancellationToken).ConfigureAwait(false) is null) return false;

            var calCollection = await RetrieveCalendarCollection().ConfigureAwait(false);

            calCollection.Remove(calName);

            var scheduler = await col.FindOneAsync(s => s.InstanceName == InstanceName);
            scheduler.Calendars = calCollection;
            await col.UpdateAsync(scheduler);

            return true;
        }

        public async Task<ICalendar> RetrieveCalendar(string calName,
            CancellationToken cancellationToken = new CancellationToken())
        {
            var calCollection = await RetrieveCalendarCollection().ConfigureAwait(false);

            return calCollection.ContainsKey(calName) ? calCollection[calName] : null;
        }

        public async Task<int> GetNumberOfJobs(CancellationToken cancellationToken = new CancellationToken())
        {
            using var db = GetDatabase(true);

            return await db.GetCollection<Job>().CountAsync();
        }

        public async Task<int> GetNumberOfTriggers(CancellationToken cancellationToken = new CancellationToken())
        {
            using var db = GetDatabase(true);

            return await db.GetCollection<Trigger>().CountAsync();
        }

        public async Task<int> GetNumberOfCalendars(CancellationToken cancellationToken = new CancellationToken())
        {
            return (await RetrieveCalendarCollection().ConfigureAwait(false)).Count;
        }

        public async Task<IReadOnlyCollection<JobKey>> GetJobKeys(GroupMatcher<JobKey> matcher,
            CancellationToken cancellationToken = new CancellationToken())
        {
            using var db = GetDatabase(true);
            var col = db.GetCollection<Job>();

            var op = matcher.CompareWithOperator;
            var compareToValue = matcher.CompareToValue;

            var allJobs = await col.FindAllAsync();

            return allJobs.Where(job => op.Evaluate(job.Group, compareToValue))
                .Select(job => new JobKey(job.Name, job.Group)).ToList();
        }

        public async Task<IReadOnlyCollection<TriggerKey>> GetTriggerKeys(GroupMatcher<TriggerKey> matcher,
            CancellationToken cancellationToken = new CancellationToken())
        {
            using var db = GetDatabase(true);
            var col = db.GetCollection<Trigger>();

            var op = matcher.CompareWithOperator;
            var compareToValue = matcher.CompareToValue;

            var allTriggers = await col.FindAllAsync();

            return allTriggers.Where(trigger => op.Evaluate(trigger.Group, compareToValue))
                .Select(trigger => new TriggerKey(trigger.Name, trigger.Group)).ToList();
        }

        public async Task<IReadOnlyCollection<string>> GetJobGroupNames(
            CancellationToken cancellationToken = new CancellationToken())
        {
            using var db = GetDatabase(true);
            var col = db.GetCollection<Job>();

            return (await col
                    .Query()
                    .Select(job => new { job.Group, job.Key })
                    .ToEnumerableAsync())
                .GroupBy(x => x.Group)
                .Select(x => x.Key).ToList();
        }

        public async Task<IReadOnlyCollection<string>> GetTriggerGroupNames(
            CancellationToken cancellationToken = new CancellationToken())
        {
            using var db = GetDatabase(true);
            var col = db.GetCollection<Trigger>();

            return (await col
                    .Query()
                    .Select(trigger => new { trigger.Group, trigger.Key })
                    .ToEnumerableAsync())
                .GroupBy(x => x.Group)
                .Select(x => x.Key).ToList();
        }

        public async Task<IReadOnlyCollection<string>> GetCalendarNames(
            CancellationToken cancellationToken = new CancellationToken())
        {
            return (await RetrieveCalendarCollection().ConfigureAwait(false)).Keys.ToList();
        }

        public async Task<IReadOnlyCollection<IOperableTrigger>> GetTriggersForJob(JobKey jobKey,
            CancellationToken cancellationToken = new CancellationToken())
        {
            using var db = GetDatabase(true);
            var col = db.GetCollection<Trigger>();

            return (await col
                    .Query()
                    .Where(t => t.JobName == jobKey.Name && t.Group == jobKey.Group)
                    .ToEnumerableAsync())
                .Select(trigger => trigger.Deserialize()).ToList();
        }

        public async Task<TriggerState> GetTriggerState(TriggerKey triggerKey,
            CancellationToken cancellationToken = new CancellationToken())
        {
            using var db = GetDatabase(true);
            var col = db.GetCollection<Trigger>();

            var trigger = await col.FindOneAsync(t => t.Key == triggerKey.GetDatabaseId());

            if (trigger is null) return TriggerState.None;

            return trigger.State switch
            {
                InternalTriggerState.Complete => TriggerState.Complete,
                InternalTriggerState.Paused => TriggerState.Paused,
                InternalTriggerState.Blocked => TriggerState.Blocked,
                InternalTriggerState.PausedAndBlocked => TriggerState.Paused,
                InternalTriggerState.Error => TriggerState.Error,
                _ => TriggerState.Normal
            };
        }

        public async Task ResetTriggerFromErrorState(TriggerKey triggerKey,
            CancellationToken cancellationToken = new CancellationToken())
        {
            using var db = GetDatabase();
            var triggerCollection = db.GetCollection<Trigger>();
            var schedulerCollection = db.GetCollection<Scheduler>();

            var trigger = await triggerCollection.FindOneAsync(t => t.Key == triggerKey.GetDatabaseId());

            if (trigger is null) return;

            // is the trigger in error state?
            if (trigger.State != InternalTriggerState.Error) return;

            var scheduler = await schedulerCollection
                .Include(s => s.PausedJobGroups)
                .FindOneAsync(s => s.InstanceName == InstanceName);

            trigger.State = scheduler.PausedJobGroups.Contains(triggerKey.Group)
                ? InternalTriggerState.Paused
                : InternalTriggerState.Waiting;

            await triggerCollection.UpdateAsync(trigger);
            await db.CommitAsync();
        }

        public async Task PauseTrigger(TriggerKey triggerKey,
            CancellationToken cancellationToken = new CancellationToken())
        {
            using var db = GetDatabase();
            var col = db.GetCollection<Trigger>();

            var trigger = await col.FindOneAsync(t => t.Key == triggerKey.GetDatabaseId());

            // if the trigger doesn't exist or is "complete" pausing it does not make sense...
            if (trigger is null) return;

            if (trigger.State == InternalTriggerState.Complete) return;

            trigger.State = trigger.State == InternalTriggerState.Blocked
                ? InternalTriggerState.PausedAndBlocked
                : InternalTriggerState.Paused;

            await col.UpdateAsync(trigger);
            await db.CommitAsync();
        }

        public async Task<IReadOnlyCollection<string>> PauseTriggers(GroupMatcher<TriggerKey> matcher,
            CancellationToken cancellationToken = new CancellationToken())
        {
            var triggerKeysForMatchedGroup = await GetTriggerKeys(matcher, cancellationToken).ConfigureAwait(false);

            foreach (var triggerKey in triggerKeysForMatchedGroup)
            {
                await PauseTrigger(triggerKey, cancellationToken).ConfigureAwait(false);
            }

            return triggerKeysForMatchedGroup.Select(k => k.Group).ToList();
        }

        public async Task PauseJob(JobKey jobKey, CancellationToken cancellationToken = new CancellationToken())
        {
            var triggersForJobs = await GetTriggersForJob(jobKey, cancellationToken).ConfigureAwait(false);

            foreach (var trigger in triggersForJobs)
            {
                await PauseTrigger(trigger.Key, cancellationToken).ConfigureAwait(false);
            }
        }

        public async Task<IReadOnlyCollection<string>> PauseJobs(GroupMatcher<JobKey> matcher,
            CancellationToken cancellationToken = new CancellationToken())
        {
            var jobKeysForMatchedGroup = await GetJobKeys(matcher, cancellationToken);

            foreach (var jobKey in jobKeysForMatchedGroup)
            {
                await PauseJob(jobKey, cancellationToken);
            }

            using var db = GetDatabase();
            var schedulerCollection = db.GetCollection<Scheduler>();

            var scheduler = await schedulerCollection.FindOneAsync(s => s.InstanceName == InstanceName);

            foreach (var jobKey in jobKeysForMatchedGroup)
            {
                scheduler.PausedJobGroups.Add(jobKey.Group);
            }

            await schedulerCollection.UpdateAsync(scheduler);
            await db.CommitAsync();

            return jobKeysForMatchedGroup.Select(key => key.Group).ToList();
        }

        public async Task ResumeTrigger(TriggerKey triggerKey,
            CancellationToken cancellationToken = new CancellationToken())
        {
            using var db = GetDatabase();
            var triggerCollection = db.GetCollection<Trigger>();
            var schedulerCollection = db.GetCollection<Scheduler>();

            var trigger = await triggerCollection
                .Include(t => t.Scheduler)
                .FindOneAsync(t => t.Key == triggerKey.GetDatabaseId());

            if (trigger is null) return;

            // if the trigger is not paused resuming it does not make sense... **
            if (trigger.State != InternalTriggerState.Paused &&
                trigger.State != InternalTriggerState.PausedAndBlocked)
                return;

            var blocked = (await schedulerCollection
                .Include(s => s.BlockedJobs)
                .FindOneAsync(s => s.InstanceName == InstanceName)).BlockedJobs;

            trigger.State = blocked.Contains(trigger.JobKey)
                ? InternalTriggerState.Blocked
                : InternalTriggerState.Waiting;

            await triggerCollection.UpdateAsync(trigger);

            await ApplyMisfire(trigger, cancellationToken);
            await db.CommitAsync();
        }

        public async Task<IReadOnlyCollection<string>> ResumeTriggers(GroupMatcher<TriggerKey> matcher,
            CancellationToken cancellationToken = new CancellationToken())
        {
            var keys = await GetTriggerKeys(matcher, cancellationToken);
            foreach (var triggerKey in keys)
            {
                await ResumeTrigger(triggerKey, cancellationToken);
            }

            return keys.Select(k => k.Group).ToList();
        }

        public async Task<IReadOnlyCollection<string>> GetPausedTriggerGroups(
            CancellationToken cancellationToken = new CancellationToken())
        {
            using var db = GetDatabase(true);
            var col = db.GetCollection<Trigger>();

            return (await col
                    .Query()
                    .Where(t =>
                        t.State == InternalTriggerState.Paused ||
                        t.State == InternalTriggerState.PausedAndBlocked)
                    .ToEnumerableAsync())
                .Distinct()
                .Select(t => t.Group)
                .ToList();
        }

        public async Task ResumeJob(JobKey jobKey, CancellationToken cancellationToken = new CancellationToken())
        {
            var triggersForJob = await GetTriggersForJob(jobKey, cancellationToken).ConfigureAwait(false);

            foreach (var trigger in triggersForJob) await ResumeTrigger(trigger.Key, cancellationToken);
        }

        public async Task<IReadOnlyCollection<string>> ResumeJobs(GroupMatcher<JobKey> matcher,
            CancellationToken cancellationToken = new CancellationToken())
        {
            var keys = await GetJobKeys(matcher, cancellationToken);

            using var db = GetDatabase();
            var col = db.GetCollection<Scheduler>();

            var scheduler = await col
                .Include(s => s.PausedJobGroups)
                .FindOneAsync(s => s.InstanceName == InstanceName);

            var resumedGroups = scheduler.PausedJobGroups.Where(pausedJobGroup =>
                matcher.CompareWithOperator.Evaluate(pausedJobGroup, matcher.CompareToValue));

            scheduler.PausedJobGroups.RemoveWhere(s =>
                resumedGroups.Any(r => r.Equals(s, StringComparison.InvariantCultureIgnoreCase)));

            await col.UpdateAsync(scheduler);

            foreach (var key in keys)
            {
                var triggers = await GetTriggersForJob(key, cancellationToken);

                foreach (var trigger in triggers) await ResumeTrigger(trigger.Key, cancellationToken);
            }

            return resumedGroups.ToList();
        }

        public async Task PauseAll(CancellationToken cancellationToken = new CancellationToken())
        {
            var triggerGroupNames = await GetTriggerGroupNames(cancellationToken);

            foreach (var groupName in triggerGroupNames)
                await PauseTriggers(GroupMatcher<TriggerKey>.GroupEquals(groupName), cancellationToken);
        }

        public async Task ResumeAll(CancellationToken cancellationToken = new CancellationToken())
        {
            using var db = GetDatabase();
            var col = db.GetCollection<Scheduler>();

            var scheduler = await col
                .Include(s => s.PausedJobGroups)
                .FindOneAsync(s => s.InstanceName == InstanceName);

            scheduler.PausedJobGroups.Clear();
            await col.UpdateAsync(scheduler);

            var triggerGroupNames = await GetTriggerGroupNames(cancellationToken);

            foreach (var groupName in triggerGroupNames)
                await ResumeTriggers(GroupMatcher<TriggerKey>.GroupEquals(groupName), cancellationToken);
        }

        public async Task<IReadOnlyCollection<IOperableTrigger>> AcquireNextTriggers(DateTimeOffset noLaterThan,
            int maxCount,
            TimeSpan timeWindow,
            CancellationToken cancellationToken = new CancellationToken())
        {
            using var db = GetDatabase();
            var triggerCollection = db.GetCollection<Trigger>();
            var jobCollection = db.GetCollection<Job>();

            var result = new List<IOperableTrigger>();
            var acquiredJobKeysForNoConcurrentExec = new HashSet<JobKey>();

            var query = await triggerCollection
                .Query()
                .Where(t =>
                    t.State == InternalTriggerState.Waiting &&
                    t.NextFireTimeUtc <= (noLaterThan + timeWindow).UtcDateTime).ToEnumerableAsync();

            var triggers = new SortedSet<Trigger>(query
                    .OrderBy(t => t.NextFireTimeTicks)
                    .ThenBy(t => t.Priority),
                new TriggerComparator());

            while (true)
            {
                // return empty list if store has no such triggers.
                if (!triggers.Any()) return new List<IOperableTrigger>(0);

                var candidateTrigger = triggers.First();
                if (candidateTrigger == null) break;
                if (!triggers.Remove(candidateTrigger)) break;
                if (candidateTrigger.NextFireTimeUtc == null) continue;

                if (await ApplyMisfire(candidateTrigger, cancellationToken))
                {
                    if (candidateTrigger.NextFireTimeUtc != null) triggers.Add(candidateTrigger);
                    continue;
                }

                if (candidateTrigger.NextFireTimeUtc > noLaterThan + timeWindow) break;

                // If trigger's job is set as @DisallowConcurrentExecution, and it has already been added to result, then
                // put it back into the timeTriggers set and continue to search for next trigger.
                var jobKey = new JobKey(candidateTrigger.JobName, candidateTrigger.Group);
                var job = await jobCollection
                    .FindOneAsync(job => job.Key == candidateTrigger.JobKey);

                if (job.ConcurrentExecutionDisallowed)
                {
                    if (acquiredJobKeysForNoConcurrentExec.Contains(jobKey))
                        continue; // go to next trigger in store.
                    acquiredJobKeysForNoConcurrentExec.Add(jobKey);
                }

                candidateTrigger.State = InternalTriggerState.Acquired;
                candidateTrigger.FireInstanceId = GetFiredTriggerRecordId();

                await triggerCollection.UpdateAsync(candidateTrigger);

                result.Add(candidateTrigger.Deserialize());

                if (result.Count == maxCount) break;
            }

            await db.CommitAsync();

            return result;
        }

        public async Task ReleaseAcquiredTrigger(IOperableTrigger trigger,
            CancellationToken cancellationToken = new CancellationToken())
        {
            using var db = GetDatabase();
            var col = db.GetCollection<Trigger>();

            var trig = await col.FindOneAsync(t => t.Key == trigger.GetDatabaseId());

            if (trig is null || trig.State != InternalTriggerState.Acquired) return;
            trig.State = InternalTriggerState.Waiting;
            await col.UpdateAsync(trig);
            await db.CommitAsync();
        }

        public async Task<IReadOnlyCollection<TriggerFiredResult>> TriggersFired(
            IReadOnlyCollection<IOperableTrigger> triggers,
            CancellationToken cancellationToken = new CancellationToken())
        {
            var results = new List<TriggerFiredResult>();

            using var db = GetDatabase();
            var triggerCollection = db.GetCollection<Trigger>();
            var jobCollection = db.GetCollection<Job>();
            var schedulerCollection = db.GetCollection<Scheduler>();

            try
            {
                foreach (var tr in triggers)
                {
                    // was the trigger deleted since being acquired?
                    var trigger = await triggerCollection
                        .Include(t => t.JobKey)
                        .FindOneAsync(t => t.Key == tr.GetDatabaseId());

                    // was the trigger completed, paused, blocked, etc. since being acquired?
                    if (trigger?.State != InternalTriggerState.Acquired) continue;

                    ICalendar cal = null;
                    if (trigger.CalendarName != null)
                    {
                        cal = await RetrieveCalendar(trigger.CalendarName, cancellationToken);
                        if (cal == null) continue;
                    }

                    var prevFireTime = trigger.PreviousFireTimeUtc;

                    var trig = trigger.Deserialize();
                    trig.Triggered(cal);

                    var dbJob = (await jobCollection
                        .FindOneAsync(job => job.Key == trig.JobKey.GetDatabaseId())).Deserialize();

                    var bundle = new TriggerFiredBundle(
                        dbJob,
                        trig,
                        cal,
                        false,
                        SystemTime.UtcNow(),
                        trig.GetPreviousFireTimeUtc(),
                        prevFireTime,
                        trig.GetNextFireTimeUtc());

                    var job = bundle.JobDetail;

                    trigger.UpdateFireTimes(trig);
                    trigger.State = InternalTriggerState.Waiting;

                    await triggerCollection.UpdateAsync(trigger);

                    if (job.ConcurrentExecutionDisallowed)
                    {
                        var tCollection = await triggerCollection
                            .Query()
                            .Include(t => t.Scheduler)
                            .Where(t =>
                                t.Group == job.Key.Group &&
                                t.JobName == job.Key.Name)
                            .ToListAsync();

                        foreach (var t in tCollection)
                        {
                            if (t.State == InternalTriggerState.Waiting)
                                t.State = InternalTriggerState.Blocked;
                            if (t.State == InternalTriggerState.Paused)
                                t.State = InternalTriggerState.PausedAndBlocked;
                        }

                        var scheduler = await schedulerCollection
                            .Include(s => s.BlockedJobs)
                            .FindOneAsync(s => s.InstanceName == InstanceName);

                        scheduler.BlockedJobs.Add(job.Key.GetDatabaseId());
                        await triggerCollection.UpdateAsync(tCollection);
                        await schedulerCollection.UpdateAsync(scheduler);
                    }

                    results.Add(new TriggerFiredResult(bundle));
                }
            }
            finally
            {
                await db.CommitAsync();
            }

            return results;
        }

        public async Task TriggeredJobComplete(IOperableTrigger trigger, IJobDetail jobDetail,
            SchedulerInstruction triggerInstCode,
            CancellationToken cancellationToken = new CancellationToken())
        {
            using var db = GetDatabase();
            var jobCollection = db.GetCollection<Job>();
            var triggerCollection = db.GetCollection<Trigger>();
            var schedulerCollection = db.GetCollection<Scheduler>();

            var entry = await triggerCollection
                .Include(t => t.Scheduler)
                .Include(t => t.JobKey)
                .FindOneAsync(t => t.Key == trigger.GetDatabaseId());

            var scheduler = await schedulerCollection
                .Include(s => s.BlockedJobs)
                .FindOneAsync(s => s.InstanceName == InstanceName);

            // It's possible that the job or trigger is null if it was deleted during execution
            var job = await jobCollection
                .FindOneAsync(j => j.Key == trigger.GetJobDatabaseId());

            if (job != null)
            {
                if (jobDetail.PersistJobDataAfterExecution) job.JobDataMap = jobDetail.JobDataMap;

                if (job.ConcurrentExecutionDisallowed)
                {
                    scheduler.BlockedJobs.Remove(job.Key);

                    var trigCollection = await triggerCollection
                        .Query()
                        .Where(t =>
                            t.Group == job.Group &&
                            t.JobName == job.Name)
                        .ToListAsync();

                    foreach (var t in trigCollection)
                    {
                        var triggerToUpdate = await triggerCollection
                            .FindOneAsync(ti => ti.Key == t.Key);

                        triggerToUpdate.State = t.State switch
                        {
                            InternalTriggerState.Blocked => InternalTriggerState.Waiting,
                            InternalTriggerState.PausedAndBlocked => InternalTriggerState.Paused,
                            _ => triggerToUpdate.State
                        };
                    }

                    _signaler.SignalSchedulingChange(null, cancellationToken);
                }
            }
            else
            {
                // even if it was deleted, there may be cleanup to do
                scheduler.BlockedJobs.Remove(jobDetail.Key.GetDatabaseId());
            }

            // check for trigger deleted during execution...
            if (trigger != null)
            {
                switch (triggerInstCode)
                {
                    case SchedulerInstruction.DeleteTrigger:
                    {
                        // Deleting triggers
                        var d = trigger.GetNextFireTimeUtc();
                        if (!d.HasValue)
                        {
                            // double check for possible reschedule within job 
                            // execution, which would cancel the need to delete...
                            d = entry.NextFireTimeUtc;
                            if (!d.HasValue)
                                await RemoveTrigger(trigger.Key, cancellationToken);
                        }
                        else
                        {
                            await RemoveTrigger(trigger.Key, cancellationToken);
                            _signaler.SignalSchedulingChange(null, cancellationToken);
                        }

                        break;
                    }
                    case SchedulerInstruction.SetTriggerComplete:
                        entry.State = InternalTriggerState.Complete;
                        _signaler.SignalSchedulingChange(null, cancellationToken);
                        break;
                    case SchedulerInstruction.SetTriggerError:
                        entry.State = InternalTriggerState.Error;
                        _signaler.SignalSchedulingChange(null, cancellationToken);
                        break;
                    case SchedulerInstruction.SetAllJobTriggersError:
                        await SetAllTriggersOfJobToState(trigger.JobKey, InternalTriggerState.Error,
                            cancellationToken);
                        _signaler.SignalSchedulingChange(null, cancellationToken);
                        break;
                    case SchedulerInstruction.SetAllJobTriggersComplete:
                        await SetAllTriggersOfJobToState(trigger.JobKey, InternalTriggerState.Complete,
                            cancellationToken);
                        _signaler.SignalSchedulingChange(null, cancellationToken);
                        break;
                }
            }

            await triggerCollection.UpdateAsync(entry);
            await db.CommitAsync();
        }
    }
}