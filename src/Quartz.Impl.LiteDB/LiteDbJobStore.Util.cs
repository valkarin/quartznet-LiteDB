using System;
using System.Collections.Generic;
using System.Globalization;
using System.Threading;
using System.Threading.Tasks;
using LiteDB;
using LiteDB.Async;
using Quartz.Impl.Calendar;
using Quartz.Impl.LiteDB.Domains;
using Quartz.Impl.LiteDB.Extensions;
using Quartz.Simpl;
using Quartz.Spi;
using Quartz.Util;

namespace Quartz.Impl.LiteDB
{
    public partial class LiteDbJobStore : IJobStore
    {
        private ConnectionString _connectionString;

        private LiteDatabaseAsync GetDatabase(bool readOnly = false)
        {
            _connectionString ??= new ConnectionString(ConnectionString)
            {
                Connection = ConnectionType.Shared
            };
            _connectionString.ReadOnly = readOnly;
            return new LiteDatabaseAsync(_connectionString);
        }

        private async Task SetSchedulerState(SchedulerState state)
        {
            using var db = GetDatabase();
            var collection = db.GetCollection<Scheduler>();
            var scheduler = await collection.FindOneAsync(s => s.InstanceName == InstanceName).ConfigureAwait(false);
            scheduler.State = state;
            await collection.UpdateAsync(scheduler);
            await db.CommitAsync();
        }

        protected virtual async Task RecoverSchedulerData(LiteDatabaseAsync db)
        {
            try
            {
                var triggerCollection = db.GetCollection<Trigger>();
                var jobCollection = db.GetCollection<Job>();

                var queryResult = await triggerCollection
                    .Query()
                    .Where(t =>
                        t.Scheduler == InstanceName &&
                        (t.State == InternalTriggerState.Acquired ||
                         t.State == InternalTriggerState.Blocked)).ToListAsync().ConfigureAwait(false);

                foreach (var trigger in queryResult)
                    trigger.State = InternalTriggerState.Waiting;

                await triggerCollection.UpdateAsync(queryResult).ConfigureAwait(false);

                // recover jobs marked for recovery that were not fully executed
                IList<IOperableTrigger> recoveringJobTriggers = new List<IOperableTrigger>();

                var queryResultJobs = await jobCollection.FindAsync(j =>
                    j.Scheduler == InstanceName && j.RequestsRecovery).ConfigureAwait(false);

                foreach (var job in queryResultJobs)
                    ((List<IOperableTrigger>)recoveringJobTriggers).AddRange(
                        await GetTriggersForJob(new JobKey(job.Name, job.Group)));

                foreach (var trigger in recoveringJobTriggers)
                {
                    if (!await CheckExists(trigger.JobKey)) continue;

                    trigger.ComputeFirstFireTimeUtc(null);
                    await StoreTrigger(trigger, true);
                }

                // remove lingering 'complete' triggers...
                await triggerCollection.DeleteManyAsync(t =>
                    t.Scheduler == InstanceName &&
                    t.State == InternalTriggerState.Complete).ConfigureAwait(false);

                await SetSchedulerState(SchedulerState.Started);

                await db.CommitAsync().ConfigureAwait(false);
            }
            catch (Exception e)
            {
                throw new JobPersistenceException("Couldn't recover jobs: " + e.Message, e);
            }
        }

        private async Task RemoveTriggerOnly(TriggerKey triggerKey)
        {
            using var db = GetDatabase();
            var col = db.GetCollection<Trigger>();
            await col.DeleteAsync((await col.FindOneAsync(t => t.Key == triggerKey.GetDatabaseId())).Id)
                .ConfigureAwait(false);
            await db.CommitAsync().ConfigureAwait(false);
        }

        public async Task<Dictionary<string, ICalendar>> RetrieveCalendarCollection()
        {
            using var db = GetDatabase(true);
            var col = db.GetCollection<Scheduler>();

            var scheduler = await col
                .Include(s => s.Calendars)
                .FindOneAsync(s => s.InstanceName == InstanceName);

            if (scheduler is null)
                throw new NullReferenceException(string.Format(CultureInfo.InvariantCulture,
                    "Scheduler with instance name '{0}' is null", InstanceName));

            if (scheduler.Calendars is null)
                throw new NullReferenceException(string.Format(CultureInfo.InvariantCulture,
                    "Calendar collection in '{0}' is null", InstanceName));

            return scheduler.Calendars;
        }

        protected virtual async Task<bool> ApplyMisfire(Trigger trigger, CancellationToken cancellationToken)
        {
            var misfireTime = SystemTime.UtcNow();
            if (MisfireThreshold > TimeSpan.Zero)
                misfireTime = misfireTime.AddMilliseconds(-1 * MisfireThreshold.TotalMilliseconds);

            var fireTimeUtc = trigger.NextFireTimeUtc;
            if (!fireTimeUtc.HasValue || fireTimeUtc.Value > misfireTime
                                      || trigger.MisfireInstruction == MisfireInstruction.IgnoreMisfirePolicy)
                return false;

            ICalendar cal = null;
            if (trigger.CalendarName != null) cal = await RetrieveCalendar(trigger.CalendarName, cancellationToken);

            // Deserialize to an IOperableTrigger to apply original methods on the trigger
            var trig = trigger.Deserialize();
            await _signaler.NotifyTriggerListenersMisfired(trig, cancellationToken);
            trig.UpdateAfterMisfire(cal);
            trigger.UpdateFireTimes(trig);

            if (!trig.GetNextFireTimeUtc().HasValue)
            {
                await _signaler.NotifySchedulerListenersFinalized(trig, cancellationToken);
                trigger.State = InternalTriggerState.Complete;
            }
            else if (fireTimeUtc.Equals(trig.GetNextFireTimeUtc()))
            {
                return false;
            }

            return true;
        }

        /// <summary>
        ///     Gets the fired trigger record id.
        /// </summary>
        /// <returns>The fired trigger record id.</returns>
        protected virtual string GetFiredTriggerRecordId()
        {
            var value = Interlocked.Increment(ref _ftrCtr);
            return Convert.ToString(value, CultureInfo.InvariantCulture);
        }

        protected virtual async Task SetAllTriggersOfJobToState(JobKey jobKey, InternalTriggerState state,
            CancellationToken cancellationToken)
        {
            using var db = GetDatabase();
            var col = db.GetCollection<Trigger>();

            var triggers = await col
                .Query()
                .Where(t =>
                    t.Group == jobKey.Group &&
                    t.JobName == jobKey.Name
                ).ToListAsync();

            foreach (var trigger in triggers)
            {
                trigger.State = state;
            }

            await col.UpdateAsync(triggers);
            await db.CommitAsync();
        }

        private void RegisterCustomTypes()
        {
            BsonMapper.Global.RegisterType(
                serialize: value => value.AssemblyQualifiedNameWithoutVersion(),
                deserialize: value => _typeLoadHelper.LoadType(value)
            );
            BsonMapper.Global.RegisterType<DateTimeOffset?>(
                serialize: value => value.HasValue ? new BsonValue(value.Value.ToString("O")) : BsonValue.Null,
                deserialize: bsonValue =>
                {
                    if (bsonValue == BsonValue.Null)
                        return null;
                    return DateTimeOffset.Parse(bsonValue.AsString);
                });
            BsonMapper.Global.RegisterType(
                serialize: value => new BsonValue(value.ToString("O")),
                deserialize: bsonValue => DateTimeOffset.Parse(bsonValue.AsString));
            BsonMapper.Global.RegisterType(
                serialize: obj =>
                {
                    var doc = new BsonDocument
                    {
                        ["Hour"] = obj.Hour,
                        ["Minute"] = obj.Minute,
                        ["Second"] = obj.Second
                    };
                    return doc;
                },
                deserialize: doc => new TimeOfDay(doc["Hour"].AsInt32, doc["Minute"].AsInt32, doc["Second"].AsInt32));
            BsonMapper.Global.RegisterType(
                serialize: value => new BsonValue(value.Id),
                deserialize: bsonValue => TimeZoneInfo.FindSystemTimeZoneById(bsonValue.AsString));
            BsonMapper.Global.RegisterType(
                serialize: value =>
                {
                    var doc = new BsonDocument
                    {
                        ["Description"] = new BsonValue(value.Description),
                        ["Expression"] = new BsonValue(value.CronExpression.CronExpressionString),
                        ["TimeZoneId"] = value.TimeZone.Id
                    };
                    return doc;
                },
                deserialize: bsonValue =>
                {
                    var cron = new CronCalendar(bsonValue["Expression"].AsString)
                    {
                        TimeZone = TimeZoneInfo.FindSystemTimeZoneById(bsonValue["TimeZoneId"].AsString),
                        Description = bsonValue["Description"].AsString
                    };
                    return cron;
                });
        }
    }
}