using System;
using System.Collections.Generic;
using LiteDB;
using Quartz.Impl.Triggers;
using Quartz.Simpl;
using Quartz.Spi;

namespace Quartz.Impl.LiteDB.Domains
{
    public partial class Trigger
    {
        // Make Type.GetType happy.
        internal Trigger()
        {
        }

        public Trigger(IOperableTrigger newTrigger, string schedulerInstanceName)
        {
            if (newTrigger == null) return;

            Name = newTrigger.Key.Name;
            Group = newTrigger.Key.Group;

            JobName = newTrigger.JobKey.Name;
            JobGroup = newTrigger.JobKey.Group;
            JobKey = $"{JobName}/{newTrigger.JobKey.Group}";
            Scheduler = schedulerInstanceName;

            State = InternalTriggerState.Waiting;
            Description = newTrigger.Description;
            CalendarName = newTrigger.CalendarName;
            JobDataMap = newTrigger.JobDataMap.WrappedMap;
            FinalFireTimeUtc = newTrigger.FinalFireTimeUtc;
            MisfireInstruction = newTrigger.MisfireInstruction;
            Priority = newTrigger.Priority;
            HasMillisecondPrecision = newTrigger.HasMillisecondPrecision;
            FireInstanceId = newTrigger.FireInstanceId;
            EndTimeUtc = newTrigger.EndTimeUtc;
            StartTimeUtc = newTrigger.StartTimeUtc;
            NextFireTimeUtc = newTrigger.GetNextFireTimeUtc();
            PreviousFireTimeUtc = newTrigger.GetPreviousFireTimeUtc();

            if (NextFireTimeUtc != null) NextFireTimeTicks = NextFireTimeUtc.Value.UtcTicks;

            // Init trigger specific properties according to type of newTrigger. 
            // If an option doesn't apply to the type of trigger it will stay null by default.

            switch (newTrigger)
            {
                case CronTriggerImpl cronTriggerImpl:
                    Cron = new CronOptions
                    {
                        CronExpression = cronTriggerImpl.CronExpressionString,
                        TimeZoneId = cronTriggerImpl.TimeZone.Id
                    };
                    return;
                case SimpleTriggerImpl simpTriggerImpl:
                    Simp = new SimpleOptions
                    {
                        RepeatCount = simpTriggerImpl.RepeatCount,
                        RepeatInterval = simpTriggerImpl.RepeatInterval
                    };
                    return;
                case CalendarIntervalTriggerImpl calTriggerImpl:
                    Cal = new CalendarOptions
                    {
                        RepeatIntervalUnit = calTriggerImpl.RepeatIntervalUnit,
                        RepeatInterval = calTriggerImpl.RepeatInterval,
                        TimesTriggered = calTriggerImpl.TimesTriggered,
                        TimeZoneId = calTriggerImpl.TimeZone.Id,
                        PreserveHourOfDayAcrossDaylightSavings = calTriggerImpl.PreserveHourOfDayAcrossDaylightSavings,
                        SkipDayIfHourDoesNotExist = calTriggerImpl.SkipDayIfHourDoesNotExist
                    };
                    return;
                case DailyTimeIntervalTriggerImpl dayTriggerImpl:
                    Day = new DailyTimeOptions
                    {
                        RepeatCount = dayTriggerImpl.RepeatCount,
                        RepeatIntervalUnit = dayTriggerImpl.RepeatIntervalUnit,
                        RepeatInterval = dayTriggerImpl.RepeatInterval,
                        StartTimeOfDay = dayTriggerImpl.StartTimeOfDay,
                        EndTimeOfDay = dayTriggerImpl.EndTimeOfDay,
                        DaysOfWeek = dayTriggerImpl.DaysOfWeek,
                        TimesTriggered = dayTriggerImpl.TimesTriggered,
                        TimeZoneId = dayTriggerImpl.TimeZone.Id
                    };
                    break;
            }
        }

        public ObjectId Id { get; set; }

        public string Name { get; set; }
        public string Group { get; set; }
        public string Key => $"{Name}/{Group}";

        public string JobName { get; set; }
        public string JobGroup { get; set; }
        public string JobKey { get; set; }
        public string Scheduler { get; set; }

        public InternalTriggerState State { get; set; }
        public string Description { get; set; }
        public string CalendarName { get; set; }
        public IDictionary<string, object> JobDataMap { get; set; }
        public string FireInstanceId { get; set; }
        public int MisfireInstruction { get; set; }
        public DateTimeOffset? FinalFireTimeUtc { get; set; }
        public DateTimeOffset? EndTimeUtc { get; set; }
        public DateTimeOffset StartTimeUtc { get; set; }

        public DateTimeOffset? NextFireTimeUtc { get; set; }

        // Used for sorting triggers by time - more efficient than sorting strings
        public long NextFireTimeTicks { get; set; }

        public DateTimeOffset? PreviousFireTimeUtc { get; set; }
        public int Priority { get; set; }
        public bool HasMillisecondPrecision { get; set; }

        public CronOptions Cron { get; set; }
        public SimpleOptions Simp { get; set; }
        public CalendarOptions Cal { get; set; }
        public DailyTimeOptions Day { get; set; }

        /// <summary>
        ///     Converts this <see cref="Trigger"/> back into an <see cref="IOperableTrigger"/>.
        /// </summary>
        /// <returns>The built <see cref="IOperableTrigger"/>.</returns>
        public IOperableTrigger Deserialize()
        {
            var triggerBuilder = TriggerBuilder.Create()
                .WithIdentity(Name, Group)
                .WithDescription(Description)
                .ModifiedByCalendar(CalendarName)
                .WithPriority(Priority)
                .StartAt(StartTimeUtc)
                .EndAt(EndTimeUtc)
                .ForJob(new JobKey(JobName, JobGroup))
                .UsingJobData(new JobDataMap(JobDataMap));

            if (Cron != null)
                triggerBuilder = triggerBuilder.WithCronSchedule(Cron.CronExpression, builder =>
                {
                    builder
                        .InTimeZone(TimeZoneInfo.FindSystemTimeZoneById(Cron.TimeZoneId));
                });
            else if (Simp != null)
                triggerBuilder = triggerBuilder.WithSimpleSchedule(builder =>
                {
                    builder
                        .WithInterval(Simp.RepeatInterval)
                        .WithRepeatCount(Simp.RepeatCount);
                });
            else if (Cal != null)
                triggerBuilder = triggerBuilder.WithCalendarIntervalSchedule(builder =>
                {
                    builder
                        .WithInterval(Cal.RepeatInterval, Cal.RepeatIntervalUnit)
                        .InTimeZone(TimeZoneInfo.FindSystemTimeZoneById(Cal.TimeZoneId))
                        .PreserveHourOfDayAcrossDaylightSavings(Cal.PreserveHourOfDayAcrossDaylightSavings)
                        .SkipDayIfHourDoesNotExist(Cal.SkipDayIfHourDoesNotExist);
                });
            else if (Day != null)
                triggerBuilder = triggerBuilder.WithDailyTimeIntervalSchedule(builder =>
                {
                    builder
                        .WithRepeatCount(Day.RepeatCount)
                        .WithInterval(Day.RepeatInterval, Day.RepeatIntervalUnit)
                        .InTimeZone(TimeZoneInfo.FindSystemTimeZoneById(Day.TimeZoneId))
                        .EndingDailyAt(Day.EndTimeOfDay)
                        .StartingDailyAt(Day.StartTimeOfDay)
                        .OnDaysOfTheWeek(Day.DaysOfWeek);
                });

            var trigger = triggerBuilder.Build();

            var returnTrigger = (IOperableTrigger)trigger;
            returnTrigger.SetNextFireTimeUtc(NextFireTimeUtc);
            returnTrigger.SetPreviousFireTimeUtc(PreviousFireTimeUtc);
            returnTrigger.FireInstanceId = FireInstanceId;

            return returnTrigger;
        }

        public void UpdateFireTimes(ITrigger trig)
        {
            NextFireTimeUtc = trig.GetNextFireTimeUtc();
            PreviousFireTimeUtc = trig.GetPreviousFireTimeUtc();
            if (NextFireTimeUtc != null) NextFireTimeTicks = NextFireTimeUtc.Value.UtcTicks;
        }
    }
}