using System;
using System.Collections.Generic;
using LiteDB;

namespace Quartz.Impl.LiteDB.Domains
{
    public enum SchedulerState
    {
        Unknown,
        Started,
        Paused,
        Resumed,
        Shutdown
    }

    public class Scheduler
    {
        public ObjectId Id { get; set; }
        
        public string InstanceName { get; set; }

        public DateTimeOffset LastCheckinTime { get; set; } = DateTimeOffset.MinValue;

        public DateTimeOffset CheckinInterval { get; set; } = DateTimeOffset.MinValue;

        public SchedulerState State { get; set; } = SchedulerState.Started;

        public Dictionary<string, ICalendar> Calendars { get; set; } = new Dictionary<string, ICalendar>();

        public HashSet<string> PausedJobGroups { get; set; } = new HashSet<string>();

        public HashSet<string> BlockedJobs { get; set; } = new HashSet<string>();
    }
}