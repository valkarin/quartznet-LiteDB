using System;
using System.Collections.Generic;

namespace Quartz.Impl.LiteDB.Domains
{
    public partial class Trigger
    {
        public class DailyTimeOptions
        {
            public int RepeatCount { get; set; }

            public IntervalUnit RepeatIntervalUnit { get; set; }

            public int RepeatInterval { get; set; }
            
            public TimeOfDay StartTimeOfDay { get; set; }
            
            public TimeOfDay EndTimeOfDay { get; set; }

            public IReadOnlyCollection<DayOfWeek> DaysOfWeek { get; set; }

            public int TimesTriggered { get; set; }

            public string TimeZoneId { get; set; }
        }
    }
}