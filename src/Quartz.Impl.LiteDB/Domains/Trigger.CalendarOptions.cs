namespace Quartz.Impl.LiteDB.Domains
{
    public partial class Trigger
    {
        public class CalendarOptions
        {
            public IntervalUnit RepeatIntervalUnit { get; set; }
            public int RepeatInterval { get; set; }
            public int TimesTriggered { get; set; }
            public string TimeZoneId { get; set; }
            public bool PreserveHourOfDayAcrossDaylightSavings { get; set; }
            public bool SkipDayIfHourDoesNotExist { get; set; }
        }
    }
}