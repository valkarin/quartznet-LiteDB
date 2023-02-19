namespace Quartz.Impl.LiteDB.Domains
{
    public partial class Trigger
    {
        public class CronOptions
        {
            public string CronExpression { get; set; }
            public string TimeZoneId { get; set; }
        }
    }
}