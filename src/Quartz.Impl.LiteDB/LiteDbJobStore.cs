using System;
using System.Runtime.CompilerServices;
using Quartz.Spi;

namespace Quartz.Impl.LiteDB
{
    public partial class LiteDbJobStore
    {
        private static long _ftrCtr = SystemTime.UtcNow().Ticks;
        private TimeSpan _misfireThreshold = TimeSpan.FromSeconds(5);

        private ISchedulerSignaler _signaler;
        private ITypeLoadHelper _typeLoadHelper;
        
        public string ConnectionString { get; set; }
        
        /// <summary>
        ///     The time span by which a trigger must have missed its
        ///     next-fire-time, in order for it to be considered "misfired" and thus
        ///     have its misfire instruction applied.
        /// </summary>
        [TimeSpanParseRule(TimeSpanParseRule.Milliseconds)]
        public virtual TimeSpan MisfireThreshold
        {
            [MethodImpl(MethodImplOptions.Synchronized)]
            get => _misfireThreshold;
            [MethodImpl(MethodImplOptions.Synchronized)]
            set
            {
                if (value.TotalMilliseconds < 1) throw new ArgumentException("MisfireThreshold must be larger than 0");
                _misfireThreshold = value;
            }
        }

        public bool SupportsPersistence => true;
        public long EstimatedTimeToReleaseAndAcquireTrigger => 100;
        public bool Clustered => false;
        public string InstanceId { get; set; } = "instance_two";
        public string InstanceName { get; set; } = "UnitTestScheduler";
        public int ThreadPoolSize { get; set; }
    }
}