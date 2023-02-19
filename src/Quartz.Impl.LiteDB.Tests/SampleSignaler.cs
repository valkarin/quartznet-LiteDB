using System;
using System.Threading;
using System.Threading.Tasks;
using Quartz.Spi;

namespace Quartz.Impl.LiteDB.Tests
{
    public class SampleSignaler : ISchedulerSignaler
    {
        private int _fMisfireCount;

        public Task NotifyTriggerListenersMisfired(ITrigger trigger, CancellationToken cancellationToken = default)
        {
            _fMisfireCount++;
            return Task.CompletedTask;
        }

        public Task NotifySchedulerListenersFinalized(ITrigger trigger,
            CancellationToken cancellationToken = default)
        {
            return Task.CompletedTask;
        }

        public Task NotifySchedulerListenersJobDeleted(JobKey jobKey, CancellationToken cancellationToken = default)
        {
            return Task.CompletedTask;
        }

        public void SignalSchedulingChange(DateTimeOffset? candidateNewNextFireTimeUtc,
            CancellationToken cancellationToken = default)
        {
        }

        public Task NotifySchedulerListenersError(string message, SchedulerException jpe,
            CancellationToken cancellationToken = default)
        {
            return Task.CompletedTask;
        }
    }
}