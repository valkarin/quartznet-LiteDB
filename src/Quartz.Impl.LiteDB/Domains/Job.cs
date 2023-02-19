using System;
using System.Collections.Generic;
using LiteDB;

namespace Quartz.Impl.LiteDB.Domains
{
    /// <summary>
    ///     A database serializable variant of <see cref="IJobDetail"/>.
    /// </summary>
    internal class Job
    {
        // Make Type.GetType happy.
        internal Job()
        {
        }

        public Job(IJobDetail newJob, string schedulerInstanceName)
        {
            if (newJob == null) return;

            Name = newJob.Key.Name;
            Group = newJob.Key.Group;
            
            Scheduler = schedulerInstanceName;

            Description = newJob.Description;
            JobType = newJob.JobType;
            Durable = newJob.Durable;
            ConcurrentExecutionDisallowed = newJob.ConcurrentExecutionDisallowed;
            PersistJobDataAfterExecution = newJob.PersistJobDataAfterExecution;
            RequestsRecovery = newJob.RequestsRecovery;
            JobDataMap = new Dictionary<string, object>(newJob.JobDataMap.WrappedMap);
        }
        
        public ObjectId Id { get; set; }

        public string Name { get; set; }
        public string Group { get; set; }
        public string Key => $"{Name}/{Group}";
        public string Scheduler { get; set; }

        public string Description { get; set; }
        
        public Type JobType { get; set; }
        public bool Durable { get; set; }
        public bool ConcurrentExecutionDisallowed { get; set; }
        public bool PersistJobDataAfterExecution { get; set; }
        public bool RequestsRecovery { get; set; }
        public IDictionary<string, object> JobDataMap { get; set; }

        /// <summary>
        ///     Converts this <see cref="Job"/> back into an <see cref="IJobDetail"/>.
        /// </summary>
        /// <returns>The built <see cref="IJobDetail"/>.</returns>
        public IJobDetail Deserialize()
        {
            return JobBuilder.Create()
                .WithIdentity(Name, Group)
                .WithDescription(Description)
                .OfType(JobType)
                .RequestRecovery(RequestsRecovery)
                .SetJobData(new JobDataMap(JobDataMap))
                .StoreDurably(Durable)
                .Build();

            // A JobDetail doesn't have builder methods for two properties:   IsNonConcurrent,IsUpdateData
            // they are determined according to attributes on the job class
        }
    }
}