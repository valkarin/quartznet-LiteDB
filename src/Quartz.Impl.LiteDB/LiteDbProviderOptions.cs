namespace Quartz.Impl.LiteDB
{
    public class LiteDbProviderOptions
    {
        private readonly SchedulerBuilder.PersistentStoreOptions _options;

        protected internal LiteDbProviderOptions(SchedulerBuilder.PersistentStoreOptions options)
        {
            _options = options;
        }

        /// <summary>
        ///     The connection string to database to use for the scheduler data.
        /// </summary>
        public string ConnectionString
        {
            set => _options.SetProperty("quartz.jobStore.connectionString", value);
        }
    }
}