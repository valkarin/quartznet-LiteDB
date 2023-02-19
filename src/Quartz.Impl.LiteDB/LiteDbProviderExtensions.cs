using System;
using JetBrains.Annotations;
using Quartz.Util;

namespace Quartz.Impl.LiteDB
{
    public static class LiteDbProviderExtensions
    {
        [UsedImplicitly]
        public static void UseLiteDb(this SchedulerBuilder.PersistentStoreOptions options,
            Action<LiteDbProviderOptions> config = null)
        {
            options.SetProperty(StdSchedulerFactory.PropertyJobStoreType,
                typeof(LiteDbJobStore).AssemblyQualifiedNameWithoutVersion());
            config?.Invoke(new LiteDbProviderOptions(options));
        }
    }
}