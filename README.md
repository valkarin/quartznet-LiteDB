# Quartz.NET-LiteDB
[![Build status](https://ci.appveyor.com/api/projects/status/f72w6n320hkufv8j?svg=true)](https://ci.appveyor.com/project/valkarin/quartznet-litedb)
![Nuget](https://img.shields.io/nuget/v/Quartznet-LiteDB?style=plastic)

JobStore implementation for Quartz.NET scheduler using LiteDB.
## About
[Quartz.NET](https://github.com/quartznet/quartznet) is a full-featured, open source job scheduling system that can be used from smallest apps to large scale enterprise systems.
[Quartz.NET on LiteDB](https://github.com/valkarin/Quartz.Impl.LiteDB) is a new provider written for Quartz.NET which lets us use the [LiteDB](https://github.com/mbdavid/LiteDB/) NoSQL database as the persistent Job Store for scheduling data (instead of the SQL solutions that are built-in Quartz.NET).

[Quartz.NET on LiteDB](https://github.com/valkarin/Quartz.Impl.LiteDB) is a provider that is forked and adapted to LiteDB from [Quartz.NET on RavenDB](https://github.com/ravendb/quartznet-RavenDB).

## Installation
First add scheduling to your app using Quartz.NET ([example](https://www.quartz-scheduler.net/documentation/quartz-3.x/quick-start.html)).
Then install the NuGet [package](https://www.nuget.org/packages/Quartz.Impl.LiteDB/).
## Configuration & Usage
### .NET Framework
In your code, where you would have [normally configured](https://www.quartz-scheduler.net/documentation/quartz-3.x/tutorial/job-stores.html) Quartz to use a persistent job store, you must add the following configuration properties:
```csharp
// In your application where you want to setup the scheduler:
NameValueCollection properties = new NameValueCollection
{
    // Normal scheduler properties
    ["quartz.scheduler.instanceName"] = "TestScheduler",
    ["quartz.scheduler.instanceId"] = "instance_one",
    ["quartz.threadPool.type"] = "Quartz.Simpl.SimpleThreadPool, Quartz",
    ["quartz.threadPool.threadCount"] = "1",
    ["quartz.threadPool.threadPriority"] = "Normal",
    // LiteDB JobStore property
    ["quartz.jobStore.type"] = "Quartz.Impl.LiteDB.LiteDbJobStore, Quartz.Impl.LiteDB"
    // LiteDB Connection String
    ["quartz.jobStore.connectionString"] = "Filename=Quartz-Demo.db;",
};
// Init scheduler with the desired configuration properties
ISchedulerFactory sf = new StdSchedulerFactory(properties);
IScheduler scheduler = sf.GetScheduler();
```
### .NET Core
For use in .NET Core Workers or ASP.NET Core use the recommended Fluent API when setting up a Quartz in `ConfigureServices` like:
```csharp
var configuration = new ConfigurationBuilder()
                        .AddJsonFile("appsettings.json")
                        .Build();
var section = configuration.GetSection("MyService");
var config = section.Get<MyServiceConfig>();
services.AddQuartz(q =>
    {
        q.UseMicrosoftDependencyInjectionJobFactory();
        q.UseDefaultThreadPool(tp =>
        {
            tp.MaxConcurrency = 10;
        });
        q.UsePersistentStore(s =>
        {
            s.UseLiteDb(options =>
            {
                options.ConnectionString = "Filename=Quartz-Worker.db";
            });
            s.UseBinarySerializer();
        });
        
        // Add jobs and triggers as you wish
    }
);
services.AddQuartzHostedService(
    q => q.WaitForJobsToComplete = true);
```
