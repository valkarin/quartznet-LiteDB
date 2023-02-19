﻿using System;
using System.Collections.Specialized;
using System.Threading;

namespace Quartz.Impl.LiteDB.ConsoleExample
{
    class Program
    {
        static void Main(string[] args)
        {
            var properties = new NameValueCollection
            {
                // Setting some scheduler properties
                ["quartz.scheduler.instanceName"] = "QuartzRavenDBDemo",
                ["quartz.scheduler.instanceId"] = "instance_one",
                ["quartz.threadPool.type"] = "Quartz.Simpl.SimpleThreadPool, Quartz",
                ["quartz.threadPool.threadCount"] = "1",
                ["quartz.threadPool.threadPriority"] = "Normal",
                // Setting LiteDB as the persisted JobStore
                ["quartz.jobStore.type"] = "Quartz.Impl.LiteDB.LiteDbJobStore, Quartz.Impl.LiteDB",
                // One or more URLs to database server
                ["quartz.jobStore.connectionString"] = $"Filename=ConsoleExample.db;",
                ["quartz.serializer.type"] = "json"
            };

            try
            {
                ISchedulerFactory sf = new StdSchedulerFactory(properties);
                var scheduler = sf.GetScheduler().Result;
                scheduler.Start();

                var emptyFridgeJob = JobBuilder.Create<EmptyFridge>()
                    .WithIdentity("EmptyFridgeJob", "Office")
                    .RequestRecovery()
                    .Build();

                var turnOffLightsJob = JobBuilder.Create<TurnOffLights>()
                    .WithIdentity("TurnOffLightsJob", "Office")
                    .RequestRecovery()
                    .Build();

                var checkAliveJob = JobBuilder.Create<CheckAlive>()
                    .WithIdentity("CheckAliveJob", "Office")
                    .RequestRecovery()
                    .Build();

                var visitJob = JobBuilder.Create<Visit>()
                    .WithIdentity("VisitJob", "Office")
                    .RequestRecovery()
                    .Build();

                // Weekly, Friday at 10 AM (Cron Trigger)
                var emptyFridgeTrigger = TriggerBuilder.Create()
                    .WithIdentity("EmptyFridge", "Office")
                    .WithCronSchedule("0 0 10 ? * FRI")
                    .ForJob("EmptyFridgeJob", "Office")
                    .Build();

                // Daily at 6 PM (Daily Interval Trigger)
                var turnOffLightsTrigger = TriggerBuilder.Create()
                    .WithIdentity("TurnOffLights", "Office")
                    .WithDailyTimeIntervalSchedule(s => s
                        .WithIntervalInHours(24)
                        .OnEveryDay()
                        .StartingDailyAt(TimeOfDay.HourAndMinuteOfDay(18, 0)))
                    .Build();

                // Periodic check every 10 seconds (Simple Trigger)
                var checkAliveTrigger = TriggerBuilder.Create()
                    .WithIdentity("CheckAlive", "Office")
                    .StartAt(DateTime.UtcNow.AddSeconds(3))
                    .WithSimpleSchedule(x => x
                        .WithIntervalInSeconds(10)
                        .RepeatForever())
                    .Build();

                var visitTrigger = TriggerBuilder.Create()
                    .WithIdentity("Visit", "Office")
                    .StartAt(DateTime.UtcNow.AddSeconds(3))
                    .Build();


                scheduler.ScheduleJob(checkAliveJob, checkAliveTrigger);
                scheduler.ScheduleJob(emptyFridgeJob, emptyFridgeTrigger);
                scheduler.ScheduleJob(turnOffLightsJob, turnOffLightsTrigger);
                scheduler.ScheduleJob(visitJob, visitTrigger);

                // some sleep to show what's happening
                Thread.Sleep(TimeSpan.FromSeconds(600));

                scheduler.Shutdown();
            }
            catch (SchedulerException se)
            {
                Console.WriteLine(se);
            }

            Console.WriteLine("Press any key to close the application");
            Console.ReadKey();
        }
    }
}