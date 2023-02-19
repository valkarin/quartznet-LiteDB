using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Quartz.Impl.LiteDB;
using Serilog;
using Serilog.Events;

namespace Quartz.Impl.LiteDb.WorkerExample
{
    public class Program
    {
        public static void Main(string[] args)
        {
            Log.Logger = new LoggerConfiguration()
                .MinimumLevel.Verbose()
                .MinimumLevel.Override("Microsoft", LogEventLevel.Information)
                .Enrich.FromLogContext()
                .WriteTo.Console()
                .CreateLogger();
            
            CreateHostBuilder(args).Build().Run();
        }

        public static IHostBuilder CreateHostBuilder(string[] args) =>
            Host.CreateDefaultBuilder(args)
                .ConfigureServices((hostContext, services) =>
                {
                    
                    services.AddHostedService<Worker>();

                    services.AddQuartz(q =>
                        {
                            q.UseMicrosoftDependencyInjectionJobFactory();

                            q.UseDefaultThreadPool(tp => { tp.MaxConcurrency = 10; });

                            q.UsePersistentStore(s =>
                            {
                                s.UseLiteDb(options =>
                                {
                                    options.ConnectionString = "Filename=WorkerExample.db;";
                                });
                                s.UseBinarySerializer();
                            });
                        }
                    );

                    services.AddQuartzHostedService(
                        q => q.WaitForJobsToComplete = true);
                });
    }
}