using System;
using System.Net;
using System.Runtime.ExceptionServices;
using System.Threading.Tasks;
using Microsoft.Azure.Documents.ChangeFeedProcessor.PartitionManagement;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using SqlDataMovementLib.Sink;
using SqlDataMovementLib.Source;

namespace CosmosDbDataMovement
{
    class Program
    {
        static void Main(string[] args)
        {
            MainAsync(args).GetAwaiter().GetResult();
        }

        static async Task MainAsync(string[] args)
        {
            try
            {
                //setup our DI
                var serviceProvider = new ServiceCollection()
                    .AddLogging((builder) =>
                    {
                        builder
                        .AddConsole()
                        .SetMinimumLevel(LogLevel.Information);
                    })
                    .AddSingleton<CosmosDbSink>()
                    .BuildServiceProvider();

                var loggerFactory = serviceProvider.GetService<ILoggerFactory>();
                var logger = loggerFactory.CreateLogger<Program>();
                logger.LogDebug("Starting application");

                AppDomain.CurrentDomain.FirstChanceException +=
                    new FirstChanceExceptionLogger(loggerFactory).FirstChanceHandler;

                // Set the maximum number of concurrent connections
                ServicePointManager.DefaultConnectionLimit = 1200;

                var changeFeed = new ChangeFeed(new CosmosDbSink(loggerFactory), loggerFactory);

                IChangeFeedProcessor processor = await changeFeed.StartAsync();

                logger.LogInformation("Changefeed started successfully");

                Console.WriteLine("Press any key to stop listening for changes...");
                Console.ReadKey();

                await processor.StopAsync();
            }
            catch(Exception error)
            {
                Console.WriteLine("UNHANDLED EXCEPTION: {0}", error);

                // TODO fabianm REMOVE
                Console.ReadKey();
                throw;
            }
        }

        class FirstChanceExceptionLogger
        {
            private readonly ILogger logger;

            public FirstChanceExceptionLogger(ILoggerFactory loggerFactory)
            {
                if (loggerFactory == null) { throw new ArgumentNullException(nameof(loggerFactory)); }

                this.logger = loggerFactory.CreateLogger<FirstChanceExceptionLogger>();
            }

            public void FirstChanceHandler(object source, FirstChanceExceptionEventArgs e)
            {
                this.logger.LogDebug("FirstChanceException event raised in {0}: {1}",
                    AppDomain.CurrentDomain.FriendlyName, e.Exception);
            }
        }
    }
}
