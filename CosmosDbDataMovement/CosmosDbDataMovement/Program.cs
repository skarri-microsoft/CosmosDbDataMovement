using System;
using System.Net;
using System.Runtime.ExceptionServices;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using SqlDataMovementLib.Sink;
using SqlDataMovementLib.Source;

namespace CosmosDbDataMovement
{
    public class Program
    {
        static void Main(string[] args)
        {
            MainAsync(CancellationToken.None).GetAwaiter().GetResult();
        }

        internal static async Task MainAsync(CancellationToken token)
        {
            try
            {
                //setup our DI
                using (var serviceProvider = new ServiceCollection().AddLogging(builder =>
                    {
                        builder.AddConsole()
                            .SetMinimumLevel(LogLevel.Information);
                    })
                    .AddSingleton<CosmosDbSink>()
                    .BuildServiceProvider())
                {
                    var loggerFactory = serviceProvider.GetService<ILoggerFactory>();

                    var logger = loggerFactory.CreateLogger<Program>();
                    logger.LogDebug("Starting application");

                    AppDomain.CurrentDomain.FirstChanceException +=
                        new FirstChanceExceptionLogger(loggerFactory).FirstChanceHandler;

                    // Set the maximum number of concurrent connections
                    ServicePointManager.DefaultConnectionLimit = 1200;

                    var changeFeed = new ChangeFeed(new CosmosDbSink(loggerFactory), loggerFactory);

                    var processor = await changeFeed.StartAsync(token);

                    logger.LogInformation("Changefeed started successfully");

                    Console.WriteLine("Press any key to stop listening for changes...");
                    Console.ReadKey();

                    await processor.StopAsync();
                }
            }
            catch(Exception error)
            {
                Console.WriteLine("UNHANDLED EXCEPTION: {0}", error);
                throw;
            }
            
        }

        internal class FirstChanceExceptionLogger
        {
            private readonly ILogger logger;

            public FirstChanceExceptionLogger(ILoggerFactory loggerFactory)
            {
                if (loggerFactory == null) { throw new ArgumentNullException(nameof(loggerFactory)); }

                logger = loggerFactory.CreateLogger<FirstChanceExceptionLogger>();
            }

            public void FirstChanceHandler(object source, FirstChanceExceptionEventArgs e)
            {
                logger.LogDebug(
                    "FirstChanceException event raised in {0}: {1}",
                    AppDomain.CurrentDomain.FriendlyName, 
                    e.Exception);
            }
        }
    }
}
