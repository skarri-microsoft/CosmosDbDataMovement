using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Net;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.WindowsAzure;
using Microsoft.WindowsAzure.Diagnostics;
using Microsoft.WindowsAzure.ServiceRuntime;
using Microsoft.WindowsAzure.Storage;

namespace CosmosDbSqlDataMovement
{
    using SqlDataMovementLib.Sink;
    using SqlDataMovementLib.Source;

    public class WorkerRole : RoleEntryPoint
    {
        private readonly CancellationTokenSource cancellationTokenSource = new CancellationTokenSource();
        private readonly ManualResetEvent runCompleteEvent = new ManualResetEvent(false);

        public override void Run()
        {
            Trace.TraceInformation("CosmosDbSqlDataMovement is running");

            try
            {
                this.RunAsync(this.cancellationTokenSource.Token).Wait();
            }
            finally
            {
                this.runCompleteEvent.Set();
            }
        }

        public override bool OnStart()
        {
            // Set the maximum number of concurrent connections
            ServicePointManager.DefaultConnectionLimit = 12;

            // For information on handling configuration changes
            // see the MSDN topic at https://go.microsoft.com/fwlink/?LinkId=166357.

            bool result = base.OnStart();

            Trace.TraceInformation("CosmosDbSqlDataMovement has been started");

            return result;
        }

        public override void OnStop()
        {
            Trace.TraceInformation("CosmosDbSqlDataMovement is stopping");

            this.cancellationTokenSource.Cancel();
            this.runCompleteEvent.WaitOne();

            base.OnStop();

            Trace.TraceInformation("CosmosDbSqlDataMovement has stopped");
        }

        private async Task RunAsync(CancellationToken cancellationToken)
        {
            await new ChangeFeed(new CosmosDbSink()).StartAsync();

            await Task.Delay(Int32.MaxValue);
        }
    }
}
