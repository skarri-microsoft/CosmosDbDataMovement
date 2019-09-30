using System;
using System.Collections.Generic;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Azure.Documents;
using Microsoft.Azure.Documents.ChangeFeedProcessor.FeedProcessing;
using Microsoft.Azure.EventHubs;
using Microsoft.Extensions.Logging;

namespace Common.ChangeFeed
{
    public class EventHubFeedObserver : IChangeFeedObserver
    {
        const int MaxBatchSizeInBytes = 2 * 1000 * 1000;
        const int MaxCompressedSizeInBytes = 256 * 1000;
        const string AzureBlobErrorsContainer = "EventHubSinkErrorsData";

        private readonly ILogger logger;
        private readonly EventHubClient eventHubClient;

        public EventHubFeedObserver(
            EventHubClient eventHubClient,
            ILoggerFactory loggerFactory)
        {
            if (eventHubClient == null) { throw new ArgumentNullException(nameof(eventHubClient)); }
            if (loggerFactory == null) { throw new ArgumentNullException(nameof(loggerFactory)); }

            this.logger = loggerFactory.CreateLogger<EventHubFeedObserver>();
            this.eventHubClient = eventHubClient;
        }

        public Task OpenAsync(IChangeFeedObserverContext context)
        {
            if (context == null) throw new ArgumentNullException(nameof(context));

            this.logger.LogInformation("Observer opened, {0}", context.PartitionKeyRangeId);

            return Task.CompletedTask;
        }

        public Task CloseAsync(IChangeFeedObserverContext context, ChangeFeedObserverCloseReason reason)
        {
            this.logger.LogInformation("Observer closed, {0} - Reason for shutdown {1}",
                context.PartitionKeyRangeId,
                reason);

            return Task.CompletedTask;
        }

        public async Task ProcessChangesAsync(
            IChangeFeedObserverContext context,
            IReadOnlyList<Document> docs,
            CancellationToken cancellationToken)
        {
            int batchSizeInBytes = 0;
            StringBuilder data = new StringBuilder();
            foreach (Document doc in docs)
            {
                if (cancellationToken.IsCancellationRequested)
                {
                    return;
                }

                try
                {
                    int docSize = doc.ToByteArray().Length;

                    if (batchSizeInBytes + docSize > MaxBatchSizeInBytes)
                    {
                        //Flush it
                        await this.SendCompressedMessageAsync(data.ToString());

                        // Reset buffer and batch size
                        data = new StringBuilder();
                        batchSizeInBytes = docSize;
                        data.AppendLine(doc.ToString());
                    }
                    else
                    {
                        data.AppendLine(doc.ToString());
                        batchSizeInBytes = batchSizeInBytes + doc.ToByteArray().Length;
                    }
                }
                catch (Exception e)
                {
                    this.logger.LogError(
                        "Update failed for partition {0} - docs count: {1} - message : {2} - Complete Exception {3}",
                        context.PartitionKeyRangeId,
                        docs.Count,
                        e.Message,
                        e);
                }
            }

            if (batchSizeInBytes > 0)
            {
                await this.SendCompressedMessageAsync(data.ToString());
            }
        }

        private async Task SendCompressedMessageAsync(string data)
        {
            byte[] compressed = await DataCompression.GetGZipContentInBytesAsync(data);
            // Event hub only takes only , 256byte per size
            // bigger ones please save to azure blob
            if (compressed.Length > MaxCompressedSizeInBytes)
            {
                // Write to blob
                new AzureBlobUploader(AzureBlobErrorsContainer).UploadBlob(compressed, System.Guid.NewGuid().ToString() + ".zip",
                    false);
            }
            else
            {
                // Send to event hub
                await this.eventHubClient.SendAsync(new EventData(compressed));
            }
        }
    }
}