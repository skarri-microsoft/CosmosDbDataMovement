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
        internal const int MaxBatchSizeInBytes = 2 * 1000 * 1000;
        internal const int MaxCompressedSizeInBytes = 256 * 1000;
        internal const string AzureBlobErrorsContainer = "EventHubSinkErrorsData";

        private readonly ILogger logger;
        private readonly EventHubClient eventHubClient;

        public EventHubFeedObserver(
            EventHubClient eventHubClient,
            ILoggerFactory loggerFactory)
        {
            if (loggerFactory == null) { throw new ArgumentNullException(nameof(loggerFactory)); }

            logger = loggerFactory.CreateLogger<EventHubFeedObserver>();
            this.eventHubClient = eventHubClient ?? throw new ArgumentNullException(nameof(eventHubClient));
        }

        public Task OpenAsync(IChangeFeedObserverContext context)
        {
            if (context == null) throw new ArgumentNullException(nameof(context));

            logger.LogInformation("Observer opened, {0}", context.PartitionKeyRangeId);

            return Task.CompletedTask;
        }

        public Task CloseAsync(IChangeFeedObserverContext context, ChangeFeedObserverCloseReason reason)
        {
            logger.LogInformation("Observer closed, {0} - Reason for shutdown {1}", context.PartitionKeyRangeId, reason);

            return Task.CompletedTask;
        }

        [System.Diagnostics.CodeAnalysis.SuppressMessage("Design", "CA1031:Do not catch general exception types", Justification = "Should always continue")]
        public async Task ProcessChangesAsync(
            IChangeFeedObserverContext context,
            IReadOnlyList<Document> docs,
            CancellationToken cancellationToken)
        {
            var batchSizeInBytes = 0;
            var data = new StringBuilder();
            foreach (var doc in docs)
            {
                if (cancellationToken.IsCancellationRequested)
                {
                    return;
                }

                try
                {
                    var docSize = doc.ToByteArray().Length;

                    if (batchSizeInBytes + docSize > MaxBatchSizeInBytes)
                    {
                        //Flush it
                        await SendCompressedMessageAsync(data.ToString());

                        // Reset buffer and batch size
                        data = new StringBuilder();
                        batchSizeInBytes = docSize;
                        data.AppendLine(doc.ToString());
                    }
                    else
                    {
                        data.AppendLine(doc.ToString());
                        batchSizeInBytes += doc.ToByteArray().Length;
                    }
                }
                catch (Exception e)
                {
                    logger.LogError(
                        "Update failed for partition {0} - docs count: {1} - message : {2} - Complete Exception {3}",
                        context.PartitionKeyRangeId,
                        docs.Count,
                        e.Message,
                        e);
                }
            }

            if (batchSizeInBytes > 0)
            {
                await SendCompressedMessageAsync(data.ToString());
            }
        }

        private async Task SendCompressedMessageAsync(string data)
        {
            var compressed = await DataCompression.GetGZipContentInBytesAsync(data);
            // Event hub only takes only , 256byte per size
            // bigger ones please save to azure blob
            if (compressed.Length > MaxCompressedSizeInBytes)
            {
                // Write to blob
                var azureBlobUploader = new AzureBlobUploader(AzureBlobErrorsContainer);
                await azureBlobUploader.UploadBlobAsync(compressed, $"{Guid.NewGuid()}.zip", false);
            }
            else
            {
                // Send to event hub
                using (var eventData = new EventData(compressed))
                {
                    await eventHubClient.SendAsync(eventData);
                }
            }
        }
    }
}