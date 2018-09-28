namespace Common.ChangeFeed
{
    using System;
    using System.Collections.Generic;
    using System.Diagnostics;
    using System.Text;
    using System.Threading;
    using System.Threading.Tasks;
    using Microsoft.Azure.Documents;
    using Microsoft.Azure.Documents.ChangeFeedProcessor.FeedProcessing;
    using Microsoft.Azure.EventHubs;

    public class EventHubFeedObserver : IChangeFeedObserver
    {

        private EventHubClient eventHubClient;
        private int maxBatchSizeInBytes = 2 * 1000 * 1000;
        private int maxCompressedSizeInBytes = 256 * 1000;
        private string azureBlobErrorsContainer = "EventHubSinkErrorsData";


        public EventHubFeedObserver(EventHubClient eventHubClient)
        {
            this.eventHubClient = eventHubClient;
        }

        public Task OpenAsync(IChangeFeedObserverContext context)
        {
            Trace.TraceInformation("Observer opened, {0}", context.PartitionKeyRangeId);
            return Task.CompletedTask;
        }

        public Task CloseAsync(IChangeFeedObserverContext context, ChangeFeedObserverCloseReason reason)
        {
            Trace.TraceInformation("Observer closed, {0}", context.PartitionKeyRangeId);
            Trace.TraceInformation("Reason for shutdown, {0}", reason);
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

                    if (batchSizeInBytes + docSize > this.maxBatchSizeInBytes)
                    {
                        //Flush it
                        SendCompressedMessage(data.ToString());

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
                    Trace.TraceError(
                        "Update failed for partition {0} - docs count: {1} - message : {2} - stack trace {3}",
                        context.PartitionKeyRangeId,
                        docs.Count,
                        e.Message,
                        e.StackTrace);

                }
            }

            if (batchSizeInBytes > 0)
            {
                SendCompressedMessage(data.ToString());
            }


        }

        private void SendCompressedMessage(string data)
        {
            byte[] compressed = DataCompression.GetGZipContentInBytes(data);
            // Event hub only takes only , 256byte per size
            // bigger ones please save to azure blob
            if (compressed.Length > this.maxCompressedSizeInBytes)
            {
                // Write to blob
                new AzureBlobUploader(this.azureBlobErrorsContainer).UploadBlob(compressed, System.Guid.NewGuid().ToString() + ".zip",
                    false);
            }
            else
            {
                // Send to event hub
                eventHubClient.SendAsync(new EventData(compressed)).Wait();
            }
        }


    }


}