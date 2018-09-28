namespace Common.ChangeFeed
{
    using System;
    using System.Collections.Generic;
    using System.Diagnostics;
    using System.Threading;
    using System.Threading.Tasks;
    using Microsoft.Azure.Documents;
    using Microsoft.Azure.Documents.ChangeFeedProcessor;
    using Microsoft.Azure.Documents.ChangeFeedProcessor.FeedProcessing;
    using Microsoft.Azure.Documents.Client;
    using SdkExtensions;
    using SinkContracts;
    using ChangeFeedObserverCloseReason = Microsoft.Azure.Documents.ChangeFeedProcessor.FeedProcessing.ChangeFeedObserverCloseReason;
    using IChangeFeedObserver = Microsoft.Azure.Documents.ChangeFeedProcessor.FeedProcessing.IChangeFeedObserver;

    public class CosmosDbFeedObserver : IChangeFeedObserver
    {
        private readonly SqlClientExtension client;
        private readonly Uri destinationCollectionUri;
        private ICosmosDbSink cosmosDbSink;
        private bool isBulkIngestion;


        public CosmosDbFeedObserver(
            SqlClientExtension client, 
            ICosmosDbSink cosmosDbSink)
        {
            this.client = client;
            this.destinationCollectionUri = UriFactory.CreateDocumentCollectionUri(
                client.DatabaseName, 
                client.CollectionName);
            this.cosmosDbSink = cosmosDbSink;
            this.isBulkIngestion = ConfigHelper.IsBulkIngestion();
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
            if (!isBulkIngestion)
            {
                cosmosDbSink.IngestDocs(
                    this.client,
                    context,
                    docs,
                    cancellationToken,
                    this.destinationCollectionUri);
            }
            else
            {
                cosmosDbSink.IngestDocsInBulk(
                    this.client,
                    context,
                    docs,
                    cancellationToken,
                    this.destinationCollectionUri);
            }
        }
    }
}
