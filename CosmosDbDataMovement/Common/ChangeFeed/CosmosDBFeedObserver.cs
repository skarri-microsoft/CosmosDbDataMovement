using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using Common.SdkExtensions;
using Common.SinkContracts;
using Microsoft.Azure.Documents;
using Microsoft.Azure.Documents.ChangeFeedProcessor.FeedProcessing;
using Microsoft.Azure.Documents.Client;
using Microsoft.Extensions.Logging;
using MongoDB.Bson;
using MongoDB.Driver;
using ChangeFeedObserverCloseReason = Microsoft.Azure.Documents.ChangeFeedProcessor.FeedProcessing.ChangeFeedObserverCloseReason;
using IChangeFeedObserver = Microsoft.Azure.Documents.ChangeFeedProcessor.FeedProcessing.IChangeFeedObserver;

namespace Common.ChangeFeed
{
    public class CosmosDbFeedObserver : IChangeFeedObserver
    {
        private readonly SqlClientExtension client;
        private readonly Uri destinationCollectionUri;
        private readonly ILogger logger;
        private readonly ICosmosDbSink cosmosDbSink;
        private readonly IMongoCollection<BsonDocument> destDocStoreCollection;
        private readonly bool isBulkIngestion;
        private readonly int insertRetries;

        public CosmosDbFeedObserver(
            SqlClientExtension client, 
            ICosmosDbSink cosmosDbSink,
            ILoggerFactory loggerFactory)
        {
            if (client == null) { throw new ArgumentNullException(nameof(client)); }
            if (cosmosDbSink == null) { throw new ArgumentNullException(nameof(cosmosDbSink)); }
            if (loggerFactory == null) { throw new ArgumentNullException(nameof(loggerFactory)); }

            this.logger = loggerFactory.CreateLogger<CosmosDbFeedObserver>();
            this.client = client;
            this.destinationCollectionUri = UriFactory.CreateDocumentCollectionUri(
                client.DatabaseName, 
                client.CollectionName);
            this.cosmosDbSink = cosmosDbSink;
            this.isBulkIngestion = ConfigHelper.IsBulkIngestion();
        }

        public CosmosDbFeedObserver(
            IMongoCollection<BsonDocument> destDocStoreCollection,
            int insertRetries,
            ICosmosDbSink cosmosDbSink,
            ILoggerFactory loggerFactory)
        {
            if (destDocStoreCollection == null) { throw new ArgumentNullException(nameof(destDocStoreCollection)); }
            if (cosmosDbSink == null) { throw new ArgumentNullException(nameof(cosmosDbSink)); }
            if (loggerFactory == null) { throw new ArgumentNullException(nameof(loggerFactory)); }

            this.logger = loggerFactory.CreateLogger<CosmosDbFeedObserver>();
            this.destDocStoreCollection = destDocStoreCollection;
            this.insertRetries = insertRetries;
            this.cosmosDbSink = cosmosDbSink;
            this.isBulkIngestion = ConfigHelper.IsBulkIngestion();
        }

        public Task OpenAsync(IChangeFeedObserverContext context)
        {
            this.logger.LogInformation("Observer opened, {0}", context.PartitionKeyRangeId);
            return Task.CompletedTask;
        }

        public Task CloseAsync(IChangeFeedObserverContext context, ChangeFeedObserverCloseReason reason)
        {
            this.logger.LogInformation("Observer closed, {0}", context.PartitionKeyRangeId);
            this.logger.LogInformation("Reason for shutdown, {0}", reason);
            return Task.CompletedTask;
        }

        public async Task ProcessChangesAsync(
            IChangeFeedObserverContext context,
            IReadOnlyList<Document> docs,
            CancellationToken cancellationToken)
        {
            if (this.client != null)
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
            else if(destDocStoreCollection!=null)
            {
               
                    cosmosDbSink.IngestDocs(
                        this.destDocStoreCollection,
                        insertRetries,
                        context,
                        docs,
                        cancellationToken,
                        this.destinationCollectionUri);
              
            }
        }
    }
}
