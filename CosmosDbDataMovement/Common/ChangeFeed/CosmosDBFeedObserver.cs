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
            if (loggerFactory == null) { throw new ArgumentNullException(nameof(loggerFactory)); }

            logger = loggerFactory.CreateLogger<CosmosDbFeedObserver>();
            this.client = client ?? throw new ArgumentNullException(nameof(client));
            destinationCollectionUri = UriFactory.CreateDocumentCollectionUri(
                client.DatabaseName, 
                client.CollectionName);
            this.cosmosDbSink = cosmosDbSink ?? throw new ArgumentNullException(nameof(cosmosDbSink));
            isBulkIngestion = ConfigHelper.IsBulkIngestion();
        }

        public CosmosDbFeedObserver(
            IMongoCollection<BsonDocument> destDocStoreCollection,
            int insertRetries,
            ICosmosDbSink cosmosDbSink,
            ILoggerFactory loggerFactory)
        {
            if (loggerFactory == null) { throw new ArgumentNullException(nameof(loggerFactory)); }

            logger = loggerFactory.CreateLogger<CosmosDbFeedObserver>();
            this.destDocStoreCollection = destDocStoreCollection ?? throw new ArgumentNullException(nameof(destDocStoreCollection));
            this.insertRetries = insertRetries;
            this.cosmosDbSink = cosmosDbSink ?? throw new ArgumentNullException(nameof(cosmosDbSink));
            isBulkIngestion = ConfigHelper.IsBulkIngestion();
        }

        public Task OpenAsync(IChangeFeedObserverContext context)
        {
            logger.LogInformation("Observer opened, {0}", context.PartitionKeyRangeId);
            return Task.CompletedTask;
        }

        public Task CloseAsync(IChangeFeedObserverContext context, ChangeFeedObserverCloseReason reason)
        {
            logger.LogInformation("Observer closed, {0}", context.PartitionKeyRangeId);
            logger.LogInformation("Reason for shutdown, {0}", reason);
            return Task.CompletedTask;
        }

        public async Task ProcessChangesAsync(
            IChangeFeedObserverContext context,
            IReadOnlyList<Document> docs,
            CancellationToken cancellationToken)
        {
            if (client != null)
            {
                if (!isBulkIngestion)
                {
                    await cosmosDbSink.IngestDocsAsync(
                        client,
                        context,
                        docs,
                        destinationCollectionUri, 
                        cancellationToken);
                }
                else
                {
                    await cosmosDbSink.IngestDocsInBulkAsync(
                        client,
                        context,
                        docs,
                        destinationCollectionUri, 
                        cancellationToken);
                }
            }
            else if(destDocStoreCollection != null)
            {
               
                await cosmosDbSink.IngestDocsAsync(
                        destDocStoreCollection,
                        insertRetries,
                        context,
                        docs,
                        destinationCollectionUri, 
                        cancellationToken);
              
            }
        }
    }
}
