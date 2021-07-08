using System;
using Common.SdkExtensions;
using Common.SinkContracts;
using Microsoft.Azure.Documents.ChangeFeedProcessor.FeedProcessing;
using Microsoft.Azure.EventHubs;
using Microsoft.Extensions.Logging;
using MongoDB.Bson;
using MongoDB.Driver;

namespace Common.ChangeFeed
{
    public class ChangeFeedObserverFactory : IChangeFeedObserverFactory
    {
        private readonly ILogger logger;
        private readonly ILoggerFactory loggerFactory;
        private readonly SqlClientExtension destClient;
        private readonly EventHubClient eventHubClient;
        private readonly IMongoCollection<BsonDocument> destDocStoreCollection;
        private readonly DestinationType? destinationType;
        private readonly ICosmosDbSink cosmosDbSink;
        private readonly int insertRetries;

        public ChangeFeedObserverFactory(
            SqlClientExtension destClient,
            ICosmosDbSink cosmosDbSink,
            ILoggerFactory loggerFactory)
        {
            destinationType = DestinationType.CosmosDB;
            this.destClient = destClient ?? throw new ArgumentNullException(nameof(destClient));
            this.cosmosDbSink = cosmosDbSink ?? throw new ArgumentNullException(nameof(cosmosDbSink));
            this.loggerFactory = loggerFactory ?? throw new ArgumentNullException(nameof(loggerFactory));
            logger = loggerFactory.CreateLogger<ChangeFeedObserverFactory>();
        }

        public ChangeFeedObserverFactory(
            EventHubClient eventHubClient,
            ILoggerFactory loggerFactory)
        {
            destinationType = DestinationType.EventHub;
            this.eventHubClient = eventHubClient ?? throw new ArgumentNullException(nameof(eventHubClient));
            this.loggerFactory = loggerFactory ?? throw new ArgumentNullException(nameof(loggerFactory));
            logger = loggerFactory.CreateLogger<ChangeFeedObserverFactory>();
        }

        public ChangeFeedObserverFactory(
            IMongoCollection<BsonDocument> destDocStoreCollection,
            int insertRetries,
            ICosmosDbSink cosmosDbSink,
            ILoggerFactory loggerFactory)
        {
            destinationType = DestinationType.MongoDB;
            this.destDocStoreCollection = destDocStoreCollection ?? throw new ArgumentNullException(nameof(destDocStoreCollection));
            this.insertRetries = insertRetries;
            this.cosmosDbSink = cosmosDbSink;
            this.loggerFactory = loggerFactory ?? throw new ArgumentNullException(nameof(loggerFactory));
            logger = loggerFactory.CreateLogger<ChangeFeedObserverFactory>();
        }

        public IChangeFeedObserver CreateObserver()
        {
            if (destinationType == null)
            {
                logger.LogError("Destination type is not defined");

                throw new NotSupportedException("Destination type is not defined");
            }

            if (destinationType == DestinationType.EventHub)
            {
                var newConsumer = new EventHubFeedObserver(eventHubClient, loggerFactory);

                return newConsumer;
            }

            if (destinationType == DestinationType.CosmosDB)
            {
                var cosmosDbFeedConsumer =
                    new CosmosDbFeedObserver(
                        destClient,
                        cosmosDbSink,
                        loggerFactory);

                return cosmosDbFeedConsumer;
            }

            if (destinationType == DestinationType.MongoDB)
            {
                var cosmosDbFeedConsumer =
                    new CosmosDbFeedObserver(
                        destDocStoreCollection,
                        insertRetries,
                        cosmosDbSink,
                        loggerFactory);

                return cosmosDbFeedConsumer;
            }

            var message = $"Destination type '{destinationType}' is not supported.";
            logger.LogError(message);
            throw new NotSupportedException(message);
        }
    }
}