using System;
using Common.SdkExtensions;
using Common.SinkContracts;
using Microsoft.Azure.EventHubs;
using Microsoft.Extensions.Logging;
using MongoDB.Bson;
using MongoDB.Driver;
using IChangeFeedObserver = Microsoft.Azure.Documents.ChangeFeedProcessor.FeedProcessing.IChangeFeedObserver;
using IChangeFeedObserverFactory = Microsoft.Azure.Documents.ChangeFeedProcessor.FeedProcessing.IChangeFeedObserverFactory;

namespace Common.ChangeFeed
{
    public class ChangeFeedObserverFactory : IChangeFeedObserverFactory
    {
        private readonly ILogger logger;
        private readonly ILoggerFactory loggerFactory;
        private readonly SqlClientExtension destClient;
        private readonly EventHubClient eventHubClient;
        private readonly IMongoCollection<BsonDocument> destDocStoreCollection;
        private readonly DestinationType? destinationType = null;
        private readonly ICosmosDbSink cosmosDbSink;
        private readonly int insertRetries;

        public ChangeFeedObserverFactory(
            SqlClientExtension destClient,
            ICosmosDbSink cosmosDbSink,
            ILoggerFactory loggerFactory)
        {
            if (destClient == null) { throw new ArgumentNullException(nameof(destClient)); }
            if (cosmosDbSink == null) { throw new ArgumentNullException(nameof(cosmosDbSink)); }
            if (loggerFactory == null) { throw new ArgumentNullException(nameof(loggerFactory)); }

            this.destinationType = DestinationType.CosmosDB;
            this.destClient = destClient;
            this.cosmosDbSink = cosmosDbSink;
            this.loggerFactory = loggerFactory;
            this.logger = loggerFactory.CreateLogger<ChangeFeedObserverFactory>();
        }

        public ChangeFeedObserverFactory(
            EventHubClient eventHubClient,
            ILoggerFactory loggerFactory)
        {
            if (eventHubClient == null) { throw new ArgumentNullException(nameof(eventHubClient)); }
            if (loggerFactory == null) { throw new ArgumentNullException(nameof(loggerFactory)); }
            this.destinationType = DestinationType.EventHub;
            this.eventHubClient = eventHubClient;
            this.loggerFactory = loggerFactory;
            this.logger = loggerFactory.CreateLogger<ChangeFeedObserverFactory>(); ;
        }

        public ChangeFeedObserverFactory(
            IMongoCollection<BsonDocument> destDocStoreCollection,
            int insertRetries,
            ICosmosDbSink cosmosDbSink,
            ILoggerFactory loggerFactory)
        {
            if (destDocStoreCollection == null) { throw new ArgumentNullException(nameof(destDocStoreCollection)); }
            if (loggerFactory == null) { throw new ArgumentNullException(nameof(loggerFactory)); }

            this.destinationType = DestinationType.MongoDB;
            this.destDocStoreCollection = destDocStoreCollection;
            this.insertRetries = insertRetries;
            this.cosmosDbSink = cosmosDbSink;
            this.loggerFactory = loggerFactory;
            this.logger = loggerFactory.CreateLogger<ChangeFeedObserverFactory>();
        }

        public IChangeFeedObserver CreateObserver()
        {
            if (destinationType == null)
            {
                this.logger.LogError("Destination type is not defined");

                throw new NotSupportedException("Destination type is not defined");
            }

            if (this.destinationType == DestinationType.EventHub)
            {
                EventHubFeedObserver newConsumer = new EventHubFeedObserver(this.eventHubClient, this.loggerFactory);

                return newConsumer;
            }
            else if (this.destinationType == DestinationType.CosmosDB)
            {
                CosmosDbFeedObserver cosmosDbFeedConsumer =
                    new CosmosDbFeedObserver(
                        this.destClient,
                        this.cosmosDbSink,
                        this.loggerFactory);

                return cosmosDbFeedConsumer;
            }
            else if (this.destinationType == DestinationType.MongoDB)
            {
                CosmosDbFeedObserver cosmosDbFeedConsumer =
                    new CosmosDbFeedObserver(
                        this.destDocStoreCollection,
                        insertRetries,
                        this.cosmosDbSink,
                        this.loggerFactory);

                return cosmosDbFeedConsumer;
            }

            string message = $"Destination type '{this.destinationType}' is not supported.";
            this.logger.LogError(message);
            throw new NotSupportedException(message);
        }
    }
}