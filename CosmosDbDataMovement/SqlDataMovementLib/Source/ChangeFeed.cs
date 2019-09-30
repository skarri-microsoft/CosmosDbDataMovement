using System;
using System.Configuration;
using System.Threading.Tasks;
using Common;
using Common.ChangeFeed;
using Common.SdkExtensions;
using Common.SinkContracts;
using Microsoft.Azure.Documents;
using Microsoft.Azure.Documents.ChangeFeedProcessor;
using Microsoft.Azure.Documents.ChangeFeedProcessor.PartitionManagement;
using Microsoft.Azure.Documents.Client;
using Microsoft.Extensions.Logging;
using MongoDB.Bson;
using MongoDB.Driver;

namespace SqlDataMovementLib.Source
{
    public class ChangeFeed
    {
        private readonly ICosmosDbSink cosmosDbSink;
        private readonly ILogger logger;
        private readonly ILoggerFactory loggerFactory;

        private CosmosDbConfig monitorCollection = null;
        private CosmosDbConfig leaseCollection = null;
        private CosmosDbConfig destinationCollection = null;

        private SqlClientExtension monitorClientExtension;
        private SqlClientExtension leaseClientExtension;
        private SqlClientExtension destClientExtension;

        private static MongoClient destMongoClient;
        private static readonly string destDbName = ConfigurationManager.AppSettings["mongodestDbName"];
        private static readonly string destCollectionName = ConfigurationManager.AppSettings["mongodestCollectionName"];
        private static readonly int insertRetries = Int32.Parse(ConfigurationManager.AppSettings["insertRetries"]);
        private static IMongoDatabase destDatabase;
        private static IMongoCollection<BsonDocument> destDocStoreCollection;

        public ChangeFeed(
            ICosmosDbSink cosmosDbSink,
            ILoggerFactory loggerFactory)
        {
            if (cosmosDbSink == null) { throw new ArgumentNullException(nameof(cosmosDbSink)); }
            if (loggerFactory == null) { throw new ArgumentNullException(nameof(loggerFactory)); }

            this.cosmosDbSink = cosmosDbSink;
            this.loggerFactory = loggerFactory;
            this.logger = loggerFactory.CreateLogger<ChangeFeed>();
        }

        public async Task<IChangeFeedProcessor> StartAsync()
        {
            monitorCollection = CosmosDbConfig.GetMonitorConfig(this.logger);
            leaseCollection = CosmosDbConfig.GetLeaseConfig(this.logger);

            monitorClientExtension = new SqlClientExtension(
                monitorCollection,
                ConsistencyLevel.Session,
                ConnectionPolicy.Default);

            leaseClientExtension = new SqlClientExtension(
                leaseCollection,
                ConsistencyLevel.Session,
                ConnectionPolicy.Default);

            DestinationType? destinationType = ConfigHelper.GetDestinationType(this.logger);

            if (destinationType == DestinationType.CosmosDB)
            {
                destinationCollection = CosmosDbConfig.GetDestinationConfig(this.logger);

                destClientExtension = new SqlClientExtension(
                    destinationCollection,
                    ConsistencyLevel.Session,
                    ConnectionPolicy.Default);
            }
            else if (destinationType == DestinationType.MongoDB)
            {
                string destConnectionString =
                    ConfigurationManager.AppSettings["dest-conn"];
                MongoClientSettings destSettings = MongoClientSettings.FromUrl(
                    new MongoUrl(destConnectionString)
                );
                destMongoClient = new MongoClient(destSettings);
                destDatabase = destMongoClient.GetDatabase(destDbName);
                destDocStoreCollection = destDatabase.GetCollection<BsonDocument>(destCollectionName);
            }

            // Not advised to use this code in production.
            this.CreateCollectionsInDevMode();

            return await this.RunChangeFeedHostAsync();
        }

        public void CreateCollectionsInDevMode()
        {
            if (!ConfigHelper.IsDevMode())
            {
                return;
            }
            this.monitorClientExtension.CreateCollectionIfNotExistsAsync().Wait();
            this.leaseClientExtension.CreateCollectionIfNotExistsAsync().Wait();
            if (ConfigHelper.GetDestinationType(this.logger) == DestinationType.CosmosDB)
            {
                this.destClientExtension.CreateCollectionIfNotExistsAsync().Wait();
            }
        }

        public async Task<IChangeFeedProcessor> RunChangeFeedHostAsync()
        {
            // monitored collection info 
            DocumentCollectionInfo monitorDocumentCollectionLocation = new DocumentCollectionInfo
            {
                Uri = new Uri(monitorCollection.AccountEndPoint),
                MasterKey = monitorCollection.Key,
                DatabaseName = monitorCollection.DbName,
                CollectionName = monitorCollection.CollectionName
            };

            // lease collection info 
            DocumentCollectionInfo leaseCollectionLocation = new DocumentCollectionInfo
            {
                Uri = new Uri(leaseCollection.AccountEndPoint),
                MasterKey = leaseCollection.Key,
                DatabaseName = leaseCollection.DbName,
                CollectionName = leaseCollection.CollectionName
            };

            DestinationType? destinationType = ConfigHelper.GetDestinationType(this.logger);

            if (destinationType == null)
            {
                this.logger.LogError("Unsupported destination type. Supported only Cosmos DB and Event Hub. " +
                                "Please update your app.config", true);
            }

            if (destinationType == DestinationType.CosmosDB)
            {
                return await RunCosmosDBSink(monitorDocumentCollectionLocation, leaseCollectionLocation);
            }
            else if (destinationType == DestinationType.EventHub)
            {
                return await RunEventHubSink(monitorDocumentCollectionLocation, leaseCollectionLocation);
            }
            else if (destinationType == DestinationType.MongoDB)
            {
                return await RunMongoDBSink(monitorDocumentCollectionLocation, leaseCollectionLocation);
            }

            throw new NotSupportedException($"Unexpected destination type '{destinationType}'!");
        }

        public async Task<IChangeFeedProcessor> RunCosmosDBSink(
            DocumentCollectionInfo monitorDocumentCollectionInfo,
            DocumentCollectionInfo leaseDocumentCollectionInfo)
        {
            string hostName = Guid.NewGuid().ToString();
            this.logger.LogInformation("Cosmos DB Sink Host name {0}", hostName);

            // destination collection info 
            DocumentCollectionInfo destCollInfo = new DocumentCollectionInfo
            {
                Uri = new Uri(destinationCollection.AccountEndPoint),
                MasterKey = destinationCollection.Key,
                DatabaseName = destinationCollection.DbName,
                CollectionName = destinationCollection.CollectionName
            };

            DocumentClient destClient = new DocumentClient(destCollInfo.Uri, destCollInfo.MasterKey);
            
            ChangeFeedObserverFactory docConsumerFactory = new ChangeFeedObserverFactory(
                destClientExtension,
                cosmosDbSink,
                this.loggerFactory);

            return await GetChangeFeedProcessor(
                docConsumerFactory,
                hostName,
                monitorDocumentCollectionInfo,
                leaseDocumentCollectionInfo);
        }

        public async Task<IChangeFeedProcessor> RunMongoDBSink(
            DocumentCollectionInfo monitorDocumentCollectionInfo,
            DocumentCollectionInfo leaseDocumentCollectionInfo)
        {
            string hostName = Guid.NewGuid().ToString();
            this.logger.LogInformation("Mongo DB Sink Host name {0}", hostName);

            ChangeFeedObserverFactory docConsumerFactory = new ChangeFeedObserverFactory(
                destDocStoreCollection,
                insertRetries,
                cosmosDbSink,
                this.loggerFactory);

            return await GetChangeFeedProcessor(
                docConsumerFactory,
                hostName,
                monitorDocumentCollectionInfo,
                leaseDocumentCollectionInfo);
        }

        public async Task<IChangeFeedProcessor> GetChangeFeedProcessor(
            Microsoft.Azure.Documents.ChangeFeedProcessor.FeedProcessing.IChangeFeedObserverFactory changeFeedObserverFactory,
            string hostName,
            DocumentCollectionInfo monitorDocumentCollectionInfo,
            DocumentCollectionInfo leaseDocumentCollectionInfo)
        {
            ChangeFeedConfig changeFeedConfig=ChangeFeedConfig.GetChangeFeedConfig();
            ChangeFeedProcessorOptions processorOptions = new ChangeFeedProcessorOptions();
            processorOptions.StartFromBeginning = true;
            processorOptions.MaxItemCount = changeFeedConfig.MaxItemCount;
            processorOptions.LeaseRenewInterval = changeFeedConfig.LeaseRenewInterval;

            this.logger.LogInformation("Processor options Starts from Beginning - {0}, Lease renew interval - {1}",
                processorOptions.StartFromBeginning,
                processorOptions.LeaseRenewInterval.ToString());

            var processor = await new ChangeFeedProcessorBuilder()
                .WithObserverFactory(changeFeedObserverFactory)
                .WithHostName(hostName)
                .WithFeedCollection(monitorDocumentCollectionInfo)
                .WithLeaseCollection(leaseDocumentCollectionInfo)
                .WithProcessorOptions(processorOptions)
                .BuildAsync();
            await processor.StartAsync().ConfigureAwait(false);
            return processor;
        }

        public async Task<IChangeFeedProcessor> RunEventHubSink(
            DocumentCollectionInfo monitorDocumentCollectionInfo,
            DocumentCollectionInfo leaseDocumentCollectionInfo)
        {
            string hostName = Guid.NewGuid().ToString();
            this.logger.LogInformation("Event Hub Sink Host name {0}", hostName);
            ChangeFeedObserverFactory docConsumerFactory = new ChangeFeedObserverFactory(null, this.loggerFactory);

            return await GetChangeFeedProcessor(
                docConsumerFactory,
                hostName,
                monitorDocumentCollectionInfo,
                leaseDocumentCollectionInfo);
        }
    }
}