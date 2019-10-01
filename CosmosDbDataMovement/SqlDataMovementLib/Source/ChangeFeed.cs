using System;
using System.Configuration;
using System.Threading;
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
using IChangeFeedObserverFactory = Microsoft.Azure.Documents.ChangeFeedProcessor.FeedProcessing.IChangeFeedObserverFactory;

namespace SqlDataMovementLib.Source
{
    public class ChangeFeed
    {
        private readonly ICosmosDbSink cosmosDbSink;
        private readonly ILogger logger;
        private readonly ILoggerFactory loggerFactory;

        private CosmosDbConfig monitorCollection;
        private CosmosDbConfig leaseCollection;
        private CosmosDbConfig destinationCollection;

        private SqlClientExtension monitorClientExtension;
        private SqlClientExtension leaseClientExtension;
        private SqlClientExtension destClientExtension;

        private static MongoClient destMongoClient;
        private static readonly string destDbName = ConfigurationManager.AppSettings["mongodestDbName"];
        private static readonly string destCollectionName = ConfigurationManager.AppSettings["mongodestCollectionName"];
        private static readonly int insertRetries = int.Parse(ConfigurationManager.AppSettings["insertRetries"]);
        private static IMongoDatabase destDatabase;
        private static IMongoCollection<BsonDocument> destDocStoreCollection;

        public ChangeFeed(
            ICosmosDbSink cosmosDbSink,
            ILoggerFactory loggerFactory)
        {
            this.cosmosDbSink = cosmosDbSink ?? throw new ArgumentNullException(nameof(cosmosDbSink));
            this.loggerFactory = loggerFactory ?? throw new ArgumentNullException(nameof(loggerFactory));
            logger = loggerFactory.CreateLogger<ChangeFeed>();
        }

        public async Task<IChangeFeedProcessor> StartAsync(CancellationToken token)
        {
            monitorCollection = CosmosDbConfig.GetMonitorConfig(logger);
            leaseCollection = CosmosDbConfig.GetLeaseConfig(logger);

            monitorClientExtension = new SqlClientExtension(
                monitorCollection,
                ConsistencyLevel.Session,
                ConnectionPolicy.Default);

            leaseClientExtension = new SqlClientExtension(
                leaseCollection,
                ConsistencyLevel.Session,
                ConnectionPolicy.Default);

            var destinationType = ConfigHelper.GetDestinationType(logger);

            if (destinationType == DestinationType.CosmosDB)
            {
                destinationCollection = CosmosDbConfig.GetDestinationConfig(logger);

                destClientExtension = new SqlClientExtension(
                    destinationCollection,
                    ConsistencyLevel.Session,
                    ConnectionPolicy.Default);
            }
            else if (destinationType == DestinationType.MongoDB)
            {
                var destConnectionString =
                    ConfigurationManager.AppSettings["dest-conn"];
                var destSettings = MongoClientSettings.FromUrl(
                    new MongoUrl(destConnectionString)
                );
                destMongoClient = new MongoClient(destSettings);
                destDatabase = destMongoClient.GetDatabase(destDbName);
                destDocStoreCollection = destDatabase.GetCollection<BsonDocument>(destCollectionName);
            }

            // Not advised to use this code in production.
            await CreateCollectionsInDevModeAsync(token);

            return await RunChangeFeedHostAsync();
        }

        public async Task CreateCollectionsInDevModeAsync(CancellationToken token)
        {
            if (!ConfigHelper.IsDevMode())
            {
                return;
            }
            await monitorClientExtension.CreateCollectionIfNotExistsAsync(token);
            await leaseClientExtension.CreateCollectionIfNotExistsAsync(token);
            if (ConfigHelper.GetDestinationType(logger) == DestinationType.CosmosDB)
            {
                await destClientExtension.CreateCollectionIfNotExistsAsync(token);
            }
        }

        public async Task<IChangeFeedProcessor> RunChangeFeedHostAsync()
        {
            // monitored collection info 
            var monitorDocumentCollectionLocation = new DocumentCollectionInfo
            {
                Uri = new Uri(monitorCollection.AccountEndPoint),
                MasterKey = monitorCollection.Key,
                DatabaseName = monitorCollection.DbName,
                CollectionName = monitorCollection.CollectionName
            };

            // lease collection info 
            var leaseCollectionLocation = new DocumentCollectionInfo
            {
                Uri = new Uri(leaseCollection.AccountEndPoint),
                MasterKey = leaseCollection.Key,
                DatabaseName = leaseCollection.DbName,
                CollectionName = leaseCollection.CollectionName
            };

            var destinationType = ConfigHelper.GetDestinationType(logger);

            if (destinationType == null)
            {
                logger.LogError(
                    "Unsupported destination type. Supported only Cosmos DB and Event Hub. " +
                    "Please update your app.config", true);
            }

            if (destinationType == DestinationType.CosmosDB)
            {
                return await RunCosmosDbSinkAsync(monitorDocumentCollectionLocation, leaseCollectionLocation);
            }

            if (destinationType == DestinationType.EventHub)
            {
                return await RunEventHubSinkAsync(monitorDocumentCollectionLocation, leaseCollectionLocation);
            }

            if (destinationType == DestinationType.MongoDB)
            {
                return await RunMongoDbSinkAsync(monitorDocumentCollectionLocation, leaseCollectionLocation);
            }

            throw new NotSupportedException($"Unexpected destination type '{destinationType}'!");
        }

        public async Task<IChangeFeedProcessor> RunCosmosDbSinkAsync(
            DocumentCollectionInfo monitorDocumentCollectionInfo,
            DocumentCollectionInfo leaseDocumentCollectionInfo)
        {
            var hostName = Guid.NewGuid().ToString();
            logger.LogInformation("Cosmos DB Sink Host name {0}", hostName);

            // destination collection info 
            var destCollInfo = new DocumentCollectionInfo
            {
                Uri = new Uri(destinationCollection.AccountEndPoint),
                MasterKey = destinationCollection.Key,
                DatabaseName = destinationCollection.DbName,
                CollectionName = destinationCollection.CollectionName
            };

            var destClient = new DocumentClient(destCollInfo.Uri, destCollInfo.MasterKey);
            
            var docConsumerFactory = new ChangeFeedObserverFactory(
                destClientExtension,
                cosmosDbSink,
                loggerFactory);

            return await GetChangeFeedProcessorAsync(
                docConsumerFactory,
                hostName,
                monitorDocumentCollectionInfo,
                leaseDocumentCollectionInfo);
        }

        public async Task<IChangeFeedProcessor> RunMongoDbSinkAsync(
            DocumentCollectionInfo monitorDocumentCollectionInfo,
            DocumentCollectionInfo leaseDocumentCollectionInfo)
        {
            var hostName = Guid.NewGuid().ToString();
            logger.LogInformation("Mongo DB Sink Host name {0}", hostName);

            var docConsumerFactory = new ChangeFeedObserverFactory(
                destDocStoreCollection,
                insertRetries,
                cosmosDbSink,
                loggerFactory);

            return await GetChangeFeedProcessorAsync(
                docConsumerFactory,
                hostName,
                monitorDocumentCollectionInfo,
                leaseDocumentCollectionInfo);
        }

        public async Task<IChangeFeedProcessor> GetChangeFeedProcessorAsync(
            IChangeFeedObserverFactory changeFeedObserverFactory,
            string hostName,
            DocumentCollectionInfo monitorDocumentCollectionInfo,
            DocumentCollectionInfo leaseDocumentCollectionInfo)
        {
            var changeFeedConfig=ChangeFeedConfig.GetChangeFeedConfig();
            var processorOptions = new ChangeFeedProcessorOptions
            {
                StartFromBeginning = true,
                MaxItemCount = changeFeedConfig.MaxItemCount,
                LeaseRenewInterval = changeFeedConfig.LeaseRenewInterval
            };

            logger.LogInformation("Processor options Starts from Beginning - {0}, Lease renew interval - {1}",
                processorOptions.StartFromBeginning,
                processorOptions.LeaseRenewInterval.ToString());

            var processor = await new ChangeFeedProcessorBuilder()
                .WithObserverFactory(changeFeedObserverFactory)
                .WithHostName(hostName)
                .WithFeedCollection(monitorDocumentCollectionInfo)
                .WithLeaseCollection(leaseDocumentCollectionInfo)
                .WithProcessorOptions(processorOptions)
                .BuildAsync();
            await processor.StartAsync();
            return processor;
        }

        public async Task<IChangeFeedProcessor> RunEventHubSinkAsync(
            DocumentCollectionInfo monitorDocumentCollectionInfo,
            DocumentCollectionInfo leaseDocumentCollectionInfo)
        {
            var hostName = Guid.NewGuid().ToString();
            logger.LogInformation("Event Hub Sink Host name {0}", hostName);
            var docConsumerFactory = new ChangeFeedObserverFactory(null, loggerFactory);

            return await GetChangeFeedProcessorAsync(
                docConsumerFactory,
                hostName,
                monitorDocumentCollectionInfo,
                leaseDocumentCollectionInfo);
        }
    }
}