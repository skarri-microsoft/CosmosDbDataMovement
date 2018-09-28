namespace SqlDataMovementLib.Source
{
    using System;
    using System.Threading.Tasks;
    using System.Configuration;
    using System.Diagnostics;
    using Common;
    using Common.ChangeFeed;
    using Common.SdkExtensions;
    using Common.SinkContracts;
    using Microsoft.Azure.Documents;
    using Microsoft.Azure.Documents.ChangeFeedProcessor;
    using Microsoft.Azure.Documents.ChangeFeedProcessor.PartitionManagement;
    using Microsoft.Azure.Documents.Client;
    using Microsoft.Azure.EventHubs;
    using Sink;

    public class ChangeFeed
    {
        private CosmosDbConfig monitorCollection = null;
        private CosmosDbConfig leaseCollection = null;
        private CosmosDbConfig destinationCollection = null;
        private ICosmosDbSink cosmosDbSink;

        private SqlClientExtension monitorClientExtension;
        private SqlClientExtension leaseClientExtension;
        private SqlClientExtension destClientExtension;
        public ChangeFeed(ICosmosDbSink cosmosDbSink)
        {
            this.cosmosDbSink = cosmosDbSink;
        }

        public async Task StartAsync()
        {
            monitorCollection = CosmosDbConfig.GetMonitorConfig();

            leaseCollection = CosmosDbConfig.GetLeaseConfig();

            destinationCollection = CosmosDbConfig.GetDestinationConfig();

            monitorClientExtension = new SqlClientExtension(
                monitorCollection,
                ConsistencyLevel.Session,
                ConnectionPolicy.Default);

            leaseClientExtension = new SqlClientExtension(
                leaseCollection,
                ConsistencyLevel.Session,
                ConnectionPolicy.Default);

            destClientExtension = new SqlClientExtension(
                destinationCollection,
                ConsistencyLevel.Session,
                ConnectionPolicy.Default);

            // Not advised to use this code in production.
            this.CreateCollectionsInDevMode();

            await this.RunChangeFeedHostAsync();
        }

        public void CreateCollectionsInDevMode()
        {
            if (!ConfigHelper.IsDevMode())
            {
                return;
            }
            this.monitorClientExtension.CreateCollectionIfNotExistsAsync().Wait();
            this.leaseClientExtension.CreateCollectionIfNotExistsAsync().Wait();
            this.destClientExtension.CreateCollectionIfNotExistsAsync().Wait();
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

            DestinationType? destinationType = ConfigHelper.GetDestinationType();

            if (destinationType == null)
            {
                Logger.LogError("Unsupported destination type. Supported only Cosmos DB and Event Hub. " +
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

            return null;
        }

        public async Task<IChangeFeedProcessor> RunCosmosDBSink(
            DocumentCollectionInfo monitorDocumentCollectionInfo,
            DocumentCollectionInfo leaseDocumentCollectionInfo)
        {
            string hostName = Guid.NewGuid().ToString();
            Trace.TraceInformation("Cosmos DB Sink Host name {0}", hostName);

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
                cosmosDbSink);

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

            Trace.TraceInformation("Processor options Starts from Beginning - {0}, Lease renew interval - {1}",
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
            Trace.TraceInformation("Event Hub Sink Host name {0}", hostName);
            var client = EventHubClient.CreateFromConnectionString(ConfigurationManager.AppSettings["destEhConnStr"]);

            ChangeFeedObserverFactory docConsumerFactory = new ChangeFeedObserverFactory(client);

            return await GetChangeFeedProcessor(
                docConsumerFactory,
                hostName,
                monitorDocumentCollectionInfo,
                leaseDocumentCollectionInfo);
        }
    }
}