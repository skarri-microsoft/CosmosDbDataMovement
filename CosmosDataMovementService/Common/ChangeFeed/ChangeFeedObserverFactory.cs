namespace Common.ChangeFeed
{
    using Microsoft.Azure.Documents.ChangeFeedProcessor;
    using Microsoft.Azure.Documents.Client;
    using Microsoft.Azure.EventHubs;
    using SdkExtensions;
    using SinkContracts;
    using IChangeFeedObserver = Microsoft.Azure.Documents.ChangeFeedProcessor.FeedProcessing.IChangeFeedObserver;
    using IChangeFeedObserverFactory = Microsoft.Azure.Documents.ChangeFeedProcessor.FeedProcessing.IChangeFeedObserverFactory;

    public class ChangeFeedObserverFactory : IChangeFeedObserverFactory
    {
        private SqlClientExtension destClient;
        private EventHubClient eventHubClient;
        private DestinationType? destinationType = null;
        private ICosmosDbSink cosmosDbSink;

        public ChangeFeedObserverFactory(
            SqlClientExtension destClient,
            ICosmosDbSink cosmosDbSink)
        {
            this.destinationType = DestinationType.CosmosDB;
            this.destClient = destClient;
            this.cosmosDbSink = cosmosDbSink;
        }

        public ChangeFeedObserverFactory(EventHubClient eventHubClient)
        {
            this.destinationType = DestinationType.EventHub;
            this.eventHubClient = eventHubClient;
        }

        public IChangeFeedObserver CreateObserver()
        {
            if (destinationType == null)
            {
                Logger.LogError("Destination type is not defined", true);
            }

            if (this.destinationType == DestinationType.EventHub)
            {
                EventHubFeedObserver newConsumer = new EventHubFeedObserver(this.eventHubClient);
                return newConsumer;
            }
            else if (this.destinationType == DestinationType.CosmosDB)
            {
                CosmosDbFeedObserver cosmosDbFeedConsumer =
                    new CosmosDbFeedObserver(
                        this.destClient,
                        this.cosmosDbSink);
                return cosmosDbFeedConsumer;
            }
            return null;
        }
    }
}