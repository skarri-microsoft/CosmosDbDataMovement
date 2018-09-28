namespace SqlDataMovementLib.Sink
{
    using System;
    using System.Collections.Generic;
    using System.Diagnostics;
    using System.Threading;
    using Common.SdkExtensions;
    using Common.SinkContracts;
    using Microsoft.Azure.CosmosDB.BulkExecutor;
    using Microsoft.Azure.CosmosDB.BulkExecutor.BulkImport;
    using Microsoft.Azure.Documents;
    using Microsoft.Azure.Documents.ChangeFeedProcessor.FeedProcessing;
    using Microsoft.Azure.Documents.Client;

    public class CosmosDbSink : ICosmosDbSink
    {
        private static IBulkExecutor bulkExecutor = null;


        private static readonly ConnectionPolicy ConnectionPolicy = new ConnectionPolicy
        {
            ConnectionMode = ConnectionMode.Direct,
            ConnectionProtocol = Protocol.Tcp
        };

        public async void IngestDocs(
            SqlClientExtension client,
            IChangeFeedObserverContext context,
            IReadOnlyList<Document> docs,
            CancellationToken cancellationToken,
            Uri destinationCollectionUri)
        {
            foreach (Document doc in docs)
            {
                if (cancellationToken.IsCancellationRequested)
                {
                    return;
                }

                await client.DocumentClient.UpsertDocumentAsync(destinationCollectionUri, doc);
            }
        }

        public async void IngestDocsInBulk(
           SqlClientExtension client,
           IChangeFeedObserverContext context,
           IReadOnlyList<Document> docs,
           CancellationToken cancellationToken,
           Uri destinationCollectionUri)
        {
            DocumentCollection documentCollection = await client.GetDocumentCollectionAsync();

            InitBulkExecutor(client.DocumentClient, documentCollection);

            BulkImportResponse bulkImportResponse = null;
            var tokenSource = new CancellationTokenSource();
            var token = tokenSource.Token;

            try
            {
                bulkImportResponse = await bulkExecutor.BulkImportAsync(
                    documents: docs,
                    enableUpsert: true,
                    disableAutomaticIdGeneration: true,
                    maxConcurrencyPerPartitionKeyRange: null,
                    maxInMemorySortingBatchSize: null,
                    cancellationToken: token);
            }
            catch (DocumentClientException de)
            {
                Trace.TraceError("Document client exception: {0}", de);
            }
            catch (Exception e)
            {
                Trace.TraceError("Exception: {0}", e);
            }

        }
        public static async void InitBulkExecutor(DocumentClient client, DocumentCollection documentCollection)
        {
            if (bulkExecutor != null) return;

            bulkExecutor = new BulkExecutor(client, documentCollection);
            await bulkExecutor.InitializeAsync();
            client.ConnectionPolicy.RetryOptions.MaxRetryWaitTimeInSeconds = 0;
            client.ConnectionPolicy.RetryOptions.MaxRetryAttemptsOnThrottledRequests = 0;
        }
    }
}
