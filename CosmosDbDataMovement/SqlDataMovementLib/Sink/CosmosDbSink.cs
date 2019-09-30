namespace SqlDataMovementLib.Sink
{
    using System;
    using System.Collections.Generic;
    using System.Configuration;
    using System.IO;
    using System.Linq;
    using System.Text;
    using System.Threading;
    using System.Threading.Tasks;
    using Common;
    using Common.SdkExtensions;
    using Common.SinkContracts;
    using CosmosDBInteropSchemaDecoder;
    using Microsoft.Azure.CosmosDB.BulkExecutor;
    using Microsoft.Azure.CosmosDB.BulkExecutor.BulkImport;
    using Microsoft.Azure.Documents;
    using Microsoft.Azure.Documents.ChangeFeedProcessor.FeedProcessing;
    using Microsoft.Azure.Documents.Client;
    using Microsoft.Extensions.Logging;
    using MongoDB.Bson;
    using MongoDB.Bson.Serialization;
    using MongoDB.Driver;
    using Newtonsoft.Json.Linq;
    using ConnectionMode = Microsoft.Azure.Documents.Client.ConnectionMode;

    public class CosmosDbSink : ICosmosDbSink
    {
        private static readonly Random rnd = new Random();
        private static IBulkExecutor bulkExecutor = null;
        private static IMongoCollection<BsonDocument> destDocStoreCollection = null;

        // Default retries
        private static int insertRetries = 3;

        private List<string> insertFailedDocs = null;
        private List<string> parseErrors = null;

        // TODO: move to app.config
        private static int minWait = 1500;
        private static int maxWait = 3000;
        private static long docsCount = 0;

        // TODO: move to configuration
        private const string ParserFailuresContainer = "parserfailures";
        private const string FailedDocumentsContainer = "faileddocuments";

        private static readonly string schema = ConfigurationManager.AppSettings["schema"];

        private static readonly ConnectionPolicy ConnectionPolicy = new ConnectionPolicy
        {
            ConnectionMode = ConnectionMode.Direct,
            ConnectionProtocol = Protocol.Tcp
        };

        private readonly ILogger logger;

        public CosmosDbSink(ILoggerFactory loggerFactory)
        {
            if (loggerFactory == null) { throw new ArgumentNullException(nameof(loggerFactory)); }

            this.logger = loggerFactory.CreateLogger<CosmosDbSink>();
        }

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
                JObject objects = JObject.Parse(doc.ToString());
                objects.Remove("_lsn");
                objects.Remove("_metadata");
                string json = objects.ToString();
                using (MemoryStream ms = new MemoryStream(Encoding.UTF8.GetBytes(json)))
                {
                    await client.DocumentClient.CreateDocumentAsync(destinationCollectionUri,
                        Document.LoadFrom<Document>(ms));
                }
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
                this.logger.LogError("Document client exception: {0}", de);
            }
            catch (Exception e)
            {
                this.logger.LogError("Exception: {0}", e);
            }
        }

        public void IngestDocs(
            IMongoCollection<BsonDocument> mongoCollection,
            int documentRetries,
            IChangeFeedObserverContext context,
            IReadOnlyList<Document> docs,
            CancellationToken cancellationToken,
            Uri destinationCollectionUri)
        {
            if (destDocStoreCollection == null)
            {
                insertRetries = documentRetries;
                destDocStoreCollection = mongoCollection;
            }

            insertFailedDocs = new List<string>();
            parseErrors = new List<string>();

            InsertAllDocuments(docs).Wait();

            if (parseErrors.Any())
            {
                UploadDataAsync(parseErrors, ParserFailuresContainer).Wait();
                parseErrors = null;
            }
            if (insertFailedDocs.Any())
            {
                UploadDataAsync(insertFailedDocs, FailedDocumentsContainer).Wait();
                insertFailedDocs = null;
            }
        }

        public static async void InitBulkExecutor(
            DocumentClient client,
            DocumentCollection documentCollection)
        {
            if (bulkExecutor != null) return;

            bulkExecutor = new BulkExecutor(client, documentCollection);
            await bulkExecutor.InitializeAsync();
            client.ConnectionPolicy.RetryOptions.MaxRetryWaitTimeInSeconds = 0;
            client.ConnectionPolicy.RetryOptions.MaxRetryAttemptsOnThrottledRequests = 0;
        }

        private static bool IsThrottled(Exception ex)
        {
            return ex.Message.ToLower().Contains("Request rate is large".ToLower());
        }

        private async Task InsertAllDocuments(IEnumerable<Document> docs)
        {
            var tasks = new List<Task>();
            for (int j = 0; j < docs.Count(); j++)
            {
                tasks.Add(InsertDocument(docs.ToList()[j]));
            }

            await Task.WhenAll(tasks).ConfigureAwait(false);
            docsCount = docsCount + docs.Count();
            this.logger.LogInformation("Total documents copied so far: {0}", docsCount);
        }

        private async Task InsertDocument(dynamic doc)
        {
            bool isSucceed = false;

            try
            {
                JObject objects = JObject.Parse(doc.ToString());
                objects.Remove("_lsn");
                objects.Remove("_metadata");

                BsonDocument bsonDocument = null;

                // Old schema is plain json, no need to convert it.
                if (schema.ToLower() == "old")
                {
                    bsonDocument = BsonSerializer.Deserialize<BsonDocument>(objects.ToString());
                }
                else
                {
                    bsonDocument = CosmosDbSchemaDecoder.GetBsonDocument(objects.ToString(), false);
                }

                for (int i = 0; i < insertRetries; i++)
                {
                    try
                    {
                        await destDocStoreCollection.InsertOneAsync(bsonDocument);

                        isSucceed = true;
                        //Operation succeed just break the loop
                        break;
                    }
                    catch (Exception ex)
                    {

                        if (!IsThrottled(ex))
                        {
                            this.logger.LogError("ERROR: With collection {0}", ex.ToString());
                            throw;
                        }
                        else
                        {
                            // Thread will wait in between 1.5 secs and 3 secs.
                            await Task.Delay(rnd.Next(minWait, maxWait));
                        }
                    }
                }

                if (!isSucceed)
                {
                    insertFailedDocs.Add(objects.ToString());
                }
            }
            catch (Exception e)
            {
                // Parse Error
                parseErrors.Add(doc.ToString());
            }
        }
        private async Task UploadDataAsync(List<String> dataItems, string containerName)
        {
            StringBuilder sr = new StringBuilder();

            foreach (var item in dataItems)
            {
                sr.AppendLine(item);
            }

            await new AzureBlobUploader(containerName).UploadTextAsync(
                sr.ToString(),
                Guid.NewGuid().ToString() + ".json");
        }
    }
}
