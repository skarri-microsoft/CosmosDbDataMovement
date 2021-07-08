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

namespace SqlDataMovementLib.Sink
{

    public class CosmosDbSink : ICosmosDbSink
    {
        private static readonly Random rnd = new Random();
        private static IBulkExecutor bulkExecutor;
        private static IMongoCollection<BsonDocument> destDocStoreCollection;

        // Default retries
        private static int insertRetries = 3;

        private List<string> insertFailedDocs;
        private List<string> parseErrors;

        // TODO: move to app.config
        private static int minWait = 1500;
        private static int maxWait = 3000;
        private static long docsCount;

        // TODO: move to configuration
        private const string ParserFailuresContainer = "parserfailures";
        private const string FailedDocumentsContainer = "faileddocuments";

        private static readonly string schema = ConfigurationManager.AppSettings["schema"];

        private readonly ILogger logger;

        public CosmosDbSink(ILoggerFactory loggerFactory)
        {
            if (loggerFactory == null) { throw new ArgumentNullException(nameof(loggerFactory)); }

            logger = loggerFactory.CreateLogger<CosmosDbSink>();
        }

        public async Task IngestDocsAsync(
            SqlClientExtension client,
            IChangeFeedObserverContext context,
            IReadOnlyList<Document> docs,
            Uri destinationCollectionUri,
            CancellationToken cancellationToken)
        {
            foreach (var doc in docs)
            {
                if (cancellationToken.IsCancellationRequested)
                    return;

                var objects = JObject.Parse(doc.ToString());
                objects.Remove("_lsn");
                objects.Remove("_metadata");
                var json = objects.ToString();
                using (var ms = new MemoryStream(Encoding.UTF8.GetBytes(json)))
                {
                    await client.DocumentClient.CreateDocumentAsync(
                        destinationCollectionUri,
                        JsonSerializable.LoadFrom<Document>(ms), 
                        cancellationToken: cancellationToken);
                }
            }
        }

        public async Task IngestDocsInBulkAsync(
            SqlClientExtension client,
            IChangeFeedObserverContext context,
            IReadOnlyList<Document> docs,
            Uri destinationCollectionUri,
            CancellationToken cancellationToken)
        {
            var documentCollection = await client.GetDocumentCollectionAsync();

            await InitBulkExecutorAsync(client.DocumentClient, documentCollection);

            using (var tokenSource = new CancellationTokenSource())
            {
                CancellationToken token = tokenSource.Token;

                try
                {
                    await bulkExecutor.BulkImportAsync(
                        docs,
                        enableUpsert: true,
                        disableAutomaticIdGeneration: true,
                        maxConcurrencyPerPartitionKeyRange: null,
                        maxInMemorySortingBatchSize: null,
                        token);
                }
                catch (DocumentClientException de)
                {
                    logger.LogError("Document client exception: {0}", de);
                }
                catch (Exception e)
                {
                    logger.LogError("Exception: {0}", e);
                }
            }
        }

        public async Task IngestDocsAsync(
            IMongoCollection<BsonDocument> mongoCollection,
            int documentRetries,
            IChangeFeedObserverContext context,
            IReadOnlyList<Document> docs,
            Uri destinationCollectionUri,
            CancellationToken cancellationToken)
        {
            if (destDocStoreCollection == null)
            {
                insertRetries = documentRetries;
                destDocStoreCollection = mongoCollection;
            }

            insertFailedDocs = new List<string>();
            parseErrors = new List<string>();

            await InsertAllDocumentsAsync(docs, cancellationToken);

            if (parseErrors.Any())
            {
                await UploadDataAsync(parseErrors, ParserFailuresContainer, cancellationToken);
                parseErrors = null;
            }

            if (insertFailedDocs.Any())
            {
                await UploadDataAsync(insertFailedDocs, FailedDocumentsContainer, cancellationToken);
                insertFailedDocs = null;
            }
        }

        public static async Task InitBulkExecutorAsync(
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

        private async Task InsertAllDocumentsAsync(IEnumerable<Document> docs, CancellationToken cancellationToken)
        {
            var tasks = new List<Task>();
            for (var j = 0; j < docs.Count(); j++)
            {
                tasks.Add(InsertDocumentAsync(docs.ToList()[j], cancellationToken));
            }

            await Task.WhenAll(tasks);
            docsCount = docsCount + docs.Count();
            logger.LogInformation("Total documents copied so far: {0}", docsCount);
        }

        private async Task InsertDocumentAsync(dynamic doc, CancellationToken token)
        {
            var isSucceed = false;

            try
            {
                JObject objects = JObject.Parse(doc.ToString());
                objects.Remove("_lsn");
                objects.Remove("_metadata");

                BsonDocument bsonDocument;

                // Old schema is plain json, no need to convert it.
                if (schema.ToLower() == "old")
                {
                    bsonDocument = BsonSerializer.Deserialize<BsonDocument>(objects.ToString());
                }
                else
                {
                    bsonDocument = CosmosDbSchemaDecoder.GetBsonDocument(objects.ToString(), false);
                }

                for (var i = 0; i < insertRetries; i++)
                {
                    try
                    {
                        await destDocStoreCollection.InsertOneAsync(bsonDocument, cancellationToken: token);

                        isSucceed = true;
                        //Operation succeed just break the loop
                        break;
                    }
                    catch (Exception ex)
                    {

                        if (!IsThrottled(ex))
                        {
                            logger.LogError("ERROR: With collection {0}", ex.ToString());
                            throw;
                        }

                        // Thread will wait in between 1.5 secs and 3 secs.
                        await Task.Delay(rnd.Next(minWait, maxWait), token);
                    }
                }

                if (!isSucceed)
                {
                    insertFailedDocs.Add(objects.ToString());
                }
            }
#pragma warning disable 168
            catch (Exception e)
#pragma warning restore 168
            {
                // Parse Error
                parseErrors.Add(doc.ToString());
            }
        }

        private async Task UploadDataAsync(IEnumerable<string> dataItems, string containerName, CancellationToken token)
        {
            var sr = new StringBuilder();

            foreach (var item in dataItems)
            {
                sr.AppendLine(item);
            }

            var azureBlobUploader = new AzureBlobUploader(containerName);
            await azureBlobUploader.UploadTextAsync(
                sr.ToString(),
                $"{Guid.NewGuid()}.json", 
                token);
        }
    }
}
