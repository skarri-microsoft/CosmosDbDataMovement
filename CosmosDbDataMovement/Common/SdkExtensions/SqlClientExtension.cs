using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Azure.Documents;
using Microsoft.Azure.Documents.Client;
using Microsoft.Azure.Documents.Linq;

namespace Common.SdkExtensions
{
    public class SqlClientExtension
    {
        private readonly string endPoint;
        private readonly string masterKey;
        private readonly int throughput;
        private readonly string partitionKey;
        private readonly List<string> includePaths;
        private readonly int ttlInDays;

        public SqlClientExtension(
            CosmosDbConfig config,
            ConsistencyLevel consistencyLevel,
            ConnectionPolicy connectionPolicy)
        {
            endPoint = config.AccountEndPoint;
            masterKey = config.Key;
            DatabaseName = config.DbName;
            CollectionName = config.CollectionName;
            throughput = config.Throughput;
            partitionKey = config.PartitionKey;
            includePaths = config.IncludePaths;
            ttlInDays = config.TtlInDays;
            InitClient(consistencyLevel, connectionPolicy);
        }

        public SqlClientExtension(
            string endPoint,
            string masterKey,
            string databaseName,
            string collectionName,
            int throughput,
            ConsistencyLevel consistencyLevel,
            ConnectionPolicy connectionPolicy)
        {
            this.endPoint = endPoint;
            this.masterKey = masterKey;
            this.DatabaseName = databaseName;
            this.CollectionName = collectionName;
            this.throughput = throughput;
            InitClient(consistencyLevel, connectionPolicy);
        }

        public string CollectionName { get; }

        public string DatabaseName { get; }

        public DocumentClient DocumentClient { get; private set; }

        private void InitClient(ConsistencyLevel consistencyLevel, ConnectionPolicy connectionPolicy)
        {
            if (connectionPolicy == null)
            {
                connectionPolicy = ConnectionPolicy.Default;
            }

            DocumentClient = new DocumentClient(new Uri(endPoint), masterKey, connectionPolicy, consistencyLevel);
        }

        public async Task<DocumentCollection> GetDocumentCollectionAsync()
        {
            var documentCollectionTask = await DocumentClient.ReadDocumentCollectionAsync(
                UriFactory.CreateDocumentCollectionUri(DatabaseName, CollectionName));
            return documentCollectionTask;
        }

        public async Task CreateCollectionIfNotExistsAsync(CancellationToken token)
        {
            await DocumentClient.CreateDatabaseIfNotExistsAsync(
                new Database { Id = DatabaseName });

            var collection = new DocumentCollection();

            collection.Id = CollectionName;
            if (!string.IsNullOrEmpty(partitionKey))
            {
                collection.PartitionKey.Paths.Add($"/{partitionKey}");
            }

            if (includePaths != null)
            {
                collection.IndexingPolicy.Automatic = true;
                collection.IndexingPolicy.IndexingMode = IndexingMode.Consistent;
                collection.IndexingPolicy.IncludedPaths.Clear();

                foreach (var includePath in includePaths)
                {
                    var path = new IncludedPath();

                    var pathInfo = includePath.Split('|');
                    path.Path = $"/{pathInfo[0]}/?";

                    if (pathInfo[1].ToLower() == "string")
                    {
                        path.Indexes.Add(new RangeIndex(DataType.String) { Precision = -1 });
                    }
                    else if (pathInfo[1].ToLower() == "number")
                    {
                        path.Indexes.Add(new RangeIndex(DataType.Number) { Precision = -1 });
                    }
                    collection.IndexingPolicy.IncludedPaths.Add(path);
                }
                collection.IndexingPolicy.ExcludedPaths.Clear();
                collection.IndexingPolicy.ExcludedPaths.Add(new ExcludedPath { Path = "/*" });
            }

            if (ttlInDays > 0)
            {
                collection.DefaultTimeToLive = ttlInDays * 86400;
            }

            await DocumentClient.CreateDocumentCollectionIfNotExistsAsync(
                    UriFactory.CreateDatabaseUri(DatabaseName),
                    collection,
                    new RequestOptions { OfferThroughput = throughput });

        }

        public async Task CreateDocumentAsync(object doc, bool isUpsert = false)
        {
            var collectionUri = UriFactory.CreateDocumentCollectionUri(DatabaseName, CollectionName);
            if (!isUpsert)
            {
                await DocumentClient.CreateDocumentAsync(collectionUri, doc);
                return;
            }
            await DocumentClient.UpsertDocumentAsync(collectionUri, doc);
        }

        public async Task<Document> UpdateItemAsync(Document oldDoc, object newDoc)
        {
            var condition = new AccessCondition();
            condition.Type = AccessConditionType.IfMatch;
            condition.Condition = oldDoc.ETag;

            var options = new RequestOptions();
            options.AccessCondition = condition;

            var response =
            await DocumentClient.ReplaceDocumentAsync(oldDoc.SelfLink, newDoc, options);
            return response;
        }

        public async Task DeleteDocumentAsync(string docId, string partitionKey = null)
        {
            Console.WriteLine("\n1.7 - Deleting a document");
            var options = new RequestOptions();
            if (!string.IsNullOrEmpty(partitionKey))
            {
                options.PartitionKey = new PartitionKey(partitionKey);
            }
            var response = await DocumentClient.DeleteDocumentAsync(
                UriFactory.CreateDocumentUri(DatabaseName, CollectionName, docId),
                options);

            Console.WriteLine("Request charge of delete operation: {0}", response.RequestCharge);
            Console.WriteLine("StatusCode of operation: {0}", response.StatusCode);
        }

        public async Task<List<object>> QueryDocsAsync(string queryText, string partitionKey = null)
        {
            var docs = new List<object>();

            // 0 maximum parallel tasks, effectively serial execution
            FeedOptions options;
            if (!string.IsNullOrEmpty(partitionKey))
            {
                options = new FeedOptions
                {
                    PartitionKey = new PartitionKey(partitionKey)
                };
            }
            else
            {

                options = new FeedOptions
                {
                    MaxDegreeOfParallelism = 0,
                    MaxBufferedItemCount = 100,
                    EnableCrossPartitionQuery = true
                };
            }

            var collectionUri = UriFactory.CreateDocumentCollectionUri(DatabaseName, CollectionName);

            using (var query = DocumentClient.CreateDocumentQuery<object>(collectionUri, queryText, options).AsDocumentQuery())
            {
                while (query.HasMoreResults)
                {
                    foreach (Document doc in await query.ExecuteNextAsync())
                    {
                        docs.Add(doc);
                    }
                }
                
                return docs;
            }

        }

    }
}
