using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using Microsoft.Azure.Documents;
using Microsoft.Azure.Documents.ChangeFeedProcessor;
using Microsoft.Azure.Documents.Client;
using Microsoft.Azure.Documents.Linq;

namespace Common.SdkExtensions
{
    public class SqlClientExtension
    {
        private readonly string endPoint;
        private readonly string masterKey;
        private readonly string databaseName;
        private readonly string collectionName;
        private readonly int throughput;
        private readonly string partitionKey;
        private readonly List<string> includePaths;
        private readonly int ttlInDays;

        private DocumentClient documentClient;

        public SqlClientExtension(
            CosmosDbConfig config,
            ConsistencyLevel consistencyLevel,
            ConnectionPolicy connectionPolicy)
        {
            this.endPoint = config.AccountEndPoint;
            this.masterKey = config.Key;
            this.databaseName = config.DbName;
            this.collectionName = config.CollectionName;
            this.throughput = config.Throughput;
            this.partitionKey = config.PartitionKey;
            this.includePaths = config.IncludePaths;
            this.ttlInDays = config.TtlInDays;
            this.initClient(consistencyLevel, connectionPolicy);
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
            this.databaseName = databaseName;
            this.collectionName = collectionName;
            this.throughput = throughput;
            this.initClient(consistencyLevel, connectionPolicy);
        }

        public string CollectionName
        {
            get { return this.collectionName; }
        }

        public string DatabaseName
        {
            get { return this.databaseName; }
        }

        public DocumentClient DocumentClient
        {
            get { return this.documentClient; }
        }

        private void initClient(ConsistencyLevel consistencyLevel, ConnectionPolicy connectionPolicy)
        {
            if (connectionPolicy == null)
            {
                connectionPolicy = ConnectionPolicy.Default;
            }

            this.documentClient = new DocumentClient(new Uri(this.endPoint), this.masterKey, connectionPolicy, consistencyLevel);
        }

        public DocumentCollectionInfo GetCollectionInfo()
        {
            return new DocumentCollectionInfo
            {
                Uri = new Uri(this.endPoint),
                MasterKey = this.masterKey,
                DatabaseName = this.databaseName,
                CollectionName = this.collectionName
            };
        }

        public async Task<DocumentCollection> GetDocumentCollectionAsync()
        {
            var documentCollectionTask = await this.documentClient.ReadDocumentCollectionAsync(
                UriFactory.CreateDocumentCollectionUri(this.databaseName, this.collectionName));
            return documentCollectionTask;
        }

        public async Task CreateCollectionIfNotExistsAsync()
        {

            await documentClient.CreateDatabaseIfNotExistsAsync(
                new Database { Id = this.databaseName });

            DocumentCollection collection = new DocumentCollection();

            collection.Id = this.collectionName;
            if (!string.IsNullOrEmpty(this.partitionKey))
            {
                collection.PartitionKey.Paths.Add(string.Format("/{0}", this.partitionKey));
            }

            if (this.includePaths != null)
            {
                collection.IndexingPolicy.Automatic = true;
                collection.IndexingPolicy.IndexingMode = IndexingMode.Consistent;
                collection.IndexingPolicy.IncludedPaths.Clear();

                foreach (var includePath in includePaths)
                {
                    IncludedPath path = new IncludedPath();

                    string[] pathInfo = includePath.Split('|');
                    path.Path = string.Format("/{0}/?", pathInfo[0]);

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

            if (this.ttlInDays > 0)
            {
                collection.DefaultTimeToLive = (this.ttlInDays * 86400);
            }

            await documentClient.CreateDocumentCollectionIfNotExistsAsync(
                    UriFactory.CreateDatabaseUri(this.databaseName),
                    collection,
                    new RequestOptions { OfferThroughput = this.throughput });

        }
        public async Task CreateDocument(object doc, bool isUpsert = false)
        {
            Uri collectionUri = UriFactory.CreateDocumentCollectionUri(this.databaseName, this.collectionName);
            if (!isUpsert)
            {
                await this.documentClient.CreateDocumentAsync(collectionUri, doc);
                return;
            }
            await this.documentClient.UpsertDocumentAsync(collectionUri, doc);
        }

        public async Task<Document> UpdateItem(Document oldDoc, object newDoc)
        {
            AccessCondition condition = new AccessCondition();
            condition.Type = AccessConditionType.IfMatch;
            condition.Condition = oldDoc.ETag;

            RequestOptions options = new RequestOptions();
            options.AccessCondition = condition;

            ResourceResponse<Document> response =
            await this.documentClient.ReplaceDocumentAsync(oldDoc.SelfLink, newDoc, options);
            return response;
        }

        public async Task DeleteDocumentAsync(string docId, string partitionKey = null)
        {
            Console.WriteLine("\n1.7 - Deleting a document");
            RequestOptions options = new RequestOptions();
            if (!string.IsNullOrEmpty(partitionKey))
            {
                options.PartitionKey = new PartitionKey(partitionKey);
            }
            ResourceResponse<Document> response = await this.documentClient.DeleteDocumentAsync(
                UriFactory.CreateDocumentUri(databaseName, collectionName, docId),
                options);

            Console.WriteLine("Request charge of delete operation: {0}", response.RequestCharge);
            Console.WriteLine("StatusCode of operation: {0}", response.StatusCode);
        }

        public async Task<List<object>> queryDocs(string queryText, string partitionKey = null)
        {
            List<object> docs = new List<object>();

            // 0 maximum parallel tasks, effectively serial execution
            FeedOptions options = null;
            if (!string.IsNullOrEmpty(partitionKey))
            {
                options = new FeedOptions()
                {
                    PartitionKey = new PartitionKey(partitionKey),
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

            Uri collectionUri = UriFactory.CreateDocumentCollectionUri(this.databaseName, this.collectionName);
                var query = this.documentClient.CreateDocumentQuery<object>(collectionUri, queryText, options).AsDocumentQuery();
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
