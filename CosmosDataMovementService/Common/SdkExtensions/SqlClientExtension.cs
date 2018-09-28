namespace Common.SdkExtensions
{
    using System;
    using System.Collections.Generic;
    using System.Threading.Tasks;
    using Microsoft.Azure.Documents;
    using Microsoft.Azure.Documents.ChangeFeedProcessor;
    using Microsoft.Azure.Documents.Client;
    using Microsoft.Azure.Documents.Linq;

    public class SqlClientExtension
    {
        public string EndPoint;
        public string MasterKey;
        public string DatabaseName;
        public string CollectionName;
        public int Throughput;
        public string PartitionKey;
        public List<string> IncludePaths;
        public DocumentClient DocumentClient;
        public int TtlInDays;

        public SqlClientExtension(
            CosmosDbConfig config,
            ConsistencyLevel consistencyLevel,
            ConnectionPolicy connectionPolicy)
        {
            this.EndPoint = config.AccountEndPoint;
            this.MasterKey = config.Key;
            this.DatabaseName = config.DbName;
            this.CollectionName = config.CollectionName;
            this.Throughput = config.Throughput;
            this.PartitionKey = config.PartitionKey;
            this.IncludePaths = config.IncludePaths;
            this.TtlInDays = config.TtlInDays;
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
            this.EndPoint = endPoint;
            this.MasterKey = masterKey;
            this.DatabaseName = databaseName;
            this.CollectionName = collectionName;
            this.Throughput = throughput;
            this.initClient(consistencyLevel, connectionPolicy);
        }

        private void initClient(ConsistencyLevel consistencyLevel, ConnectionPolicy connectionPolicy)
        {
            if (connectionPolicy == null)
            {
                connectionPolicy = ConnectionPolicy.Default;
            }

            this.DocumentClient = new DocumentClient(new Uri(this.EndPoint), this.MasterKey, connectionPolicy, consistencyLevel);

        }

        public DocumentCollectionInfo GetCollectionInfo()
        {
            return new DocumentCollectionInfo
            {
                Uri = new Uri(this.EndPoint),
                MasterKey = this.MasterKey,
                DatabaseName = this.DatabaseName,
                CollectionName = this.CollectionName
            };
        }

        public async Task<DocumentCollection> GetDocumentCollectionAsync()
        {
            var documentCollectionTask = await this.DocumentClient.ReadDocumentCollectionAsync(
                UriFactory.CreateDocumentCollectionUri(this.DatabaseName, this.CollectionName));
            return documentCollectionTask;
        }

        public async Task CreateCollectionIfNotExistsAsync()
        {

            await DocumentClient.CreateDatabaseIfNotExistsAsync(
                new Database { Id = this.DatabaseName });

            DocumentCollection collection = new DocumentCollection();

            collection.Id = this.CollectionName;
            if (!string.IsNullOrEmpty(this.PartitionKey))
            {
                collection.PartitionKey.Paths.Add(string.Format("/{0}", this.PartitionKey));
            }

            if (this.IncludePaths != null)
            {
                collection.IndexingPolicy.Automatic = true;
                collection.IndexingPolicy.IndexingMode = IndexingMode.Consistent;
                collection.IndexingPolicy.IncludedPaths.Clear();

                foreach (var includePath in IncludePaths)
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

            if (this.TtlInDays > 0)
            {
                collection.DefaultTimeToLive = (this.TtlInDays * 86400);
            }

            await DocumentClient.CreateDocumentCollectionIfNotExistsAsync(
                    UriFactory.CreateDatabaseUri(this.DatabaseName),
                    collection,
                    new RequestOptions { OfferThroughput = this.Throughput });

        }
        public async Task CreateDocument(object doc, bool isUpsert = false)
        {
            Uri collectionUri = UriFactory.CreateDocumentCollectionUri(this.DatabaseName, this.CollectionName);
            if (!isUpsert)
            {
                await this.DocumentClient.CreateDocumentAsync(collectionUri, doc);
                return;
            }
            await this.DocumentClient.UpsertDocumentAsync(collectionUri, doc);
        }

        public async Task<Document> UpdateItem(Document oldDoc, object newDoc)
        {
            AccessCondition condition = new AccessCondition();
            condition.Type = AccessConditionType.IfMatch;
            condition.Condition = oldDoc.ETag;

            RequestOptions options = new RequestOptions();
            options.AccessCondition = condition;

            ResourceResponse<Document> response =
            await this.DocumentClient.ReplaceDocumentAsync(oldDoc.SelfLink, newDoc, options);
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
            ResourceResponse<Document> response = await this.DocumentClient.DeleteDocumentAsync(
                UriFactory.CreateDocumentUri(DatabaseName, CollectionName, docId),
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

            Uri collectionUri = UriFactory.CreateDocumentCollectionUri(this.DatabaseName, this.CollectionName);
                var query = this.DocumentClient.CreateDocumentQuery<object>(collectionUri, queryText, options).AsDocumentQuery();
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
