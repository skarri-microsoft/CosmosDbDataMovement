using System;
using System.Collections.Generic;
using System.Threading;
using Common.SdkExtensions;
using Microsoft.Azure.Documents;
using Microsoft.Azure.Documents.ChangeFeedProcessor.FeedProcessing;
using MongoDB.Bson;
using MongoDB.Driver;

namespace Common.SinkContracts
{
    public interface ICosmosDbSink
    {
          void IngestDocs(
            SqlClientExtension client,
            IChangeFeedObserverContext context,
            IReadOnlyList<Document> docs,
            CancellationToken cancellationToken,
            Uri destinationCollectionUri);

         void IngestDocsInBulk(
            SqlClientExtension client,
            IChangeFeedObserverContext context,
            IReadOnlyList<Document> docs,
            CancellationToken cancellationToken,
            Uri destinationCollectionUri);

        void IngestDocs(
            IMongoCollection<BsonDocument> mongoCollection,
            int insertRetries,
            IChangeFeedObserverContext context,
            IReadOnlyList<Document> docs,
            CancellationToken cancellationToken,
            Uri destinationCollectionUri);
    }
}
