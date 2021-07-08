using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using Common.SdkExtensions;
using Microsoft.Azure.Documents;
using Microsoft.Azure.Documents.ChangeFeedProcessor.FeedProcessing;
using MongoDB.Bson;
using MongoDB.Driver;

namespace Common.SinkContracts
{
    public interface ICosmosDbSink
    {
          Task IngestDocsAsync(
              SqlClientExtension client,
              IChangeFeedObserverContext context,
              IReadOnlyList<Document> docs,
              Uri destinationCollectionUri,
              CancellationToken cancellationToken);

          Task IngestDocsInBulkAsync(
              SqlClientExtension client,
              IChangeFeedObserverContext context,
              IReadOnlyList<Document> docs,
              Uri destinationCollectionUri,
              CancellationToken cancellationToken);

          Task IngestDocsAsync(
              IMongoCollection<BsonDocument> mongoCollection,
              int insertRetries,
              IChangeFeedObserverContext context,
              IReadOnlyList<Document> docs,
              Uri destinationCollectionUri,
              CancellationToken cancellationToken);
    }
}
