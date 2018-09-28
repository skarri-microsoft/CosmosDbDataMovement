namespace Common.SinkContracts
{
    using System;
    using System.Collections.Generic;
    using System.Threading;
    using Microsoft.Azure.Documents;
    using Microsoft.Azure.Documents.ChangeFeedProcessor.FeedProcessing;
    using Microsoft.Azure.Documents.Client;
    using SdkExtensions;

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
    }
}
