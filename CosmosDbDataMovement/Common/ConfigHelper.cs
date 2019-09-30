using System;
using System.Collections.Generic;
using System.Configuration;
using System.Linq;
using Microsoft.Extensions.Logging;

namespace Common
{
    public class ConfigHelper
    {
        public static DestinationType? GetDestinationType(ILogger logger)
        {
            if (logger == null) { throw new ArgumentNullException(nameof(logger)); }

            if (ConfigurationManager.AppSettings["destinationType"].ToLower() == DestinationType.CosmosDB.ToString().ToLower())
            {
                return DestinationType.CosmosDB;
            }
            else if (ConfigurationManager.AppSettings["destinationType"].ToLower() == DestinationType.EventHub.ToString().ToLower())
            {
                return DestinationType.EventHub;
            }
            else if (ConfigurationManager.AppSettings["destinationType"].ToLower() == DestinationType.MongoDB.ToString().ToLower())
            {
                return DestinationType.MongoDB;
            }

            string message =
                $"Missing 'destination type in app.config'. Allowed values are '{DestinationType.CosmosDB}', '{DestinationType.EventHub}' and '{DestinationType.MongoDB}'";
            logger.LogError(message);

            throw new NotSupportedException(message);
        }

        public static bool IsBulkIngestion()
        {
            if (ConfigurationManager.AppSettings["isBulkIngestion"].ToLower() != null)
            {
                return bool.Parse(ConfigurationManager.AppSettings["isBulkIngestion"]);
            }
            return false;
        }

        public static bool IsDevMode()
        {
            return bool.Parse(ConfigurationManager.AppSettings["devMode"]);
        }

    }

    public class CosmosDbConfig
    {
        public string AccountEndPoint { get; set; }
        public string Key { get; set; }
        public string DbName { get; set; }
        public string CollectionName { get; set; }
        public int Throughput { get; set; }
        public string PartitionKey { get; set; }
        public List<string> IncludePaths { get; set; }
        public int TtlInDays { get; set; }

        public static CosmosDbConfig GetMonitorConfig(ILogger logger)
        {
            if (logger == null) { throw new ArgumentNullException(nameof(logger)); }

            CosmosDbConfig cosmosDbConfig = new CosmosDbConfig()
            {
                AccountEndPoint = ConfigurationManager.AppSettings["monitoredUri"],
                Key = ConfigurationManager.AppSettings["monitoredSecretKey"],
                DbName = ConfigurationManager.AppSettings["monitoredDbName"],
                CollectionName = ConfigurationManager.AppSettings["monitoredCollectionName"],
                Throughput = int.Parse(ConfigurationManager.AppSettings["monitoredThroughput"])
            };
            ValidateConfig(logger, cosmosDbConfig, "Monitor");
            return cosmosDbConfig;
        }

        public static CosmosDbConfig GetLeaseConfig(ILogger logger)
        {
            if (logger == null) { throw new ArgumentNullException(nameof(logger)); }

            CosmosDbConfig cosmosDbConfig = new CosmosDbConfig()
            {
                AccountEndPoint = ConfigurationManager.AppSettings["leaseUri"],
                Key = ConfigurationManager.AppSettings["leaseSecretKey"],
                DbName = ConfigurationManager.AppSettings["leaseDbName"],
                CollectionName = ConfigurationManager.AppSettings["leaseCollectionName"],
                Throughput = int.Parse(ConfigurationManager.AppSettings["leaseThroughput"])
            };
            ValidateConfig(logger, cosmosDbConfig, "Lease");
            return cosmosDbConfig;
        }

        public static CosmosDbConfig GetDestinationConfig(ILogger logger)
        {
            if (logger == null) { throw new ArgumentNullException(nameof(logger)); }

            CosmosDbConfig cosmosDbConfig = new CosmosDbConfig()
            {
                AccountEndPoint = ConfigurationManager.AppSettings["destUri"],
                Key = ConfigurationManager.AppSettings["destSecretKey"],
                DbName = ConfigurationManager.AppSettings["destDbName"],
                CollectionName = ConfigurationManager.AppSettings["destCollectionName"],
                Throughput = int.Parse(ConfigurationManager.AppSettings["destThroughput"])
            };
            if (ConfigurationManager.AppSettings["destPartitionKey"] != null)
            {
                cosmosDbConfig.PartitionKey = ConfigurationManager.AppSettings["destPartitionKey"];
            }

            if (ConfigurationManager.AppSettings["destIncludePaths"] != null)
            {
                cosmosDbConfig.IncludePaths =
                    ConfigurationManager.AppSettings["destIncludePaths"].Split(';').ToList();
            }
            ValidateConfig(logger, cosmosDbConfig, "Destination");
            return cosmosDbConfig;
        }


        private static void ValidateConfig(
            ILogger logger,
            CosmosDbConfig config,
            string collectionType)
        {
            if (logger == null) { throw new ArgumentNullException(nameof(logger)); }

            if (string.IsNullOrEmpty(config.AccountEndPoint) ||
                string.IsNullOrEmpty(config.Key) ||
                string.IsNullOrEmpty(config.DbName) ||
                string.IsNullOrEmpty(config.CollectionName))
            {
                string message = $"Missing values in app.config for Cosmos DB Config for collection type '{collectionType}'";
                logger.LogError(message);

                throw new NotSupportedException(message);
            }
        }

        public static CosmosDbConfig GetCosmosDbConfig(ILogger logger)
        {
            if (logger == null) { throw new ArgumentNullException(nameof(logger)); }

            CosmosDbConfig cosmosDbConfig = new CosmosDbConfig()
            {
                AccountEndPoint = ConfigurationManager.AppSettings["endpoint"],
                Key = ConfigurationManager.AppSettings["secretKey"],
                DbName = ConfigurationManager.AppSettings["dbName"],
                CollectionName = ConfigurationManager.AppSettings["collectionName"],
                Throughput = int.Parse(ConfigurationManager.AppSettings["throughput"]),
                PartitionKey = ConfigurationManager.AppSettings["partitionKey"]
            };

            if (ConfigurationManager.AppSettings["partitionKey"] != null)
            {
                cosmosDbConfig.PartitionKey = ConfigurationManager.AppSettings["partitionKey"];
            }

            if (ConfigurationManager.AppSettings["includePaths"] != null)
            {
                cosmosDbConfig.IncludePaths =
                    ConfigurationManager.AppSettings["includePaths"].Split(';').ToList();
            }

            return cosmosDbConfig;
        }
    }

    public class EventHubConfig
    {
        public string ConnectionString { get; set; }

        public static EventHubConfig GetDestinationEventHubConfig(ILogger logger)
        {
            if (logger == null) { throw new ArgumentNullException(nameof(logger)); }

            EventHubConfig eventHubConfig = new EventHubConfig()
            {
                ConnectionString = ConfigurationManager.AppSettings["destEhConnStr"]
            };
            ValidateConfig(logger, eventHubConfig, "Destination");
            return eventHubConfig;
        }
        private static void ValidateConfig(
            ILogger logger, EventHubConfig config, string eventHubType)
        {
            if (logger == null) { throw new ArgumentNullException(nameof(logger)); }

            if (string.IsNullOrEmpty(config.ConnectionString))
            {
                string message = $"Missing value in app.config for '{eventHubType}' EventHub.";
                logger.LogError(message);

                throw new NotSupportedException(message);
            }
        }
    }

    public class AzureBlobConfig
    {
        public string ConnectionString { get; set; }

        public static AzureBlobConfig GetDestinationAzureBlobConfig(ILogger logger)
        {
            if (logger == null) { throw new ArgumentNullException(nameof(logger)); }

            AzureBlobConfig azureBlobConfig = new AzureBlobConfig()
            {
                ConnectionString = ConfigurationManager.AppSettings["storageConnStr"]
            };

            ValidateConfig(logger, azureBlobConfig, "Destination");
            return azureBlobConfig;
        }

        private static void ValidateConfig(ILogger logger, AzureBlobConfig config, string azureBlobType)
        {
            if (logger == null) { throw new ArgumentNullException(nameof(logger)); }

            if (string.IsNullOrEmpty(config.ConnectionString))
            {
                string message = $"Missing value in app.config for '{azureBlobType}' Azure Blob Store.";
                logger.LogError(message);

                throw new NotSupportedException(message);
            }
        }
    }

    public class ChangeFeedConfig
    {
        public int MaxItemCount { get; set; }
        public TimeSpan LeaseRenewInterval { get; set; }

        public static ChangeFeedConfig GetChangeFeedConfig()
        {
            ChangeFeedConfig changeFeedConfig = new ChangeFeedConfig()
            {
                MaxItemCount = int.Parse(ConfigurationManager.AppSettings["cfMaxItemCount"]),
                LeaseRenewInterval =
                    TimeSpan.FromSeconds(int.Parse(ConfigurationManager.AppSettings["cfLeaseRenewIntervalInSecs"]))
            };

            return changeFeedConfig;
        }

    }
}
