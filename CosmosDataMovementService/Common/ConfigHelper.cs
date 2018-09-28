namespace Common
{
    using System;
    using System.Collections.Generic;
    using System.Configuration;
    using System.Linq;
    using Microsoft.Azure.Amqp.Serialization;

    public class ConfigHelper
    {
        public static DestinationType? GetDestinationType()
        {
            if (ConfigurationManager.AppSettings["destinationType"].ToLower() == DestinationType.CosmosDB.ToString().ToLower())
            {
                return DestinationType.CosmosDB;
            }
            else if (ConfigurationManager.AppSettings["destinationType"].ToLower() == DestinationType.EventHub.ToString().ToLower())
            {
                return DestinationType.EventHub;
            }

            Logger.LogError(string.Format("Missing 'destination type in app.config'. Allowed values are {0}, {1}",
                DestinationType.CosmosDB, DestinationType.EventHub), true);

            return null;
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

        public static CosmosDbConfig GetMonitorConfig()
        {
            CosmosDbConfig cosmosDbConfig = new CosmosDbConfig()
            {
                AccountEndPoint = ConfigurationManager.AppSettings["monitoredUri"],
                Key = ConfigurationManager.AppSettings["monitoredSecretKey"],
                DbName = ConfigurationManager.AppSettings["monitoredDbName"],
                CollectionName = ConfigurationManager.AppSettings["monitoredCollectionName"],
                Throughput = int.Parse(ConfigurationManager.AppSettings["monitoredThroughput"])
            };
            ValidateConfig(cosmosDbConfig, "Monitor");
            return cosmosDbConfig;
        }

        public static CosmosDbConfig GetLeaseConfig()
        {
            CosmosDbConfig cosmosDbConfig = new CosmosDbConfig()
            {
                AccountEndPoint = ConfigurationManager.AppSettings["leaseUri"],
                Key = ConfigurationManager.AppSettings["leaseSecretKey"],
                DbName = ConfigurationManager.AppSettings["leaseDbName"],
                CollectionName = ConfigurationManager.AppSettings["leaseCollectionName"],
                Throughput = int.Parse(ConfigurationManager.AppSettings["leaseThroughput"])
            };
            ValidateConfig(cosmosDbConfig, "Lease");
            return cosmosDbConfig;
        }

        public static CosmosDbConfig GetDestinationConfig()
        {
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
            ValidateConfig(cosmosDbConfig, "Destination");
            return cosmosDbConfig;
        }


        private static void ValidateConfig(CosmosDbConfig config, string collectionType)
        {
            if (string.IsNullOrEmpty(config.AccountEndPoint) ||
                string.IsNullOrEmpty(config.Key) ||
                string.IsNullOrEmpty(config.DbName) ||
                string.IsNullOrEmpty(config.CollectionName))
            {
                Logger.LogError(string.Format("Missing values in app.config for Cosmos DB Config for collection type {0}",
                    collectionType), true);

            }
        }

        public static CosmosDbConfig GetCosmosDbConfig(
            )
        {
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

        public static EventHubConfig GetDestinationEventHubConfig()
        {
            EventHubConfig eventHubConfig = new EventHubConfig()
            {
                ConnectionString = ConfigurationManager.AppSettings["destEhConnStr"]
            };
            ValidateConfig(eventHubConfig, "Destination");
            return eventHubConfig;
        }
        private static void ValidateConfig(EventHubConfig config, string eventHubType)
        {
            if (string.IsNullOrEmpty(config.ConnectionString))
            {
                Logger.LogError(string.Format("Missing value in app.config for {0} EventHub.",
                    eventHubType), true);
            }
        }
    }

    public class AzureBlobConfig
    {
        public string ConnectionString { get; set; }

        public static AzureBlobConfig GetDestinationAzureBlobConfig()
        {
            AzureBlobConfig azureBlobConfig = new AzureBlobConfig()
            {
                ConnectionString = ConfigurationManager.AppSettings["storageConnStr"]
            };
            ValidateConfig(azureBlobConfig, "Destination");
            return azureBlobConfig;
        }
        private static void ValidateConfig(AzureBlobConfig config, string azureBlobType)
        {
            if (string.IsNullOrEmpty(config.ConnectionString))
            {
                Logger.LogError(string.Format("Missing value in app.config for {0} Azure Blob Store.",
                    azureBlobType), true);
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
