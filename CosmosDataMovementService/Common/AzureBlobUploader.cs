namespace Common
{
    using System;
    using System.Configuration;
    using System.IO;
    using System.IO.Compression;
    using System.Security.Cryptography;
    using Microsoft.WindowsAzure.Storage;
    using Microsoft.WindowsAzure.Storage.Blob;

    public class AzureBlobUploader
    {
        CloudStorageAccount storageAccount = CloudStorageAccount.Parse(ConfigurationManager.AppSettings["storageConnStr"]);
        CloudBlobClient blobClient;
        CloudBlobContainer container;


        public AzureBlobUploader(string containername)
        {
            // Create the blob client.
            blobClient = storageAccount.CreateCloudBlobClient();
            container = blobClient.GetContainerReference(containername);
            container.CreateIfNotExistsAsync().Wait();
        }


        public CloudBlockBlob UploadBlob(byte[] data, string filename, bool compressed = true)
        {

            string origMD5 = MD5(data);

            if (compressed)
            {
                using (MemoryStream comp = new MemoryStream())
                {
                    using (GZipStream gzip = new GZipStream(comp, CompressionLevel.Optimal))
                    {
                        gzip.Write(data, 0, data.Length);
                    }
                    data = comp.ToArray();
                }
            }

            CloudBlockBlob blob = container.GetBlockBlobReference(filename);
            blob.Metadata.Add("compressed", compressed.ToString());
            blob.Metadata.Add("origMD5", origMD5);
            blob.UploadFromByteArrayAsync(data, 0, data.Length).Wait();
            return blob;
        }

        public byte[] DownloadBlob(string filename)
        {
            CloudBlockBlob blob = container.GetBlockBlobReference(filename);

            byte[] data;

            using (MemoryStream ms = new MemoryStream())
            {
                blob.DownloadToStreamAsync(ms).Wait();
                ms.Seek(0, SeekOrigin.Begin);
                data = ms.ToArray();
            }

            blob.FetchAttributesAsync().Wait();

            if (Convert.ToBoolean(blob.Metadata["compressed"]))
            {
                using (MemoryStream comp = new MemoryStream(data))
                {
                    using (MemoryStream decomp = new MemoryStream())
                    {
                        using (GZipStream gzip = new GZipStream(comp, CompressionMode.Decompress))
                        {
                            gzip.CopyTo(decomp);
                        }
                        data = decomp.ToArray();
                    }
                }
            }

            string origMD5 = blob.Metadata["origMD5"];
            string newMD5 = MD5(data);

            if (origMD5 != newMD5)
            {
                throw new Exception("MD5 hashes do not match after download");
            }

            return data;
        }

        private static string MD5(byte[] data)
        {
            MD5CryptoServiceProvider x = new System.Security.Cryptography.MD5CryptoServiceProvider();
            byte[] bs = data;
            bs = x.ComputeHash(bs);
            System.Text.StringBuilder s = new System.Text.StringBuilder();
            foreach (byte b in bs)
            {
                s.Append(b.ToString("x2").ToLower());
            }
            return s.ToString();
        }

    }
}
