using System;
using System.Configuration;
using System.IO;
using System.IO.Compression;
using System.Security.Cryptography;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.WindowsAzure.Storage;
using Microsoft.WindowsAzure.Storage.Blob;

namespace Common
{
    public class AzureBlobUploader
    {
        private readonly string containername;

        private readonly CloudStorageAccount storageAccount =
            CloudStorageAccount.Parse(ConfigurationManager.AppSettings["storageConnStr"]);
        
        public AzureBlobUploader(string containername)
        {
            this.containername = containername;
        }

        private async Task<CloudBlobContainer> EnsureContainerExistAsync(CancellationToken cancellationToken)
        {
            CloudBlobClient blobClient = storageAccount.CreateCloudBlobClient();
            var container = blobClient.GetContainerReference(containername);

            try
            {
                await container.CreateIfNotExistsAsync();
            }
            catch (Exception e)
            {
                Console.WriteLine(e);
                throw;
            }

            return container;
        }

        public async Task<CloudBlob> UploadTextAsync(string data, string fileName, CancellationToken cancellationToken)
        {
            var container = await EnsureContainerExistAsync(cancellationToken);
            var blob = container.GetBlockBlobReference(fileName);
            await blob.UploadTextAsync(data);
            return blob;
        }

        public async Task<CloudBlockBlob> UploadBlobAsync(byte[] data, string filename, bool compressed = true, CancellationToken token = default)
        {
            var origMD5 = MD5(data);

            if (compressed)
            {
                using (var comp = new MemoryStream())
                {
                    using (var gzip = new GZipStream(comp, CompressionLevel.Optimal))
                    {
                        await gzip.WriteAsync(data, 0, data.Length, token);
                    }

                    data = comp.ToArray();
                }
            }

            var container = await EnsureContainerExistAsync(token);
            var blob = container.GetBlockBlobReference(filename);
            blob.Metadata.Add("compressed", compressed.ToString());
            blob.Metadata.Add("origMD5", origMD5);
            await blob.UploadFromByteArrayAsync(data, 0, data.Length);
            return blob;
        }

        public async Task<byte[]> DownloadBlobAsync(string filename, CancellationToken cancellationToken)
        {
            var container = await EnsureContainerExistAsync(cancellationToken);
            var blob = container.GetBlockBlobReference(filename);

            byte[] data;

            using (var ms = new MemoryStream())
            {
                await blob.DownloadToStreamAsync(ms);
                ms.Seek(0, SeekOrigin.Begin);
                data = ms.ToArray();
            }

            await blob.FetchAttributesAsync();

            if (Convert.ToBoolean(blob.Metadata["compressed"]))
            {
                using (var comp = new MemoryStream(data))
                using (var decomp = new MemoryStream())
                {
                    using (var gzip = new GZipStream(comp, CompressionMode.Decompress))
                    {
                        await gzip.CopyToAsync(decomp);
                    }

                    data = decomp.ToArray();
                }
            }

            var origMD5 = blob.Metadata["origMD5"];
            var newMD5 = MD5(data);

            if (origMD5 != newMD5)
            {
                throw new Exception("MD5 hashes do not match after download");
            }

            return data;
        }

        private static string MD5(byte[] data)
        {
            using (var x = new MD5CryptoServiceProvider())
            {
                var bs = data;
                bs = x.ComputeHash(bs);

                var s = new StringBuilder();
                foreach (var b in bs)
                {
                    s.Append(b.ToString("x2").ToLower());
                }

                return s.ToString();
            }
        }
    }
}
