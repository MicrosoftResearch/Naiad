/*
 * Naiad ver. 0.6
 * Copyright (c) Microsoft Corporation
 * All rights reserved. 
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); 
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0 
 *
 * THIS CODE IS PROVIDED ON AN *AS IS* BASIS, WITHOUT WARRANTIES OR
 * CONDITIONS OF ANY KIND, EITHER EXPRESS OR IMPLIED, INCLUDING WITHOUT
 * LIMITATION ANY IMPLIED WARRANTIES OR CONDITIONS OF TITLE, FITNESS FOR
 * A PARTICULAR PURPOSE, MERCHANTABLITY OR NON-INFRINGEMENT.
 *
 * See the Apache Version 2.0 License for specific language governing
 * permissions and limitations under the License.
 */
using System;
using System.IO;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Text.RegularExpressions;
using System.Threading.Tasks;

using Microsoft.WindowsAzure.Storage.Blob;

using Microsoft.Research.Naiad.Runtime.FaultTolerance;
using Microsoft.Research.Peloponnese.Azure;

namespace FaultToleranceExamples
{
    internal class AzureStreamSequence : IStreamSequence
    {
        private AzureLogAppendStream currentStream;
        private readonly string accountName;
        private readonly string accountKey;
        private readonly string containerName;
        private string streamBaseName;

        public string Name { get { return this.streamBaseName; } }

        public Stream CurrentStream
        {
            get { return this.currentStream; }
        }

        public void Flush(Stream stream)
        {
        }

        public void Shutdown()
        {
            Stream stream = this.CloseCurrentStream();
            if (stream != null)
            {
                this.Close(stream);
            }
        }

        public void Close(Stream stream)
        {
            stream.Close();
            stream.Dispose();
        }

        public Stream CloseCurrentStream()
        {
            Stream stream = this.currentStream;
            this.currentStream = null;
            return stream;
        }

        public void OpenWriteStream(int index)
        {
            string streamName = String.Format("{0}.{1:D4}", this.streamBaseName, index);
            this.currentStream = new AzureLogAppendStream(this.accountName, this.accountKey, this.containerName,
                streamName, 0, true, true, null);
        }

        public Stream OpenReadStream(int index)
        {
            string streamName = String.Format("{0}.{1:D4}", this.streamBaseName, index);
            return new AzureLogReaderStream(this.accountName, this.accountKey, this.containerName, streamName);
        }

        public void GarbageCollectStream(int index)
        {
            var _container = Utils.InitContainer(this.accountName, this.accountKey, this.containerName, true);

            string streamName = String.Format("{0}.{1:D4}", this.streamBaseName, index);
            var _pageBlob = _container.GetPageBlobReference(streamName);

            BlobRequestOptions options = new BlobRequestOptions();

            Utils.WrapInRetry(null, async () => { await _pageBlob.DeleteAsync(DeleteSnapshotsOption.None, null, options, null); }).Wait();
        }

        public IEnumerable<int> FindStreams()
        {
            var _container = Utils.InitContainer(this.accountName, this.accountKey, this.containerName, true);

            return _container.ListBlobs(this.streamBaseName)
                .Where(x => x.GetType() == typeof(CloudPageBlob))
                .Select(x => x as CloudPageBlob)
                .Select(x => int.Parse(x.Name.Substring(this.streamBaseName.Length + 1)));
        }

        public AzureStreamSequence(string accountName, string accountKey, string containerName, string streamBaseName)
        {
            this.accountName = accountName;
            this.accountKey = accountKey;
            this.containerName = containerName;
            this.streamBaseName = streamBaseName;
        }
    }
}
