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

using Microsoft.Research.Naiad.Runtime.FaultTolerance;
using Microsoft.Research.Naiad.Runtime.Networking;
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Microsoft.Research.Naiad.Serialization
{
    internal interface SerializedMessageSender : IDisposable
    {
        int SendBufferSegment(MessageHeader hdr, BufferSegment segment, bool flushStream);
    }

    internal class StreamSerializedMessageSender : SerializedMessageSender
    {
        private readonly Stream stream;
        private int nextPageIndex;

        private readonly int pageSize;

        private bool disposed = false;

        public int SendBufferSegment(MessageHeader hdr, BufferSegment segment, bool flushStream)
        {
            ArraySegment<byte> byteArraySegment = segment.ToArraySegment();
            if (byteArraySegment.Offset == 0)
            {
                this.stream.Write(byteArraySegment.Array, 0, this.pageSize);
            }
            else
            {
                this.stream.Write(byteArraySegment.Array, byteArraySegment.Offset, byteArraySegment.Count);
                // Now pad the write to the full page size.
                this.stream.Seek(this.pageSize - byteArraySegment.Count, SeekOrigin.Current);
            }

            if (flushStream)
            {
                this.stream.Flush();
            }

            return this.nextPageIndex++;
        }

        internal StreamSerializedMessageSender(Stream stream, int pageSize)
        {
            this.nextPageIndex = 0;
            this.stream = stream;

            this.pageSize = pageSize;
        }

        public void Dispose()
        {
            if (!this.disposed)
            {
                this.stream.Dispose();
                this.disposed = true;
            }
        }
    }

    internal class StreamSequenceSerializedMessageSender : SerializedMessageSender
    {
        private readonly StreamSequence stream;
        private int nextPageIndex;

        private readonly int pageSize;

        private bool disposed = false;

        public int SendBufferSegment(MessageHeader hdr, BufferSegment segment, bool flushStream)
        {
            ArraySegment<byte> byteArraySegment = segment.ToArraySegment();

            if (byteArraySegment.Offset == 0)
            {
                this.stream.Write(byteArraySegment.Array, 0, this.pageSize);
            }
            else
            {
                this.stream.Write(byteArraySegment.Array, byteArraySegment.Offset, byteArraySegment.Count);
                this.stream.Write(byteArraySegment.Array, 0, this.pageSize - byteArraySegment.Count);
            }

            return this.nextPageIndex++;
        }

        internal StreamSequenceSerializedMessageSender(StreamSequence stream, int pageSize)
        {
            this.nextPageIndex = 0;
            this.stream = stream;
            this.pageSize = pageSize;
        }

        public void Dispose()
        {
            if (!this.disposed)
            {
                this.disposed = true;
            }
        }

        public Task CompletedTask()
        {
            return this.stream.FlushAsync();
        }
    }

    internal class NetworkChannelSerializedMessageSender : SerializedMessageSender
    {
        private readonly NetworkChannel networkChannel;
        private int destProcessID;
        private int nextPageIndex;

        public int SendBufferSegment(MessageHeader hdr, BufferSegment segment, bool flushStream)
        {
            segment.Copy();
            this.networkChannel.SendBufferSegment(hdr, this.destProcessID, segment, true);
            return this.nextPageIndex++;
        }

        public NetworkChannelSerializedMessageSender(NetworkChannel networkChannel, int destProcessID)
        {
            this.nextPageIndex = 0;
            this.networkChannel = networkChannel;
            this.destProcessID = destProcessID;
        }

        public void Dispose() { }
    }



}
