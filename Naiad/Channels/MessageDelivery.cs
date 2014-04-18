/*
 * Naiad ver. 0.4
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
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Microsoft.Research.Naiad.DataStructures;
using System.Collections.Concurrent;
using System.Diagnostics;
using Microsoft.Research.Naiad.Serialization;
using System.Threading;
using System.Net.Sockets;
using Microsoft.Research.Naiad.Scheduling;
using System.Net;
using System.IO;
using Microsoft.Research.Naiad.Runtime.Controlling;
using Microsoft.Research.Naiad.Runtime.Networking;

namespace Microsoft.Research.Naiad.Dataflow
{

    /// <summary>
    /// Describes the origin of a message
    /// </summary>
    public struct ReturnAddress
    {
        /// <summary>
        /// Process-local thread identifier of the sender
        /// </summary>
        public readonly int ThreadIndex;

        /// <summary>
        /// Vertex identifier of the sender
        /// </summary>
        public readonly int VertexID;

        /// <summary>
        /// Process identifier of the sender
        /// </summary>
        public readonly int ProcessID;


        /// <summary>
        /// Constructs a return address from process and vertex identifiers
        /// </summary>
        /// <param name="processId">process identifier</param>
        /// <param name="vertexId">vertex identifier</param>
        public ReturnAddress(int processId, int vertexId)
        {
            this.VertexID = vertexId;
            this.ProcessID = processId;
            this.ThreadIndex = 0;
        }

        /// <summary>
        /// Constructs a return address from process, vertex, thread identifiers
        /// </summary>
        /// <param name="processId">process identifier</param>
        /// <param name="vertexId">vertex identifier</param>
        /// <param name="threadIndex">thread identifier</param>
        public ReturnAddress(int processId, int vertexId, int threadIndex)
        {
            this.VertexID = vertexId;
            this.ProcessID = processId;
            this.ThreadIndex = threadIndex;
        }
    }


}

namespace Microsoft.Research.Naiad.Dataflow.Channels
{
    internal class PostOfficeChannel<S, T> : Cable<S, T>
        where T : Time<T>
    {
        private readonly int channelID;
        public int ChannelId { get { return channelID; } }

        private readonly StageOutput<S, T> sendBundle;
        private readonly StageInput<S, T> recvBundle;

        private readonly Dictionary<int, Postbox<S, T>> postboxes;
        private readonly Mailbox<S, T>[] mailboxes;

        public PostOfficeChannel(StageOutput<S, T> sendBundle, StageInput<S, T> recvBundle, Func<S, int> routingHashcodeFunction, NetworkChannel networkChannel, int channelId, Channel.Flags flags)
        {
            this.sendBundle = sendBundle;
            this.recvBundle = recvBundle;

            this.postboxes = new Dictionary<int, Postbox<S, T>>();
            this.mailboxes = new Mailbox<S, T>[recvBundle.ForStage.Placement.Count];

            this.channelID = channelId;

            var computation = sendBundle.ForStage.InternalComputation;

            foreach (VertexLocation loc in recvBundle.ForStage.Placement)
            {
                if (loc.ProcessId == computation.Controller.Configuration.ProcessID)
                {
                    var postOffice = this.recvBundle.GetPin(loc.VertexId).Vertex.Scheduler.State(computation).PostOffice;
                    LocalMailbox<S, T> localMailbox;

                    var progressBuffer = new Runtime.Progress.ProgressUpdateBuffer<T>(this.ChannelId, this.recvBundle.GetPin(loc.VertexId).Vertex.Scheduler.State(computation).Producer); 

                    if ((flags & Channel.Flags.SpillOnRecv) == Channel.Flags.SpillOnRecv)
                    {
                        //localMailbox = new SpillingLocalMailbox<S, T>(postOffice, this.recvBundle.GetPin(loc.VertexId), channelID, loc.VertexId, progressBuffer);
                        throw new Exception("SpillingLocalMailbox not currently supported");
                    }
                    else
                    {
                        localMailbox = new LegacyLocalMailbox<S, T>(postOffice, this.recvBundle.GetPin(loc.VertexId), channelID, loc.VertexId, progressBuffer);
                    }

                    this.mailboxes[loc.VertexId] = localMailbox;
                    postOffice.RegisterMailbox(localMailbox);
                    if (networkChannel != null)
                        networkChannel.RegisterMailbox(localMailbox);
                }
                else
                    this.mailboxes[loc.VertexId] = new RemoteMailbox<S, T>(this.channelID, loc.ProcessId, loc.VertexId, sendBundle.ForStage.InternalComputation);
            }

            foreach (VertexLocation loc in sendBundle.ForStage.Placement)
                if (loc.ProcessId == sendBundle.ForStage.InternalComputation.Controller.Configuration.ProcessID)
                {
                    var progressBuffer = new Runtime.Progress.ProgressUpdateBuffer<T>(this.ChannelId, this.sendBundle.GetFiber(loc.VertexId).Vertex.Scheduler.State(computation).Producer);

                    this.postboxes[loc.VertexId] = new Postbox<S, T>(this.channelID, this.sendBundle.GetFiber(loc.VertexId), this.recvBundle, this.mailboxes, routingHashcodeFunction, networkChannel, progressBuffer);
                }
            
        }

        public override string ToString()
        {
            return String.Format("Exchange channel [{0}]: {1} -> {2}", this.channelID, sendBundle, recvBundle);
        }

        public Dataflow.Stage SourceStage { get { return this.sendBundle.ForStage; } }
        public Dataflow.Stage DestinationStage { get { return this.recvBundle.ForStage; } }

        public SendChannel<S, T> GetSendChannel(int i)
        {
            return this.postboxes[i];
        }
    }

    internal class Postbox<S, T> : SendChannel<S, T>
        where T : Time<T>
    {
        private readonly int threadindex;
        public int ThreadIndex { get { return this.threadindex; } }

        private readonly int channelID;
        private readonly VertexOutput<S, T> sender;
        private readonly int vertexid;
        public int VertexID { get { return this.vertexid; } }

        public int ProcessID { get { return this.sender.Vertex.Stage.InternalComputation.Controller.Configuration.ProcessID; } }
        
        private readonly StageInput<S, T> receiverBundle;

        private readonly Func<S, int> routingHashcodeFunction;
        private readonly NetworkChannel networkChannel;

        //private readonly int[,] sendSequenceNumbers;

        private readonly ReturnAddress returnAddress;

        private readonly Mailbox<S, T>[] mailboxes;

        //private readonly LocalMailbox<S, T>[] mailboxes;
        //private readonly RemoteMailbox<S, T>[,] remoteMailboxes;
        
        private Runtime.Progress.ProgressUpdateBuffer<T> progressBuffer;

        public Postbox(int channelID, VertexOutput<S, T> sender, StageInput<S, T> receiverBundle, Mailbox<S, T>[] mailboxes, Func<S, int> routingHashcodeFunction, NetworkChannel networkChannel, Runtime.Progress.ProgressUpdateBuffer<T> progressBuffer)
            : this(channelID, sender, receiverBundle, mailboxes, routingHashcodeFunction, networkChannel)
        {
            this.progressBuffer = progressBuffer;
        }

        public Postbox(int channelID, VertexOutput<S, T> sender, StageInput<S, T> receiverBundle, Mailbox<S, T>[] mailboxes, Func<S, int> routingHashcodeFunction, NetworkChannel networkChannel)
        {
            this.threadindex = sender.Vertex.Scheduler.Index;
            this.channelID = channelID;
            this.sender = sender;
            this.vertexid = sender.Vertex.VertexId;
            this.receiverBundle = receiverBundle;
            this.routingHashcodeFunction = routingHashcodeFunction;
            this.networkChannel = networkChannel;
            this.mailboxes = mailboxes;
            this.returnAddress = new ReturnAddress(this.ProcessID, this.vertexid, this.threadindex);

            this.localTempBuffer = new Message<S, T>();

            //this.sendSequenceNumbers = new int[Naiad.Processes, this.receiverBundle.LocalParallelism];
        }

        private Message<S, T>? localTempBuffer;

#if false
        /// <summary>
        /// Sends a record
        /// </summary>
        /// <param name="record"></param>
        public void Send(Pair<S, T> record)
        {
            int destVertexID;
            if (this.routingHashcodeFunction == null)
            {
                destVertexID = this.vertexid % this.mailboxes.Length;
            }
            else
            {
                int hashcode = this.routingHashcodeFunction(record.v1);
                destVertexID = (hashcode & int.MaxValue) % this.mailboxes.Length; // this.routingTable.GetRoute(hashcode);
            }

            Debug.Assert(destVertexID < this.mailboxes.Length);
            this.mailboxes[destVertexID].Send(record, new RemotePostbox(this.ProcessID, this.VertexID, this.ThreadIndex));

            if (progressBuffer != null)
                progressBuffer.Update(record.v2, +1);
        } 
#endif
        public void Send(Message<S, T> records)
        {
            if (progressBuffer != null)
                progressBuffer.Update(records.time, records.length);

            int lastDestID = -1;

            Message<S, T> tempBuffer = localTempBuffer.HasValue ? localTempBuffer.Value : new Message<S, T>();
            localTempBuffer = null; // XXX: Perhaps overly cautious if we disable channel re-entrancy.

            if (tempBuffer.Unallocated)
                tempBuffer.Allocate();

            tempBuffer.time = records.time;

            for (int i = 0; i < records.length; i++)
            {
                var record = records.payload[i];

                int destVertexID;
                if (this.routingHashcodeFunction == null)
                {
                    destVertexID = this.vertexid % this.mailboxes.Length;
                }
                else
                {
                    int hashcode = this.routingHashcodeFunction(record);
                    destVertexID = (hashcode & int.MaxValue) % this.mailboxes.Length; // this.routingTable.GetRoute(hashcode);
                }

                Debug.Assert(destVertexID < this.mailboxes.Length);

                if (tempBuffer.length > 0 && lastDestID != destVertexID)
                {
                    this.mailboxes[lastDestID].Send(tempBuffer, this.returnAddress);
                    tempBuffer.length = 0;
                }

                lastDestID = destVertexID;
                tempBuffer.payload[tempBuffer.length++] = record;

                //this.mailboxes[destVertexID].Send(record.PairWith(records.time), this.remotePostbox);
            }

            if (tempBuffer.length > 0)
                this.mailboxes[lastDestID].Send(tempBuffer, this.returnAddress);

            tempBuffer.length = 0;
            if (this.localTempBuffer.HasValue)
                tempBuffer.Release();
            else
                this.localTempBuffer = tempBuffer;
        }

        /// <summary>
        /// Flushes the mailboxes and makes sure the scheduler knows about the latest counts
        /// </summary>
        public void Flush()
        {
            // first flush all the non-local mailboxes
            for (int i = 0; i < this.mailboxes.Length; ++i)
            {
                if (this.mailboxes[i].ThreadIndex != this.threadindex)
                {
                    this.mailboxes[i].Flush(new ReturnAddress(this.ProcessID, this.VertexID, this.ThreadIndex));
                }
            }

            // now flush the local mailboxes, which may fall through into executing actual code
            for (int i = 0; i < this.mailboxes.Length; ++i)
            {
                if (this.mailboxes[i].ThreadIndex == this.threadindex)
                {
                    this.mailboxes[i].Flush(new ReturnAddress(this.ProcessID, this.VertexID, this.ThreadIndex));
                }
            }

            //Console.WriteLine("Flushing " + this.channelID);
            if (progressBuffer != null)
                progressBuffer.Flush();
        }
    }

    internal class PostOffice
    {
        private readonly Scheduler scheduler;
        public readonly InternalController Controller;
        private readonly List<UntypedLocalMailbox> mailboxes;

        public PostOffice(Scheduling.Scheduler scheduler)
        {
            this.scheduler = scheduler;
            this.Controller = scheduler.Controller;
            this.mailboxes = new List<UntypedLocalMailbox>();
        }

        public void DrainAllQueues()
        {
            foreach (UntypedLocalMailbox mailbox in this.mailboxes)
            {
                if (mailbox != null)
                {
                    mailbox.Drain();
                    mailbox.Flush();
                }
            }
        }

        public void Signal() { this.scheduler.Signal(); }

        public void RegisterMailbox(UntypedLocalMailbox mailbox)
        {
            this.mailboxes.Add(mailbox);
        }
    }

    internal interface UntypedMailbox
    {
        void DeliverSerializedMessage(SerializedMessage message, ReturnAddress from);

        int GraphId { get; }
        int Id { get; }
        int VertexId { get; }
    }

    internal interface UntypedLocalMailbox : UntypedMailbox
    {
        void Drain();
        void Flush();
    }

    internal interface Mailbox<S, T> : UntypedMailbox
        where T : Time<T>
    {
        // void Send(Pair<S, T> record, RemotePostbox from);

        void Send(Message<S, T> message, ReturnAddress from);

        void Flush(ReturnAddress from);
        // returns the ThreadId for mailboxes on the same computer, and -1 for remote ones
        int ThreadIndex { get; }
    }


    internal abstract class LocalMailbox<S, T> : UntypedLocalMailbox, Mailbox<S, T>
        where T : Time<T>
    {
        protected readonly PostOffice postOffice;

        protected volatile bool dirty;
        public bool Dirty
        {
            get
            {
                return this.dirty;
            }
        }

        protected readonly VertexInput<S, T> endpoint;

        public readonly int id;         // mailbox id (== channel id?)
        public readonly int vertexId;    // receiver
        public readonly int graphId;

        public int Id { get { return this.id; } }
        public int VertexId { get { return this.vertexId; } }
        public int GraphId { get { return this.graphId; } }
        public int ThreadIndex { get { return this.endpoint.Vertex.scheduler.Index; } }

        protected Runtime.Progress.ProgressUpdateBuffer<T> progressBuffer;

        public LocalMailbox(PostOffice postOffice, VertexInput<S, T> endpoint, int id, int vertexId)
        {
            this.postOffice = postOffice;
            this.dirty = false;
            this.endpoint = endpoint;
            this.id = id;
            this.vertexId = vertexId;
            this.graphId = endpoint.Vertex.Stage.InternalComputation.Index;
        }

        public abstract void DeliverSerializedMessage(SerializedMessage message, ReturnAddress from);
        public abstract void Flush(ReturnAddress from);
        public abstract void Flush();

        public abstract void Send(Message<S, T> message, ReturnAddress from);
        public abstract void Drain();
    }

    internal class LegacyLocalMailbox<S, T> : LocalMailbox<S, T>
        where T : Time<T>
    {
        private readonly ConcurrentQueue<Message<S, T>> sharedQueue;
        private readonly ConcurrentQueue<SerializedMessage> sharedSerializedQueue;
        private readonly Queue<Message<S, T>> privateQueue;
        
        private readonly Message<S, T>[] messagesFromLocalVertices;

        private readonly AutoSerializedMessageDecoder<S, T> decoder;

        /// <summary>
        /// Move records from the private queue into the operator state by giving them to the endpoint.
        /// </summary>
        public override void Drain()
        {
            // entrancy is checked beforehand in LLM, and not needed when called from the scheduler. 
            if (this.dirty)
            {
                this.dirty = false;

                // Pull messages received from other processes
                SerializedMessage superChannelMessage;
                while (this.sharedSerializedQueue.TryDequeue(out superChannelMessage))
                {
                    this.endpoint.SerializedMessageReceived(superChannelMessage, new ReturnAddress());
                    if (progressBuffer != null)
                    {
                        Pair<T, int> delta = this.decoder.Time(superChannelMessage);
                        progressBuffer.Update(delta.First, -delta.Second);
                    }

                    // superChannelMessage.Dispose();
                }

                // Pull messages received from the local process
                var message = new Message<S, T>();
                while (this.sharedQueue.TryDequeue(out message))
                {
                    this.endpoint.OnReceive(message, new ReturnAddress());

                    if (progressBuffer != null)
                        progressBuffer.Update(message.time, -message.length);

                    message.Release();
                }
            }
        }

        public override void Flush()
        {
            endpoint.Flush();
            if (progressBuffer != null)
                progressBuffer.Flush();
        }

        int TypedQueueHighWatermark = 10;

        /// <summary>
        /// Forwards a local message by moving it to the shared queue.  This makes the message visible to the receiver.
        /// </summary>
        /// <param name="message"></param>
        private void Post(Message<S, T> message)
        {
            typedMessageCount++;
            typedMessageLength += message.length;

            // Deliver the message
            this.sharedQueue.Enqueue(message);

            if (this.sharedQueue.Count > TypedQueueHighWatermark)
            {
                //Console.Error.WriteLine("{3} WARNING: {0}:SharedQueue High Watermark={1} exceeded, count={2}", this.endpoint.ForOperator, TypedQueueHighWatermark, this.sharedQueue.Count, this.endpoint.ForOperator.scheduler.Index);
                TypedQueueHighWatermark *= 2;
            }

            // Notify postoffice that there is a message in the shared queue
            if (!this.dirty)
                this.dirty = true;
            this.postOffice.Signal();
        }

        int typedMessageCount = 0;
        int typedMessageLength = 0;

        // if you provide a destinationCollectionId for the mailbox, it will use progress magic. 
        public LegacyLocalMailbox(PostOffice postOffice, VertexInput<S, T> endpoint, int id, int vertexId, Runtime.Progress.ProgressUpdateBuffer<T> progressBuffer)
            : this(postOffice, endpoint, id, vertexId)
        {
            this.progressBuffer = progressBuffer;
        }
        
        public LegacyLocalMailbox(PostOffice postOffice, VertexInput<S, T> endpoint, int id, int vertexId)
            : base(postOffice, endpoint, id, vertexId)
        {
            this.sharedQueue = new ConcurrentQueue<Message<S, T>>();
            this.sharedSerializedQueue = new ConcurrentQueue<SerializedMessage>();
            this.privateQueue = new Queue<Message<S, T>>();

            this.messagesFromLocalVertices = new Message<S, T>[this.postOffice.Controller.Workers.Count];
            for (int i = 0; i < this.postOffice.Controller.Workers.Count; ++i)
                this.messagesFromLocalVertices[i] = new Message<S, T>();

            this.decoder = new AutoSerializedMessageDecoder<S, T>(endpoint.Vertex.SerializationFormat);
        }

        int SerializedQueueHighWatermark = 10;

        /// <summary>
        /// Enqueues the incoming network message on the shared queue, called by the network channel.
        /// </summary>
        /// <param name="message">message</param>
        /// <param name="from">sender</param>
        public override void DeliverSerializedMessage(SerializedMessage message, ReturnAddress from)
        {
            this.sharedSerializedQueue.Enqueue(message);

            if (this.sharedSerializedQueue.Count > SerializedQueueHighWatermark)
                SerializedQueueHighWatermark *= 2;

            if (!this.dirty)
                this.dirty = true;

            this.postOffice.Signal();
        }

        /// <summary>
        /// Flushes the queue of local messages.  
        /// Called by the postbox.
        /// </summary>
        /// <param name="from">Postbox of the upstream operator</param>
        public override void Flush(ReturnAddress from)
        {
            if (this.messagesFromLocalVertices[from.ThreadIndex].length > 0)
                this.PostBufferAndPossiblyCutThrough(from);

            // XXX : Assumes placement is the same, which is brittle
            if (this.VertexId == from.VertexID)
            {
                if (this.endpoint.AvailableEntrancy >= 0)
                {
                    this.endpoint.AvailableEntrancy = this.endpoint.AvailableEntrancy - 1;
                    endpoint.Flush();
                    this.endpoint.AvailableEntrancy = this.endpoint.AvailableEntrancy + 1;
                }

                this.progressBuffer.Flush();
            }
        }

        public override void Send(Message<S, T> records, ReturnAddress from)
        {
            for (int i = 0; i < records.length; i++)
            {
                // Message has been disabled to conserve memory.
                if (this.messagesFromLocalVertices[from.ThreadIndex].Unallocated)
                {
                    this.messagesFromLocalVertices[from.ThreadIndex].Allocate();
                    this.messagesFromLocalVertices[from.ThreadIndex].time = records.time;
                }

                // we may need to flush the buffered message because it has a different time.
                if (!this.messagesFromLocalVertices[from.ThreadIndex].time.Equals(records.time))
                {
                    PostBufferAndPossiblyCutThrough(from);

                    // it is possible that cut through (and re-entrancy) results in records still present here.
                    if (!this.messagesFromLocalVertices[from.ThreadIndex].Unallocated)
                    {
                        this.Post(this.messagesFromLocalVertices[from.ThreadIndex]);
                        this.messagesFromLocalVertices[from.ThreadIndex] = new Message<S, T>();
                    }

                    this.messagesFromLocalVertices[from.ThreadIndex].Allocate();
                    this.messagesFromLocalVertices[from.ThreadIndex].time = records.time;
                }

                // actually put the record into the buffer
                this.messagesFromLocalVertices[from.ThreadIndex].payload[this.messagesFromLocalVertices[from.ThreadIndex].length++] = records.payload[i];

                // if the buffer is now full, might as well ship the data [and consider cutting through to other dataflow vertices]
                if (this.messagesFromLocalVertices[from.ThreadIndex].length == this.messagesFromLocalVertices[from.ThreadIndex].payload.Length)
                    PostBufferAndPossiblyCutThrough(from);
            }
        }

        private void PostBufferAndPossiblyCutThrough(ReturnAddress from)
        {
            // no matter what, post the buffer and refresh local storage.
            this.Post(this.messagesFromLocalVertices[from.ThreadIndex]);
            this.messagesFromLocalVertices[from.ThreadIndex] = new Message<S, T>();

            // we may want to cut-through. 
            if (this.endpoint.Vertex.Scheduler.Index == from.ThreadIndex && this.endpoint.AvailableEntrancy >= 0)
            {
                this.endpoint.AvailableEntrancy = this.endpoint.AvailableEntrancy - 1;
                this.Drain();
                this.endpoint.AvailableEntrancy = this.endpoint.AvailableEntrancy + 1;
            }
        }
    }

#if false
    internal class SpillFile<T> : IDisposable
    {
        private class CircularBuffer<U>
        {
            private readonly U[] buffer;
            private int producerPointer;
            private int consumerPointer;
            private int elementsInBuffer;

            public int Free { get { return this.buffer.Length - this.elementsInBuffer; } }

            public CircularBuffer(int capacity)
            {
                this.buffer = new U[capacity];
                this.producerPointer = 0;
                this.consumerPointer = 0;
                this.elementsInBuffer = 0;
            }

            public bool TryAdd(U element)
            {
                if (this.elementsInBuffer < this.buffer.Length)
                {
                    this.buffer[this.producerPointer] = element;
                    this.producerPointer = (this.producerPointer + 1) % this.buffer.Length;
                    this.elementsInBuffer++;
                    return true;
                }
                else
                    return false;
            }

            public bool TryTake(out U element)
            {
                if (this.elementsInBuffer > 0)
                {
                    // Prefer to return elements from the buffer.
                    element = this.buffer[this.consumerPointer];
                    this.consumerPointer = (this.consumerPointer + 1) % this.buffer.Length;
                    this.elementsInBuffer--;
                    return true;
                }
                else
                {
                    element = default(U);
                    return false;
                }
            }
        }

        private readonly FileStream spillStream;
        private readonly FileStream consumeStream;

        private readonly CircularBuffer<T> typedBuffer;

        private readonly SerializedMessageEncoder<T> encoder;
        private readonly SerializedMessageEncoder<T> logEncoder;
        private readonly SerializedMessageDecoder<T> decoder;

        private SerializedMessage currentRereadMessage;
        private IEnumerator<T> currentRereadMessageEnumerator;

        private byte[] rereadMessageHeaderBuffer;
        private byte[] rereadMessageBodyBuffer;

        private readonly bool logAllElements;

        private readonly NaiadSerialization<MessageHeader> headerSerializer;

        public SpillFile(string filename, int bufferLength, SerializedMessageEncoder<T> encoder, SerializedMessageDecoder<T> decoder, int pageSize, NaiadSerialization<MessageHeader> headerSerializer)
            : this(filename, bufferLength, encoder, null, decoder, pageSize, headerSerializer)
        {}

        public SpillFile(string filename, int bufferLength, SerializedMessageEncoder<T> spillEncoder, SerializedMessageEncoder<T> logEncoder, SerializedMessageDecoder<T> decoder, int pageSize, NaiadSerialization<MessageHeader> headerSerializer)
        {
            this.typedBuffer = new CircularBuffer<T>(bufferLength);

            this.encoder = spillEncoder;
            this.encoder.CompletedMessage += encoder_CompletedMessage;

            this.decoder = decoder;

            this.spillStream = new FileStream(filename, FileMode.CreateNew, FileAccess.Write, FileShare.ReadWrite, 1 << 20);//, FileOptions.WriteThrough);
            this.consumeStream = File.Open(filename, FileMode.Open, FileAccess.Read, FileShare.ReadWrite);

            this.rereadMessageHeaderBuffer = new byte[MessageHeader.SizeOf];
            this.rereadMessageBodyBuffer = new byte[pageSize];

            this.logAllElements = logEncoder != null;
            if (this.logAllElements)
            {
                this.logEncoder = logEncoder;
                this.logEncoder.CompletedMessage += logEncoder_CompletedMessage;
            }

            this.headerSerializer = headerSerializer;
        }

        void logEncoder_CompletedMessage(object sender, CompletedMessageArgs e)
        {
            ArraySegment<byte> message = e.Segment.ToArraySegment();
            this.spillStream.Write(message.Array, message.Offset, message.Count);
            e.Segment.Dispose();
        }

        public void Dispose()
        {
            this.spillStream.Dispose();
            this.consumeStream.Dispose();
            if (this.currentRereadMessageEnumerator != null)
                this.currentRereadMessageEnumerator.Dispose();
            if (this.currentRereadMessage != null)
                this.currentRereadMessage.Dispose();
        }

        public void Write(T element)
        {
            bool success = this.typedBuffer.TryAdd(element);
            if (!success)
                this.encoder.Write(element);
            else if (this.logAllElements)
                this.logEncoder.Write(element);
        }

        public void Write(SerializedMessage message)
        {
            if (this.typedBuffer.Free > 0)
            {
                // Attempt to process the records in memory (but may spill some if there is no capacity).
                foreach (T record in this.decoder.Elements(message))
                    this.Write(record);
            }
            else
            {
                // Write the message directly to the file.
                MessageHeader.WriteHeaderToBuffer(this.rereadMessageHeaderBuffer, 0, message.Header, this.headerSerializer);
                this.spillStream.Write(this.rereadMessageHeaderBuffer, 0, this.rereadMessageHeaderBuffer.Length);
                this.spillStream.Write(message.Body.Buffer, message.Body.CurrentPos, message.Body.End - message.Body.CurrentPos);
            }
        }

        public void Flush()
        {
            this.encoder.Flush();
            if (this.logEncoder != null)
                this.logEncoder.Flush();
            this.spillStream.Flush();
        }

        private void encoder_CompletedMessage(object sender, CompletedMessageArgs e)
        {
            ArraySegment<byte> message = e.Segment.ToArraySegment();
            this.spillStream.Write(message.Array, message.Offset, message.Count);
            e.Segment.Dispose();
        }

        public bool TryGetNextElement(out T element)
        {
            if (this.typedBuffer.TryTake(out element))
                return true;
            else
                return TryGetNextElementFromRereadMessage(out element);
        }

        private bool TryGetNextElementFromRereadMessage(out T element)
        {
            if (this.currentRereadMessageEnumerator != null)
            {
                // Otherwise, if we are current processing a spilled message, return messages from that.
                element = this.currentRereadMessageEnumerator.Current;
                if (!this.currentRereadMessageEnumerator.MoveNext())
                {
                    this.currentRereadMessageEnumerator.Dispose();
                    this.currentRereadMessageEnumerator = null;
                    this.currentRereadMessage.Dispose();
                    this.currentRereadMessage = null;
                }
                return true;
            }
            return TryGetNextElementByRereadingAnotherMessage(out element);
        }
        
        private bool TryGetNextElementByRereadingAnotherMessage(out T element)
        {
            if (TryReadNextMessage())
            {
                TryGetNextElementFromRereadMessage(out element);
                return true;
            }
            element = default(T);
            return false;
        }

        private bool TryReadNextMessage()
        {
            MessageHeader nextMessageHeader = default(MessageHeader);
            int bytesRead = this.consumeStream.Read(this.rereadMessageHeaderBuffer, 0, this.rereadMessageHeaderBuffer.Length);
            if (bytesRead < this.rereadMessageHeaderBuffer.Length)
            {
                // Reset to the start of a new message.
                this.consumeStream.Seek(-bytesRead, SeekOrigin.Current);
                return false;
            }

            MessageHeader.ReadHeaderFromBuffer(this.rereadMessageHeaderBuffer, 0, ref nextMessageHeader, headerSerializer);

            if (nextMessageHeader.Type == SerializedMessageType.CheckpointData)
            {
                // Skip this because we have already buffered these elements in memory.
                this.consumeStream.Seek(nextMessageHeader.Length, SeekOrigin.Current);
                Console.Error.WriteLine("Skipping a log message of length {0}", nextMessageHeader.Length);
                return TryReadNextMessage();
            }

            bytesRead = this.consumeStream.Read(this.rereadMessageBodyBuffer, 0, nextMessageHeader.Length);
            if (bytesRead < nextMessageHeader.Length)
            {
                // Reset to the start of a new message.
                this.consumeStream.Seek(-(bytesRead + MessageHeader.SizeOf), SeekOrigin.Current);
                return false;
            }

            RecvBuffer messageBody = new RecvBuffer(this.rereadMessageBodyBuffer, 0, nextMessageHeader.Length);
            this.currentRereadMessage = new SerializedMessage(-1, nextMessageHeader, messageBody);
            this.currentRereadMessageEnumerator = this.decoder.Elements(currentRereadMessage).GetEnumerator();
            this.currentRereadMessageEnumerator.MoveNext();
            return true;
        }
    }
#endif

#if false
    internal class SpillingLocalMailbox<S, T> : LocalMailbox<S, T>
        where T : Time<T>
    {

        private FileStream spillStream;
        private FileStream consumeStream;

        private readonly SendBufferPage[] messagesFromLocalThreads;

        private int pagesRead;
        private int pagesWritten;
        private volatile int pagesFlushed;

        public override void DeliverSerializedMessage(SerializedMessage message, RemotePostbox from)
        {
            byte[] headerBuffer = new byte[MessageHeader.SizeOf];
            MessageHeader.WriteHeaderToBuffer(headerBuffer, 0, message.Header);
            lock (this)
            {
                if (message.Header.Length == 0)
                {
                    this.spillStream.Flush(true);
                    this.pagesFlushed = this.pagesWritten;
                    this.dirty = true;
                }
                else
                {
                    Debug.Assert(this.spillStream.Position % this.pageSize== 0);
                    this.spillStream.Write(headerBuffer, 0, headerBuffer.Length);
                    int count = message.Body.End - message.Body.CurrentPos;
                    this.spillStream.Write(message.Body.Buffer, message.Body.CurrentPos, count);
                    int padEnd = this.pageSize - (count + MessageHeader.SizeOf);
                    if (padEnd != 0)
                        this.spillStream.Seek(padEnd, SeekOrigin.Current);

                    ++this.pagesWritten;
                    Debug.Assert(this.spillStream.Position % this.pageSize == 0);
                }
            }
            message.Dispose();
            this.postOffice.Signal();
        }

        private void SpillBuffer(int padStart, byte[] buffer, int offset, int count, int padEnd)
        {
            if (padStart != 0)
                this.spillStream.Seek(padStart, SeekOrigin.Current);
            this.spillStream.Write(buffer, offset, count);
            if (padEnd != 0)
                this.spillStream.Seek(padEnd, SeekOrigin.Current);
            ++this.pagesWritten;
        }

        private void SpillBufferSegment(BufferSegment segment, int padStart)
        {
            ArraySegment<byte> arraySegment = segment.ToArraySegment();
            int padEnd = this.pageSize - (padStart + arraySegment.Count);
            this.SpillBuffer(padStart, arraySegment.Array, arraySegment.Offset, arraySegment.Count, padEnd);
        }

        private void SpillLocalMessage(RemotePostbox from)
        {
            lock (this)
            {
                Debug.Assert(this.spillStream.Position % this.pageSize == 0);
                this.messagesFromLocalThreads[from.ThreadIndex].FinalizeLastMessage();
                using (BufferSegment segment = this.messagesFromLocalThreads[from.ThreadIndex].Consume())
                    this.SpillBufferSegment(segment, 0);
                this.messagesFromLocalThreads[from.ThreadIndex].Release();
                this.messagesFromLocalThreads[from.ThreadIndex] = null;
                Debug.Assert(this.spillStream.Position % this.pageSize == 0);
            }
        }

        public override void Flush(RemotePostbox from)
        {
            if (this.messagesFromLocalThreads[from.ThreadIndex] != null)
            {
                lock (this)
                {
                    this.SpillLocalMessage(from);
                    this.spillStream.Flush(true);
                    Debug.Assert(this.spillStream.Position % this.pageSize == 0);
                    this.pagesFlushed = this.pagesWritten;
                    this.dirty = true;
                }
                this.postOffice.Signal();
            }
        }

        public override void Flush()
        {
        }

        private NaiadSerialization<Pair<S, T>> serializer;

        public override void Send(Pair<S, T> record, RemotePostbox from)
        {
            if (this.serializer == null)
                this.serializer = AutoSerialization.GetSerializer<Pair<S, T>>();

            SendBufferPage page = this.messagesFromLocalThreads[from.ThreadIndex];
            if (page == null)
            {
                this.messagesFromLocalThreads[from.ThreadIndex] = new SendBufferPage(GlobalBufferPool<byte>.pool, this.pageSize);
                page = this.messagesFromLocalThreads[from.ThreadIndex];
                page.WriteHeader(new MessageHeader(from.VertexID, /* TODO FIXME: this.sendSequenceNumbers[destProcessID, destVertexID]++ */ 0, this.Id, this.VertexId, SerializedMessageType.Data));                
            }

            if (!page.WriteRecord(this.serializer, record))
            {
                this.Flush(from);
                this.messagesFromLocalThreads[from.ThreadIndex] = new SendBufferPage(GlobalBufferPool<byte>.pool, this.pageSize);
                page = this.messagesFromLocalThreads[from.ThreadIndex];
                page.WriteHeader(new MessageHeader(from.VertexID, /* TODO FIXME: this.sendSequenceNumbers[destProcessID, destVertexID]++ */ 0, this.Id, this.VertexId, SerializedMessageType.Data));

                bool success = page.WriteRecord(this.serializer, record);
                if (!success)
                    throw new Exception("Record too long to spill");
            }
        }

        public override void Drain()
        {
            int recordsReceived = 0;
            byte[] consumeBuffer = null;
            for (; this.pagesRead < this.pagesFlushed; ++this.pagesRead)
            {
                Debug.Assert(this.consumeStream.Position % this.pageSize == 0);
                if (consumeBuffer == null)
                    consumeBuffer = ThreadLocalBufferPools<byte>.pool.Value.CheckOut(this.pageSize);
                int bytesRead = this.consumeStream.Read(consumeBuffer, 0, this.pageSize);
                if (bytesRead < this.pageSize)
                {
                    // We must be at the end of the file, where zero-bytes appear to be truncated.

                    MessageHeader innerHeader = default(MessageHeader);
                    MessageHeader.ReadHeaderFromBuffer(consumeBuffer, 0, ref innerHeader);
                    if (innerHeader.Length + MessageHeader.SizeOf == bytesRead)
                    {
                        Logging.Info("Read complete data from partial page {0} bytes", bytesRead);

                        // Get the consume stream in the correct place for the next read.
                        this.consumeStream.Seek(this.pageSize - bytesRead, SeekOrigin.Current);
                    }
                    else
                    {
                        Debug.Assert(false);
                    }
                }
                Debug.Assert(this.consumeStream.Position % this.pageSize == 0);
                
                if (this.serializer == null)
                    this.serializer = AutoSerialization.GetSerializer<Pair<S, T>>();

                MessageHeader header = default(MessageHeader);
                MessageHeader.ReadHeaderFromBuffer(consumeBuffer, 0, ref header);

                RecvBuffer messageBody = new RecvBuffer(consumeBuffer, MessageHeader.SizeOf, MessageHeader.SizeOf + header.Length);

                Pair<S, T> record;
                while (this.serializer.TryDeserialize(ref messageBody, out record))
                {
                    this.endpoint.RecordReceived(record, new RemotePostbox());
                    if (progressBuffer != null)
                        progressBuffer.Update(record.v2, -1);
                    ++recordsReceived;
                }
                Logging.Info("Drained a spilt page into operator {1}", recordsReceived, this.endpoint.Vertex);
            }
            if (recordsReceived > 0)
            {
                this.endpoint.Flush();
                if (this.progressBuffer != null)
                    this.progressBuffer.Flush();

                this.postOffice.Controller.Workers.NotifyOperatorReceivedRecords(this.endpoint.Vertex, this.id, recordsReceived);
                Logging.Info("Drained {0} records into operator {1}", recordsReceived, this.endpoint.Vertex);
            }
            if (consumeBuffer != null)
                ThreadLocalBufferPools<byte>.pool.Value.CheckIn(consumeBuffer);
        }

        private readonly int pageSize;

        public SpillingLocalMailbox(PostOffice postoffice, VertexInput<S, T> endpoint, int id, int vertexId, Runtime.Progress.ProgressUpdateBuffer<T> progressBuffer)
            : base(postoffice, endpoint, id, vertexId)
        {
            this.pageSize = this.endpoint.Vertex.Stage.InternalGraphManager.Controller.Configuration.SendPageSize;

            this.progressBuffer = progressBuffer;

            this.messagesFromLocalThreads = new SendBufferPage[endpoint.Vertex.Stage.InternalGraphManager.Controller.Workers.Count];
            
            string fileName = Path.Combine(Path.GetTempPath(), Path.GetRandomFileName());

            this.spillStream = new FileStream(fileName, FileMode.CreateNew, FileAccess.Write, FileShare.ReadWrite, 1<<20);//, FileOptions.WriteThrough);

            this.consumeStream = File.Open(fileName, FileMode.Open, FileAccess.Read, FileShare.ReadWrite);

            this.pagesWritten = 0;
            this.pagesRead = 0;
            this.pagesFlushed = 0;
        }
    }
#endif
    internal class RemoteMailbox<S, T> : Mailbox<S, T>
        where T : Time<T>
    {
        private readonly int channelID;
        public int Id { get { return this.channelID; } }

        private readonly int processID;

        private readonly int vertexID;
        public int VertexId { get { return this.vertexID; } }

        private readonly int graphID;
        public int GraphId { get { return this.graphID; } }

        private readonly NetworkChannel networkChannel;
        private readonly AutoSerializedMessageEncoder<S, T>[] encodersFromLocalVertices;
        
        public int ThreadIndex { get { return -1; } }

        public RemoteMailbox(int channelID, int processID, int vertexID, InternalComputation manager)
        {
            this.channelID = channelID;
            this.processID = processID;
            this.vertexID = vertexID;
            this.graphID = manager.Index;

            this.networkChannel = manager.Controller.NetworkChannel;

            var controller = manager.Controller;
            this.encodersFromLocalVertices = new AutoSerializedMessageEncoder<S, T>[controller.Workers.Count];

            for (int i = 0; i < controller.Workers.Count; ++i )
            {
                this.encodersFromLocalVertices[i] = new AutoSerializedMessageEncoder<S, T>(this.vertexID, this.graphID << 16 | this.channelID, this.networkChannel.GetBufferPool(this.processID, i), this.networkChannel.SendPageSize, manager.SerializationFormat, SerializedMessageType.Data, () => this.networkChannel.GetSequenceNumber(this.processID));
                this.encodersFromLocalVertices[i].CompletedMessage += (o, a) => { this.networkChannel.SendBufferSegment(a.Hdr, this.processID, a.Segment); };
            }
        }

        public void Flush(ReturnAddress from)
        {

            this.encodersFromLocalVertices[from.ThreadIndex].Flush();
        }

        public void Send(Pair<S, T> record, ReturnAddress from)
        {
            this.encodersFromLocalVertices[from.ThreadIndex].Write(record, from.VertexID);
        }

        public void Send(Message<S, T> message, ReturnAddress from)
        {
            this.encodersFromLocalVertices[from.ThreadIndex].SetCurrentTime(message.time);
            this.encodersFromLocalVertices[from.ThreadIndex].Write(new ArraySegment<S>(message.payload, 0, message.length), from.VertexID);
            //for (int i = 0; i < message.length; ++i)
            //    this.encodersFromLocalVertices[from.ThreadIndex].Write(message.payload[i], from.VertexID);
        }

        public void DeliverSerializedMessage(SerializedMessage message, ReturnAddress from)
        {
            throw new NotImplementedException("Attempted to deliver message from a remote sender to a remote recipient, which is not currently supported.");
        }
    }
}
