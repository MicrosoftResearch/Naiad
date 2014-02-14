/*
 * Naiad ver. 0.2
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
using Naiad.DataStructures;
using System.Collections.Concurrent;
using System.Diagnostics;
using Naiad.CodeGeneration;
using System.Threading;
using System.Net.Sockets;
using Naiad.Scheduling;
using System.Net;
using System.IO;
using Naiad.Runtime.Controlling;
using Naiad.Runtime.Networking;

namespace Naiad.Dataflow.Channels
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

            var graphManager = sendBundle.ForStage.InternalGraphManager;

            foreach (VertexLocation loc in recvBundle.ForStage.Placement)
            {
                if (loc.ProcessId == graphManager.Controller.Configuration.ProcessID)
                {
                    var postOffice = this.recvBundle.GetPin(loc.VertexId).Vertex.Scheduler.State(graphManager).PostOffice;
                    LocalMailbox<S, T> localMailbox;

                    var progressBuffer = new Runtime.Progress.ProgressUpdateBuffer<T>(this.ChannelId, this.recvBundle.GetPin(loc.VertexId).Vertex.Scheduler.State(graphManager).Producer); 

                    if ((flags & Channel.Flags.SpillOnRecv) == Channel.Flags.SpillOnRecv)
                    {
                        localMailbox = new SpillingLocalMailbox<S, T>(postOffice, this.recvBundle.GetPin(loc.VertexId), channelID, loc.VertexId, progressBuffer);
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
                    this.mailboxes[loc.VertexId] = new RemoteMailbox<S, T>(this.channelID, loc.ProcessId, loc.VertexId, sendBundle.ForStage.InternalGraphManager);
            }

            foreach (VertexLocation loc in sendBundle.ForStage.Placement)
                if (loc.ProcessId == sendBundle.ForStage.InternalGraphManager.Controller.Configuration.ProcessID)
                {
                    var progressBuffer = new Runtime.Progress.ProgressUpdateBuffer<T>(this.ChannelId, this.sendBundle.GetFiber(loc.VertexId).Vertex.Scheduler.State(graphManager).Producer);

                    this.postboxes[loc.VertexId] = new Postbox<S, T>(this.channelID, this.sendBundle.GetFiber(loc.VertexId), this.recvBundle, this.mailboxes, routingHashcodeFunction, networkChannel, progressBuffer);
                }
            
        }

        public override string ToString()
        {
            return String.Format("Exchange channel [{0}]: {1} -> {2}", this.channelID, sendBundle, recvBundle);
        }

        public Dataflow.Stage SourceStage { get { return this.sendBundle.ForStage; } }
        public Dataflow.Stage DestinationStage { get { return this.recvBundle.ForStage; } }

        public SendWire<S, T> GetSendFiber(int i)
        {
            return this.postboxes[i];
        }

        public RecvWire<S, T> GetRecvFiber(int i)
        {
            return (LocalMailbox<S, T>)this.mailboxes[i];
        }
    }

    public interface UntypedPostbox
    {
        int ThreadIndex { get; }
        int ShardID { get; }
        int ProcessID { get; }
    }

    internal class DummyPostbox : UntypedPostbox
    {
        public int ThreadIndex
        {
            get { return 0; }
        }

        public int ShardID
        {
            get { return 0; }
        }

        public int ProcessID
        {
            get { return 0; }
        }

        public static DummyPostbox Instance = new DummyPostbox();
    }

    public struct RemotePostbox : UntypedPostbox
    {
        private readonly int threadIndex;
        public int ThreadIndex { get { return this.threadIndex; } }
        private readonly int shardId;
        public int ShardID { get { return this.shardId; } }
        private readonly int processId;
        public int ProcessID { get { return this.processId; } }

        public RemotePostbox(int processId, int shardId)
        {
            this.shardId = shardId;
            this.processId = processId;
            this.threadIndex = 0;
        }

        public RemotePostbox(int processId, int shardId, int threadIndex)
        {
            this.shardId = shardId;
            this.processId = processId;
            this.threadIndex = threadIndex;
        }
    }

    internal class Postbox<S, T> : SendWire<S, T>, UntypedPostbox
        where T : Time<T>
    {
        public int RecordSizeHint
        {
            get
            {
                RemoteMailbox<S, T> remote = this.mailboxes.FirstOrDefault(x => x is RemoteMailbox<S, T>) as RemoteMailbox<S, T>;
                if (remote == null)
                    return int.MaxValue;
                else
                    return remote.RecordSizeHint;
            }
        }

        private readonly int threadindex;
        public int ThreadIndex { get { return this.threadindex; } }

        private readonly int channelID;
        private readonly VertexOutput<S, T> sender;
        private readonly int shardid;
        public int ShardID { get { return this.shardid; } }

        public int ProcessID { get { return this.sender.Vertex.Stage.InternalGraphManager.Controller.Configuration.ProcessID; } }
        
        private readonly StageInput<S, T> receiverBundle;

        private readonly Func<S, int> routingHashcodeFunction;
        private readonly NetworkChannel networkChannel;

        //private readonly int[,] sendSequenceNumbers;

        private readonly RemotePostbox remotePostbox;

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
            this.shardid = sender.Vertex.VertexId;
            this.receiverBundle = receiverBundle;
            this.routingHashcodeFunction = routingHashcodeFunction;
            this.networkChannel = networkChannel;
            this.mailboxes = mailboxes;
            this.remotePostbox = new RemotePostbox(this.ProcessID, this.shardid, this.threadindex);
            
            //this.sendSequenceNumbers = new int[Naiad.Processes, this.receiverBundle.LocalParallelism];
        }

        /// <summary>
        /// Sends a record
        /// </summary>
        /// <param name="record"></param>
        public void Send(Pair<S, T> record)
        {
            int destShardID;
            if (this.routingHashcodeFunction == null)
            {
                destShardID = this.shardid % this.mailboxes.Length;
            }
            else
            {
                int hashcode = this.routingHashcodeFunction(record.v1);
                destShardID = (hashcode & int.MaxValue) % this.mailboxes.Length; // this.routingTable.GetRoute(hashcode);
            }

            Debug.Assert(destShardID < this.mailboxes.Length);
            this.mailboxes[destShardID].Send(record, new RemotePostbox(this.ProcessID, this.ShardID, this.ThreadIndex));

            if (progressBuffer != null)
                progressBuffer.Update(record.v2, +1);
        } 

        public void Send(Message<Pair<S, T>> records)
        {
            for (int i = 0; i < records.length; i++)
            {
                var record = records.payload[i];

                int destShardID;
                if (this.routingHashcodeFunction == null)
                {
                    destShardID = this.shardid % this.mailboxes.Length;
                }
                else
                {
                    int hashcode = this.routingHashcodeFunction(record.v1);
                    destShardID = (hashcode & int.MaxValue) % this.mailboxes.Length; // this.routingTable.GetRoute(hashcode);
                }

                Debug.Assert(destShardID < this.mailboxes.Length);
                this.mailboxes[destShardID].Send(record, this.remotePostbox);

                if (progressBuffer != null)
                    progressBuffer.Update(record.v2, +1);
            }
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
                    this.mailboxes[i].Flush(new RemotePostbox(this.ProcessID, this.ShardID, this.ThreadIndex));
                }
            }

            // now flush the local mailboxes, which may fall through into executing actual code
            for (int i = 0; i < this.mailboxes.Length; ++i)
            {
                if (this.mailboxes[i].ThreadIndex == this.threadindex)
                {
                    this.mailboxes[i].Flush(new RemotePostbox(this.ProcessID, this.ShardID, this.ThreadIndex));
                }
            }

            //Console.WriteLine("Flushing " + this.channelID);
            if (progressBuffer != null)
                progressBuffer.Flush();
        }
    

    }

    internal class PostOffice
    {
        private static int nextChannelId = 0;
        
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

    public interface UntypedMailbox
    {
        void DeliverSerializedMessage(SerializedMessage message, RemotePostbox sender);

        int GraphId { get; }
        int Id { get; }
        int ShardId { get; }
    }

    public interface UntypedLocalMailbox : UntypedMailbox
    {
        void Drain();
        void Flush();
    }

    internal interface Mailbox<S, T> : UntypedMailbox
        where T : Time<T>
    {
        void Send(Pair<S, T> record, RemotePostbox from);
        void Flush(RemotePostbox from);
        // returns the ThreadId for mailboxes on the same computer, and -1 for remote ones
        int ThreadIndex { get; }
    }


    internal abstract class LocalMailbox<S, T> : RecvWire<S, T>, UntypedLocalMailbox, Mailbox<S, T>
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
        public readonly int shardId;    // receiver
        public readonly int graphId;

        public int Id { get { return this.id; } }
        public int ShardId { get { return this.shardId; } }
        public int GraphId { get { return this.graphId; } }
        public int ThreadIndex { get { return this.endpoint.Vertex.scheduler.Index; } }

        protected Runtime.Progress.ProgressUpdateBuffer<T> progressBuffer;

        public LocalMailbox(PostOffice postOffice, VertexInput<S, T> endpoint, int id, int shardId)
        {
            this.postOffice = postOffice;
            this.dirty = false;
            this.endpoint = endpoint;
            this.id = id;
            this.shardId = shardId;
            this.graphId = endpoint.Vertex.Stage.InternalGraphManager.Index;
        }

        public abstract void DeliverSerializedMessage(SerializedMessage message, RemotePostbox from);
        public abstract void Flush(RemotePostbox from);
        public abstract void Flush();
        public abstract void Send(Pair<S, T> record, RemotePostbox from);
        public abstract void Drain();
    }

    internal class LegacyLocalMailbox<S, T> : LocalMailbox<S, T>
        where T : Time<T>
    {

        //private NaiadSerialization<Pair<S, T>> deserializer;

        private readonly ConcurrentQueue<Message<Pair<S, T>>> sharedQueue;
        private readonly ConcurrentQueue<SerializedMessage> sharedSerializedQueue;
        private readonly Queue<Message<Pair<S, T>>> privateQueue;
        
        private readonly Message<Pair<S, T>>[] messagesFromLocalShards;

        private readonly AutoSerializedMessageDecoder<S, T> decoder = new AutoSerializedMessageDecoder<S, T>();

        /// <summary>
        /// Move records from the private queue into the operator state by giving them to the endpoint.
        /// </summary>
        public override void Drain()
        {
            // I believe entrancy is checked before hand in LLM, and not needed when called from the scheduler. 
            if (this.dirty)
            {
                this.dirty = false;

                // Pull messages received from other processes
                SerializedMessage superChannelMessage;
                while (this.sharedSerializedQueue.TryDequeue(out superChannelMessage))
                {
                    this.endpoint.SerializedMessageReceived(superChannelMessage, new RemotePostbox());
                    if (progressBuffer != null)
                        foreach (Pair<T, int> delta in this.decoder.Times(superChannelMessage))
                            progressBuffer.Update(delta.v1, -delta.v2);

                    // superChannelMessage.Dispose();
                }

                // Pull messages received from the local process
                var message = new Message<Pair<S, T>>();
                while (this.sharedQueue.TryDequeue(out message))
                {
                    this.endpoint.MessageReceived(message, new RemotePostbox());

                    if (progressBuffer != null)
                        for (int i = 0; i < message.length; i++)
                            progressBuffer.Update(message.payload[i].v2, -1);

                    if (message.length == 0)
                        Logging.Info("Dequeued empty message");

                    // message.Disable() ?
                    ThreadLocalBufferPools<Pair<S, T>>.pool.Value.CheckIn(message.payload);
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
        private void Post(Message<Pair<S, T>> message)
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
        public LegacyLocalMailbox(PostOffice postOffice, VertexInput<S, T> endpoint, int id, int shardId, Runtime.Progress.ProgressUpdateBuffer<T> progressBuffer)
            : this(postOffice, endpoint, id, shardId)
        {
            this.progressBuffer = progressBuffer;
        }
        
        public LegacyLocalMailbox(PostOffice postOffice, VertexInput<S, T> endpoint, int id, int shardId)
            : base(postOffice, endpoint, id, shardId)
        {
            this.sharedQueue = new ConcurrentQueue<Message<Pair<S, T>>>();
            this.sharedSerializedQueue = new ConcurrentQueue<SerializedMessage>();
            this.privateQueue = new Queue<Message<Pair<S, T>>>();

            this.messagesFromLocalShards = new Message<Pair<S, T>>[this.postOffice.Controller.Workers.Count];
            for (int i = 0; i < this.postOffice.Controller.Workers.Count; ++i)
                this.messagesFromLocalShards[i] = new Message<Pair<S, T>>(ThreadLocalBufferPools<Pair<S, T>>.pool.Value.Empty);
        }

        private bool Recv(ref Message<Pair<S, T>> message)
        {
            ThreadLocalBufferPools<Pair<S, T>>.pool.Value.CheckIn(message.payload);
            if (this.privateQueue.Count > 0)
            {
                message = this.privateQueue.Dequeue();
            }
            else
            {
                message.Disable();
                message.length = -1;
            }

            return message.length >= 0;
        }

        int SerializedQueueHighWatermark = 10;

        /// <summary>
        /// Enqueues the incoming network message on the shared queue, called by the network channel.
        /// </summary>
        /// <param name="message"></param>
        public override void DeliverSerializedMessage(SerializedMessage message, RemotePostbox from)
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
        public override void Flush(RemotePostbox from)
        {
            int i = from.ThreadIndex;

            if (this.messagesFromLocalShards[i].length > 0)
            {
                if (this.endpoint.Vertex.Scheduler.Index == from.ThreadIndex && this.endpoint.AvailableEntrancy >= 0)
                {
                    for (int j = 0; j < this.messagesFromLocalShards[from.ThreadIndex].length; j++)
                        if (progressBuffer != null)
                            progressBuffer.Update(this.messagesFromLocalShards[from.ThreadIndex].payload[j].v2, -1);


                    // capturing buffer to avoid writing to it in reentrancy.
                    var buffer = this.messagesFromLocalShards[from.ThreadIndex];

                    this.messagesFromLocalShards[from.ThreadIndex].Disable();

                    this.endpoint.AvailableEntrancy = this.endpoint.AvailableEntrancy - 1;

                    this.endpoint.MessageReceived(buffer, from);

                    this.Drain();

                    this.endpoint.AvailableEntrancy = this.endpoint.AvailableEntrancy + 1;

                    ThreadLocalBufferPools<Pair<S, T>>.pool.Value.CheckIn(buffer.payload);
                    buffer.Disable();
                }
                else
                {
                    this.Post(this.messagesFromLocalShards[i]);
                    this.messagesFromLocalShards[i].Disable();
                }
            }

            if (this.ShardId == from.ShardID)
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

        public override void Send(Pair<S, T> record, RemotePostbox from)
        {
            if (this.messagesFromLocalShards[from.ThreadIndex].payload.Length == 0)
                // Message has been disabled to conserve memory.
                this.messagesFromLocalShards[from.ThreadIndex].Enable();

            this.messagesFromLocalShards[from.ThreadIndex].payload[this.messagesFromLocalShards[from.ThreadIndex].length++] = record;
            if (this.messagesFromLocalShards[from.ThreadIndex].length == this.messagesFromLocalShards[from.ThreadIndex].payload.Length)
            {
                if (this.endpoint.Vertex.Scheduler.Index == from.ThreadIndex && this.endpoint.AvailableEntrancy >= 0)
                {
                    for (int i = 0; i < this.messagesFromLocalShards[from.ThreadIndex].length; i++)
                        if (progressBuffer != null)
                            progressBuffer.Update(this.messagesFromLocalShards[from.ThreadIndex].payload[i].v2, -1);

                    // capturing buffer to avoid writing to it in reentrancy.
                    var buffer = this.messagesFromLocalShards[from.ThreadIndex];

                    this.messagesFromLocalShards[from.ThreadIndex].Disable();

                    this.endpoint.AvailableEntrancy = this.endpoint.AvailableEntrancy - 1;

                    this.endpoint.MessageReceived(buffer, from);

                    ThreadLocalBufferPools<Pair<S, T>>.pool.Value.CheckIn(buffer.payload);
                    buffer.Disable();

                    this.Drain();

                    this.endpoint.AvailableEntrancy = this.endpoint.AvailableEntrancy + 1;
                }
                else
                {
                    this.Post(this.messagesFromLocalShards[from.ThreadIndex]);
                    this.messagesFromLocalShards[from.ThreadIndex].Disable();                    
                }
            }
        }
    }

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
        private readonly CircularBuffer<SerializedMessage> incomingMessageBuffer;

        private readonly SerializedMessage[] inMemoryMessageBuffer;

        private readonly SerializedMessageEncoder<T> encoder;
        private readonly SerializedMessageEncoder<T> logEncoder;
        private readonly SerializedMessageDecoder<T> decoder;

        private SerializedMessage currentRereadMessage;
        private IEnumerator<T> currentRereadMessageEnumerator;

        private byte[] rereadMessageHeaderBuffer;
        private byte[] rereadMessageBodyBuffer;

        private readonly bool logAllElements;

        private readonly SendBufferPage currentSpillPage;

        public SpillFile(string filename, int bufferLength, SerializedMessageEncoder<T> encoder, SerializedMessageDecoder<T> decoder, int pageSize)
            : this(filename, bufferLength, encoder, null, decoder, pageSize)
        {}

        public SpillFile(string filename, int bufferLength, SerializedMessageEncoder<T> spillEncoder, SerializedMessageEncoder<T> logEncoder, SerializedMessageDecoder<T> decoder, int pageSize)
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
                MessageHeader.WriteHeaderToBuffer(this.rereadMessageHeaderBuffer, 0, message.Header);
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

            MessageHeader.ReadHeaderFromBuffer(this.rereadMessageHeaderBuffer, 0, ref nextMessageHeader);

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
                page.WriteHeader(new MessageHeader(from.ShardID, /* TODO FIXME: this.sendSequenceNumbers[destProcessID, destShardID]++ */ 0, this.Id, this.ShardId, SerializedMessageType.Data));                
            }

            if (!page.WriteRecord(this.serializer, record))
            {
                this.Flush(from);
                this.messagesFromLocalThreads[from.ThreadIndex] = new SendBufferPage(GlobalBufferPool<byte>.pool, this.pageSize);
                page = this.messagesFromLocalThreads[from.ThreadIndex];
                page.WriteHeader(new MessageHeader(from.ShardID, /* TODO FIXME: this.sendSequenceNumbers[destProcessID, destShardID]++ */ 0, this.Id, this.ShardId, SerializedMessageType.Data));

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

        public SpillingLocalMailbox(PostOffice postoffice, VertexInput<S, T> endpoint, int id, int shardId, Runtime.Progress.ProgressUpdateBuffer<T> progressBuffer)
            : base(postoffice, endpoint, id, shardId)
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

    internal class RemoteMailbox<S, T> : Mailbox<S, T>
        where T : Time<T>
    {
        private readonly int channelID;
        public int Id { get { return this.channelID; } }

        private readonly int processID;

        private readonly int shardID;
        public int ShardId { get { return this.shardID; } }

        private readonly int graphID;
        public int GraphId { get { return this.graphID; } }

        private readonly NetworkChannel networkChannel;
        private readonly AutoSerializedMessageEncoder<S, T>[] encodersFromLocalShards;
        //private readonly SendBufferPage[] messagesFromLocalShards;

        public int RecordSizeHint
        {
            get
            {
                return this.encodersFromLocalShards[0].RecordSizeHint;
            }
        }

        public int ThreadIndex { get { return -1; } }

        public RemoteMailbox(int channelID, int processID, int shardID, InternalGraphManager manager)
        {
            this.channelID = channelID;
            this.processID = processID;
            this.shardID = shardID;
            this.graphID = manager.Index;

            this.networkChannel = manager.Controller.NetworkChannel;

            var controller = manager.Controller;
            this.encodersFromLocalShards = new AutoSerializedMessageEncoder<S, T>[controller.Workers.Count];
            AutoSerializationMode mode = controller.Configuration.OneTimePerMessageSerialization ? AutoSerializationMode.OneTimePerMessage : AutoSerializationMode.Basic;

            for (int i = 0; i < controller.Workers.Count; ++i )
            {                
                this.encodersFromLocalShards[i] = new AutoSerializedMessageEncoder<S, T>(this.shardID, this.graphID << 16 | this.channelID, this.networkChannel.GetBufferPool(this.processID, i), this.networkChannel.SendPageSize, mode, SerializedMessageType.Data, () => this.networkChannel.GetSequenceNumber(this.processID));
                this.encodersFromLocalShards[i].CompletedMessage += (o, a) => { this.networkChannel.SendBufferSegment(a.Hdr, this.processID, a.Segment); };
            }
        }

        public void Flush(RemotePostbox from)
        {

            this.encodersFromLocalShards[from.ThreadIndex].Flush();
        }

        public void Send(Pair<S, T> record, RemotePostbox from)
        {
            this.encodersFromLocalShards[from.ThreadIndex].Write(record, from.ShardID);
        }

        public void DeliverSerializedMessage(SerializedMessage message, RemotePostbox from)
        {
            throw new NotImplementedException("Attempted to deliver message from a remote sender to a remote recipient, which is not currently supported.");
        }
    }
}
