/*
 * Naiad ver. 0.5
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
        public int ChannelId { get { return channelID; } }
        private readonly int channelID;

        private readonly StageOutput<S, T> sendBundle;
        private readonly StageInput<S, T> recvBundle;

        private readonly Dictionary<int, Postbox<S, T>> postboxes;
        private readonly Mailbox<S, T>[] mailboxes;

        public PostOfficeChannel(StageOutput<S, T> sendBundle, StageInput<S, T> recvBundle, Action<S[], int[], int> routingHashcodeFunction, NetworkChannel networkChannel, int channelId, Channel.Flags flags)
        {
            if ((flags & Channel.Flags.SpillOnRecv) == Channel.Flags.SpillOnRecv)
                throw new Exception("SpillingLocalMailbox not currently supported");

            this.sendBundle = sendBundle;
            this.recvBundle = recvBundle;
            this.channelID = channelId;

            var computation = sendBundle.ForStage.InternalComputation;

            // populate mailboxes; [proxy] destinations for sent messages.
            this.mailboxes = new Mailbox<S, T>[recvBundle.ForStage.Placement.Count];
            foreach (VertexLocation loc in recvBundle.ForStage.Placement)
            {
                if (loc.ProcessId == computation.Controller.Configuration.ProcessID)
                {
                    var postOffice = this.recvBundle.GetPin(loc.VertexId).Vertex.Scheduler.State(computation).PostOffice;
                    var progressBuffer = new Runtime.Progress.ProgressUpdateBuffer<T>(this.ChannelId, this.recvBundle.GetPin(loc.VertexId).Vertex.Scheduler.State(computation).Producer); 
                    
                    LocalMailbox<S, T> localMailbox = new LocalMailbox<S, T>(postOffice, this.recvBundle.GetPin(loc.VertexId), channelID, loc.VertexId, progressBuffer);
                    
                    this.mailboxes[loc.VertexId] = localMailbox;

                    postOffice.RegisterMailbox(localMailbox);
                    if (networkChannel != null)
                        networkChannel.RegisterMailbox(localMailbox);
                }
                else
                    this.mailboxes[loc.VertexId] = new RemoteMailbox<S, T>(this.channelID, loc.ProcessId, loc.VertexId, sendBundle.ForStage.InternalComputation);
            }

            // populate postboxes; collection points for each local worker.
            this.postboxes = new Dictionary<int, Postbox<S, T>>();
            foreach (VertexLocation location in sendBundle.ForStage.Placement)
            {
                if (location.ProcessId == sendBundle.ForStage.InternalComputation.Controller.Configuration.ProcessID)
                {
                    var progressBuffer = new Runtime.Progress.ProgressUpdateBuffer<T>(this.ChannelId, this.sendBundle.GetFiber(location.VertexId).Vertex.Scheduler.State(computation).Producer);

                    // determine type of postbox to use.
                    if (routingHashcodeFunction == null)
                        this.postboxes[location.VertexId] = new NoHashCodePostbox<S, T>(this.channelID, this.sendBundle.GetFiber(location.VertexId), this.recvBundle, this.mailboxes, networkChannel, progressBuffer);
                    else
                        this.postboxes[location.VertexId] = new BufferingPostbox<S, T>(this.channelID, this.sendBundle.GetFiber(location.VertexId), this.recvBundle, this.mailboxes, routingHashcodeFunction, networkChannel, progressBuffer);
                }
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

    internal abstract class Postbox<S, T> : SendChannel<S, T>
        where T : Time<T>
    {
        public int ThreadIndex { get { return this.threadindex; } }
        protected readonly int threadindex;

        public int VertexID { get { return this.vertexid; } }
        protected readonly int vertexid;

        public int ProcessID { get { return this.sender.Vertex.Stage.InternalComputation.Controller.Configuration.ProcessID; } }

        private readonly int channelID;
        private readonly VertexOutput<S, T> sender;

        protected readonly StageInput<S, T> receiverBundle;

        protected int[] destinations = new int[256];
        protected readonly Action<S[], int[], int> routingHashcodesFunction;
        
        private readonly NetworkChannel networkChannel;

        protected readonly ReturnAddress returnAddress;

        protected readonly Mailbox<S, T>[] mailboxes;

        protected Runtime.Progress.ProgressUpdateBuffer<T> progressBuffer;

        public Postbox(int channelID, VertexOutput<S, T> sender, StageInput<S, T> receiverBundle, Mailbox<S, T>[] mailboxes, Action<S[], int[], int> routingHashcodeFunction, NetworkChannel networkChannel, Runtime.Progress.ProgressUpdateBuffer<T> progressBuffer)
            : this(channelID, sender, receiverBundle, mailboxes, routingHashcodeFunction, networkChannel)
        {
            this.progressBuffer = progressBuffer;
        }

        public Postbox(int channelID, VertexOutput<S, T> sender, StageInput<S, T> receiverBundle, Mailbox<S, T>[] mailboxes, Action<S[], int[], int> routingHashcodeFunction, NetworkChannel networkChannel)
        {
            this.threadindex = sender.Vertex.Scheduler.Index;
            this.channelID = channelID;
            this.sender = sender;
            this.vertexid = sender.Vertex.VertexId;
            this.receiverBundle = receiverBundle;
            this.routingHashcodesFunction = routingHashcodeFunction;
            this.networkChannel = networkChannel;
            this.mailboxes = mailboxes;
            this.returnAddress = new ReturnAddress(this.ProcessID, this.vertexid, this.threadindex);
        }

        public abstract void Send(Message<S, T> records);

        /// <summary>
        /// Flushes the mailboxes and makes sure the scheduler knows about the latest counts
        /// </summary>
        public abstract void Flush();
    }

    internal class BufferingPostbox<S, T> : Postbox<S, T> where T : Time<T>
    {
        protected readonly Message<S, T>[] Buffers;
        protected readonly int mask;

        protected T CurrentTime;
        protected bool Busy;
        protected Queue<Message<S, T>> delayedWork = new Queue<Message<S, T>>();

        public override void Send(Message<S, T> records)
        {
            if (this.Busy)
            {
                // copy the message, because it will be recycled by the sender
                var newMessage = new Message<S, T>(records.time);
                newMessage.Allocate(AllocationReason.PostOfficeRentrancy);
                Array.Copy(records.payload, newMessage.payload, records.length);

                this.delayedWork.Enqueue(newMessage);
            }
            else
            {
                this.Busy = true;

                this.InternalSend(records);

                while (this.delayedWork.Count > 0)
                {
                    var dequeued = this.delayedWork.Dequeue();
                    this.InternalSend(dequeued);
                    dequeued.Release(AllocationReason.PostOfficeRentrancy);
                }

                this.Busy = false;
            }
        }

        private void FlushBuffers()
        {
            for (int i = 0; i < this.Buffers.Length; i++)
                this.FlushBuffer(i);
        }

        private void FlushBuffer(int index)
        {
            // capture buffer in case of re-entrancy
            var temp = this.Buffers[index];
            this.Buffers[index] = new Message<S, T>(this.CurrentTime);

            if (!temp.Unallocated)
                this.mailboxes[index].Send(temp, this.returnAddress);

            // do not release temp, because the mailbox now owns the message
        }

        private void InternalSend(Message<S, T> records)
        {
            // if we have data from a different time, we should flush it all...
            if (!this.CurrentTime.Equals(records.time))
            {
                this.CurrentTime = records.time;
                this.FlushBuffers();
            }

            // push changes to these counts
            if (progressBuffer != null)
                progressBuffer.Update(records.time, records.length);

            // populate this.hashcodes ...
            this.ComputeDestinations(records);

            // buffer each record and ship if full.
            for (int i = 0; i < records.length; i++)
            {
                int destinationVertexID = this.destinations[i];

                Debug.Assert(destinationVertexID < this.mailboxes.Length);

                // if no space allocated, allocate some.
                if (this.Buffers[destinationVertexID].Unallocated)
                    this.Buffers[destinationVertexID].Allocate(AllocationReason.PostOfficeChannel);

                this.Buffers[destinationVertexID].payload[this.Buffers[destinationVertexID].length++] = records.payload[i];
                    
                // if the buffer is now full, ship if off and replace with an unallocated one.
                if (this.Buffers[destinationVertexID].length == this.Buffers[destinationVertexID].payload.Length)
                    this.FlushBuffer(destinationVertexID);
            }
        }

        private void ComputeDestinations(Message<S, T> records)
        {
            if (this.routingHashcodesFunction == null)
            {
                var destination = this.vertexid % this.mailboxes.Length;
                for (int i = 0; i < records.length; i++)
                    this.destinations[i] = destination;
            }
            else
            {
                this.routingHashcodesFunction(records.payload, this.destinations, records.length);

                if (this.mask == -1)
                    for (int i = 0; i < records.length; i++)
                        destinations[i] = (Int32.MaxValue & this.destinations[i]) % this.mailboxes.Length;
                else
                    for (int i = 0; i < records.length; i++)
                        this.destinations[i] = this.destinations[i] & this.mask;
            }
        }

        /// <summary>
        /// Flushes the mailboxes and makes sure the scheduler knows about the latest counts
        /// </summary>
        public override void Flush()
        {
            this.FlushBuffers();

            // first flush all the non-local mailboxes
            for (int i = 0; i < this.mailboxes.Length; i++)
                if (this.mailboxes[i].ThreadIndex != this.threadindex)
                    this.mailboxes[i].RequestFlush(new ReturnAddress(this.ProcessID, this.VertexID, this.ThreadIndex));

            // now flush the local mailbox, which may cut-through to more execution
            for (int i = 0; i < this.mailboxes.Length; i++)
                if (this.mailboxes[i].ThreadIndex == this.threadindex)
                    this.mailboxes[i].RequestFlush(new ReturnAddress(this.ProcessID, this.VertexID, this.ThreadIndex));

            //Console.WriteLine("Flushing " + this.channelID);
            if (progressBuffer != null)
                progressBuffer.Flush();
        }

        public BufferingPostbox(int channelID, VertexOutput<S, T> sender, StageInput<S, T> receiverBundle, Mailbox<S, T>[] mailboxes, Action<S[], int[], int> routingHashcodeFunction, NetworkChannel networkChannel, Runtime.Progress.ProgressUpdateBuffer<T> progressBuffer)
            : base(channelID, sender, receiverBundle, mailboxes, routingHashcodeFunction, networkChannel, progressBuffer)
        {
            if (((this.mailboxes.Length - 1) & this.mailboxes.Length) == 0)
                this.mask = this.mailboxes.Length - 1;
            else
                this.mask = -1;

            this.Buffers = new Message<S, T>[this.mailboxes.Length];
        }
    }

    internal class NoHashCodePostbox<S, T> : Postbox<S, T> where T : Time<T>
    {
        private readonly int destination;

        private int recordsSent;

        public override void Send(Message<S, T> records)
        {
            var newMessage = new Message<S, T>(records.time);

            newMessage.Allocate(AllocationReason.PostOfficeChannel);

            Array.Copy(records.payload, newMessage.payload, records.length);

            newMessage.length = records.length;

            this.recordsSent += records.length;

            if (progressBuffer != null)
                progressBuffer.Update(records.time, records.length);

            this.mailboxes[this.destination].Send(newMessage, this.returnAddress);
        }

        public override void Flush()
        {
            this.mailboxes[this.destination].RequestFlush(this.returnAddress);

            if (progressBuffer != null)
                progressBuffer.Flush();
        }

        public NoHashCodePostbox(int channelID, VertexOutput<S, T> sender, StageInput<S, T> receiverBundle, Mailbox<S, T>[] mailboxes, NetworkChannel networkChannel, Runtime.Progress.ProgressUpdateBuffer<T> progressBuffer)
            : base(channelID, sender, receiverBundle, mailboxes, null, networkChannel, progressBuffer)
        {
            this.destination = this.vertexid % this.mailboxes.Length;
        }
    }


    internal class PostOffice
    {
        private readonly Scheduler scheduler;
        private readonly List<LocalMailbox> mailboxes;
        public readonly Queue<LocalMailbox> MailboxesToFlush = new Queue<LocalMailbox>();

        public PostOffice(Scheduling.Scheduler scheduler)
        {
            this.scheduler = scheduler;
            this.mailboxes = new List<LocalMailbox>();
        }

        public void DrainAllMailboxes()
        {
            foreach (LocalMailbox mailbox in this.mailboxes)
                mailbox.Drain();

            this.Flush();
        }

        public void Flush()
        {
            while (this.MailboxesToFlush.Count > 0)
                this.MailboxesToFlush.Dequeue().Flush();        
        }

        public void ProposeDrain(LocalMailbox mailbox)
        {
            if (this.scheduler.ProposeDrain(mailbox))
                mailbox.Drain();
        }

        public void Signal() { this.scheduler.Signal(); }

        public void RegisterMailbox(LocalMailbox mailbox)
        {
            this.mailboxes.Add(mailbox);
        }
    }

    internal interface Mailbox
    {
        void DeliverSerializedMessage(SerializedMessage message, ReturnAddress from);

        int GraphId { get; }
        int ChannelId { get; }
        int VertexId { get; }
    }

    internal interface LocalMailbox : Mailbox
    {
        void Drain();
        void Flush();
    }

    internal interface Mailbox<S, T> : Mailbox
        where T : Time<T>
    {
        void Send(Message<S, T> message, ReturnAddress from);

        void RequestFlush(ReturnAddress from);
        // returns the ThreadId for mailboxes on the same computer, and -1 for remote ones
        int ThreadIndex { get; }
    }

    internal class LocalMailbox<S, T> : LocalMailbox, Mailbox<S, T>
        where T : Time<T>
    {
        /// <summary>
        /// The PostOffice responsible for this mailbox.
        /// </summary>
        protected readonly PostOffice postOffice;

        protected volatile bool dirty;

        protected readonly VertexInput<S, T> endpoint;
        
        /// <summary>
        /// Reports the identifier associated with the channel.
        /// </summary>
        public int ChannelId { get { return this.channelId; } }
        private readonly int channelId;
        
        /// <summary>
        /// Reports the identifier associated with the target vertex.
        /// </summary>
        public int VertexId { get { return this.vertexId; } }
        private readonly int vertexId;    // receiver
        
        /// <summary>
        /// Reports the identifier associated with the containing computation.
        /// </summary>
        public int GraphId { get { return this.graphId; } }
        private readonly int graphId;
        
        /// <summary>
        /// Reports the identifier associated with the scheduler responsible for the target vertex.
        /// </summary>
        public int ThreadIndex { get { return this.endpoint.Vertex.scheduler.Index; } }


        protected Runtime.Progress.ProgressUpdateBuffer<T> progressBuffer;


        private readonly ConcurrentQueue<Message<S, T>> sharedQueue;
        private readonly ConcurrentQueue<SerializedMessage> sharedSerializedQueue;
        
        private readonly AutoSerializedMessageDecoder<S, T> decoder;

        private bool InFlushingQueue;

        /// <summary>
        /// Move records from the private queue into the operator state by giving them to the endpoint.
        /// </summary>
        public void Drain()
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

                    message.Release(AllocationReason.PostOfficeChannel);
                }

                // buffered progress updates and downstream work may need to be flushed.
                if (!this.InFlushingQueue)
                {
                    this.postOffice.MailboxesToFlush.Enqueue(this);
                    this.InFlushingQueue = true;
                }
            }
        }

        /// <summary>
        /// Flush the target of the channel, and then any buffered progress updates.
        /// </summary>
        public void Flush()
        {
            // only called as part of a dequeue from the flushing queue.
            this.InFlushingQueue = false;

            endpoint.Flush();
            if (progressBuffer != null)
                progressBuffer.Flush();
        }

        /// <summary>
        /// Enqueues the incoming network message on the shared queue, called by the network channel.
        /// </summary>
        /// <param name="message">message</param>
        /// <param name="from">sender</param>
        public void DeliverSerializedMessage(SerializedMessage message, ReturnAddress from)
        {
            this.sharedSerializedQueue.Enqueue(message);

            if (!this.dirty)
                this.dirty = true;

            this.postOffice.Signal();
        }

        /// <summary>
        /// Flushes the queue of local messages.  
        /// Called by the postbox.
        /// </summary>
        /// <param name="from">Identity of the upstream postbox</param>
        public void RequestFlush(ReturnAddress from) { }

        /// <summary>
        /// Forwards a local message by moving it to the shared queue.  This makes the message visible to the receiver.
        /// </summary>
        /// <param name="message">message of data</param>
        /// <param name="from">information about calling thread</param>
        public void Send(Message<S, T> message, ReturnAddress from)
        {
            // Deliver the message
            this.sharedQueue.Enqueue(message);

            // Notify postoffice that there is a message in the shared queue
            if (!this.dirty)
                this.dirty = true;

            this.postOffice.Signal();

            // questionable cut-through logic. probably better to let the scheduler decide about this ...
            if (this.endpoint.Vertex.Scheduler.Index == from.ThreadIndex && this.endpoint.AvailableEntrancy >= 0)
            {
                this.endpoint.AvailableEntrancy = this.endpoint.AvailableEntrancy - 1;
                this.postOffice.ProposeDrain(this);
                this.endpoint.AvailableEntrancy = this.endpoint.AvailableEntrancy + 1;
            }
        }

        /// <summary>
        /// Constructs a new Mailbox for a local instance of a vertex.
        /// </summary>
        /// <param name="postOffice">PostOffice managing this Mailbox</param>
        /// <param name="endpoint">target for delivered messages</param>
        /// <param name="channelId">identifier for the channel</param>
        /// <param name="vertexId">identifier for tthe target vertex</param>
        /// <param name="progressBuffer">accumulator for progress updates</param>
        public LocalMailbox(PostOffice postOffice, VertexInput<S, T> endpoint, int channelId, int vertexId, Runtime.Progress.ProgressUpdateBuffer<T> progressBuffer)
        {
            this.postOffice = postOffice;
            this.dirty = false;
            this.endpoint = endpoint;
            this.channelId = channelId;
            this.vertexId = vertexId;
            this.graphId = endpoint.Vertex.Stage.InternalComputation.Index;

            this.sharedQueue = new ConcurrentQueue<Message<S, T>>();
            this.sharedSerializedQueue = new ConcurrentQueue<SerializedMessage>();

            this.decoder = new AutoSerializedMessageDecoder<S, T>(endpoint.Vertex.SerializationFormat);

            this.progressBuffer = progressBuffer;
        }
    }

    internal class RemoteMailbox<S, T> : Mailbox<S, T>
        where T : Time<T>
    {
        public int ChannelId { get { return this.channelID; } }
        private readonly int channelID;

        public int VertexId { get { return this.vertexID; } }
        private readonly int vertexID;

        public int GraphId { get { return this.graphID; } }
        private readonly int graphID;

        private readonly int processID;

        public int ThreadIndex { get { return -1; } }


        private readonly NetworkChannel networkChannel;
        private readonly AutoSerializedMessageEncoder<S, T>[] encodersFromLocalVertices;
        

        public void RequestFlush(ReturnAddress from)
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
            message.Release(AllocationReason.PostOfficeChannel);
        }

        public void DeliverSerializedMessage(SerializedMessage message, ReturnAddress from)
        {
            throw new NotImplementedException("Attempted to deliver message from a remote sender to a remote recipient, which is not currently supported.");
        }

        public RemoteMailbox(int channelID, int processID, int vertexID, InternalComputation manager)
        {
            this.channelID = channelID;
            this.processID = processID;
            this.vertexID = vertexID;
            this.graphID = manager.Index;

            this.networkChannel = manager.Controller.NetworkChannel;

            var controller = manager.Controller;
            this.encodersFromLocalVertices = new AutoSerializedMessageEncoder<S, T>[controller.Workers.Count];

            for (int i = 0; i < controller.Workers.Count; ++i)
            {
                this.encodersFromLocalVertices[i] = new AutoSerializedMessageEncoder<S, T>(this.vertexID, this.graphID << 16 | this.channelID, this.networkChannel.GetBufferPool(this.processID, i), this.networkChannel.SendPageSize, manager.SerializationFormat, SerializedMessageType.Data, () => this.networkChannel.GetSequenceNumber(this.processID));
                this.encodersFromLocalVertices[i].CompletedMessage += (o, a) => { this.networkChannel.SendBufferSegment(a.Hdr, this.processID, a.Segment); };
            }
        }
    }
}
