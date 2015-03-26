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
using Microsoft.Research.Naiad.Runtime.FaultTolerance;
using Microsoft.Research.Naiad.Runtime.Networking;
using Microsoft.Research.Naiad.Runtime.Progress;

namespace Microsoft.Research.Naiad.Dataflow
{

    /// <summary>
    /// Describes the origin of a message
    /// </summary>
    public struct ReturnAddress
    {
        /// <summary>
        /// Job-wide stage identifier of the sender
        /// </summary>
        public readonly int StageID;

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
        /// Constructs a return address from stage, process, vertex, thread identifiers
        /// </summary>
        /// <param name="stageId">stage identifier</param>
        /// <param name="processId">process identifier</param>
        /// <param name="vertexId">vertex identifier</param>
        /// <param name="threadIndex">thread identifier</param>
        public ReturnAddress(int stageId, int processId, int vertexId, int threadIndex)
        {
            this.StageID = stageId;
            this.VertexID = vertexId;
            this.ProcessID = processId;
            this.ThreadIndex = threadIndex;
        }
    }
}

namespace Microsoft.Research.Naiad.Dataflow.Channels
{
    internal class PostOfficeChannel<TSender, S, T> : Cable<TSender, S, T>
        where TSender : Time<TSender>
        where T : Time<T>
    {
        public Edge<TSender, S, T> Edge { get { return edge; } }
        private readonly Edge<TSender, S, T> edge;

        private readonly FullyTypedStageOutput<TSender, S, T> sendBundle;
        private readonly StageInput<S, T> recvBundle;

        private readonly Dictionary<int, Postbox<TSender, S, T>> postboxes;
        private readonly Mailbox<S, T>[] mailboxes;

        public PostOfficeChannel(FullyTypedStageOutput<TSender, S, T> sendBundle, StageInput<S, T> recvBundle, Action<S[], int[], int> routingHashcodeFunction, NetworkChannel networkChannel, Edge<TSender, S, T> edge, Channel.Flags flags)
        {
            if ((flags & Channel.Flags.SpillOnRecv) == Channel.Flags.SpillOnRecv)
                throw new Exception("SpillingLocalMailbox not currently supported");

            this.sendBundle = sendBundle;
            this.recvBundle = recvBundle;
            this.edge = edge;

            var computation = sendBundle.ForStage.InternalComputation;

            // populate mailboxes; [proxy] destinations for sent messages.
            this.mailboxes = new Mailbox<S, T>[recvBundle.ForStage.Placement.Count];
            foreach (VertexLocation loc in recvBundle.ForStage.Placement)
            {
                if (loc.ProcessId == computation.Controller.Configuration.ProcessID)
                {
                    var scheduler = this.recvBundle.GetPin(loc.VertexId).Vertex.Scheduler;
                    var postOffice = scheduler.State(computation).PostOffice;
                    var progressBuffer = new Runtime.Progress.ProgressUpdateBuffer<T>(this.Edge.ChannelId, scheduler.State(computation).Producer); 

                    LocalMailbox<S, T> localMailbox = new LocalMailbox<S, T>(sendBundle.ForStage.StageId, postOffice, this.recvBundle.GetPin(loc.VertexId), this.Edge.ChannelId, loc.VertexId, progressBuffer);
                    
                    this.mailboxes[loc.VertexId] = localMailbox;

                    postOffice.RegisterMailbox(localMailbox);
                    if (networkChannel != null)
                        networkChannel.RegisterMailbox(localMailbox);
                }
                else
                {
                    this.mailboxes[loc.VertexId] = new RemoteMailbox<S, T>(this.sendBundle.ForStage.StageId, this.Edge.ChannelId, loc.ProcessId, loc.VertexId, sendBundle.ForStage.InternalComputation);
                }
            }

            // populate postboxes; collection points for each local worker.
            this.postboxes = new Dictionary<int, Postbox<TSender, S, T>>();
            foreach (VertexLocation location in sendBundle.ForStage.Placement)
            {
                if (location.ProcessId == sendBundle.ForStage.InternalComputation.Controller.Configuration.ProcessID)
                {
                    var scheduler = this.sendBundle.GetFiber(location.VertexId).Vertex.Scheduler;

                    var progressBuffer = new Runtime.Progress.ProgressUpdateBuffer<T>(this.Edge.ChannelId, scheduler.State(computation).Producer);

                    // determine type of postbox to use.
                    if (routingHashcodeFunction == null)
                        this.postboxes[location.VertexId] = new NoHashCodePostbox<TSender, S, T>(this.Edge, this.sendBundle.GetFiber(location.VertexId), this.recvBundle, this.mailboxes, progressBuffer, scheduler.GetBufferPool<S>());
                    else
                        this.postboxes[location.VertexId] = new BufferingPostbox<TSender, S, T>(this.Edge, this.sendBundle.GetFiber(location.VertexId), this.recvBundle, this.mailboxes, routingHashcodeFunction, progressBuffer, scheduler.GetBufferPool<S>());

                    this.sendBundle.TypedStage.Vertex(location.VertexId).AddReachabilityListener(this.postboxes[location.VertexId]);
                }
            }
        }

        public void ReMaterializeForRollback(Dictionary<int, Vertex> newSourceVertices, Dictionary<int, Vertex> newTargetVertices)
        {
            foreach (int vertex in newTargetVertices.Keys)
            {
                this.mailboxes[vertex].TransferEndpointForRollback(this.recvBundle.GetPin(vertex));
            }

            foreach (int vertex in newSourceVertices.Keys)
            {
                this.postboxes[vertex].TransferSenderForRollback(this.sendBundle.GetFiber(vertex));
                this.sendBundle.TypedStage.Vertex(vertex).AddReachabilityListener(this.postboxes[vertex]);
            }
        }

        public override string ToString()
        {
            return String.Format("Exchange channel [{0}]: {1} -> {2}", this.Edge.ChannelId, sendBundle, recvBundle);
        }

        public Dataflow.Stage<TSender> SourceStage { get { return this.sendBundle.TypedStage; } }
        public Dataflow.Stage<T> DestinationStage { get { return this.recvBundle.ForStage; } }

        public SendChannel<TSender, S, T> GetSendChannel(int i)
        {
            return this.postboxes[i];
        }

        public void EnableReceiveLogging()
        {
            var computation = sendBundle.ForStage.InternalComputation;
            foreach (VertexLocation loc in recvBundle.ForStage.Placement)
            {
                if (loc.ProcessId == computation.Controller.Configuration.ProcessID)
                {
                    this.recvBundle.GetPin(loc.VertexId).SetCheckpointer(this.recvBundle.GetPin(loc.VertexId).Vertex.Checkpointer);
                }
            }
        }
    }

    internal abstract class Postbox<TSender, S, T> : SendChannel<TSender, S, T>, IReachabilityListener
        where T : Time<T>
        where TSender : Time<TSender>
    {
        public int ThreadIndex { get { return this.threadindex; } }
        protected readonly int threadindex;

        public int VertexID { get { return this.vertexid; } }
        protected readonly int vertexid;

        public int ProcessID { get { return this.sender.Vertex.Stage.InternalComputation.Controller.Configuration.ProcessID; } }

        private readonly Edge<TSender, S, T> edge;
        protected VertexOutput<TSender, S, T> sender;

        protected readonly StageInput<S, T> receiverBundle;

        protected int[] destinations = new int[256];
        protected readonly Action<S[], int[], int> routingHashcodesFunction;
        
        protected readonly ReturnAddress returnAddress;

        protected readonly Mailbox<S, T>[] mailboxes;

        protected Runtime.Progress.ProgressUpdateBuffer<T> progressBuffer;

        protected BufferPool<S> BufferPool;

        protected IOutgoingMessageLogger<TSender, S, T> Logger = null;
        protected bool LoggingEnabled { get { return this.Logger != null; } }
        public void EnableLogging(IOutgoingMessageLogger<TSender, S, T> logger)
        {
            this.Logger = logger;
        }

        public Postbox(Edge<TSender, S, T> edge, VertexOutput<TSender, S, T> sender, StageInput<S, T> receiverBundle, Mailbox<S, T>[] mailboxes, Action<S[], int[], int> routingHashcodeFunction, Runtime.Progress.ProgressUpdateBuffer<T> progressBuffer, BufferPool<S> bufferPool)
        {
            this.progressBuffer = progressBuffer;
            this.BufferPool = bufferPool;

            this.threadindex = sender.Vertex.Scheduler.Index;
            this.edge = edge;
            this.sender = sender;
            this.vertexid = sender.Vertex.VertexId;
            this.receiverBundle = receiverBundle;
            this.routingHashcodesFunction = routingHashcodeFunction;
            this.mailboxes = mailboxes;
            this.returnAddress = new ReturnAddress(this.sender.Vertex.Stage.StageId, this.ProcessID, this.vertexid, this.threadindex);
        }

        public virtual void TransferSenderForRollback(VertexOutput<TSender, S, T> newSender)
        {
            this.sender = newSender;
        }

        public abstract void Send(TSender vertexTime, Message<S, T> records);

        protected abstract void DeferredSend(Message<S, T> records, int receiverVertexId);

        public virtual void UpdateReachability(List<Pointstamp> pointstamps)
        {
            // nothing to do in the base postbox case
        }

        private void ForwardLoggedMessage(Message<S, T> records, ReturnAddress addr)
        {
            this.DeferredSend(records, addr.VertexID);
        }

        public IOutgoingMessageReplayer<TSender, T> GetMessageReplayer()
        {
            return new OutgoingMessageLogReader<TSender, S, T>(
                this.edge, this.vertexid, this.ForwardLoggedMessage, this.Flush, this.BufferPool, this.progressBuffer);
        }

        /// <summary>
        /// Flushes the mailboxes and makes sure the scheduler knows about the latest counts
        /// </summary>
        public abstract void Flush();
    }

    internal class RoundRobinPostbox<TSender, S, T> : Postbox<TSender, S, T>
        where T : Time<T>
        where TSender : Time<TSender>
    {
        private readonly DiscardManager discardManager;
        private readonly Stage<TSender> senderStage;
        private Dictionary<TSender, int> counter = new Dictionary<TSender, int>();

        protected override void DeferredSend(Message<S, T> records, int receiverVertexId)
        {
            this.mailboxes[receiverVertexId].Send(records, this.returnAddress);
        }

        public override void Send(TSender vertexTime, Message<S, T> records)
        {
            int thisCounter;
            if (!this.counter.TryGetValue(vertexTime, out thisCounter))
            {
                thisCounter = 0;
                this.counter.Add(vertexTime, thisCounter);
            }

            bool isRestoring = this.receiverBundle.ForStage.InternalComputation.IsRestoring;

            // Log all messages if and only if we are not restoring
            if (this.LoggingEnabled && !isRestoring)
            {
                this.Logger.LogMessage(vertexTime, records, this.mailboxes[thisCounter].ChannelId, thisCounter);
            }

            // Send the message if the receiver isn't discarding it
            if (!this.discardManager.DiscardingAny ||
                this.senderStage.CurrentCheckpoint(this.VertexID)
                .ShouldSendFunc(this.receiverBundle.ForStage, this.discardManager)(records.time, thisCounter))
            {
                // push changes to these counts
                progressBuffer.Update(records.time, records.length);

                // defer the send if we are restoring
                if (isRestoring)
                {
                    // make a copy of the message if the send is deferred
                    var newMessage = new Message<S, T>(records.time);
                    newMessage.Allocate(AllocationReason.PostOfficeChannel, this.BufferPool);
                    Array.Copy(records.payload, newMessage.payload, records.length);
                    newMessage.length = records.length;

                    this.sender.Vertex.AddDeferredSendAction(this, () => this.DeferredSend(newMessage, thisCounter));
                }
                else
                {
                    this.mailboxes[thisCounter].Send(records, this.returnAddress);
                }
            }

            thisCounter = (thisCounter + 1) % this.mailboxes.Length;
            this.counter[vertexTime] = thisCounter;
        }

        /// <summary>
        /// Flushes the mailboxes and makes sure the scheduler knows about the latest counts
        /// </summary>
        public override void Flush()
        {
            for (int i = 0; i < this.mailboxes.Length; i++)
                this.mailboxes[i].RequestFlush(new ReturnAddress(this.senderStage.StageId, this.ProcessID, this.VertexID, this.ThreadIndex));

            progressBuffer.Flush();
        }

        public override void UpdateReachability(List<Pointstamp> pointstamps)
        {
            List<TSender> reachableTimes = new List<TSender>();
            // Convert the given Pointstamps into local T times.
            for (int i = 0; i < pointstamps.Count; i++)
            {
                reachableTimes.Add(default(TSender).InitializeFrom(pointstamps[i], pointstamps[i].Timestamp.Length));
            }

            // only keep counters for times that are still reachable
            Dictionary<TSender, int> newCounter = new Dictionary<TSender,int>();
            foreach (TSender time in this.counter.Keys)
            {
                foreach (TSender reachable in reachableTimes)
                {
                    if (reachable.CompareTo(time) <= 0)
                    {
                        newCounter.Add(time, this.counter[time]);
                        break;
                    }
                }
            }

            this.counter = newCounter;
        }

        public RoundRobinPostbox(Edge<TSender, S, T> edge, VertexOutput<TSender, S, T> sender, StageInput<S, T> receiverBundle, Mailbox<S, T>[] mailboxes, Action<S[], int[], int> routingHashcodeFunction, Runtime.Progress.ProgressUpdateBuffer<T> progressBuffer, BufferPool<S> bufferPool)
            : base(edge, sender, receiverBundle, mailboxes, routingHashcodeFunction, progressBuffer, bufferPool)
        {
            this.senderStage = sender.Vertex.TypedStage;
            this.discardManager = sender.Vertex.Scheduler.State(sender.Vertex.Stage.InternalComputation).DiscardManager;
        }
    }

    internal class BufferingPostbox<TSender, S, T> : Postbox<TSender, S, T>
        where T : Time<T>
        where TSender : Time<TSender>
    {
        protected class SendBufferCollection<TKey>
        {
            private readonly int arraySize;
            private readonly BufferPool<S> bufferPool;

            private readonly Queue<TKey> activeBuffers;
            private readonly Dictionary<TKey, Message<S, T>[]> buffersByTime;
            private readonly Queue<Message<S, T>[]> spareBuffers;
            private Action<TKey, int, Message<S, T>> sendAction;
            private Action<TKey, int, Message<S, T>> deferredSendAction;

            public void Flush(bool isRestoring)
            {
                while (this.activeBuffers.Count > 0)
                {
                    TKey time = this.activeBuffers.Dequeue();

                    Message<S, T>[] buffers = this.buffersByTime[time];
                    this.buffersByTime.Remove(time);

                    for (int i = 0; i < buffers.Length; i++)
                    {
                        if (!buffers[i].Unallocated)
                        {
                            if (isRestoring)
                            {
                                this.deferredSendAction(time, i, buffers[i]);
                            }
                            else
                            {
                                this.sendAction(time, i, buffers[i]);
                            }
                        }

                        buffers[i] = new Message<S, T>();
                    }

                    this.spareBuffers.Enqueue(buffers);
                }
            }

            public int DistributeRecords(TKey key, Message<S, T> records, int[] destinations, bool[] deliveryMask, bool isRestoring)
            {
                Message<S, T>[] buffers = null;

                int deliveredRecordCount = 0;

                // buffer each record and ship if full.
                for (int i = 0; i < records.length; i++)
                {
                    int destinationVertexID = destinations[i];

                    Debug.Assert(destinationVertexID < this.arraySize);

                    // deliveryMask is null if no destinations are discarding messages
                    if (deliveryMask == null || deliveryMask[destinationVertexID])
                    {
                        // only proceed with this record if it isn't being suppressed during a restoration

                        if (deliveredRecordCount == 0)
                        {
                            // if we're delivering at least one record, make a buffer if necessary
                            if (!this.buffersByTime.ContainsKey(key))
                            {
                                var newBuffers = this.spareBuffers.Count > 0 ? this.spareBuffers.Dequeue() : new Message<S, T>[this.arraySize];
                                this.buffersByTime.Add(key, newBuffers);
                                this.activeBuffers.Enqueue(key);
                            }

                            buffers = this.buffersByTime[key];
                        }

                        ++deliveredRecordCount;

                        // if no space allocated, allocate some.
                        if (buffers[destinationVertexID].Unallocated)
                        {
                            // assign correct time and allocate some memory
                            buffers[destinationVertexID] = new Message<S, T>(records.time);
                            buffers[destinationVertexID].Allocate(AllocationReason.PostOfficeChannel, this.bufferPool);
                        }

                        buffers[destinationVertexID].payload[buffers[destinationVertexID].length++] = records.payload[i];

                        // if the buffer is now full, ship if off and replace with an unallocated one.
                        if (buffers[destinationVertexID].length == buffers[destinationVertexID].payload.Length)
                        {
                            var temp = buffers[destinationVertexID];
                            buffers[destinationVertexID] = new Message<S, T>(records.time);

                            if (isRestoring)
                            {
                                this.deferredSendAction(key, destinationVertexID, temp);
                            }
                            else
                            {
                                this.sendAction(key, destinationVertexID, temp);
                            }
                        }
                    }
                }

                return deliveredRecordCount;
            }

            public void UpdateSendAction(Action<TKey, int, Message<S, T>> newSendAction)
            {
                this.sendAction = newSendAction;
            }

            public void UpdateDeferredSendAction(Action<TKey, int, Message<S, T>> newSendAction)
            {
                this.deferredSendAction = newSendAction;
            }

            public SendBufferCollection(
                int arraySize, BufferPool<S> bufferPool,
                Action<TKey, int, Message<S, T>> sendAction, Action<TKey, int, Message<S, T>> deferredSendAction)
            {
                this.arraySize = arraySize;
                this.sendAction = sendAction;
                this.bufferPool = bufferPool;

                this.deferredSendAction = deferredSendAction;
                this.activeBuffers = new Queue<TKey>();
                this.buffersByTime = new Dictionary<TKey, Message<S, T>[]>();
                this.spareBuffers = new Queue<Message<S, T>[]>();
            }
        }

        private readonly DiscardManager discardManager;

        protected readonly HashSet<T> BusyByTime;
        protected readonly int mask;

        protected readonly SendBufferCollection<T> SendBuffers;
        protected readonly SendBufferCollection<Pair<TSender, T>> LogBuffers;

        protected Queue<bool[]> SparePermissions;
        protected Queue<int[]> SpareDestinations;

        public override void Send(TSender vertexTime, Message<S, T> records)
        {
            if (this.BusyByTime.Add(records.time))
            {
                this.InternalSend(vertexTime, records);
                this.BusyByTime.Remove(records.time);
            }
            else
            {
                throw new Exception("PostBox: Re-entering Send() with an already busy time");
            }
        }

        private void InternalSend(TSender vertexTime, Message<S, T> records)
        {
            // populate this.hashcodes ...
            int[] destinations = this.ComputeDestinations(records);

            bool isRestoring = this.receiverBundle.ForStage.InternalComputation.IsRestoring;
            bool isDiscarding = this.discardManager.DiscardingAny;

            bool[] permittedDestinations = (isDiscarding) ? this.ComputePermittedDestinations(records) : null;

            // Log all messages if and only if we are not restoring
            if (this.LoggingEnabled && !isRestoring)
            {
                this.LogBuffers.DistributeRecords(
                    vertexTime.PairWith(records.time),
                    records, destinations, null, false);
            }

            if (this.LoggingEnabled && isRestoring)
            {

            }

            // Send messages that aren't suppressed by the receiver, deferring the send if we are restoring
            int deliveredRecordCount =
                this.SendBuffers.DistributeRecords(
                    records.time, records, destinations, permittedDestinations, isRestoring);

            if (deliveredRecordCount > 0 && progressBuffer != null)
            {
                progressBuffer.Update(records.time, deliveredRecordCount);
            }

            this.SpareDestinations.Enqueue(destinations);
            if (isDiscarding)
            {
                this.SparePermissions.Enqueue(permittedDestinations);
            }
        }

        private bool[] ComputePermittedDestinations(Message<S, T> records)
        {
            bool[] permittedDestinations;

            // slow path when discarding messages: check which messages we will suppress

            Func<T, int, bool> sendFunc = this.sender.Vertex.TypedStage.CurrentCheckpoint(this.VertexID)
                .ShouldSendFunc(this.receiverBundle.ForStage, this.discardManager);

            permittedDestinations = this.SparePermissions.Count > 0 ? this.SparePermissions.Dequeue() : new bool[this.mailboxes.Length];

            for (int i = 0; i < this.mailboxes.Length; ++i)
            {
                permittedDestinations[i] = sendFunc(records.time, i);
            }

            return permittedDestinations;
        }

        private int[] ComputeDestinations(Message<S, T> records)
        {
            var destinations = this.SpareDestinations.Count > 0 ? this.SpareDestinations.Dequeue() : new int[Message<S,T>.DefaultMessageLength];

            if (this.routingHashcodesFunction == null)
            {
                var destination = this.vertexid % this.mailboxes.Length;
                for (int i = 0; i < records.length; i++)
                    destinations[i] = destination;
            }
            else
            {
                this.routingHashcodesFunction(records.payload, this.destinations, records.length);

                if (this.mask == -1)
                    for (int i = 0; i < records.length; i++)
                        destinations[i] = (Int32.MaxValue & this.destinations[i]) % this.mailboxes.Length;
                else
                    for (int i = 0; i < records.length; i++)
                        destinations[i] = this.destinations[i] & this.mask;
            }

            return destinations;
        }

        protected override void DeferredSend(Message<S, T> records, int receiverVertexId)
        {
            this.mailboxes[receiverVertexId].Send(records, this.returnAddress);
        }

        /// <summary>
        /// Flushes the mailboxes and makes sure the scheduler knows about the latest counts
        /// </summary>
        public override void Flush()
        {
            bool isRestoring = this.receiverBundle.ForStage.InternalComputation.IsRestoring;
            // flush using the send action or deferred send action depending on whether we are restoring or not
            this.SendBuffers.Flush(isRestoring);
            if (!isRestoring)
            {
                // log messages if and only if we aren't restoring
                this.LogBuffers.Flush(false);
            }

            for (int i = 0; i < this.mailboxes.Length; i++)
                this.mailboxes[i].RequestFlush(this.returnAddress);

            progressBuffer.Flush();
        }

        public override void TransferSenderForRollback(VertexOutput<TSender, S, T> newSender)
        {
            base.TransferSenderForRollback(newSender);
            this.SendBuffers.UpdateDeferredSendAction(
                (k, i, b) => this.sender.Vertex.AddDeferredSendAction(this, () => this.DeferredSend(b, i)));
        }

        public BufferingPostbox(Edge<TSender, S, T> edge, VertexOutput<TSender, S, T> sender, StageInput<S, T> receiverBundle, Mailbox<S, T>[] mailboxes, Action<S[], int[], int> routingHashcodeFunction, Runtime.Progress.ProgressUpdateBuffer<T> progressBuffer, BufferPool<S> bufferPool)
            : base(edge, sender, receiverBundle, mailboxes, routingHashcodeFunction, progressBuffer, bufferPool)
        {
            this.discardManager = sender.Vertex.Scheduler.State(sender.Vertex.Stage.InternalComputation).DiscardManager;

            if (((this.mailboxes.Length - 1) & this.mailboxes.Length) == 0)
                this.mask = this.mailboxes.Length - 1;
            else
                this.mask = -1;

            this.BusyByTime = new HashSet<T>();

            this.SpareDestinations = new Queue<int[]>();
            this.SparePermissions = new Queue<bool[]>();

            this.SendBuffers = new SendBufferCollection<T>(
                this.mailboxes.Length, this.BufferPool,
                (k, i, b) => this.mailboxes[i].Send(b, this.returnAddress),
                (k, i, b) => this.sender.Vertex.AddDeferredSendAction(this, () => this.DeferredSend(b, i)));

            // there's no deferred action for logging since we only do it when we are not restoring
            this.LogBuffers = new SendBufferCollection<Pair<TSender,T>>(
                this.mailboxes.Length, this.BufferPool,
                (k, i, b) => this.Logger.LogMessage(k.First, b, this.receiverBundle.ChannelId, i),
                (k, i, b) => { throw new NotImplementedException(); });
        }
    }

    internal class NoHashCodePostbox<TSender, S, T> : Postbox<TSender, S, T>
        where T : Time<T>
        where TSender : Time<TSender>
    {
        private readonly DiscardManager discardManager;

        private readonly int destination;

        private int recordsSent;

        protected override void DeferredSend(Message<S, T> records, int receiverVertexId)
        {
            if (receiverVertexId != this.destination)
            {
                throw new ApplicationException("Logged message sent to wrong destination");
            }

            this.mailboxes[this.destination].Send(records, this.returnAddress);
        }

        public override void Send(TSender vertexTime, Message<S, T> records)
        {
            bool isRestoring = this.receiverBundle.ForStage.InternalComputation.IsRestoring;
            bool shouldDiscard =
                this.discardManager.DiscardingAny &&
                !this.sender.Vertex.TypedStage.CurrentCheckpoint(this.VertexID)
                    .ShouldSendFunc(this.receiverBundle.ForStage, this.discardManager)(records.time, this.destination);

            this.recordsSent += records.length;

            if (this.LoggingEnabled && !isRestoring)
            {
                this.Logger.LogMessage(vertexTime, records, this.mailboxes[this.destination].ChannelId, this.destination);
            }

            if (shouldDiscard)
            {
                // we don't need to copy the message since we are not sending it
                return;
            }

            // make a copy of the message
            var newMessage = new Message<S, T>(records.time);
            newMessage.Allocate(AllocationReason.PostOfficeChannel, this.BufferPool);
            Array.Copy(records.payload, newMessage.payload, records.length);
            newMessage.length = records.length;

            if (progressBuffer != null)
                progressBuffer.Update(records.time, records.length);

            if (isRestoring)
            {
                this.sender.Vertex.AddDeferredSendAction(this, () => this.DeferredSend(newMessage, this.destination));
            }
            else
            {
                this.mailboxes[this.destination].Send(newMessage, this.returnAddress);
            }
        }

        public override void Flush()
        {
            this.mailboxes[this.destination].RequestFlush(this.returnAddress);

            if (progressBuffer != null)
                progressBuffer.Flush();
        }

        public NoHashCodePostbox(Edge<TSender, S, T> edge, VertexOutput<TSender, S, T> sender, StageInput<S, T> receiverBundle, Mailbox<S, T>[] mailboxes, Runtime.Progress.ProgressUpdateBuffer<T> progressBuffer, BufferPool<S> bufferPool)
            : base(edge, sender, receiverBundle, mailboxes, null, progressBuffer, bufferPool)
        {
            this.discardManager = sender.Vertex.Scheduler.State(sender.Vertex.Stage.InternalComputation).DiscardManager;
            this.destination = this.vertexid % this.mailboxes.Length;
        }
    }

    internal class PostOffice
    {
        private readonly int computationIndex;
        private readonly Scheduler scheduler;
        private readonly List<LocalMailbox> mailboxes;
        public readonly Queue<LocalMailbox> MailboxesToFlush = new Queue<LocalMailbox>();

        internal BufferPool<S> GetBufferPool<S>() { return this.scheduler.GetBufferPool<S>(); }

        public PostOffice(Scheduling.Scheduler scheduler, int computationIndex)
        {
            this.computationIndex = computationIndex;
            this.scheduler = scheduler;
            this.mailboxes = new List<LocalMailbox>();
        }

        public void CheckAllMailboxes()
        {
            // a queue would be nice, but would need to worry about concurrent additions
            for (int i = 0; i < this.mailboxes.Count; i++)
                this.mailboxes[i].Check();
        }

        public void CheckVertexMailboxes(Vertex vertex)
        {
            // a queue would be nice, but would need to worry about concurrent additions
            for (int i = 0; i < this.mailboxes.Count; i++)
            {
                if (this.mailboxes[i].Vertex == vertex)
                {
                    this.mailboxes[i].Check();
                }
            }
        }

        public void PruneMessagesAndRepairProgress(Dictionary<int, Stage> stages)
        {
            for (int i = 0; i < this.mailboxes.Count; i++)
                this.mailboxes[i].PruneMessagesAndRepairProgress(stages);
        }

        public void FlushAllMailboxes()
        {
            while (this.MailboxesToFlush.Count > 0)
                this.MailboxesToFlush.Dequeue().Flush();        
        }

        public void RequestDrain<TTime>(TTime time, int channelId, LocalMailbox mailbox, bool requestCutThrough) where TTime : Time<TTime>
        {
            this.scheduler.RequestDrain(channelId, time, mailbox, this.computationIndex, requestCutThrough);
        }

        public void Signal() { this.scheduler.Signal(); }

        public void RegisterMailbox(LocalMailbox mailbox)
        {
            this.mailboxes.Add(mailbox);
        }
    }

    internal interface Mailbox
    {
        string WaitingMessageCount();
        void DeliverSerializedMessage(SerializedMessage message, ReturnAddress from);

        int SenderStageId { get; }
        int GraphId { get; }
        int ChannelId { get; }
        int VertexId { get; }
    }

    internal interface LocalMailbox : Mailbox
    {
        void Flush();
        void Check();
        void Drain(Pointstamp pointstamp);
        Vertex Vertex { get; }
        void PruneMessagesAndRepairProgress(Dictionary<int, Stage> stages);
    }

    internal interface Mailbox<S, T> : Mailbox
        where T : Time<T>
    {
        void Send(Message<S, T> message, ReturnAddress from);

        void RequestFlush(ReturnAddress from);
        // returns the ThreadId for mailboxes on the same computer, and -1 for remote ones
        int ThreadIndex { get; }

        void TransferEndpointForRollback(VertexInput<S, T> newEndpoint);
    }

    internal class LocalMailbox<S, T> : LocalMailbox, Mailbox<S, T>
        where T : Time<T>
    {
        public int SenderStageId { get { return this.senderStageId; } }
        /// <summary>
        /// the stage sending to this mailbox
        /// </summary>
        protected readonly int senderStageId;

        /// <summary>
        /// The PostOffice responsible for this mailbox.
        /// </summary>
        protected readonly PostOffice postOffice;

        protected volatile bool dirty;

        protected VertexInput<S, T> endpoint;
        public Vertex Vertex { get { return this.endpoint.Vertex; } }
        
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

        protected BufferPool<S> bufferPool;

        protected Runtime.Progress.ProgressUpdateBuffer<T> progressBuffer;

        private readonly ConcurrentQueue<Pair<ReturnAddress,Message<S, T>>> sharedQueue;
        private readonly ConcurrentQueue<Pair<ReturnAddress,SerializedMessage>> sharedSerializedQueue;

        private readonly Dictionary<T, Queue<Pair<ReturnAddress, Message<S, T>>>> DifferentiatedMessageQueue = new Dictionary<T, Queue<Pair<ReturnAddress, Message<S, T>>>>();
        private readonly Dictionary<T, Queue<Pair<ReturnAddress, SerializedMessage>>> DifferentiatedSerializedMessageQueue = new Dictionary<T, Queue<Pair<ReturnAddress, SerializedMessage>>>();

        private readonly AutoSerializedMessageDecoder<S, T> decoder;


        private bool InFlushingQueue;

        /// <summary>
        /// Flush the target of the channel, and then any buffered progress updates.
        /// </summary>
        public void Flush()
        {
            // only called as part of a dequeue from the flushing queue.
            this.InFlushingQueue = false;

            endpoint.Flush();
            progressBuffer.Flush();
        }

        private HashSet<T> DrainRequestTimes = new HashSet<T>();
        private Queue<Queue<Pair<ReturnAddress, SerializedMessage>>> SpareSerializedQueues = new Queue<Queue<Pair<ReturnAddress, SerializedMessage>>>();
        private Queue<Queue<Pair<ReturnAddress,Message<S, T>>>> SpareQueues = new Queue<Queue<Pair<ReturnAddress,Message<S, T>>>>();

        public void Check()
        {
            if (this.dirty)
            {
                this.dirty = false;

                // Pull messages received from other processes
                Pair<ReturnAddress,SerializedMessage> superChannelMessage;
                while (this.sharedSerializedQueue.TryDequeue(out superChannelMessage))
                {
                    Pair<T, int> delta = this.decoder.Time(superChannelMessage.Second);

                    if (!this.DifferentiatedSerializedMessageQueue.ContainsKey(delta.First))
                    {
                        var newQueue = this.SpareSerializedQueues.Count > 0 ? this.SpareSerializedQueues.Dequeue() : new Queue<Pair<ReturnAddress,SerializedMessage>>();

                        this.DifferentiatedSerializedMessageQueue.Add(delta.First, newQueue);
                        this.DrainRequestTimes.Add(delta.First);
                    }

                    this.DifferentiatedSerializedMessageQueue[delta.First].Enqueue(superChannelMessage);
                }

                // Pull messages received from the local process
                Pair<ReturnAddress, Message<S, T>> messagePair;
                while (this.sharedQueue.TryDequeue(out messagePair))
                {
                    if (!this.DifferentiatedMessageQueue.ContainsKey(messagePair.Second.time))
                    {
                        var newQueue = this.SpareQueues.Count > 0 ? this.SpareQueues.Dequeue() : new Queue<Pair<ReturnAddress, Message<S, T>>>();

                        this.DifferentiatedMessageQueue.Add(messagePair.Second.time, newQueue);
                        this.DrainRequestTimes.Add(messagePair.Second.time);
                    }

                    this.DifferentiatedMessageQueue[messagePair.Second.time].Enqueue(messagePair);
                }

                foreach (var time in this.DrainRequestTimes)
                    this.postOffice.RequestDrain(time, this.channelId, this, false);

                this.DrainRequestTimes.Clear();
            }
        }

        public void PruneMessagesAndRepairProgress(Dictionary<int, Stage> stages)
        {
            // get everything into the differentiated queues
            this.Check();

            bool thisVertexRollingBack = stages[this.endpoint.Vertex.Stage.StageId].IsRollingBack(this.vertexId);

            foreach (var queue in this.DifferentiatedMessageQueue)
            {
                List<Pair<ReturnAddress, Message<S, T>>> keepMessages = new List<Pair<ReturnAddress, Message<S, T>>>();
                foreach (Pair<ReturnAddress, Message<S, T>> message in queue.Value)
                {
                    if (stages[message.First.StageID].IsRollingBack(message.First.VertexID) || thisVertexRollingBack)
                    {
                        message.Second.Release(AllocationReason.PostOfficeChannel, this.bufferPool);
                    }
                    else
                    {
                        keepMessages.Add(message);
                    }
                }

                queue.Value.Clear();
                long count = 0;
                foreach (var message in keepMessages)
                {
                    queue.Value.Enqueue(message);
                    count += message.Second.length;
                }
                this.progressBuffer.Update(queue.Key, count);
            }

            foreach (var queue in this.DifferentiatedSerializedMessageQueue)
            {
                var keepMessages = queue.Value.Where(p =>
                    !stages[p.First.StageID].IsRollingBack(p.First.VertexID) &&
                    !thisVertexRollingBack).ToArray();
                queue.Value.Clear();
                long count = 0;
                foreach (var message in keepMessages)
                {
                    queue.Value.Enqueue(message);
                    Pair<T, int> delta = this.decoder.Time(message.Second);
                    count += delta.Second;
                }
                this.progressBuffer.Update(queue.Key, count);
            }

            this.progressBuffer.Flush();
        }

        public void Drain(Pointstamp pointstamp)
        {
            var time = default(T).InitializeFrom(pointstamp, pointstamp.Timestamp.Length);

            // first, Check() the mailbox to retrieve messages for this time.
            this.Check();

            // drain serialized messages
            if (this.DifferentiatedSerializedMessageQueue.ContainsKey(time))
            {
                var queue = this.DifferentiatedSerializedMessageQueue[time];
                this.DifferentiatedSerializedMessageQueue.Remove(time);

                while (queue.Count > 0)
                {
                    var serializedMessagePair = queue.Dequeue();

                    this.endpoint.SerializedMessageReceived(serializedMessagePair.Second, serializedMessagePair.First);

                    Pair<T, int> delta = this.decoder.Time(serializedMessagePair.Second);
                    progressBuffer.Update(delta.First, -delta.Second);

                    // XXX : here we might want to release the serialized message to a shared pool
                }

                this.SpareSerializedQueues.Enqueue(queue);
            }

            // drain typed messages
            if (this.DifferentiatedMessageQueue.ContainsKey(time))
            {
                var queue = this.DifferentiatedMessageQueue[time];
                this.DifferentiatedMessageQueue.Remove(time);

                while (queue.Count > 0)
                {
                    var messagePair = queue.Dequeue();

                    this.endpoint.OnReceive(messagePair.Second, messagePair.First);
                    this.progressBuffer.Update(messagePair.Second.time, -messagePair.Second.length);

                    messagePair.Second.Release(AllocationReason.PostOfficeChannel, this.bufferPool);
                }

                this.SpareQueues.Enqueue(queue);
            }

            // notify postoffice that buffered progress updates and downstream work may need to be flushed.
            if (!this.InFlushingQueue)
            {
                this.postOffice.MailboxesToFlush.Enqueue(this);
                this.InFlushingQueue = true;
            }
        }

        private static void AddDeltaToDictionary(Dictionary<T, int> dictionary, Pair<T, int> delta)
        {
            int existing;
            if (dictionary.TryGetValue(delta.First, out existing))
            {
                dictionary[delta.First] = existing + delta.Second;
            }
            else
            {
                dictionary[delta.First] = delta.Second;
            }
        }

        public string WaitingMessageCount()
        {
            Dictionary<T, int> counts = new Dictionary<T,int>();
            foreach (var message in this.sharedSerializedQueue)
            {
                AddDeltaToDictionary(counts, this.decoder.Time(message.Second));
            }

            foreach (var queue in this.DifferentiatedSerializedMessageQueue)
            {
                int count = 0;
                foreach (var message in queue.Value)
                {
                    count += this.decoder.Time(message.Second).Second;
                }
                AddDeltaToDictionary(counts, queue.Key.PairWith(count));
            }

            foreach (var queue in this.DifferentiatedMessageQueue)
            {
                int count = 0;
                foreach (var message in queue.Value)
                {
                    count += message.Second.length;
                }
                AddDeltaToDictionary(counts, queue.Key.PairWith(count));
            }

            if (counts.Count > 0)
            {
                var builder = new StringBuilder();
                foreach (var count in counts)
                {
                    builder.Append(" ");
                    builder.Append(count.Key);
                    builder.Append(":");
                    builder.Append(count.Value);
                }

                return builder.ToString();
            }
            else
            {
                return null;
            }
        }

        /// <summary>
        /// Enqueues the incoming network message on the shared queue, called by the network channel.
        /// </summary>
        /// <param name="message">message</param>
        /// <param name="from">sender</param>
        public void DeliverSerializedMessage(SerializedMessage message, ReturnAddress from)
        {
            this.sharedSerializedQueue.Enqueue(new Pair<ReturnAddress, SerializedMessage>(from, message));

            if (!this.dirty)
            {
                this.dirty = true;
                this.postOffice.Signal();
            }
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
            // if the message is from the same thread, deliver directly.
            if (this.endpoint.Vertex.scheduler.Index == from.ThreadIndex)
            {
                var newTime = false;
                if (!this.DifferentiatedMessageQueue.ContainsKey(message.time))
                {
                    this.DifferentiatedMessageQueue.Add(message.time, this.SpareQueues.Count > 0 ? this.SpareQueues.Dequeue() : new Queue<Pair<ReturnAddress,Message<S, T>>>());
                    newTime = true;
                }

                this.DifferentiatedMessageQueue[message.time].Enqueue(from.PairWith(message));

                if (this.endpoint.AvailableEntrancy >= 0)
                {
                    this.endpoint.AvailableEntrancy = this.endpoint.AvailableEntrancy - 1;
                    this.postOffice.RequestDrain(message.time, this.channelId, this, true);
                    this.endpoint.AvailableEntrancy = this.endpoint.AvailableEntrancy + 1;
                }
                else if (newTime)
                    this.postOffice.RequestDrain(message.time, this.channelId, this, false);
            }
            else
            {
                // Deliver the message
                this.sharedQueue.Enqueue(new Pair<ReturnAddress, Message<S, T>>(from, message));

                // Notify postoffice that there is a message in the shared queue
                if (!this.dirty)
                    this.dirty = true;

                this.postOffice.Signal();
            }
        }

        public void TransferEndpointForRollback(VertexInput<S, T> newEndpoint)
        {
            this.endpoint = newEndpoint;
        }

        /// <summary>
        /// Constructs a new Mailbox for a local instance of a vertex.
        /// </summary>
        /// <param name="senderStageId">stage id that is sending into this mailbox</param>
        /// <param name="postOffice">PostOffice managing this Mailbox</param>
        /// <param name="endpoint">target for delivered messages</param>
        /// <param name="channelId">identifier for the channel</param>
        /// <param name="vertexId">identifier for tthe target vertex</param>
        /// <param name="progressBuffer">accumulator for progress updates</param>
        public LocalMailbox(int senderStageId, PostOffice postOffice, VertexInput<S, T> endpoint, int channelId, int vertexId, Runtime.Progress.ProgressUpdateBuffer<T> progressBuffer)
        {
            this.senderStageId = senderStageId;
            this.postOffice = postOffice;
            this.dirty = false;
            this.endpoint = endpoint;
            this.channelId = channelId;
            this.vertexId = vertexId;
            this.graphId = endpoint.Vertex.Stage.InternalComputation.Index;

            this.sharedQueue = new ConcurrentQueue<Pair<ReturnAddress, Message<S, T>>>();
            this.sharedSerializedQueue = new ConcurrentQueue<Pair<ReturnAddress,SerializedMessage>>();

            this.decoder = new AutoSerializedMessageDecoder<S, T>(endpoint.Vertex.SerializationFormat, this.postOffice.GetBufferPool<S>());

            this.progressBuffer = progressBuffer;

            this.bufferPool = this.postOffice.GetBufferPool<S>();
        }
    }

    internal class RemoteMailbox<S, T> : Mailbox<S, T>
        where T : Time<T>
    {
        public int SenderStageId { get { return this.senderStageId; } }
        private readonly int senderStageId;
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
            message.Release(AllocationReason.PostOfficeChannel, this.bufferPools[from.ThreadIndex]);
        }

        public string WaitingMessageCount()
        {
            throw new NotImplementedException("Shouldn't call on remote mailbox");
        }

        public void DeliverSerializedMessage(SerializedMessage message, ReturnAddress from)
        {
            throw new NotImplementedException("Attempted to deliver message from a remote sender to a remote recipient, which is not currently supported.");
        }

        public void TransferEndpointForRollback(VertexInput<S, T> newEndpoint)
        {
            throw new NotImplementedException("Attempted to transfer an endpoint to a remote mailbox.");
        }

        private BufferPool<S>[] bufferPools;

        public RemoteMailbox(int senderStageId, int channelID, int processID, int vertexID, InternalComputation manager)
        {
            this.senderStageId = senderStageId;
            this.channelID = channelID;
            this.processID = processID;
            this.vertexID = vertexID;
            this.graphID = manager.Index;

            this.networkChannel = manager.Controller.NetworkChannel;

            var controller = manager.Controller;
            this.encodersFromLocalVertices = new AutoSerializedMessageEncoder<S, T>[controller.Workers.Count];

            this.bufferPools = new BufferPool<S>[controller.Workers.Count];
            for (int i = 0; i < controller.Workers.Count; i++)
                this.bufferPools[i] = controller.Workers[i].GetBufferPool<S>();

            for (int i = 0; i < controller.Workers.Count; ++i)
            {
                this.encodersFromLocalVertices[i] = new AutoSerializedMessageEncoder<S, T>(this.vertexID, this.graphID << 16 | this.channelID, this.networkChannel.GetBufferPool(this.processID, i), this.networkChannel.SendPageSize, manager.SerializationFormat, false, SerializedMessageType.Data, () => this.networkChannel.GetSequenceNumber(this.processID));
                this.encodersFromLocalVertices[i].CompletedMessage += (o, a) => { this.networkChannel.SendBufferSegment(a.Hdr, this.processID, a.Segment); };
            }
        }
    }
}
