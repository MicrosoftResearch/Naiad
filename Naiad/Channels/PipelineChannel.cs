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
using System.Diagnostics;
using Microsoft.Research.Naiad.Scheduling;
using Microsoft.Research.Naiad.Runtime.FaultTolerance;
using Microsoft.Research.Naiad.Runtime.Progress;

namespace Microsoft.Research.Naiad.Dataflow.Channels
{
    internal class PipelineChannel<TSender, S, T> : Cable<TSender, S, T>
        where TSender : Time<TSender>
        where T : Time<T>
    {
        private class Fiber : SendChannel<TSender, S, T>
        {
            private IOutgoingMessageLogger<TSender, S, T> logger = null;
            private readonly ProgressUpdateBuffer<T> progressBuffer;
            private readonly DiscardManager discardManager;
            private readonly PipelineChannel<TSender, S, T> bundle;
            private readonly int threadId;
            private readonly int index;
            private readonly ReturnAddress senderAddress;
            private VertexInput<S, T> receiver;
            private BufferPool<S> bufferPool;
                        
            public Fiber(PipelineChannel<TSender, S, T> bundle, VertexInput<S, T> receiver, int index, int threadId, ProgressUpdateBuffer<T> progressBuffer)
            {
                this.progressBuffer = progressBuffer;
                this.discardManager = receiver.Vertex.Scheduler.State(bundle.SourceStage.InternalComputation).DiscardManager;
                this.bundle = bundle;
                this.index = index;
                this.threadId = threadId;
                this.receiver = receiver;
                this.senderAddress = new ReturnAddress(
                        this.bundle.SourceStage.StageId,
                        this.bundle.SourceStage.InternalComputation.Controller.Configuration.ProcessID,
                        this.index,
                        this.threadId);
                this.bufferPool = receiver.Vertex.Scheduler.GetBufferPool<S>();
            }

            public void TransferReceiverForRollback(VertexInput<S, T> newReceiver)
            {
                this.receiver = newReceiver;
            }

            private bool LoggingEnabled { get { return this.logger != null; } }
            public void EnableLogging(IOutgoingMessageLogger<TSender, S, T> logger)
            {
                this.logger = logger;
            }

            public IOutgoingMessageReplayer<TSender, T> GetMessageReplayer()
            {
                return new OutgoingMessageLogReader<TSender, S, T>(
                    this.bundle.edge, this.index, this.ForwardLoggedMessage, this.Flush, this.bufferPool, this.progressBuffer);
            }

            private void ForwardLoggedMessage(Message<S, T> records, ReturnAddress addr)
            {
                this.DeferredSend(records, addr.VertexID);
            }

            private void DeferredSend(Message<S, T> records, int receiverVertexId)
            {
                if (receiverVertexId != this.receiver.Vertex.VertexId)
                {
                    throw new ApplicationException("Logged message sent to wrong receiver");
                }

                this.receiver.OnReceive(records, this.senderAddress);

                this.progressBuffer.Update(records.time, -records.length);
                this.progressBuffer.Flush();

                records.Release(AllocationReason.PostOfficeChannel, this.bufferPool);
            }

            public void Send(TSender vertexTime, Message<S, T> records)
            {
                bool isRestoring = this.bundle.DestinationStage.InternalComputation.IsRestoring;
                bool shouldDiscard =
                    this.discardManager.DiscardingAny &&
                    !this.bundle.SourceStage.CurrentCheckpoint(this.index).ShouldSendFunc(
                        this.bundle.DestinationStage, this.discardManager)(records.time, this.index);

                // log all messages if we are not restoring
                if (this.LoggingEnabled && !isRestoring)
                {
                    logger.LogMessage(vertexTime, records, this.bundle.Edge.ChannelId, this.receiver.Vertex.VertexId);
                }

                if (shouldDiscard)
                {
                    return;
                }

                if (isRestoring)
                {
                    // add a clock hold on these messages until the deferred send happens
                    this.progressBuffer.Update(records.time, records.length);
                    this.progressBuffer.Flush();

                    var newMessage = new Message<S, T>(records.time);
                    newMessage.Allocate(AllocationReason.PostOfficeChannel, this.bufferPool);
                    Array.Copy(records.payload, newMessage.payload, records.length);
                    newMessage.length = records.length;

                    this.bundle.sender.GetFiber(this.index).Vertex.AddDeferredSendAction(this, () => DeferredSend(newMessage, this.receiver.Vertex.VertexId));
                }
                else
                {
                    this.receiver.OnReceive(records, this.senderAddress);
                }
            }

            public override string ToString()
            {
                return string.Format("Pipeline({0} => {1})", this.bundle.SourceStage, this.bundle.DestinationStage);
            }

            public void Flush()
            {
                this.receiver.Flush();
            }
        }

        private readonly FullyTypedStageOutput<TSender, S, T> sender;
        private readonly StageInput<S, T> receiver;

        private readonly Dictionary<int, Fiber> subChannels;

        private readonly Edge<TSender, S, T> edge;
        public Edge<TSender, S, T> Edge { get { return edge; } }

        public PipelineChannel(FullyTypedStageOutput<TSender, S, T> sender, StageInput<S, T> receiver, Edge<TSender, S, T> edge)
        {
            this.sender = sender;
            this.receiver = receiver;

            this.edge = edge;

            this.subChannels = new Dictionary<int, Fiber>();
            InternalComputation computation = this.sender.ForStage.InternalComputation;
            foreach (VertexLocation loc in sender.ForStage.Placement)
            {
                if (loc.ProcessId == sender.ForStage.InternalComputation.Controller.Configuration.ProcessID)
                {
                    ProgressUpdateBuffer<T> progressBuffer = new ProgressUpdateBuffer<T>(this.Edge.ChannelId, receiver.GetPin(loc.VertexId).Vertex.Scheduler.State(computation).Producer);
                    this.subChannels[loc.VertexId] = new Fiber(this, receiver.GetPin(loc.VertexId), loc.VertexId, loc.ThreadId, progressBuffer);
                }
            }
        }

        public void ReMaterializeForRollback(Dictionary<int, Vertex> newSourceVertices, Dictionary<int, Vertex> newTargetVertices)
        {
            foreach (int vertex in newSourceVertices.Keys)
            {
                this.subChannels[vertex].TransferReceiverForRollback(this.receiver.GetPin(vertex));
            }
        }

        public SendChannel<TSender, S, T> GetSendChannel(int i)
        {
            return this.subChannels[i];
        }
                
        public Dataflow.Stage<TSender> SourceStage { get { return this.sender.TypedStage; } }
        public Dataflow.Stage<T> DestinationStage { get { return this.receiver.ForStage; } }

        public void EnableReceiveLogging()
        {
            InternalComputation computation = this.sender.ForStage.InternalComputation;
            foreach (VertexLocation loc in this.receiver.ForStage.Placement)
            {
                if (loc.ProcessId == computation.Controller.Configuration.ProcessID)
                {
                    this.receiver.GetPin(loc.VertexId).SetCheckpointer(this.receiver.GetPin(loc.VertexId).Vertex.Checkpointer);
                }
            }
        }

        public override string ToString()
        {
            return String.Format("Pipeline channel: {0} -> {1}", this.sender.ForStage, this.receiver.ForStage);
        }
    }
}
