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
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Diagnostics;
using System.Threading;
using Microsoft.Research.Naiad.DataStructures;
using Microsoft.Research.Naiad.Serialization;
using Microsoft.Research.Naiad.Scheduling;
using Microsoft.Research.Naiad.Frameworks;
using Microsoft.Research.Naiad.Dataflow;
using Microsoft.Research.Naiad.Runtime.FaultTolerance;
using Microsoft.Research.Naiad.Runtime.Networking;
using Microsoft.Research.Naiad.Runtime.Controlling;
using Microsoft.Research.Naiad.Runtime.Progress;

using Microsoft.Research.Naiad.Diagnostics;

namespace Microsoft.Research.Naiad.Dataflow.Channels
{
    internal class CentralizedProgressChannel : Cable<Empty, Update, Empty>
    {
        private class Mailbox : Mailbox<Update, Empty>
        {
            private readonly int senderStageId;
            private readonly PostOffice postOffice;
            private readonly Runtime.Progress.ProgressUpdateCentralizer consumer;
            private readonly int id;
            private readonly int vertexId;
            private readonly int graphId;

            public int ChannelId { get { return this.id; } }
            public int VertexId { get { return this.vertexId; } }
            public int GraphId { get { return this.graphId; } }

            public int ThreadIndex { get { return this.consumer.scheduler.Index; } }

            private readonly AutoSerializedMessageDecoder<Update, Empty> decoder;

            private readonly BufferPool<Update> BufferPool;

            public string WaitingMessageCount()
            {
                return null;
            }

            public void DeliverSerializedMessage(SerializedMessage message, ReturnAddress from)
            {
                lock (this)
                {
                    foreach (var typedMessage in this.decoder.AsTypedMessages(message))
                    {
                        this.consumer.ProcessCountChange(typedMessage);
                        typedMessage.Release(AllocationReason.Deserializer, this.BufferPool);
                    }

                    this.RequestFlush(from);
                }
            }

            internal long recordsReceived = 0;

            public void Send(Message<Update, Empty> message, ReturnAddress from)
            {
                this.recordsReceived += message.length;
                this.consumer.scheduler.statistics[(int)RuntimeStatistic.ProgressLocalRecords] += message.length;
                this.consumer.ProcessCountChange(message);
            }

            public void RequestFlush(ReturnAddress from)
            {
            }

            public void Flush()
            {
            }

            public void TransferEndpointForRollback(VertexInput<Update, Empty> newEndpoint)
            {
                throw new NotImplementedException();
            }

            public int SenderStageId { get { return this.senderStageId; } }

            public Mailbox(int senderStageId, PostOffice postOffice, Runtime.Progress.ProgressUpdateCentralizer consumer, int id, int vertexId)
            {
                this.senderStageId = senderStageId;
                this.postOffice = postOffice;
                this.consumer = consumer;
                this.id = id;
                this.vertexId = vertexId;
                this.graphId = this.consumer.Stage.InternalComputation.Index;

                this.BufferPool = new MessageBufferPool<Update>();

                this.decoder = new AutoSerializedMessageDecoder<Update, Empty>(consumer.SerializationFormat, this.BufferPool);
            }
        }

        private class Fiber : SendChannel<Empty, Update, Empty>
        {
            private bool debug = false;

            private readonly NetworkChannel networkChannel;

            private readonly int channelID;
            private readonly int vertexID;   // used for tracing/debugging

            private readonly VertexOutput<Empty, Update, Empty> sender;
            private readonly CentralizedProgressChannel.Mailbox localMailbox;

            private readonly int receiverVertexId;
            private readonly int receiverProcessId;
            private readonly int senderProcessId;
            private readonly int numProcesses;

            private readonly ReturnAddress senderAddress;

            private AutoSerializedMessageEncoder<Update, Empty> encoder;
            
            public Fiber(int channelID, int vertexID, VertexOutput<Empty, Update, Empty> sender, CentralizedProgressChannel.Mailbox localMailbox, InternalController controller, int receiverVertexId, int receiverProcessId)
            {
                this.channelID = channelID;
                this.vertexID = vertexID;
                this.sender = sender;
                this.localMailbox = localMailbox;
                this.networkChannel = controller.NetworkChannel;
                this.numProcesses = controller.Configuration.Processes;
                this.senderProcessId = controller.Configuration.ProcessID;

                this.receiverVertexId = receiverVertexId;
                this.receiverProcessId = receiverProcessId;

                this.senderAddress = new ReturnAddress(this.sender.Vertex.Stage.StageId, this.senderProcessId, this.vertexID, 0);

                if (this.networkChannel != null)
                {
                    this.encoder = new AutoSerializedMessageEncoder<Update, Empty>(0, this.sender.Vertex.Stage.InternalComputation.Index << 16 | this.channelID, this.networkChannel.GetBufferPool(0, -1), this.networkChannel.SendPageSize, controller.SerializationFormat, false, SerializedMessageType.Data, () => this.networkChannel.GetSequenceNumber(-1));
                    this.encoder.CompletedMessage += (o, a) => { this.SendPageContents(a.Hdr, a.Segment); };
                }
            }

            internal long bytesSent = 0;
            internal long messagesSent = 0;

            public void EnableLogging(IOutgoingMessageLogger<Empty, Update, Empty> logger) { }

            public IOutgoingMessageReplayer<Empty, Empty> GetMessageReplayer()
            {
                throw new NotImplementedException();
            }

            public void Send(Empty sendTime, Message<Update, Empty> records)
            {
                if (debug)
                    for (int i = 0; i < records.length; i++)
                        Console.Error.WriteLine("  IncastChannel Send {0}->{1}  {2} {3}", vertexID, 0, records.payload[i].Delta, records.payload[i].Pointstamp);

                if (this.localMailbox == null)
                {
                    //for (int i = 0; i < records.length; i++)
                    //    this.encoder.Write(records.payload[i].PairWith(records.time));
                    this.encoder.Write(new ArraySegment<Update>(records.payload, 0, records.length), this.vertexID);
                }
                else
                    this.localMailbox.Send(records, this.senderAddress);
            }

            private void SendPageContents(MessageHeader hdr, BufferSegment segment)
            {
                if (segment.Length > 0)
                {
                    if (debug) Console.Error.WriteLine("  Sending page len {0}", segment.Length);

                    segment.Copy();             // Increment refcount for the destination process.
                    this.bytesSent += segment.Length;
                    this.messagesSent += 1;
                    this.sender.Vertex.scheduler.statistics[(int)RuntimeStatistic.TxProgressMessages] += 1;
                    this.sender.Vertex.scheduler.statistics[(int)RuntimeStatistic.TxProgressBytes] += segment.Length;

                    this.networkChannel.SendBufferSegment(hdr, this.receiverProcessId, segment, true);
                }

                // Decrement refcount for the initial call to Consume().
                segment.Dispose();
            }

            public void Flush()
            {
                if (debug) Console.Error.WriteLine("  IncastChannel Flush {0}", this.vertexID);
                if (this.localMailbox == null)
                {
                    this.encoder.Flush();
                }
                else
                {
                    this.localMailbox.Flush();
                }
            }
        }

        private readonly Edge<Empty, Update, Empty> edge;
        public Edge<Empty, Update, Empty> Edge { get { return edge; } }

        private readonly Stage<Runtime.Progress.ProgressUpdateCentralizer, Empty> consumer;

        private readonly FullyTypedStageOutput<Empty, Update, Empty> sendBundle;
        private readonly StageInput<Update, Empty> recvBundle;

        private readonly Dictionary<int, Fiber> postboxes;
        private readonly Mailbox mailbox;

        public void EnableReceiveLogging()
        {
            throw new NotImplementedException();
        }

        public void ReMaterializeForRollback(Dictionary<int, Vertex> newSourceVertices, Dictionary<int, Vertex> newTargetVertices)
        {
            throw new NotImplementedException();
        }

        /// <summary>
        /// Constructor
        /// </summary>
        public CentralizedProgressChannel(Stage<ProgressUpdateCentralizer, Empty> consumer,
                             FullyTypedStageOutput<Empty, Update, Empty> stream, StageInput<Update, Empty> recvPort,
                             InternalController controller,
                             Edge<Empty, Update, Empty> edge)
        {
            this.consumer = consumer;

            this.sendBundle = stream;// producer.Output;
            this.recvBundle = recvPort;// consumer.Input;

            this.postboxes = new Dictionary<int, Fiber>();

            this.edge = edge;

            // Get the vertex id and process id of the single consumer
            var consumerVertexId = consumer.Placement.Single().VertexId;
            var consumerProcessId = consumer.Placement.Single().ProcessId;

            var computation = sendBundle.ForStage.InternalComputation;

            var myProcessId = computation.Controller.Configuration.ProcessID;

            if (myProcessId == consumerProcessId)
            {
                VertexInput<Update, Empty> recvFiber = this.recvBundle.GetPin(consumerProcessId);

                this.mailbox = new Mailbox(consumer.StageId, recvFiber.Vertex.Scheduler.State(computation).PostOffice,
                                           consumer.GetVertex(consumerVertexId), this.Edge.ChannelId, consumerVertexId);
                
                //recvFiber.Vertex.Scheduler.State(computation).PostOffice.RegisterMailbox(this.mailbox);

                if (controller.NetworkChannel != null)
                    controller.NetworkChannel.RegisterMailbox(this.mailbox);
            }

            foreach (VertexLocation loc in sendBundle.ForStage.Placement)
            {
                if (loc.ProcessId == sendBundle.ForStage.InternalComputation.Controller.Configuration.ProcessID)
                {
                    var postbox = new Fiber(this.Edge.ChannelId, loc.VertexId, this.sendBundle.GetFiber(loc.VertexId),
                                            this.mailbox, controller, consumerVertexId, consumerProcessId);
                    this.postboxes[loc.VertexId] = postbox;
                }
            }
            Logging.Info("Allocated CentralizedProgressChannel [{0}]: {1} -> {2}", this.Edge.ChannelId, sendBundle, recvBundle);
            NaiadTracing.Trace.ChannelInfo(this.Edge.ChannelId, SourceStage.StageId, DestinationStage.StageId, true, true);
        }

        public Dataflow.Stage<Empty> SourceStage
        {
            get
            {
                return this.sendBundle.TypedStage;
            }
        }

        public Dataflow.Stage<Empty> DestinationStage
        {
            get
            {
                return this.recvBundle.ForStage;
            }
        }

        public SendChannel<Empty, Update, Empty> GetSendChannel(int i)
        {
            return this.postboxes[i];
        }

        internal long TotalBytesSent
        {
            get
            {
                return this.postboxes.Values.Sum(x => x.bytesSent);
            }
        }
        internal long TotalMessagesSent
        {
            get
            {
                return this.postboxes.Values.Sum(x => x.messagesSent);
            }
        }
        internal long TotalRecordsReceived
        {
            get
            {
                if (this.mailbox != null)
                    return this.mailbox.recordsReceived;
                else
                    return 0;
            }
        }
    }

}
