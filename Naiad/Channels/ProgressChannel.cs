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
using Microsoft.Research.Naiad.Scheduling;
using Microsoft.Research.Naiad.Frameworks;
using System.Collections.Concurrent;
using Microsoft.Research.Naiad.Serialization;
using System.Diagnostics;

using Microsoft.Research.Naiad.Dataflow;
using Microsoft.Research.Naiad.Diagnostics;
using Microsoft.Research.Naiad.Runtime.Networking;
using Microsoft.Research.Naiad.Runtime.Controlling;
using Microsoft.Research.Naiad.Runtime.Progress;
using Microsoft.Research.Naiad.Runtime.FaultTolerance;

namespace Microsoft.Research.Naiad.Dataflow.Channels
{
    internal class ProgressChannel : Cable<Empty, Update, Empty>
    {
        private class Mailbox : Mailbox<Update, Empty>//, UntypedMailbox
        {
            private readonly int senderStageId;
            private readonly PostOffice postOffice;
            private readonly Runtime.Progress.ProgressUpdateConsumer consumer; 
            private readonly int id;
            private readonly int vertexId;
            private readonly int graphid;

            public int SenderStageId { get { return this.senderStageId; } }
            public int GraphId { get { return this.graphid; } }

            public int ChannelId { get { return this.id; } }
            public int VertexId { get { return this.vertexId; } }
            public int ThreadIndex { get { return this.consumer.scheduler.Index; } }

            private readonly AutoSerializedMessageDecoder<Update, Empty> decoder;

            private int[] nextSequenceNumbers;

            private readonly BufferPool<Update> BufferPool;

            public string WaitingMessageCount()
            {
                return null;
            }

            public void DeliverSerializedMessage(SerializedMessage message, ReturnAddress from)
            {
                lock (this)
                {
                    if (true) //(message.Header.SequenceNumber == this.nextSequenceNumbers[from.VertexID])
                    {
                        //Console.Error.WriteLine("Delivering message {0} L = {1} A = {2}", message.Header.SequenceNumber, message.Header.Length, message.Body.Available);
                        //foreach (Pair<Int64, Pointstamp> currentRecord in this.decoder.Elements(message))
                        //    Console.Error.WriteLine("-- {0}", currentRecord); 

                        //this.nextSequenceNumber++;
                        this.nextSequenceNumbers[from.VertexID]++;

                        foreach (var typedMessage in this.decoder.AsTypedMessages(message))
                        {
                            this.consumer.ProcessCountChange(typedMessage);
                            typedMessage.Release(AllocationReason.Deserializer, this.BufferPool);
                        }

                        this.RequestFlush(from);
                    }
                    else
                    {
                        //Console.Error.WriteLine("Discarding message {0} (expecting {1}) L = {2} A = {3}", message.Header.SequenceNumber, this.nextSequenceNumber, message.Header.Length, message.Body.Available);
                        //foreach (Pair<Int64, Pointstamp> currentRecord in this.decoder.Elements(message))
                        //    Console.Error.WriteLine("-- {0}", currentRecord);
                    }
                }
            }

            internal long recordsReceived = 0;

            public void Send(Message<Update, Empty> message, ReturnAddress from)
            {
                this.recordsReceived += message.length;
                this.consumer.scheduler.statistics[(int)RuntimeStatistic.ProgressLocalRecords] += message.length;
                this.consumer.ProcessCountChange(message);
            }

            public void Drain() { }
            public void RequestFlush(ReturnAddress from) { }

            public void TransferEndpointForRollback(VertexInput<Update, Empty> newEndpoint)
            {
                throw new NotImplementedException();
            }

            public Mailbox(int senderStageId, PostOffice postOffice, Runtime.Progress.ProgressUpdateConsumer consumer, int id, int vertexId, int numProducers)
            {
                this.senderStageId = senderStageId;
                this.postOffice = postOffice;
                this.consumer = consumer;
                this.graphid = this.consumer.Stage.InternalComputation.Index;

                this.id = id;
                this.vertexId = vertexId;

                this.nextSequenceNumbers = new int[numProducers];

                this.BufferPool = new MessageBufferPool<Update>();

                this.decoder = new AutoSerializedMessageDecoder<Update, Empty>(consumer.SerializationFormat, this.BufferPool);
            }
        }
        
        private class Fiber : SendChannel<Empty, Update, Empty>
        {
            private readonly NetworkChannel networkChannel;

            private readonly int channelID;
            private readonly int vertexID;

            private readonly VertexOutput<Empty, Update, Empty> sender;
            private readonly ProgressChannel.Mailbox localMailbox;

            private readonly int numProcesses;
            private readonly int processId;
            private readonly ReturnAddress senderAddress;

            private AutoSerializedMessageEncoder<Update, Empty> encoder;

            public Fiber(int channelID, int vertexID, VertexOutput<Empty, Update, Empty> sender, ProgressChannel.Mailbox localMailbox, InternalController controller)
            {
                this.processId = controller.Configuration.ProcessID;
                this.channelID = channelID;
                this.vertexID = vertexID;
                this.sender = sender;
                this.localMailbox = localMailbox;
                this.networkChannel = controller.NetworkChannel;
                this.numProcesses = controller.Configuration.Processes;
                this.senderAddress = new ReturnAddress(this.sender.Vertex.Stage.StageId, this.processId, this.sender.Vertex.VertexId, 0);
                if (this.networkChannel != null)
                {
                    this.encoder = new AutoSerializedMessageEncoder<Update, Empty>(-1, this.sender.Vertex.Stage.InternalComputation.Index << 16 | this.channelID, this.networkChannel.GetBufferPool(-1, -1), this.networkChannel.SendPageSize, controller.SerializationFormat, false, SerializedMessageType.Data, () => this.GetNextSequenceNumber());
                    this.encoder.CompletedMessage += (o, a) => { this.BroadcastPageContents(a.Hdr, a.Segment); /* Console.WriteLine("Sending progress message"); */};
                }
            }

            private int nextSequenceNumber = 0;
            public int GetNextSequenceNumber()
            {
                return nextSequenceNumber++;
            }

            public void EnableLogging(IOutgoingMessageLogger<Empty, Update, Empty> logger) { }

            public IOutgoingMessageReplayer<Empty, Empty> GetMessageReplayer()
            {
                throw new NotImplementedException();
            }

            public void Send(Empty sendTime, Message<Update, Empty> records)
            {
                if (this.networkChannel != null)
                    this.encoder.Write(new ArraySegment<Update>(records.payload, 0, records.length), this.vertexID);

                this.localMailbox.Send(records, this.senderAddress);
            }

            internal long bytesSent = 0;
            internal long messagesSent = 0;

            private void BroadcastPageContents(MessageHeader hdr, BufferSegment segment)
            {
                var nmsgs = this.networkChannel.BroadcastBufferSegment(hdr, segment);

                this.bytesSent += nmsgs * segment.Length;
                this.messagesSent += nmsgs;
                var s = sender.Vertex.scheduler;
                s.statistics[(int)RuntimeStatistic.TxProgressMessages] += nmsgs;
                s.statistics[(int)RuntimeStatistic.TxProgressBytes] += nmsgs * segment.Length;
            }

            public void Flush()
            {
                if (this.networkChannel != null)
                {
                    this.encoder.Flush();
                }
            }
        }

        private readonly Edge<Empty, Update, Empty> edge;
        public Edge<Empty, Update, Empty> Edge { get { return edge; } }

        private readonly FullyTypedStageOutput<Empty, Update, Empty> sendBundle;
        private readonly StageInput<Update, Empty> recvBundle;

        private readonly Dictionary<int, Fiber> postboxes;
        private readonly Mailbox mailbox;

        public ProgressChannel(int producerPlacementCount, 
                            ProgressUpdateConsumer consumerVertex, 
                            FullyTypedStageOutput<Empty, Update, Empty> stream, 
                            StageInput<Update, Empty> recvPort, 
                            InternalController controller,
                            Edge<Empty, Update, Empty> edge)
        {
            this.sendBundle = stream;
            this.recvBundle = recvPort;
            this.edge = edge;

            var computation = sendBundle.ForStage.InternalComputation;
            var recvFiber = this.recvBundle.GetPin(computation.Controller.Configuration.ProcessID);
            
            this.mailbox = new Mailbox(this.sendBundle.ForStage.StageId, recvFiber.Vertex.Scheduler.State(computation).PostOffice, consumerVertex, this.Edge.ChannelId, consumerVertex.VertexId, producerPlacementCount);
            
            // recvFiber.Vertex.Scheduler.State(graphManager).PostOffice.RegisterMailbox(this.mailbox);

            this.postboxes = new Dictionary<int, Fiber>();
            foreach (VertexLocation loc in sendBundle.ForStage.Placement)
                if (loc.ProcessId == controller.Configuration.ProcessID)
                    this.postboxes[loc.VertexId] = new Fiber(this.Edge.ChannelId, loc.VertexId, this.sendBundle.GetFiber(loc.VertexId), this.mailbox, controller);

            if (controller.NetworkChannel != null)
                controller.NetworkChannel.RegisterMailbox(this.mailbox);

            Logging.Info("Allocated progress channel [{0}]: {1} -> {2}", this.Edge.ChannelId, sendBundle, recvBundle);
            NaiadTracing.Trace.ChannelInfo(this.Edge.ChannelId, SourceStage.StageId, DestinationStage.StageId, true, true);
        }

        public Dataflow.Stage<Empty> SourceStage       { get { return this.sendBundle.TypedStage; } }
        public Dataflow.Stage<Empty> DestinationStage  { get { return this.recvBundle.ForStage; } }

        public void EnableReceiveLogging()
        {
            throw new NotImplementedException();
        }

        public void ReMaterializeForRollback(Dictionary<int, Vertex> newSourceVertices, Dictionary<int, Vertex> newTargetVertices)
        {
            throw new NotImplementedException();
        }

        public SendChannel<Empty, Update, Empty> GetSendChannel(int i)
        {
            return this.postboxes[i];
        }

        internal long TotalBytesSent        { get { return this.postboxes.Values.Sum(x => x.bytesSent); } }
        internal long TotalMessagesSent     { get { return this.postboxes.Values.Sum(x => x.messagesSent); } }
        internal long TotalRecordsReceived  { get { return this.mailbox.recordsReceived; } }
    }
}
