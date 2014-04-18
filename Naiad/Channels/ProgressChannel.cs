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
using Microsoft.Research.Naiad.Scheduling;
using Microsoft.Research.Naiad.Frameworks;
using System.Collections.Concurrent;
using Microsoft.Research.Naiad.Serialization;
using System.Diagnostics;

using Microsoft.Research.Naiad.Dataflow;
using Microsoft.Research.Naiad.Runtime.Networking;
using Microsoft.Research.Naiad.Runtime.Controlling;
using Microsoft.Research.Naiad.Runtime.Progress;

using Microsoft.Research.Naiad.Diagnostics;

namespace Microsoft.Research.Naiad.Dataflow.Channels
{
    internal class ProgressChannel : Cable<Update, Empty>
    {
        private class Mailbox : Mailbox<Update, Empty>//, UntypedMailbox
        {
            private readonly PostOffice postOffice;
            private readonly Runtime.Progress.ProgressUpdateConsumer consumer; 
            private readonly int id;
            private readonly int vertexId;
            private readonly int graphid;

            public int GraphId { get { return this.graphid; } }

            public int Id { get { return this.id; } }
            public int VertexId { get { return this.vertexId; } }
            public int ThreadIndex { get { return this.consumer.scheduler.Index; } }

            private readonly AutoSerializedMessageDecoder<Update, Empty> decoder;

            private int[] nextSequenceNumbers;

            public void DeliverSerializedMessage(SerializedMessage message, ReturnAddress from)
            {
                lock (this)
                {
                    if (message.Header.SequenceNumber == this.nextSequenceNumbers[from.VertexID])
                    {
                        //Console.Error.WriteLine("Delivering message {0} L = {1} A = {2}", message.Header.SequenceNumber, message.Header.Length, message.Body.Available);
                        //foreach (Pair<Int64, Pointstamp> currentRecord in this.decoder.Elements(message))
                        //    Console.Error.WriteLine("-- {0}", currentRecord); 

                        //this.nextSequenceNumber++;
                        this.nextSequenceNumbers[from.VertexID]++;

                        foreach (var typedMessage in this.decoder.AsTypedMessages(message))
                        {
                            this.consumer.ProcessCountChange(typedMessage);
                            typedMessage.Release();
                        }

                        this.Flush(from);
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
            public void Flush(ReturnAddress from) { }

            public Mailbox(PostOffice postOffice, Runtime.Progress.ProgressUpdateConsumer consumer, int id, int vertexId, int numProducers)
            {
                this.postOffice = postOffice;
                this.consumer = consumer;
                this.graphid = this.consumer.Stage.InternalComputation.Index;

                this.id = id;
                this.vertexId = vertexId;

                this.nextSequenceNumbers = new int[numProducers];

                this.decoder = new AutoSerializedMessageDecoder<Update, Empty>(consumer.SerializationFormat);
            }
        }
        
        private class Fiber : SendChannel<Update, Empty>
        {
            private readonly NetworkChannel networkChannel;

            private readonly int channelID;
            private readonly int vertexID;

            private readonly VertexOutput<Update, Empty> sender;
            private readonly ProgressChannel.Mailbox localMailbox;

            private readonly int numProcesses;
            private readonly int processId;

            private AutoSerializedMessageEncoder<Update, Empty> encoder;

            public Fiber(int channelID, int vertexID, VertexOutput<Update, Empty> sender, ProgressChannel.Mailbox localMailbox, InternalController controller)
            {
                this.processId = controller.Configuration.ProcessID;
                this.channelID = channelID;
                this.vertexID = vertexID;
                this.sender = sender;
                this.localMailbox = localMailbox;
                this.networkChannel = controller.NetworkChannel;
                this.numProcesses = controller.Configuration.Processes;
                int processID = controller.Configuration.ProcessID;
                if (this.networkChannel != null)
                {
                    this.encoder = new AutoSerializedMessageEncoder<Update, Empty>(-1, this.sender.Vertex.Stage.InternalComputation.Index << 16 | this.channelID, this.networkChannel.GetBufferPool(-1, -1), this.networkChannel.SendPageSize, controller.SerializationFormat, SerializedMessageType.Data, () => this.GetNextSequenceNumber());
                    this.encoder.CompletedMessage += (o, a) => { this.BroadcastPageContents(a.Hdr, a.Segment); /* Console.WriteLine("Sending progress message"); */};
                }
            }

            private int nextSequenceNumber = 0;
            public int GetNextSequenceNumber()
            {
                return nextSequenceNumber++;
            }


            public void Send(Message<Update, Empty> records)
            {
                if (this.networkChannel != null)
                    this.encoder.Write(new ArraySegment<Update>(records.payload, 0, records.length), 0);

                this.localMailbox.Send(records, new ReturnAddress());
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

        private readonly int channelID;
        public int ChannelId { get { return channelID; } }

        private readonly StageOutput<Update, Empty> sendBundle;
        private readonly StageInput<Update, Empty> recvBundle;

        private readonly Dictionary<int, Fiber> postboxes;
        private readonly Mailbox mailbox;

        public ProgressChannel(int producerPlacementCount, 
                            ProgressUpdateConsumer consumerVertex, 
                            StageOutput<Update, Empty> stream, 
                            StageInput<Update, Empty> recvPort, 
                            InternalController controller,
                            int channelId)
        {
            this.sendBundle = stream;
            this.recvBundle = recvPort;
            this.channelID = channelId;

            var computation = sendBundle.ForStage.InternalComputation;
            var recvFiber = this.recvBundle.GetPin(computation.Controller.Configuration.ProcessID);
            
            this.mailbox = new Mailbox(recvFiber.Vertex.Scheduler.State(computation).PostOffice, consumerVertex, this.channelID, consumerVertex.VertexId, producerPlacementCount);
            
            // recvFiber.Vertex.Scheduler.State(graphManager).PostOffice.RegisterMailbox(this.mailbox);

            this.postboxes = new Dictionary<int, Fiber>();
            foreach (VertexLocation loc in sendBundle.ForStage.Placement)
                if (loc.ProcessId == controller.Configuration.ProcessID)
                    this.postboxes[loc.VertexId] = new Fiber(this.channelID, loc.VertexId, this.sendBundle.GetFiber(loc.VertexId), this.mailbox, controller);

            if (controller.NetworkChannel != null)
                controller.NetworkChannel.RegisterMailbox(this.mailbox);

            Logging.Info("Allocated progress channel [{0}]: {1} -> {2}", this.channelID, sendBundle, recvBundle);
        }

        public Dataflow.Stage SourceStage       { get { return this.sendBundle.ForStage; } }
        public Dataflow.Stage DestinationStage  { get { return this.recvBundle.ForStage; } }

        public SendChannel<Update, Empty> GetSendChannel(int i)
        {
            return this.postboxes[i];
        }

        internal long TotalBytesSent        { get { return this.postboxes.Values.Sum(x => x.bytesSent); } }
        internal long TotalMessagesSent     { get { return this.postboxes.Values.Sum(x => x.messagesSent); } }
        internal long TotalRecordsReceived  { get { return this.mailbox.recordsReceived; } }
    }
}
