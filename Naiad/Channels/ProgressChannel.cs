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

﻿using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Naiad.Scheduling;
using Naiad.Frameworks;
using System.Collections.Concurrent;
using Naiad.CodeGeneration;
using System.Diagnostics;

using Naiad.Dataflow;
using Naiad.Runtime.Networking;
using Naiad.Runtime.Controlling;
using Naiad.Runtime.Progress;

namespace Naiad.Dataflow.Channels
{
    internal class ProgressChannel : Cable<Int64, Pointstamp>
    {
        private class Mailbox : Mailbox<Int64, Pointstamp>, UntypedLocalMailbox, RecvWire<Int64, Pointstamp>
        {
            private readonly PostOffice postOffice;
            private readonly Runtime.Progress.ProgressUpdateConsumer consumer; 
            private readonly int id;
            private readonly int shardId;
            private readonly int graphid;

            public int GraphId { get { return this.graphid; } }

            public int Id { get { return this.id; } }
            public int ShardId { get { return this.shardId; } }
            public int ThreadIndex { get { return this.consumer.scheduler.Index; } }

            private AutoSerializedMessageDecoder<Int64, Pointstamp> decoder = new AutoSerializedMessageDecoder<long,Pointstamp>();

            private int nextSequenceNumber = 0;

            private int[] nextSequenceNumbers;

            public void DeliverSerializedMessage(SerializedMessage message, RemotePostbox from)
            {
                lock (this)
                {
                    if (message.Header.SequenceNumber == this.nextSequenceNumbers[from.ShardID])
                    {
                        //Console.Error.WriteLine("Delivering message {0} L = {1} A = {2}", message.Header.SequenceNumber, message.Header.Length, message.Body.Available);
                        //foreach (Pair<Int64, Pointstamp> currentRecord in this.decoder.Elements(message))
                        //    Console.Error.WriteLine("-- {0}", currentRecord); 

                        //this.nextSequenceNumber++;
                        this.nextSequenceNumbers[from.ShardID]++;

                        var counter = 0;
                        foreach (Pair<Int64, Pointstamp> currentRecord in this.decoder.Elements(message))
                        {
                            this.consumer.ProcessCountChange(currentRecord.v2, currentRecord.v1);
                            counter++;
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

            /// <summary>
            /// "Sends" a progress message by poking the consumer directly.
            /// </summary>
            public void Send(Pair<Int64, Pointstamp> record, RemotePostbox from)
            {
                this.recordsReceived += 1;
                this.consumer.scheduler.statistics[(int)RuntimeStatistic.ProgressLocalRecords] += 1;
                this.consumer.ProcessCountChange(record.v2, record.v1);
            }

            public void Drain() { }
            public void GatherMessagesFromAllSenders() { }
            public void Flush(RemotePostbox from) { }
            public void Flush() { }

            public Mailbox(PostOffice postOffice, Runtime.Progress.ProgressUpdateConsumer consumer, int id, int shardId, int numProducers)
            {
                this.postOffice = postOffice;
                this.consumer = consumer;
                this.graphid = this.consumer.Stage.InternalGraphManager.Index;

                this.id = id;
                this.shardId = shardId;

                this.nextSequenceNumbers = new int[numProducers];
            }
        }
        
        private class Fiber : SendWire<Int64, Pointstamp>
        {
            private readonly NetworkChannel networkChannel;

            private readonly int channelID;
            private readonly int shardID;

            private readonly VertexOutput<Int64, Pointstamp> sender;
            private readonly ProgressChannel.Mailbox localMailbox;

            private readonly int numProcesses;
            private readonly int processId;

            private AutoSerializedMessageEncoder<Int64, Pointstamp> encoder;

            public int RecordSizeHint { get { return int.MaxValue; } }

            public Fiber(int channelID, int shardID, VertexOutput<Int64, Pointstamp> sender, ProgressChannel.Mailbox localMailbox, InternalController controller)
            {
                this.processId = controller.Configuration.ProcessID;
                this.channelID = channelID;
                this.shardID = shardID;
                this.sender = sender;
                this.localMailbox = localMailbox;
                this.networkChannel = controller.NetworkChannel;
                this.numProcesses = controller.Configuration.Processes;
                int processID = controller.Configuration.ProcessID;
                if (this.networkChannel != null)
                {
                    this.encoder = new AutoSerializedMessageEncoder<Int64, Pointstamp>(-1, this.sender.Vertex.Stage.InternalGraphManager.Index << 16 | this.channelID, this.networkChannel.GetBufferPool(-1, -1), this.networkChannel.SendPageSize, AutoSerializationMode.Basic, SerializedMessageType.Data, () => this.GetNextSequenceNumber());
                    this.encoder.CompletedMessage += (o, a) => { this.BroadcastPageContents(a.Hdr, a.Segment); /* Console.WriteLine("Sending progress message"); */};
                }
            }

            private int nextSequenceNumber = 0;
            public int GetNextSequenceNumber()
            {
                return nextSequenceNumber++;
            }

            public void Send(Pair<Int64, Pointstamp> record)
            {
                if (this.networkChannel != null)
                {
                    this.encoder.Write(record, this.shardID);
                }
                
                // Fortunately, this particular local mailbox doesn't look at the postbox argument
                this.localMailbox.Send(record, new RemotePostbox());
            }

            public void Send(Message<Pair<Int64, Pointstamp>> records)
            {
                for (int i = 0; i < records.length; i++)
                    this.Send(records.payload[i]);
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
                this.localMailbox.Flush();
            }
        }

        private readonly int channelID;
        public int ChannelId { get { return channelID; } }

        private readonly StageOutput<Int64, Pointstamp> sendBundle;
        private readonly StageInput<Int64, Pointstamp> recvBundle;

        private readonly Dictionary<int, Fiber> postboxes;
        private readonly Mailbox mailbox;

        public ProgressChannel(int producerPlacementCount, 
                            ProgressUpdateConsumer consumerVertex, 
                            StageOutput<Int64, Pointstamp> stream, 
                            StageInput<Int64, Pointstamp> recvPort, 
                            InternalController controller,
                            int channelId)
        {
            this.sendBundle = stream;
            this.recvBundle = recvPort;
            this.channelID = channelId;

            var graphManager = sendBundle.ForStage.InternalGraphManager;
            var recvFiber = this.recvBundle.GetPin(graphManager.Controller.Configuration.ProcessID);
            
            this.mailbox = new Mailbox(recvFiber.Vertex.Scheduler.State(graphManager).PostOffice, consumerVertex, this.channelID, consumerVertex.VertexId, producerPlacementCount);
            recvFiber.Vertex.Scheduler.State(graphManager).PostOffice.RegisterMailbox(this.mailbox);

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

        public SendWire<Int64, Pointstamp> GetSendFiber(int i)
        {
            return this.postboxes[i];
        }

        public RecvWire<Int64, Pointstamp> GetRecvFiber(int i)
        {
            Debug.Assert(i == this.sendBundle.ForStage.InternalGraphManager.Controller.Configuration.ProcessID);
            return this.mailbox;
        }

        internal long TotalBytesSent        { get { return this.postboxes.Values.Sum(x => x.bytesSent); } }
        internal long TotalMessagesSent     { get { return this.postboxes.Values.Sum(x => x.messagesSent); } }
        internal long TotalRecordsReceived  { get { return this.mailbox.recordsReceived; } }
    }
}
