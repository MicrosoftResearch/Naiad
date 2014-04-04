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
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Diagnostics;
using System.Threading;
using Microsoft.Research.Naiad.DataStructures;
using Microsoft.Research.Naiad.CodeGeneration;
using Microsoft.Research.Naiad.Scheduling;
using Microsoft.Research.Naiad.Frameworks;
using Microsoft.Research.Naiad.Dataflow;
using Microsoft.Research.Naiad.Runtime.Networking;
using Microsoft.Research.Naiad.Runtime.Controlling;
using Microsoft.Research.Naiad.Runtime.Progress;

namespace Microsoft.Research.Naiad.Dataflow.Channels
{
    internal class CentralizedProgressChannel : Cable<Update, Empty>
    {
        private bool debug = false;

        private class Mailbox : Mailbox<Update, Empty>, UntypedLocalMailbox, RecvWire<Update, Empty>
        {
            private readonly PostOffice postOffice;
            private readonly Runtime.Progress.ProgressUpdateCentralizer consumer;
            private readonly int id;
            private readonly int vertexId;
            private readonly int graphId;

            public int Id { get { return this.id; } }
            public int VertexId { get { return this.vertexId; } }
            public int GraphId { get { return this.graphId; } }

            public int ThreadIndex { get { return this.consumer.scheduler.Index; } }

            private readonly AutoSerializedMessageDecoder<Update, Empty> decoder;

            public void DeliverSerializedMessage(SerializedMessage message, RemotePostbox from)
            {
                //foreach (var currentRecord in this.decoder.Elements(message))
                //    this.consumer.ProcessCountChange(currentRecord.v1.Pointstamp, currentRecord.v1.Delta);
                foreach (var typedMessage in this.decoder.AsTypedMessages(message))
                {
                    this.consumer.ProcessCountChange(typedMessage);
                    typedMessage.Release();
                }

                this.Flush(from);
            }

            internal long recordsReceived = 0;
           /*
            /// <summary>
            /// "Sends" a progress message by poking the consumer directly.
            /// </summary>
            /// <param name="record"></param>
            /// <param name="from"></param>
            public void Send(Pair<Update, Empty> record, RemotePostbox from)
            {
                this.recordsReceived += 1;
                this.consumer.scheduler.statistics[(int)RuntimeStatistic.ProgressLocalRecords] += 1;
                this.consumer.ProcessCountChange(record.v1.Pointstamp, record.v1.Delta);
            }
            */
            public void Send(Message<Update, Empty> message, RemotePostbox from)
            {
                this.recordsReceived += message.length;
                this.consumer.scheduler.statistics[(int)RuntimeStatistic.ProgressLocalRecords] += message.length;
                this.consumer.ProcessCountChange(message);
            }

            public void Drain()
            {
                //throw new NotImplementedException();
            }

            /// <summary>
            /// No-op for progress
            /// </summary>
            public void GatherMessagesFromAllSenders()
            {
            }

            public void Flush(RemotePostbox from)
            {
            }

            public void Flush()
            {
            }

            public Mailbox(PostOffice postOffice, Runtime.Progress.ProgressUpdateCentralizer consumer, int id, int vertexId)
            {
                this.postOffice = postOffice;
                this.consumer = consumer;
                this.id = id;
                this.vertexId = vertexId;
                this.graphId = this.consumer.Stage.InternalGraphManager.Index;

                this.decoder = new AutoSerializedMessageDecoder<Update, Empty>(consumer.CodeGenerator);
            }
        }

        private class Fiber : SendWire<Update, Empty>
        {
            private bool debug = false;

            private readonly NetworkChannel networkChannel;

            private readonly int channelID;
            private readonly int vertexID;   // used for tracing/debugging

            private readonly VertexOutput<Update, Empty> sender;
            private readonly CentralizedProgressChannel.Mailbox localMailbox;

            private readonly int receiverVertexId;
            private readonly int receiverProcessId;

            private readonly int numProcesses;

            private AutoSerializedMessageEncoder<Update, Empty> encoder;

            public int RecordSizeHint
            {
                get
                {
                    return int.MaxValue;
                }
            }

            public Fiber(int channelID, int vertexID, VertexOutput<Update, Empty> sender, CentralizedProgressChannel.Mailbox localMailbox, InternalController controller, int receiverVertexId, int receiverProcessId)
            {
                this.channelID = channelID;
                this.vertexID = vertexID;
                this.sender = sender;
                this.localMailbox = localMailbox;
                this.networkChannel = controller.NetworkChannel;
                this.numProcesses = controller.Configuration.Processes;
                int processID = controller.Configuration.ProcessID;

                this.receiverVertexId = receiverVertexId;
                this.receiverProcessId = receiverProcessId;

                if (this.networkChannel != null)
                {
                    this.encoder = new AutoSerializedMessageEncoder<Update, Empty>(0, this.sender.Vertex.Stage.InternalGraphManager.Index << 16 | this.channelID, this.networkChannel.GetBufferPool(0, -1), this.networkChannel.SendPageSize, controller.CodeGenerator, SerializedMessageType.Data, () => this.networkChannel.GetSequenceNumber(-1));
                    this.encoder.CompletedMessage += (o, a) => { this.SendPageContents(a.Hdr, a.Segment); };
                }
            }

            internal long bytesSent = 0;
            internal long messagesSent = 0;

            /*
            public void Send(Update record)
            {
                int receiverVertexId = 0;

                if (debug) Console.Error.WriteLine("  IncastChannel Send {0}->{1}  {2} {3}", vertexID, receiverVertexId, record.v1, record.v2);

                if (this.localMailbox == null)      // We are not local to destination
                {
                    this.encoder.Write(record, this.vertexID);
                }
                else
                {
                    // Fortunately, this particular local mailbox doesn't look at the postbox argument
                    this.localMailbox.Send(record, new RemotePostbox());
                }
            }
            */
            public void Send(Message<Update, Empty> records)
            {
                if (debug)
                    for (int i = 0; i < records.length; i++)
                        Console.Error.WriteLine("  IncastChannel Send {0}->{1}  {2} {3}", vertexID, 0, records.payload[i].Delta, records.payload[i].Pointstamp);

                if (this.localMailbox == null)
                {
                    //for (int i = 0; i < records.length; i++)
                    //    this.encoder.Write(records.payload[i].PairWith(records.time));
                    this.encoder.Write(new ArraySegment<Update>(records.payload, 0, records.length), 0);
                }
                else
                    this.localMailbox.Send(records, new RemotePostbox());


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

        private readonly int channelID;
        public int ChannelId { get { return channelID; } }

        //private readonly Stage<Runtime.Progress.ProgressUpdateProducer, Pointstamp> producer;
        private readonly Stage<Runtime.Progress.ProgressUpdateCentralizer, Empty> consumer;

        private readonly StageOutput<Update, Empty> sendBundle;
        private readonly StageInput<Update, Empty> recvBundle;

        private readonly Dictionary<int, Fiber> postboxes;
        private readonly Mailbox mailbox;

        /// <summary>
        /// Constructor
        /// </summary>

        public CentralizedProgressChannel(Stage<ProgressUpdateCentralizer, Empty> consumer,
                             StageOutput<Update, Empty> stream, StageInput<Update, Empty> recvPort,
                             InternalController controller,
                             int channelId)
        {
            this.consumer = consumer;

            this.sendBundle = stream;// producer.Output;
            this.recvBundle = recvPort;// consumer.Input;

            this.postboxes = new Dictionary<int, Fiber>();

            this.channelID = channelId;

            // Get the vertex id and process id of the single consumer
            var consumerVertexId = consumer.Placement.Single().VertexId;
            var consumerProcessId = consumer.Placement.Single().ProcessId;

            var graphManager = sendBundle.ForStage.InternalGraphManager;

            if (debug) Console.Error.WriteLine("  IncastChannel create ProcessId = {0}", graphManager.Controller.Configuration.ProcessID);

            var myProcessId = graphManager.Controller.Configuration.ProcessID;

            if (myProcessId == consumerProcessId)
            {
                if (debug) Console.Error.WriteLine("  IncastChannel creating receive mailbox");
                VertexInput<Update, Empty> recvFiber = this.recvBundle.GetPin(consumerProcessId);

                this.mailbox = new Mailbox(recvFiber.Vertex.Scheduler.State(graphManager).PostOffice,
                                           consumer.GetVertex(consumerVertexId), this.channelID, consumerVertexId);
                recvFiber.Vertex.Scheduler.State(graphManager).PostOffice.RegisterMailbox(this.mailbox);
                if (controller.NetworkChannel != null)
                    controller.NetworkChannel.RegisterMailbox(this.mailbox);
            }

            foreach (VertexLocation loc in sendBundle.ForStage.Placement)
            {
                if (loc.ProcessId == sendBundle.ForStage.InternalGraphManager.Controller.Configuration.ProcessID)
                {
                    if (debug) Console.Error.WriteLine("  IncastChannel loc = {0}/{1}/{2}", loc.ProcessId, loc.VertexId, loc.ThreadId);
                    var postbox = new Fiber(this.channelID, loc.VertexId, this.sendBundle.GetFiber(loc.VertexId),
                                            this.mailbox, controller, consumerVertexId, consumerProcessId);
                    this.postboxes[loc.VertexId] = postbox;
                }
            }
            Logging.Info("Allocated incast channel [{0}]: {1} -> {2}", this.channelID, sendBundle, recvBundle);
        }

        public Dataflow.Stage SourceStage
        {
            get
            {
                return this.sendBundle.ForStage;
            }
        }

        public Dataflow.Stage DestinationStage
        {
            get
            {
                return this.recvBundle.ForStage;
            }
        }

        public SendWire<Update, Empty> GetSendFiber(int i)
        {
            return this.postboxes[i];
        }

        public RecvWire<Update, Empty> GetRecvFiber(int i)
        {
            Debug.Assert(i == this.sendBundle.ForStage.InternalGraphManager.Controller.Configuration.ProcessID);
            return this.mailbox;
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
