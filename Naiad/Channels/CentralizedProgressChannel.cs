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
using Naiad.DataStructures;
using Naiad.CodeGeneration;
using Naiad.Scheduling;
using Naiad.Frameworks;
using Naiad.Dataflow;
using Naiad.Runtime.Networking;
using Naiad.Runtime.Controlling;

namespace Naiad.Dataflow.Channels
{
    internal class CentralizedProgressChannel : Cable<Int64, Pointstamp>
    {
        private bool debug = false;

        private class Mailbox : Mailbox<Int64, Pointstamp>, UntypedLocalMailbox, RecvWire<Int64, Pointstamp>
        {
            private readonly PostOffice postOffice;
            private readonly Runtime.Progress.ProgressUpdateCentralizer consumer;
            private readonly int id;
            private readonly int shardId;
            private readonly int graphId;

            public int Id { get { return this.id; } }
            public int ShardId { get { return this.shardId; } }
            public int GraphId { get { return this.graphId; } }

            public int ThreadIndex { get { return this.consumer.scheduler.Index; } }

            private AutoSerializedMessageDecoder<Int64, Pointstamp> decoder = new AutoSerializedMessageDecoder<long, Pointstamp>();

            public void DeliverSerializedMessage(SerializedMessage message, RemotePostbox from)
            {

                foreach (Pair<Int64, Pointstamp> currentRecord in this.decoder.Elements(message))
                    this.consumer.ProcessCountChange(currentRecord.v2, currentRecord.v1);

                this.Flush(from);
            }

            internal long recordsReceived = 0;

            /// <summary>
            /// "Sends" a progress message by poking the consumer directly.
            /// </summary>
            /// <param name="record"></param>
            /// <param name="from"></param>
            public void Send(Pair<Int64, Pointstamp> record, RemotePostbox from)
            {
                this.recordsReceived += 1;
                this.consumer.scheduler.statistics[(int)RuntimeStatistic.ProgressLocalRecords] += 1;
                this.consumer.ProcessCountChange(record.v2, record.v1);
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

            public Mailbox(PostOffice postOffice, Runtime.Progress.ProgressUpdateCentralizer consumer, int id, int shardId)
            {
                this.postOffice = postOffice;
                this.consumer = consumer;
                this.id = id;
                this.shardId = shardId;
                this.graphId = this.consumer.Stage.InternalGraphManager.Index;
            }
        }

        private class Fiber : SendWire<Int64, Pointstamp>
        {
            private bool debug = false;

            private readonly NetworkChannel networkChannel;

            private readonly int channelID;
            private readonly int shardID;   // used for tracing/debugging

            private readonly VertexOutput<Int64, Pointstamp> sender;
            private readonly CentralizedProgressChannel.Mailbox localMailbox;

            private readonly int receiverShardId;
            private readonly int receiverProcessId;

            private readonly int numProcesses;

            private AutoSerializedMessageEncoder<Int64, Pointstamp> encoder;

            public int RecordSizeHint
            {
                get
                {
                    return int.MaxValue;
                }
            }

            public Fiber(int channelID, int shardID, VertexOutput<Int64, Pointstamp> sender, CentralizedProgressChannel.Mailbox localMailbox, InternalController controller, int receiverShardId, int receiverProcessId)
            {
                this.channelID = channelID;
                this.shardID = shardID;
                this.sender = sender;
                this.localMailbox = localMailbox;
                this.networkChannel = controller.NetworkChannel;
                this.numProcesses = controller.Configuration.Processes;
                int processID = controller.Configuration.ProcessID;

                this.receiverShardId = receiverShardId;
                this.receiverProcessId = receiverProcessId;

                if (this.networkChannel != null)
                {
                    this.encoder = new AutoSerializedMessageEncoder<Int64, Pointstamp>(0,
                        this.sender.Vertex.Stage.InternalGraphManager.Index << 16 | this.channelID, this.networkChannel.GetBufferPool(0, -1), this.networkChannel.SendPageSize, AutoSerializationMode.Basic, SerializedMessageType.Data,
                        () => this.networkChannel.GetSequenceNumber(-1));
                    this.encoder.CompletedMessage += (o, a) => { this.SendPageContents(a.Hdr, a.Segment); };
                }
            }

            internal long bytesSent = 0;
            internal long messagesSent = 0;

            public void Send(Pair<Int64, Pointstamp> record)
            {
                int receiverShardId = 0;

                if (debug) Console.Error.WriteLine("  IncastChannel Send {0}->{1}  {2} {3}", shardID, receiverShardId, record.v1, record.v2);

                if (this.localMailbox == null)      // We are not local to destination
                {
                    this.encoder.Write(record, this.shardID);
                }
                else
                {
                    // Fortunately, this particular local mailbox doesn't look at the postbox argument
                    this.localMailbox.Send(record, new RemotePostbox());
                }
            }

            public void Send(Message<Pair<Int64, Pointstamp>> records)
            {
                for (int i = 0; i < records.length; i++)
                    this.Send(records.payload[i]);
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
                if (debug) Console.Error.WriteLine("  IncastChannel Flush {0}", this.shardID);
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
        private readonly Stage<Runtime.Progress.ProgressUpdateCentralizer, Pointstamp> consumer;

        private readonly StageOutput<Int64, Pointstamp> sendBundle;
        private readonly StageInput<Int64, Pointstamp> recvBundle;

        private readonly Dictionary<int, Fiber> postboxes;
        private readonly Mailbox mailbox;

        /// <summary>
        /// Constructor
        /// </summary>

        public CentralizedProgressChannel(Stage<Runtime.Progress.ProgressUpdateCentralizer, Pointstamp> consumer,
                             StageOutput<Int64, Pointstamp> stream, StageInput<Int64, Pointstamp> recvPort,
                             InternalController controller,
                             int channelId)
        {
            this.consumer = consumer;

            this.sendBundle = stream;// producer.Output;
            this.recvBundle = recvPort;// consumer.Input;

            this.postboxes = new Dictionary<int, Fiber>();

            this.channelID = channelId;

            // Get the shard id and process id of the single consumer
            var consumerShardId = consumer.Placement.Single().VertexId;
            var consumerProcessId = consumer.Placement.Single().ProcessId;

            var graphManager = sendBundle.ForStage.InternalGraphManager;

            if (debug) Console.Error.WriteLine("  IncastChannel create ProcessId = {0}", graphManager.Controller.Configuration.ProcessID);

            var myProcessId = graphManager.Controller.Configuration.ProcessID;

            if (myProcessId == consumerProcessId)
            {
                if (debug) Console.Error.WriteLine("  IncastChannel creating receive mailbox");
                VertexInput<Int64, Pointstamp> recvFiber = this.recvBundle.GetPin(consumerProcessId);

                this.mailbox = new Mailbox(recvFiber.Vertex.Scheduler.State(graphManager).PostOffice,
                                           consumer.GetShard(consumerShardId), this.channelID, consumerShardId);
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
                                            this.mailbox, controller, consumerShardId, consumerProcessId);
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

        public SendWire<Int64, Pointstamp> GetSendFiber(int i)
        {
            return this.postboxes[i];
        }

        public RecvWire<Int64, Pointstamp> GetRecvFiber(int i)
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
