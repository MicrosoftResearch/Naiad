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

//#define DUPLEX_SOCKET
#define SYNC_SEND
#define SYNC_RECV
//#define SEND_HIGH_PRIORITY
//#define RECV_HIGH_PRIORITY
//#define SEND_AFFINITY
//#define RECV_AFFINITY
#define HIGH_PRIORITY_QUEUE
using System;
using System.Collections.Generic;
using System.Linq;
using System.Runtime.Remoting.Messaging;
using System.Text;
using System.Threading;
using System.Net;
using System.Collections.Concurrent;
using System.Net.Sockets;
using System.Diagnostics;
using System.IO;
using System.Threading.Tasks;
using Microsoft.Research.Naiad.Utilities;
using Microsoft.Research.Naiad.Scheduling;
using Microsoft.Research.Naiad.DataStructures;
using Microsoft.Research.Naiad.Runtime.Controlling;
using Microsoft.Research.Naiad.Runtime.FaultTolerance;
using Microsoft.Research.Naiad.Runtime.Progress;
using Microsoft.Research.Naiad.Dataflow.Channels;
using Microsoft.Research.Naiad.Serialization;

using Microsoft.Research.Naiad.Diagnostics;
using Microsoft.Research.Naiad.Dataflow;

namespace Microsoft.Research.Naiad.Runtime.Networking
{
    /// <summary>
    /// Represents a mechanism for sending untyped messages to a distributed group of processes.
    /// </summary>
    internal interface NetworkChannel : IDisposable
    {
        /// <summary>
        /// Returns the process-local unique ID of this network channel.
        /// 
        /// In current use, this is always zero.
        /// </summary>
        int Id { get; }

        /// <summary>
        /// Sends the given buffer segment to the given destination process.
        /// </summary>
        /// <param name="header">The header of the message.</param>
        /// <param name="destProcessID">The ID for the destination process, or -1 for broadcast messages.</param>
        /// <param name="segment">The buffer segment containing the message header and body.</param>
        /// <param name="HighPriority">Indicates whether the message should be sent with high or normal priority.</param>
        /// <param name="wakeUp">Indicates whether the message should be sent immediately.</param>
        void SendBufferSegment(MessageHeader header, int destProcessID, BufferSegment segment, bool HighPriority = false, bool wakeUp = true);

        /// <summary>
        /// Sends the given buffer segment to all other processes.
        /// </summary>
        /// <param name="header">The header of the message.</param>
        /// <param name="segment">The buffer segment containing the message header and body.</param>
        int BroadcastBufferSegment(MessageHeader header, BufferSegment segment);

        /// <summary>
        /// Registers the given mailbox to receive messages.
        /// </summary>
        /// <param name="mailbox">The mailbox to which messages with the same channel and destination vertex ID should be sent.</param>
        void RegisterMailbox(Mailbox mailbox);

        /// <summary>
        /// Returns the size (in bytes) of a page of serialized data used for sending.
        /// </summary>
        int SendPageSize { get; }

        /// <summary>
        /// Returns a buffer pool to be used for messages sent to the given process.
        /// </summary>
        /// <param name="processID">The ID for the destination process, or -1 for broadcast messages.</param>
        /// <param name="workerID">The local ID of the worker that is requesting the pool, or -1 for a shared pool.</param>
        /// <returns>A buffer pool to be used for messages sent to the given process.</returns>
        BufferPool<byte> GetBufferPool(int processID, int workerID);
        
        /// <summary>
        /// Returns the next sequence number for a message to the given process.
        /// </summary>
        /// <param name="destProcessId">The ID for the destination process, or -1 for broadcast messages.</param>
        /// <returns>The next sequence number for a message to the given process.</returns>
        int GetSequenceNumber(int destProcessId);

        /// <summary>
        /// Returns when connections have been established to and from all processes.
        /// </summary>
        void WaitForAllConnections();

        /// <summary>
        /// Starts delivering outgoing and incoming messages.
        /// </summary>
        void StartMessageDelivery();

        /// <summary>
        /// Blocks until all processes have acknowledged startup.
        /// </summary>
        void DoStartupBarrier();

        /// <summary>
        /// Returns the value of the given statistic.
        /// </summary>
        /// <param name="s">The statistic to be queried.</param>
        /// <returns>The value of the given statistic.</returns>
        long QueryStatistic(RuntimeStatistic s);
    }

    internal interface Snapshottable
    {
        void ShowWaitingMessageCounts();
        void StartRollback(IEnumerable<int> processes);
        void WaitForWorkerPausedMessages(IEnumerable<int> processes);
        void BroadcastCheckpoints(int graphId, IEnumerable<CheckpointLowWatermark> frontiers);
        void AnnounceFailure(int failedProcess, int delay, bool fromCoordinator);
        void AnnounceWorkersPaused();
        void AnnounceRollbackBarrier();
        void WaitForAllRollbackProgressMessages();
        void WaitForAllRollbackBarrierMessages();
        void SignalProgressRepaired(bool fromCoordinator);
        void ResumeAfterRollback();
    }
    
    internal class TcpNetworkChannel : NetworkChannel, Snapshottable
    {
        private readonly int sendPageSize;
        public int SendPageSize { get { return this.sendPageSize; } }

        private enum ReceiveResult
        {
            Continue = 0,
            Block = 1,
            Shutdown = 2
        }

        public readonly int id;
        public int Id { get { return this.id; } }

        private readonly List<List<List<Mailbox>>> graphmailboxes;

        //private readonly AutoResetEvent sendEvent;
        //private readonly AutoResetEvent[] sendEvents;

        private readonly int localProcessID;

        private readonly CountdownEvent shutdownRecvCountdown;
        private readonly CountdownEvent shutdownSendCountdown;

        private readonly CountdownEvent startupRecvCountdown;

        private readonly CountdownEvent sendConnectionCountdown;
        private readonly CountdownEvent recvConnectionCountdown;

        private readonly UdpClient udpClient;

        private readonly ManualResetEvent startCommunicatingEvent;

        private readonly List<ConnectionState> connections;
        private int broadcastSequenceNumber;
        
        private enum ConnectionStatus
        {
            Initialized,
            Accepting,
            Connecting,
            Idle,
            Sending,
            ShuttingDown,
            ShutdownSent,
        }

        private const int MAX_INFLIGHT_SEGMENTS = 1;
        private readonly int MAX_SEND_SIZE; 
        
        private class ConnectionState : IDisposable
        {
            public readonly int Id;
            public EndPoint EndPoint;
            private ConnectionStatus status;
            public ConnectionStatus Status { get { return this.status; } set { this.status = value; } }
            public readonly ConcurrentQueue<BufferSegment> SegmentQueue;
            public readonly ConcurrentQueue<BufferSegment> HighPrioritySegmentQueue;
            //public readonly NaiadList<BufferSegment> InflightSegments;
            //public readonly NaiadList<ArraySegment<byte>> InflightArraySegments;
            public RecvBufferSheaf RecvBufferSheaf;
            public Thread RecvThread;
            public Thread SendThread;
            //public readonly CircularBuffer RecvBuffer;
            public readonly AutoResetEvent SendEvent;
            
            public BufferPool<byte> SendPool;

            public readonly AutoResetEvent WorkerPauseEvent;
            public readonly AutoResetEvent RollbackPauseEvent;
            public readonly AutoResetEvent RollbackProgressEvent;
            public readonly AutoResetEvent RollbackResumeEvent;

            public Socket SendSocket;
            public Socket RecvSocket;

            public long BytesSent;
            public long DataSegmentsSent;
            public long ProgressSegmentsSent;
            public long RecordsSent;
            public long RecordsRecv;

            public readonly Dictionary<int, BufferSegment> DeferredDisposalSegments;

            public int NextSequenceNumber;

            public int SequenceNumberReceived;
            public int SequenceNumberToAcknowledge;
            public int SequenceNumberAcknowledged;

            // Trying to be cache-friendly with separate arrays for send/recv threads
            internal long[] sendStatistics;
            internal long[] recvStatistics;

            public int sequenceNumber;

            public int ReceivedCheckpointMessages;
            public int LastCheckpointSequenceNumber;

            public bool EverReceivedIncomingConnection;

            public ConnectionState(int id, ConnectionStatus status, int recvBufferLength, BufferPool<byte> sendPool)
            {
                this.Id = id;
                this.status = status;
                this.SegmentQueue = new ConcurrentQueue<BufferSegment>();
                this.HighPrioritySegmentQueue = new ConcurrentQueue<BufferSegment>();
                //this.InflightSegments = new NaiadList<BufferSegment>(MAX_INFLIGHT_SEGMENTS);
                //this.InflightArraySegments = new NaiadList<ArraySegment<byte>>(MAX_INFLIGHT_SEGMENTS);
                this.RecvBufferSheaf = new RecvBufferSheaf(id, recvBufferLength / RecvBufferPage.PAGE_SIZE, GlobalBufferPool<byte>.pool);

                this.SendPool = sendPool;

                this.SendSocket = null;
                this.RecvSocket = null;

                this.SendThread = null;
                this.RecvThread = null;

                this.BytesSent = 0;
                this.DataSegmentsSent = 0;
                this.ProgressSegmentsSent = 0;
                this.RecordsSent = 0;
                this.RecordsRecv = 0;
                this.sendStatistics = new long[(int)RuntimeStatistic.NUM_STATISTICS];
                this.recvStatistics = new long[(int)RuntimeStatistic.NUM_STATISTICS];

                this.DeferredDisposalSegments = new Dictionary<int, BufferSegment>();
                this.NextSequenceNumber = 0;

                this.SequenceNumberReceived = -1;
                this.SequenceNumberAcknowledged = -1;
                this.SequenceNumberToAcknowledge = -1;

                this.ReceivedCheckpointMessages = 0;
                this.LastCheckpointSequenceNumber = -1;

                this.SendEvent = new AutoResetEvent(false);

                this.WorkerPauseEvent = new AutoResetEvent(false);
                this.RollbackPauseEvent = new AutoResetEvent(false);
                this.RollbackProgressEvent = new AutoResetEvent(false);
                this.RollbackResumeEvent = new AutoResetEvent(false);

                this.sequenceNumber = 1;

                this.EverReceivedIncomingConnection = false;
            }

            public void Dispose()
            {
                if (this.SendThread != null)
                    this.SendThread.Join();
                if (this.RecvThread != null)
                    this.RecvThread.Join();

                Logging.Progress("Shutting down sockets for connection {0}", this.Id);

                
                //Ensure all data on the socket gets delivered
                this.SendSocket.Shutdown(SocketShutdown.Both);
                this.SendSocket.Close(5);

                if (this.RecvSocket != this.SendSocket) {
                    this.RecvSocket.Shutdown(SocketShutdown.Both);
                    this.RecvSocket.Close(5);
                }

                this.WorkerPauseEvent.Dispose();
                this.SendEvent.Dispose();
                this.RollbackPauseEvent.Dispose();
                this.RollbackResumeEvent.Dispose();
            }
        }

        public void StartMessageDelivery()
        {
            this.startCommunicatingEvent.Set();
        }
        
        public int GetSequenceNumber(int destProcessId)
        {

            if (destProcessId == -1)
                return -(Interlocked.Increment(ref this.broadcastSequenceNumber));
            else
            {
                int seqno = Interlocked.Increment(ref this.connections[destProcessId].sequenceNumber);
                //Console.Error.WriteLine("+GetSequenceNumber({0}) returning {1}", destProcessId, seqno);

                return seqno; //Interlocked.Increment(ref this.connections[destProcessId].sequenceNumber);
            }
        }

        public void PrintTrafficMatrix(TextWriter writer)
        {
            for (int i = 0; i < this.connections.Count; ++i)
                if (i == this.localProcessID)
                    writer.WriteLine("{0} ---", i);
                else
                    writer.WriteLine("{0} S = {1}\tR = {2}\tQ = {3}\tState = {4}\tIFS = {5}", i, this.connections[i].RecordsSent, this.connections[i].RecordsRecv, this.connections[i].SegmentQueue.Count, this.connections[i].Status.ToString(), "DEPRECATED");
        }

        public readonly InternalController Controller;

        private readonly bool useBroadcastWakeup;
        private readonly EventCount wakeUpEvent;

        //TOCHECK: config is passed in but inside the method we use this.Controller.Configuration a lot
        internal TcpNetworkChannel(int id, InternalController controller, Configuration config)
        {
            this.id = id;
            this.Controller = controller;

            this.localProcessID = this.Controller.Configuration.ProcessID;

            this.graphmailboxes = new List<List<List<Mailbox>>>();

            this.connections = new List<ConnectionState>();

            this.sendConnectionCountdown = new CountdownEvent(1);
            this.recvConnectionCountdown = new CountdownEvent(1);

            this.shutdownRecvCountdown = new CountdownEvent(1);
            this.shutdownSendCountdown = new CountdownEvent(1);

            this.startupRecvCountdown = new CountdownEvent(1);

            this.startCommunicatingEvent = new ManualResetEvent(false);

            if (controller.Configuration.UseNetworkBroadcastWakeup)
            {
                this.useBroadcastWakeup = true;
                this.wakeUpEvent = new EventCount();
            }
            else
            {
                this.useBroadcastWakeup = false;
                this.wakeUpEvent = null;
            }

            this.broadcastSequenceNumber = 1;

            // UDP broadcast setup.
            if (this.Controller.Configuration.Broadcast == Configuration.BroadcastProtocol.UdpOnly
            || this.Controller.Configuration.Broadcast == Configuration.BroadcastProtocol.TcpUdp)
            {

                this.udpClient = new UdpClient(new IPEndPoint(this.Controller.Configuration.Endpoints[this.Controller.Configuration.ProcessID].Address, this.Controller.Configuration.BroadcastAddress.Port));
                
                IPEndPoint multicastGroupEndpoint = this.Controller.Configuration.BroadcastAddress;
                byte[] addrbytes = multicastGroupEndpoint.Address.GetAddressBytes();

                Logging.Progress("Configuring UDP broadcast channel using address {0}", multicastGroupEndpoint);

                if (this.Controller.Configuration.ProcessID != 0)
                {
                    if ((addrbytes[0] & 0xF0) == 224)
                    {
                        //Console.WriteLine("Multicast!");
                        this.udpClient.JoinMulticastGroup(multicastGroupEndpoint.Address);
                    }
                    else
                    {
                        //Console.WriteLine("Broadcast?");
                    }
                    Thread udpclientThread = new Thread(() => this.UdpReceiveThread(multicastGroupEndpoint));
                    udpclientThread.IsBackground = true;
                    udpclientThread.Start();
                }
                else
                {
                    if ((addrbytes[0] & 0xF0) == 224)
                    {
                        //Console.WriteLine("Multicast!");
                        this.udpClient.Connect(multicastGroupEndpoint);
                    }
                    else
                    {
                        //Console.WriteLine("Broadcast?");
                        this.udpClient.Connect(multicastGroupEndpoint);
                        this.udpClient.EnableBroadcast = true;
                    }
                }
            }
 
            this.sendPageSize = this.Controller.Configuration.SendPageSize;

            for (int i = 0; i < this.Controller.Configuration.Endpoints.Length; ++i)
                if (i != this.Controller.Configuration.ProcessID)
                    this.AddEndPointOutgoing(i, this.Controller.Configuration.Endpoints[i]);
            this.blockReceiveEvent = new ManualResetEventSlim[this.Controller.Configuration.Endpoints.Length];

            this.MAX_SEND_SIZE = 32 * this.sendPageSize;

            this.globalPool = new BoundedBufferPool2<byte>(this.sendPageSize, this.Controller.Configuration.SendPageCount);
        }

        private void UdpReceiveThread(IPEndPoint multicastGroupAddress)
        {
            NaiadTracing.Trace.ThreadName("UdpRecvThread[{0}]", multicastGroupAddress.ToString());
            IPEndPoint from = multicastGroupAddress;
            MessageHeader header = default(MessageHeader);
            //int count = 0;

            this.startCommunicatingEvent.WaitOne();

            while (true)
            {
                byte[] bytes = this.udpClient.Receive(ref from);

                MessageHeader.ReadHeaderFromBuffer(bytes, 0, ref header);
                //Console.Error.WriteLine("UdpReceiveThread: got {0} bytes from {1}. Sequence number = {2}, count = {3}", bytes.Length, from, header.SequenceNumber, count++);

                SerializedMessage message = new SerializedMessage(0, header, new RecvBuffer(bytes, MessageHeader.SizeOf, bytes.Length));
                bool success = this.AttemptDelivery(message, 0);
                Debug.Assert(success);
            }
        }
        
        private void AllocateConnectionState(int processId)
        {
            if (processId == this.localProcessID)
            {
                Logging.Error("Error: cannot add an endpoint for the local process {0}", processId);
                System.Environment.Exit(-1);
            }

            while (processId >= this.connections.Count)
                this.connections.Add(null);

            if (this.connections[processId] == null)
            {
                this.connections[processId] = new ConnectionState(processId, ConnectionStatus.Initialized, 1 << 22,
                    this.Controller.Configuration.SendBufferPolicy == Configuration.SendBufferMode.PerRemoteProcess
                    ? new BoundedBufferPool2<byte>(this.sendPageSize, this.Controller.Configuration.SendPageCount) : null);
            }
        }

        private void AddEndPointOutgoing(int processId, IPEndPoint endPoint)
        {
            lock (this)
            {
                this.AllocateConnectionState(processId);
                if (this.connections[processId].EndPoint != null)
                {
                    Logging.Error("Error: already connected to process {0}", processId);
                    System.Environment.Exit(-1);
                }

                this.sendConnectionCountdown.AddCount(1);
                this.recvConnectionCountdown.AddCount(1);
                this.shutdownSendCountdown.AddCount(1);
                this.shutdownRecvCountdown.AddCount(1);
                this.startupRecvCountdown.AddCount(1);

                this.connections[processId].EndPoint = endPoint;
                this.connections[processId].SendThread = new Thread(() => this.PerProcessSendThread(processId));
#if SEND_HIGH_PRIORITY
                this.connections[processId].SendThread.Priority = ThreadPriority.Highest;
#endif
                this.connections[processId].SendThread.Start();

            }
        }

        private void AddEndPointIncoming(int processId, Socket recvSocket)
        {
            lock (this)
            {
                this.AllocateConnectionState(processId);

                if (this.connections[processId].RecvSocket != null)
                {
                    Logging.Error("WARNING: already accepted a connection from process {0}, so shutting down existing recvThread", processId);

                    this.connections[processId].RecvSocket.Close();
                    this.connections[processId].RecvThread.Join();

                    this.connections[processId].RecvSocket = null;
                    this.connections[processId].RecvThread = null;

                    //
                    //System.Environment.Exit(-1);
                }

                if (!this.connections[processId].EverReceivedIncomingConnection)
                {
                    this.recvConnectionCountdown.Signal();
                    this.connections[processId].EverReceivedIncomingConnection = true;
                }

                this.connections[processId].RecvSocket = recvSocket;
                this.connections[processId].RecvThread = new Thread(() => this.PerProcessRecvThread(processId));
                this.connections[processId].RecvBufferSheaf = new RecvBufferSheaf(processId, (1 << 22) / RecvBufferPage.PAGE_SIZE, GlobalBufferPool<byte>.pool);
#if RECV_HIGH_PRIORITY
                this.connections[processId].RecvThread.Priority = ThreadPriority.Highest;
#endif
                this.connections[processId].RecvThread.Start();
            }
        }

        public void WaitForAllConnections()
        {
            this.sendConnectionCountdown.Signal();
            while (!this.sendConnectionCountdown.Wait(1000))
                ;
            this.recvConnectionCountdown.Signal();
            while (!this.recvConnectionCountdown.Wait(1000))
                ;
        }

        private const int PEER_ID_LENGTH = 4;
        internal void PeerConnect(Socket socket)
        {
            Logging.Info("In PeerConnect");
            byte[] peerIDBuffer = new byte[PEER_ID_LENGTH];
            socket.Receive(peerIDBuffer);
            int peerID = BitConverter.ToInt32(peerIDBuffer, 0);

            Logging.Progress("Accept()ed connection from {0}. Endpoints {1} -> {2}", peerID, socket.RemoteEndPoint, socket.LocalEndPoint);
            this.AddEndPointIncoming(peerID, socket);
        }

        public void RegisterMailbox(Mailbox mailbox)
        {
            while (this.graphmailboxes.Count <= mailbox.GraphId)
                this.graphmailboxes.Add(null);
            if (this.graphmailboxes[mailbox.GraphId] == null)
                this.graphmailboxes[mailbox.GraphId] = new List<List<Mailbox>>();

            var mailboxes = this.graphmailboxes[mailbox.GraphId];
            while (mailboxes.Count <= mailbox.ChannelId)
                mailboxes.Add(null);
            if (mailboxes[mailbox.ChannelId] == null)
                mailboxes[mailbox.ChannelId] = new List<Mailbox>();

            while (mailboxes[mailbox.ChannelId].Count <= mailbox.VertexId)
                mailboxes[mailbox.ChannelId].Add(null);
            mailboxes[mailbox.ChannelId][mailbox.VertexId] = mailbox;
            //Logging.Info("Registered Mailbox {0} Vertex {1}", mailbox.Id, mailbox.VertexID);
        }

        public void BroadcastCheckpoints(int graphId, IEnumerable<CheckpointLowWatermark> frontiers)
        {
            int seqno = this.GetSequenceNumber(-1);

            NaiadSerialization<int> intSerializer = this.Controller.SerializationFormat.GetSerializer<int>();

            int pageSize = 16 * 1024;
            while (true)
            {
                SendBufferPage sendPage = new SendBufferPage(GlobalBufferPool<byte>.pool, pageSize);
                MessageHeader sendHeader = new MessageHeader(-1, seqno, graphId, -1, 0, SerializedMessageType.RestorationFrontier);
                sendPage.WriteHeader(sendHeader, MessageHeader.Serialization);

                bool fits = sendPage.Write<int>(intSerializer, frontiers.Count());

                foreach (CheckpointLowWatermark frontier in frontiers)
                {
                    fits = fits && sendPage.Write<int>(intSerializer, frontier.stageId);
                    fits = fits && sendPage.Write<int>(intSerializer, frontier.vertexId);
                    fits = fits && frontier.frontier.TrySerialize(sendPage, this.Controller.SerializationFormat);
                }

                if (fits)
                {
                    MessageHeader finalHeader = sendPage.FinalizeLastMessage(MessageHeader.Serialization);

                    BufferSegment segment = sendPage.Consume();

                    for (int i = 0; i < this.connections.Count; ++i)
                    {
                        if (i != this.localProcessID)
                        {
                            Logging.Info("Sending checkpoint message to process {0}", i);
                            this.SendBufferSegment(finalHeader, i, segment.DeepCopy());
                        }
                    }

                    return;
                }

                pageSize *= 2;
            }
        }

        public void AnnounceWorkersPaused()
        {
            int seqno = this.GetSequenceNumber(-1);
            SendBufferPage pausedPage = SendBufferPage.CreateSpecialPage(MessageHeader.WorkersPaused, seqno);
            BufferSegment pausedSegment = pausedPage.Consume();

            if (this.Controller.Configuration.CentralizerProcessId != this.localProcessID)
            {
                Console.WriteLine("Sending worker paused message to " + this.Controller.Configuration.CentralizerProcessId);
                Logging.Info("Sending worker paused message to process {0}", this.localProcessID);
                this.SendBufferSegment(pausedPage.CurrentMessageHeader, this.Controller.Configuration.CentralizerProcessId, pausedSegment);
            }
        }

        public void AnnounceRollbackBarrier()
        {
            int seqno = this.GetSequenceNumber(-1);
            SendBufferPage rollbackPage = SendBufferPage.CreateSpecialPage(MessageHeader.RollbackBarrier, seqno);
            BufferSegment rollbackSegment = rollbackPage.Consume();

            // XXX: Hack due to new sequence numbers.

            //for (int i = 0; i < this.connections.Count - 2; ++i)
            //    checkpointSegment.Copy();

            for (int i = 0; i < this.connections.Count; ++i)
            {
                if (i != this.localProcessID)
                {
                    Logging.Info("Sending rollback message to process {0}", i);
                    this.SendBufferSegment(rollbackPage.CurrentMessageHeader, i, rollbackSegment.DeepCopy());
                }
            }
        }

        public void ShowWaitingMessageCounts()
        {
            for (int graphId=0; graphId < this.graphmailboxes.Count; ++graphId)
            {
                var graph = this.graphmailboxes[graphId];
                if (graph != null)
                {
                    for (int channelId = 0; channelId < graph.Count; ++channelId)
                    {
                        var channel = graph[channelId];
                        if (channel != null)
                        {
                            for (int vertexId=0; vertexId<channel.Count; ++vertexId)
                            {
                                var vertex = channel[vertexId];
                                if (vertex != null)
                                {
                                    string counts = vertex.WaitingMessageCount();
                                    if (counts != null)
                                    {
                                        Console.WriteLine("{0}.{1}.{2}:{3}",
                                            graphId, channelId, vertexId, counts);
                                    }
                                }
                            }
                        }
                    }
                }
            }
        }

        public void StartRollback(IEnumerable<int> processes)
        {
            int seqno = this.GetSequenceNumber(-1);
            SendBufferPage restorePage = SendBufferPage.CreateSpecialPage(MessageHeader.PrepareToRestore, seqno);
            BufferSegment restoreSegment = restorePage.Consume();

            foreach (int process in processes)
            {
                if (process != this.localProcessID)
                {
                    Logging.Info("Sending prepare to restore to process {0}", process);
                    this.SendBufferSegment(restorePage.CurrentMessageHeader, process, restoreSegment.DeepCopy());
                }
            }
        }

        public void SignalProgressRepaired(bool fromCoordinator)
        {
            int seqno = this.GetSequenceNumber(-1);
            SendBufferPage restorePage = SendBufferPage.CreateSpecialPage(MessageHeader.RestorationProgressRepaired, seqno);
            BufferSegment restoreSegment = restorePage.Consume();

            for (int i = 0; i < this.connections.Count; ++i)
            {
                if ((fromCoordinator && i != this.localProcessID) ||
                    (!fromCoordinator && i == this.Controller.Configuration.CentralizerProcessId))
                {
                    Logging.Info("Sending rollback repair complete to process {0}", i);
                    this.SendBufferSegment(restorePage.CurrentMessageHeader, i, restoreSegment.DeepCopy());
                }
            }
        }

        public void AnnounceFailure(int failedProcess, int delay, bool fromCoordinator)
        {
            int seqno = this.GetSequenceNumber(-1);
            SendBufferPage failurePage = SendBufferPage.CreateSpecialPage(MessageHeader.AnnounceFailure(failedProcess, delay), seqno);
            BufferSegment failureSegment = failurePage.Consume();

            if (fromCoordinator)
            {
                this.SendBufferSegment(failurePage.CurrentMessageHeader, failedProcess, failureSegment);
            }
            else
            {
                this.SendBufferSegment(failurePage.CurrentMessageHeader, this.Controller.Configuration.CentralizerProcessId, failureSegment);
            }
        }

        public void SimulateFailure(int delay)
        {
            lock (this)
            {
                // this will get incremented by each thread that blocks, and signaled after it wakes up
                this.unblockCountdown = new CountdownEvent(1);

                for (int i = 0; i < this.connections.Count; ++i)
                {
                    if (i != this.localProcessID && i != this.Controller.Configuration.CentralizerProcessId)
                    {
                        // make a new event that the other threads will block on
                        this.blockReceiveEvent[i] = new ManualResetEventSlim(false);
                    }
                }
            }

            // go to sleep for the specified delay time
            this.Controller.SimulateFailure(delay);

            List<ManualResetEventSlim> toDispose = new List<ManualResetEventSlim>();

            lock (this)
            {
                // tell everyone they can wake up again
                for (int i = 0; i < this.connections.Count; ++i)
                {
                    if (i != this.localProcessID && i != this.Controller.Configuration.CentralizerProcessId)
                    {
                        // save this to dispose it after everyone wakes up
                        toDispose.Add(this.blockReceiveEvent[i]);
                        // tell the thread to wake up
                        this.blockReceiveEvent[i].Set();
                        // make sure the thread doesn't block again
                        this.blockReceiveEvent[i] = null;
                    }
                }
            }

            // remove our hold on the countdown
            this.unblockCountdown.Signal();

            // wait until all the other receive threads have woken up
            this.unblockCountdown.Wait();

            this.unblockCountdown.Dispose();
            this.unblockCountdown = null;

            foreach (ManualResetEventSlim ev in toDispose)
            {
                ev.Dispose();
            }
        }

        public void WaitForWorkerPausedMessages(IEnumerable<int> processes)
        {
            foreach (int process in processes)
            {
                if (process != this.localProcessID)
                    this.connections[process].WorkerPauseEvent.WaitOne();
            }
        }

        public void WaitForAllRollbackBarrierMessages()
        {
            // Could replace with a WaitHandle.WaitAll if we make this.connections[localProcessID].CheckpointPauseEvent a
            // ManualResetEvent that is pinned to true.
            for (int i = 0; i < this.connections.Count; ++i)
                if (i != this.localProcessID)
                    this.connections[i].RollbackPauseEvent.WaitOne();
        }

        public void WaitForAllRollbackProgressMessages()
        {
            // Could replace with a WaitHandle.WaitAll if we make this.connections[localProcessID].CheckpointPauseEvent a
            // ManualResetEvent that is pinned to true.
            for (int i = 0; i < this.connections.Count; ++i)
                if (i != this.localProcessID)
                    this.connections[i].RollbackProgressEvent.WaitOne();
        }

        public void ResumeAfterRollback()
        {
            for (int i = 0; i < this.connections.Count; ++i)
                if (i != this.localProcessID)
                    this.connections[i].RollbackResumeEvent.Set();
        }

        private void AnnounceShutdown()
        {
            Logging.Progress("Announcing shutdown");
            int seqno = this.GetSequenceNumber(-1);
            SendBufferPage shutdownPage = SendBufferPage.CreateShutdownMessagePage(seqno);
            BufferSegment shutdownSegment = shutdownPage.Consume();

            // XXX: Broadcast hack due to new sequence numbers.

            //for (int i = 0; i < this.connections.Count - 2; ++i)
            //    shutdownSegment.Copy();

            for (int i = 0; i < this.connections.Count; ++i)
            {
                if (i != this.localProcessID)
                {
                    Logging.Progress("Sending shutdown message to process {0}", i);
                    this.SendBufferSegment(shutdownPage.CurrentMessageHeader, i, shutdownSegment.DeepCopy());
                }
            }
        }


        private void WaitForShutdown()
        {
            this.shutdownSendCountdown.Signal();
            while (!this.shutdownSendCountdown.Wait(1000))
                ;
            this.shutdownRecvCountdown.Signal();
            while (!this.shutdownRecvCountdown.Wait(1000))
                ;
        }


        private void AnnounceStartup(int barrierId)
        {
            int seqno = this.GetSequenceNumber(-1);
            SendBufferPage startupPage = SendBufferPage.CreateSpecialPage(MessageHeader.GenerateBarrierMessageHeader(barrierId), seqno);
            BufferSegment startupSegment = startupPage.Consume();

            // XXX: Hack due to new sequence numbers.

            //for (int i = 0; i < this.connections.Count - 2; ++i)
            //    startupSegment.Copy();

            for (int i = 0; i < this.connections.Count; ++i)
            {
                if (i != this.localProcessID)
                {
                    Logging.Info("Sending startup message to process {0}", i);
                    this.SendBufferSegment(startupPage.CurrentMessageHeader, i, startupSegment.DeepCopy());
                }
            }
        }

        Dictionary<int, CountdownEvent> barrierCounts = new Dictionary<int, CountdownEvent>();
        int currentBarrierId = 0;

        public void DoStartupBarrier()
        {
            Logging.Info("Attempting startup barrier");

            var barrierId = this.currentBarrierId++;

            this.AnnounceStartup(barrierId);
            this.OnRecvBarrierMessageAndBlock(barrierId);
            //this.startupRecvCountdown.Signal();
            //while (!this.startupRecvCountdown.Wait(1000))
            //    ;
        }

        public void OnRecvBarrierMessageAndBlock(int id)
        {
            CountdownEvent countdown = null;

            lock (barrierCounts)
            {
                Logging.Progress("Bumping count for barrier {0}", id);

                if (!barrierCounts.ContainsKey(id))
                {
                    Logging.Progress("Allocating barrier for id {0}, initial value {1}", id, this.Controller.Configuration.Processes);
                    barrierCounts.Add(id, new CountdownEvent(this.Controller.Configuration.Processes));
                }

                countdown = barrierCounts[id];
            }

            countdown.Signal();
            while (!countdown.Wait(1000))
                ;
        }

        public void SendBufferSegment(MessageHeader header, int destProcessID, BufferSegment segment, bool HighPriority=false, bool wakeUp=true)
        {
            if (Controller.Configuration.DontUseHighPriorityQueue)
                HighPriority = false;

            if (HighPriority)
            {
                this.connections[destProcessID].HighPrioritySegmentQueue.Enqueue(segment);
            }
            else
            {
                this.connections[destProcessID].SegmentQueue.Enqueue(segment);
            }
            if (wakeUp)
            {
                this.connections[destProcessID].SendEvent.Set();
            }
        }

        private static SocketError SendAllBytes(Socket dest, ArraySegment<byte> segment)
        {
            SocketError result = SocketError.Success;
            try
            {

                NaiadTracing.Trace.RegionStart(NaiadTracingRegion.Send);
                int bytesToSend = segment.Count;
                int startOffset = segment.Offset;
                do
                {
                    int bytesSent = dest.Send(segment.Array, startOffset, bytesToSend, SocketFlags.None, out result);
                    startOffset += bytesSent;
                    bytesToSend -= bytesSent;
                } while (result == SocketError.Success && bytesToSend != 0);
                NaiadTracing.Trace.RegionStop(NaiadTracingRegion.Send);
                return result;
            }
            catch (Exception e)
            {
                Logging.Error("WARNING: An exception was raised when sending: {0}", e);
                NaiadTracing.Trace.SocketError(result);
                return SocketError.Fault;
            }
        }

        private static SocketError SendAllBytes(Socket dest, byte[] bytes)
        {
            return TcpNetworkChannel.SendAllBytes(dest, new ArraySegment<byte>(bytes));
        }

        private void PerProcessSendThread(int destProcessID)
        {
#if SEND_AFFINITY
            //PinnedThread pin = new PinnedThread(0xC0UL);
            PinnedThread pin = new PinnedThread(destProcessID % 8);
#endif
            NaiadTracing.Trace.ThreadName("SendThread[{0:00}]", destProcessID);
            bool doneAtLeastOnce = false;

        try_connecting_again:

            // Connect to the destination socket.
            while (true) 
            {
                Logging.Info("Connect({0}, ..., {1})", this.connections[destProcessID].EndPoint, destProcessID);

                this.connections[destProcessID].SendSocket = new Socket(this.connections[destProcessID].EndPoint.AddressFamily, SocketType.Stream, ProtocolType.Tcp);
                if (!this.Controller.Configuration.Nagling)
                {
                    this.connections[destProcessID].SendSocket.NoDelay = true;
                }
                
                try
                {
                    this.connections[destProcessID].SendSocket.Connect(this.connections[destProcessID].EndPoint);
                    break;
                }
                catch (SocketException se)
                {
                    if (se.SocketErrorCode == SocketError.TimedOut || se.SocketErrorCode == SocketError.ConnectionRefused)
                    {
                        // Remote process hasn't started yet, so retry in a second.
                        this.connections[destProcessID].SendSocket.Dispose();
                        Thread.Sleep(1000); // FIXME: Better to use a timer if we do lots of these?
                    }
                    else
                    {
                        Logging.Fatal("Fatal error connecting to {0} {1}", this.connections[destProcessID].EndPoint, se.SocketErrorCode);
                        Logging.Fatal(se.Message);
                        System.Environment.Exit(-1);
                    }
                }
            }

            SendAllBytes(this.connections[destProcessID].SendSocket, BitConverter.GetBytes((int)NaiadProtocolOpcode.PeerConnect));
            SendAllBytes(this.connections[destProcessID].SendSocket, BitConverter.GetBytes(this.id));
            SendAllBytes(this.connections[destProcessID].SendSocket, BitConverter.GetBytes(this.localProcessID));

            this.connections[destProcessID].Status = ConnectionStatus.Idle;

            if (!doneAtLeastOnce)
            {
                this.sendConnectionCountdown.Signal(1);
                doneAtLeastOnce = true;
            }

            this.startCommunicatingEvent.WaitOne();
            Socket socket;

            if (this.Controller.Configuration.DuplexSockets)
            {
                if (destProcessID > this.localProcessID)
                    socket = this.connections[destProcessID].SendSocket;
                else
                    socket = this.connections[destProcessID].RecvSocket;
            }
            else
            {
                socket = this.connections[destProcessID].SendSocket;
            }

            if (!this.Controller.Configuration.Nagling)
            {
                socket.NoDelay = true;
            }
            
            if (this.Controller.Configuration.KeepAlives)
            {
                socket.SetSocketOption(SocketOptionLevel.Socket, SocketOptionName.KeepAlive, true);
                Microsoft.Research.Naiad.Utilities.Win32.SetKeepaliveOptions(socket.Handle);
            } 

            long wakeupCount = 0;

            Stopwatch sw = new Stopwatch();
            sw.Start();
            

            KeyValuePair<int, BufferSegment>[] resendSegments;
            lock (this.connections[destProcessID].DeferredDisposalSegments)
            {
                resendSegments = this.connections[destProcessID].DeferredDisposalSegments.OrderBy(x => x.Key).ToArray();
            }

            bool shuttingDown = false;

            for (int i = 0; i < resendSegments.Length; ++i)
            {
                shuttingDown |= (resendSegments[i].Value.Type == SerializedMessageType.Shutdown);
                ArraySegment<byte> messageArraySegment = resendSegments[i].Value.ToArraySegment();

                MessageHeader header = default(MessageHeader);
                MessageHeader.ReadHeaderFromBuffer(messageArraySegment.Array, messageArraySegment.Offset, ref header);
                var channelId = header.ChannelID & 0xFFFF;
                var dest = header.DestVertexID;
                // For tracing purposes, if broadcast then replace dest vertex id=-1 with actual dest process id.
                // This assumes that broadcast is only being used by progress messages, where there is a single
                // vertex on each machine located on worker 0.
                if (dest == -1)
                {
                    dest = destProcessID;
                }
                NaiadTracing.Trace.MsgSend(channelId, header.SequenceNumber, header.Length, header.FromVertexID, dest);

                SocketError errorCode = SendAllBytes(socket, messageArraySegment);
                if (errorCode != SocketError.Success)
                {
                    NaiadTracing.Trace.SocketError(errorCode);

                    Logging.Error("WARNING: Send thread got error from peer {0}: {1}", destProcessID, errorCode);
                    socket.Close();
                    goto try_connecting_again;

                    //this.HandleSocketError(destProcessID, errorCode);
                }
            }

            while (true)
            {
                BufferSegment seg;
                int length = 0;
                
                while (this.connections[destProcessID].HighPrioritySegmentQueue.TryDequeue(out seg))
                {
                    Debug.Assert(seg.Length > 0); 
                    
                    length += seg.Length;
                    shuttingDown |= (seg.Type == SerializedMessageType.Shutdown);

                    // Rewrite the message header to use contiguous per-connection sequence numbers.
                    ArraySegment<byte> messageArraySegment = seg.ToArraySegment();
                    MessageHeader header = default(MessageHeader);
                    MessageHeader.ReadHeaderFromBuffer(messageArraySegment.Array, messageArraySegment.Offset, ref header);
                    header.SequenceNumber = this.connections[destProcessID].NextSequenceNumber++;
                    MessageHeader.WriteHeaderToBuffer(messageArraySegment.Array, messageArraySegment.Offset, header);

                    //Logging.Error("Sending message to {0}: sequence number {1}, type {2}", destProcessID, header.SequenceNumber, header.Type);

                    this.DeferDisposalUntilAcknowledged(destProcessID, header.SequenceNumber, seg);

                    var channelId = header.ChannelID & 0xFFFF;
                    var dest = header.DestVertexID;
                    // For tracing purposes, if broadcast then replace dest vertex id=-1 with actual dest process id.
                    // This assumes that broadcast is only being used by progress messages, where there is a single
                    // vertex on each machine located on worker 0.
                    if (dest == -1)
                    {
                        dest = destProcessID;
                    }
                    NaiadTracing.Trace.MsgSend(channelId, header.SequenceNumber, header.Length, header.FromVertexID, dest);
                    
                    SocketError errorCode = SendAllBytes(socket, seg.ToArraySegment());
                    if (errorCode != SocketError.Success)
                    {
                        NaiadTracing.Trace.SocketError(errorCode);

                        Logging.Error("Send thread got error from peer {0}: {1}", destProcessID, errorCode);
                        socket.Close();
                        goto try_connecting_again;
                        
                        //this.HandleSocketError(destProcessID, errorCode);
                    }

                    this.connections[destProcessID].ProgressSegmentsSent += 1;
                    this.connections[destProcessID].sendStatistics[(int)RuntimeStatistic.TxHighPriorityMessages] += 1;
                    this.connections[destProcessID].sendStatistics[(int)RuntimeStatistic.TxHighPriorityBytes] += seg.Length;
                    
                    
                }

                while (this.connections[destProcessID].SegmentQueue.TryDequeue(out seg))
                {
                    Debug.Assert(seg.Length > 0);

                    length += seg.Length;
                    shuttingDown |= (seg.Type == SerializedMessageType.Shutdown);

                    // Rewrite the message header to use contiguous per-connection sequence numbers.
                    ArraySegment<byte> messageArraySegment = seg.ToArraySegment();
                    MessageHeader header = default(MessageHeader);
                    MessageHeader.ReadHeaderFromBuffer(messageArraySegment.Array, messageArraySegment.Offset, ref header);
                    header.SequenceNumber = this.connections[destProcessID].NextSequenceNumber++;
                    MessageHeader.WriteHeaderToBuffer(messageArraySegment.Array, messageArraySegment.Offset, header);

                    //Logging.Error("Sending message to {0}: sequence number {1}, type {2}", destProcessID, header.SequenceNumber, header.Type);

                    this.DeferDisposalUntilAcknowledged(destProcessID, header.SequenceNumber, seg);

                    var channelId = header.ChannelID & 0xFFFF;
                    var dest = header.DestVertexID;
                    // For tracing purposes, if broadcast then replace dest vertex id=-1 with actual dest process id.
                    // This assumes that broadcast is only being used by progress messages, where there is a single
                    // vertex on each machine located on worker 0.
                    if (dest == -1)
                    {
                        dest = destProcessID;
                    }
                    NaiadTracing.Trace.MsgSend(channelId, header.SequenceNumber, header.Length, header.FromVertexID, dest);

                    SocketError errorCode = SendAllBytes(socket, seg.ToArraySegment());
                    if (errorCode != SocketError.Success)
                    {
                        NaiadTracing.Trace.SocketError(errorCode);

                        Logging.Error("WARNING: Send thread got error from peer {0}: {1}", destProcessID, errorCode);
                        socket.Close();
                        goto try_connecting_again;

                        //this.HandleSocketError(destProcessID, errorCode);
                    }

                    this.connections[destProcessID].DataSegmentsSent += 1;
                    this.connections[destProcessID].sendStatistics[(int)RuntimeStatistic.TxNormalPriorityMessages] += 1;
                    this.connections[destProcessID].sendStatistics[(int)RuntimeStatistic.TxNormalPriorityBytes] += seg.Length;
                }

                if (shuttingDown)
                    break;
                if (length == 0)
                {
                    int seqNumToAck = this.connections[destProcessID].SequenceNumberToAcknowledge;
                    if (seqNumToAck > this.connections[destProcessID].SequenceNumberAcknowledged)
                    {
                        MessageHeader ackHeader = new MessageHeader(-1, seqNumToAck, -1, -1, 0, SerializedMessageType.Ack);
                        byte[] ackBuffer = new byte[MessageHeader.SizeOf];
                        MessageHeader.WriteHeaderToBuffer(ackBuffer, 0, ackHeader);

                        SocketError errorCode = SendAllBytes(socket, ackBuffer);
                        if (errorCode != SocketError.Success)
                        {
                            NaiadTracing.Trace.SocketError(errorCode);

                            Logging.Error("WARNING: Send thread got error from peer {0}: {1}", destProcessID, errorCode);
                            socket.Close();
                            goto try_connecting_again;

                            //this.HandleSocketError(destProcessID, errorCode);
                        }

                        this.connections[destProcessID].SequenceNumberAcknowledged = seqNumToAck;
                    }


                    if (this.useBroadcastWakeup)
                    {
                        this.wakeUpEvent.Await(this.connections[destProcessID].SendEvent, wakeupCount + 1);
                        wakeupCount = this.wakeUpEvent.Read();
                    }
                    else
                    {
                        this.connections[destProcessID].SendEvent.WaitOne();
                    }
                    continue;
                }

                //this.connections[destProcessID].Status = shuttingDown ? ConnectionStatus.ShuttingDown : ConnectionStatus.Sending;

                this.connections[destProcessID].BytesSent += length; // Progress + Data
                
                //Logging.Progress("Sent {0} bytes to {1} (of {2})", bytesSent, destProcessID, length);
            }

            this.shutdownSendCountdown.Signal();
#if SEND_AFFINITY
            pin.Dispose();
#endif
        }

        private ManualResetEventSlim[] blockReceiveEvent;
        private CountdownEvent unblockCountdown;

#if SYNC_RECV
        private void PerProcessRecvThread(int srcProcessID)
        {
#if RECV_AFFINITY
            PinnedThread pin = new PinnedThread(srcProcessID % 8);
#endif
            NaiadTracing.Trace.ThreadName("RecvThread[{0:00}]", srcProcessID);
            Logging.Info("Initializing per-process recv thread for {0}", srcProcessID);

            this.startCommunicatingEvent.WaitOne();

            Logging.Info("Starting per-process recv thread for {0}", srcProcessID);

            Socket socket;

            if (this.Controller.Configuration.DuplexSockets)
            {
                if (srcProcessID < this.localProcessID)
                    socket = this.connections[srcProcessID].RecvSocket;
                else
                    socket = this.connections[srcProcessID].SendSocket;
            }
            else
            {
                socket = this.connections[srcProcessID].RecvSocket;
            }

            if (!this.Controller.Configuration.Nagling)
            {
                socket.NoDelay = true;
            }

            long numRecvs = 0;


            int nextConnectionSequenceNumber = 0;

            long recvBytesIn = 0;
            long recvBytesOut = 0;

            while (true)
            {
                SocketError errorCode;


                ArraySegment<byte> recvSegment = this.connections[srcProcessID].RecvBufferSheaf.GetFreeSegment();

                // Keep track of size of buffers passed to recv
                
                recvBytesIn += recvSegment.Count;

                int bytesRecvd = 0;
                try
                {
                    bytesRecvd = socket.Receive(recvSegment.Array, recvSegment.Offset, recvSegment.Count,
                        SocketFlags.None, out errorCode);
                }
                catch (Exception e)
                {
                    Logging.Error("WARNING: Got exception while receiving from {0}: {1}", srcProcessID, e);
                    errorCode = SocketError.Fault;
                }

                if (errorCode != SocketError.Success)
                {
                    //Tracing.Trace("*Socket Error {0}", errorCode);
                    Logging.Error("WARNING: Receive thread got socket error from peer {0}: {1}", srcProcessID, errorCode);
                    socket.Close();

                    lock (this)
                    {
                        this.connections[srcProcessID].RecvThread = null;
                        this.connections[srcProcessID].RecvSocket = null;
                        this.connections[srcProcessID].RecvBufferSheaf = null;
                    }

                    return;

                    //this.HandleSocketError(srcProcessID, errorCode);
                }

                // If the remote host shuts down the Socket connection with the Shutdown method,
                // and all available data has been received, the Receive method will complete 
                // immediately and return zero bytes.
                if (bytesRecvd == 0)
                {
                    Logging.Error("WARNING: Receive thread received no bytes from peer {0}", srcProcessID);
                    socket.Close();

                    lock (this)
                    {
                        this.connections[srcProcessID].RecvThread = null;
                        this.connections[srcProcessID].RecvSocket = null;
                        this.connections[srcProcessID].RecvBufferSheaf = null;
                    }

                    return;


                    //Debug.Assert(false);
                    //Logging.Info("Shutting down receive thread due to lack of data - believed to be impossible.");

                    //this.HandleSocketError(srcProcessID, errorCode);
                    //return;
                }

                this.connections[srcProcessID].RecvBufferSheaf.OnBytesProduced(bytesRecvd);

                recvBytesOut += bytesRecvd;
                numRecvs++;

                ManualResetEventSlim blocking = null;
                // when we are faking a failure, we block all the receive threads for a while
                lock (this)
                {
                    if (this.blockReceiveEvent[srcProcessID] != null)
                    {
                        blocking = this.blockReceiveEvent[srcProcessID];
                        this.unblockCountdown.AddCount(1);
                    }
                }

                if (blocking != null)
                {
                    blocking.Wait();
                    this.unblockCountdown.Signal();
                }

                //Logging.Progress("Received {0} bytes from {1}", bytesRecvd, srcProcessID);

                foreach (SerializedMessage message in this.connections[srcProcessID].RecvBufferSheaf.ConsumeMessages())
                {
                    if (message.Header.SequenceNumber != this.connections[srcProcessID].SequenceNumberReceived + 1 && message.Header.Type != SerializedMessageType.Ack)
                    {
                        Logging.Error("Dropping duplicated received message from {0}: sequence number {1}, type {2}", srcProcessID, message.Header.SequenceNumber, message.Header.Type);
                        message.Dispose();
                        continue;
                    }
                    else
                    {
                        //Logging.Error("Receiving message from {0}: sequence number {1}, type {2}", srcProcessID, message.Header.SequenceNumber, message.Header.Type);
                    }

                    message.ConnectionSequenceNumber = nextConnectionSequenceNumber++;

                    this.connections[srcProcessID].recvStatistics[(int)RuntimeStatistic.RxNetMessages] += 1;
                    this.connections[srcProcessID].recvStatistics[(int)RuntimeStatistic.RxNetBytes] += message.Header.Length;

                    //Console.WriteLine("Received {1} message: {0}", message.Header.SequenceNumber, message.Type);

                    long totalTicks;
                    long totalMicroSeconds;

                    switch (message.Type)
                    {
                        case SerializedMessageType.Startup:
                            Logging.Progress("Received startup message from {0}", srcProcessID);
                            this.OnRecvBarrierMessageAndBlock(message.Header.ChannelID);    // we put the barrier id in here
                            break;
                        case SerializedMessageType.Failure:
                            Logging.Error("Received graph failure message from {0}", srcProcessID);
                            this.Controller.GetInternalComputation(message.Header.ChannelID).Cancel(new Exception(string.Format("Received graph failure message from {0}", srcProcessID)));
                            break;
                        case SerializedMessageType.Shutdown:
                            Logging.Progress("Received shutdown message from {0}", srcProcessID);
                            Logging.Info("PerProcessRecvThread[{0}]: numRecvs {1} avgBytesIn {2} avgBytesOut {3}", srcProcessID, numRecvs, recvBytesIn / numRecvs, recvBytesOut / numRecvs);
                            this.shutdownRecvCountdown.Signal();
                            return;
                        case SerializedMessageType.RollbackBarrier:
                            Logging.Progress("Got checkpoint message from process {0}", srcProcessID);
                            totalTicks = this.Controller.Stopwatch.ElapsedTicks;
                            totalMicroSeconds = (totalTicks * 1000000L) / System.Diagnostics.Stopwatch.Frequency;
                            this.Controller.WriteLog(String.Format("{0:D3}.{1:D3} RBE {2:D11}",
                                this.Controller.Configuration.ProcessID, srcProcessID, totalMicroSeconds));
                            this.connections[srcProcessID].ReceivedCheckpointMessages++;
                            this.connections[srcProcessID].LastCheckpointSequenceNumber = message.ConnectionSequenceNumber;
                            this.connections[srcProcessID].RollbackPauseEvent.Set();

                            if (srcProcessID != this.Controller.Configuration.CentralizerProcessId &&
                                this.localProcessID != this.Controller.Configuration.CentralizerProcessId)
                            {
                                // The coordinator still needs to communicate with all the other processes for
                                // one more round, but every thread that isn't part of that round pauses now
                                // until the rollback is ready to accept external messages again
                                this.Controller.WriteLog(String.Format("{0:D3}.{1:D3} RBW {2:D11}",
                                    this.Controller.Configuration.ProcessID, srcProcessID, totalMicroSeconds));
                                Logging.Progress("Pausing recieve thread for process {0} because of {1}", srcProcessID, message.Type);
                                this.connections[srcProcessID].RollbackResumeEvent.WaitOne();
                                Logging.Progress("Resuming receive thread for process {0} after checkpoint", srcProcessID);
                            }

                            break;
                        case SerializedMessageType.Data:
                            bool success = this.AttemptDelivery(message, srcProcessID);
                            Debug.Assert(success);
                            break;
                        case SerializedMessageType.Ack:
                            this.HandleAcknowledgement(srcProcessID, message.Header.SequenceNumber);
                            break;
                        case SerializedMessageType.PrepareToRestore:
                            totalTicks = this.Controller.Stopwatch.ElapsedTicks;
                            totalMicroSeconds = (totalTicks * 1000000L) / System.Diagnostics.Stopwatch.Frequency;
                            this.Controller.WriteLog(String.Format("{0:D3}.{1:D3} PTR {2:D11}",
                                this.Controller.Configuration.ProcessID, srcProcessID, totalMicroSeconds));
                            this.Controller.SignalRestore();
                            break;
                        case SerializedMessageType.WorkersPaused:
                            totalTicks = this.Controller.Stopwatch.ElapsedTicks;
                            totalMicroSeconds = (totalTicks * 1000000L) / System.Diagnostics.Stopwatch.Frequency;
                            this.Controller.WriteLog(String.Format("{0:D3}.{1:D3} WPR {2:D11}",
                                this.Controller.Configuration.ProcessID, srcProcessID, totalMicroSeconds));
                            Logging.Progress("Got worker pause message from process {0}", srcProcessID);
                            Console.WriteLine("Got worker paused message from " + srcProcessID);
                            this.connections[srcProcessID].WorkerPauseEvent.Set();
                            break;
                        case SerializedMessageType.RestorationFrontier:
                            totalTicks = this.Controller.Stopwatch.ElapsedTicks;
                            totalMicroSeconds = (totalTicks * 1000000L) / System.Diagnostics.Stopwatch.Frequency;
                            this.Controller.WriteLog(String.Format("{0:D3}.{1:D3} RFR {2:D11}",
                                this.Controller.Configuration.ProcessID, srcProcessID, totalMicroSeconds));
                            if (srcProcessID != this.Controller.Configuration.CentralizerProcessId)
                            {
                                throw new ApplicationException("Non-Coordinator should not send frontiers");
                            }
                            this.ReceiveCheckpoints(message);

                            totalTicks = this.Controller.Stopwatch.ElapsedTicks;
                            totalMicroSeconds = (totalTicks * 1000000L) / System.Diagnostics.Stopwatch.Frequency;
                            this.Controller.WriteLog(String.Format("{0:D3}.{1:D3} SPR {2:D11}",
                                this.Controller.Configuration.ProcessID, srcProcessID, totalMicroSeconds));

                            // tell the coordinator we aren't going to send any more progress traffic
                            this.SignalProgressRepaired(false);

                            // Keep waiting while the progress traffic propagates, before we pause the receive thread
                            break;
                        case SerializedMessageType.RestorationProgressRepaired:
                            if (srcProcessID != this.Controller.Configuration.CentralizerProcessId &&
                                this.localProcessID != this.Controller.Configuration.CentralizerProcessId)
                            {
                                throw new ApplicationException("Got pause message from process " + srcProcessID);
                            }

                            totalTicks = this.Controller.Stopwatch.ElapsedTicks;
                            totalMicroSeconds = (totalTicks * 1000000L) / System.Diagnostics.Stopwatch.Frequency;
                            this.Controller.WriteLog(String.Format("{0:D3}.{1:D3} RPR {2:D11}",
                                this.Controller.Configuration.ProcessID, srcProcessID, totalMicroSeconds));

                            if (srcProcessID == this.Controller.Configuration.CentralizerProcessId)
                            {
                                // wake up the restoration thread now that progress has been repaired
                                this.Controller.SignalRestore();
                            }
                            else
                            {
                                // tell the coordinator thread that this peer has sent all its progress traffic
                                this.connections[srcProcessID].RollbackProgressEvent.Set();
                            }

                            // Progress traffic is completed on this channel now
                            Logging.Progress("Pausing receive thread for process {0} because of {1}", srcProcessID, message.Type);
                            this.connections[srcProcessID].RollbackResumeEvent.WaitOne();
                            Logging.Progress("Resuming receive thread for process {0} after checkpoint", srcProcessID);
                            break;
                        case SerializedMessageType.AnnounceFailure:

                            totalTicks = this.Controller.Stopwatch.ElapsedTicks;
                            totalMicroSeconds = (totalTicks * 1000000L) / System.Diagnostics.Stopwatch.Frequency;
                            this.Controller.WriteLog(String.Format("{0:D3}.{1:D3} AFR {2:D11}",
                                this.Controller.Configuration.ProcessID, srcProcessID, totalMicroSeconds));

                                int simulatedFailureTime = message.Header.ChannelID;
                            if (this.localProcessID == this.Controller.Configuration.CentralizerProcessId)
                            {
                                // tell the computations the process has woken up again
                                this.Controller.ReportSimulatedFailureRestart(srcProcessID);
                            }
                            else
                            {
                                // this stops all the workers and blocks all the other receive threads for the specified time
                                // (and blocks us in the call) then wakes up the workers and receive threads, but leaves the
                                // workers in the restoring state so they won't actually do anything
                                this.SimulateFailure(simulatedFailureTime);
                                // tell the coordinator we have woken up again; at this point it will trigger a rollback
                                this.AnnounceFailure(message.Header.FromVertexID, -1, false);
                            }
                            break;
                        default:
                            Logging.Progress("Received BAD msg type {0} from process {1}! ", message.Type, srcProcessID);
                            Debug.Assert(false);
                            break;
                    }

                    if (message.Header.Type != SerializedMessageType.Ack)
                    {
                        this.connections[srcProcessID].SequenceNumberReceived = message.Header.SequenceNumber;
                    }
                }

                if (this.connections[srcProcessID].SequenceNumberReceived >= 0)
                    this.AcknowledgeMessage(srcProcessID, this.connections[srcProcessID].SequenceNumberReceived);
            }
#if RECV_AFFINITY
            pin.Dispose();
#endif
        }
#endif

        private void ReceiveCheckpoints(SerializedMessage message)
        {
            NaiadSerialization<int> intSerializer = this.Controller.SerializationFormat.GetSerializer<int>();

            RecvBuffer body = message.Body;

            int graphId = message.Header.ChannelID;

            int numberOfFrontiers;
            if (!intSerializer.TryDeserialize(ref body, out numberOfFrontiers))
            {
                throw new ApplicationException("Bad restoration frontier message");
            }

            List<CheckpointLowWatermark> frontiers = new List<CheckpointLowWatermark>();

            for (int i=0; i<numberOfFrontiers; ++i)
            {
                int stageId;
                if (!intSerializer.TryDeserialize(ref body, out stageId))
                {
                    throw new ApplicationException("Bad restoration frontier message");
                }

                int vertexId;
                if (!intSerializer.TryDeserialize(ref body, out vertexId))
                {
                    throw new ApplicationException("Bad restoration frontier message");
                }

                FTFrontier frontier = new FTFrontier(false);
                if (!frontier.TryDeserialize(ref body, this.Controller.SerializationFormat))
                {
                    throw new ApplicationException("Bad restoration frontier message");
                }

                frontiers.Add(new CheckpointLowWatermark
                    {
                        stageId = stageId,
                        vertexId = vertexId,
                        frontier = frontier,
                        dstStageId = -2,
                        dstVertexId = -2
                    });
            }

            this.Controller.GetInternalComputation(graphId).ReceiveCheckpointFrontiersAndRepairProgress(frontiers);
        }

        private void HandleSocketError(int peerID, SocketError errorCode)
        {
            switch (errorCode)
            {
                default:
                    Logging.Fatal("Got socket error from peer {0}: {1} {2}\nDying...", peerID, (int)errorCode, errorCode.ToString());
                    Logging.Fatal(new SocketException((int)errorCode).ToString());
                    Logging.Stop();
                    //Debugger.Break();
                    Thread.Sleep(1000); // Wait a bit before causing all network connections to abort!
                    System.Environment.Exit((int)errorCode);
                    break;
            }
        }

        private bool AttemptDelivery(SerializedMessage message, int peerID)
        {
            int graphId = message.Header.ChannelID >> 16;
            int channelId = message.Header.ChannelID & 0xFFFF;                    

            if (message.Header.DestVertexID == -1)
            {
                if (message.Header.ChannelID < 0 || this.localProcessID < 0)    // debug check
                    throw new Exception("This shouldn't happen");

                // Special-cased logic for the progress channel, where we know that each process uses its process ID as the vertex ID.
                NaiadTracing.Trace.MsgRecv(channelId, message.Header.SequenceNumber, message.Header.Length, message.Header.FromVertexID, this.localProcessID);
                try
                {
                    Mailbox receiver = this.graphmailboxes[graphId][channelId][this.localProcessID];
                    receiver.DeliverSerializedMessage(message, new ReturnAddress(receiver.SenderStageId, peerID, message.Header.FromVertexID, -1));
                }
                catch (Exception)
                {
                    Console.Error.WriteLine("AttemptDelivery of progress message on ChannelId={0}, localProcessID={1}",
                        message.Header.ChannelID, this.localProcessID);
                    Console.Error.WriteLine("{0} mailboxes currently exist", "some");//this.mailboxes.Count);
                    System.Environment.Exit(-1);
                }
                return true;
            }
            else if (graphId >= this.graphmailboxes.Count ||
                this.graphmailboxes[graphId] == null ||
                channelId >= this.graphmailboxes[graphId].Count ||
                this.graphmailboxes[graphId][channelId] == null ||
                message.Header.DestVertexID >= this.graphmailboxes[graphId][channelId].Count ||
                this.graphmailboxes[graphId][channelId][message.Header.DestVertexID] == null)
            {
                Console.Error.WriteLine("Graphs: {0}/{1}", graphId, this.graphmailboxes.Count);
                throw new InvalidOperationException(String.Format("Failed delivery attempt"));

#if false
                to {0}:{1} (#channels = {2}, #vertices = {3}) from {4}",
                                        message.Header.ChannelID, message.Header.DestVertexID, this.mailboxes.Count,
                                        this.mailboxes.Count > message.Header.ChannelID
                                            ? this.mailboxes[message.Header.ChannelID].Count.ToString()
                                            : "NaN", peerID));
#endif
            }
            else
            {
                NaiadTracing.Trace.MsgRecv(channelId, message.Header.SequenceNumber, message.Header.Length, message.Header.FromVertexID, message.Header.DestVertexID);
                Mailbox receiver = this.graphmailboxes[graphId][channelId][message.Header.DestVertexID];
                receiver.DeliverSerializedMessage(message, new ReturnAddress(receiver.SenderStageId, peerID, message.Header.FromVertexID, -1));
                return true;
            }
        }

        private readonly BoundedBufferPool2<byte> globalPool;

        public BufferPool<byte> GetBufferPool(int processID, int workerID)
        {
            switch (this.Controller.Configuration.SendBufferPolicy)
        {
                case Configuration.SendBufferMode.Global:
                    return globalPool;
                case Configuration.SendBufferMode.PerRemoteProcess:
                    return (processID == -1  || processID == this.localProcessID) ? GlobalBufferPool<byte>.pool : this.connections[processID].SendPool;
                case Configuration.SendBufferMode.PerWorker:
                    return (workerID == -1) ? GlobalBufferPool<byte>.pool : this.Controller.Workers[workerID].SendPool;
                default:
                    Debug.Assert(false);
                    return null;
            }

        }

        public void Dispose()
        {
            this.AnnounceShutdown();
            this.WaitForShutdown();
#if !SYNC_SEND
            this.sendLoopThread.Join();
#endif
            Logging.Progress("Shutdown complete - disposing connections");

            for (int i = 0; i < this.connections.Count; ++i)
            {
                if (this.connections[i] != null)
                {
                    this.connections[i].Dispose();
                }
                }

            this.shutdownSendCountdown.Dispose();
            this.shutdownRecvCountdown.Dispose();
            this.recvConnectionCountdown.Dispose();
            this.sendConnectionCountdown.Dispose();

            this.startCommunicatingEvent.Dispose();

            //Logging.Progress("[NetChan {1}] Total network bytes sent = {0}", this.connections.Sum(x => x.SentBytes), this.Id);
        }

        public long QueryStatistic(RuntimeStatistic s)
        {
            long res = 0;
            for (int i = 0; i < this.connections.Count; i++)
            {
                if (this.connections[i] == null)
                    continue;

                // could be racy if we're still sending/receiving stuff
                res += this.connections[i].recvStatistics[(int)s];
                res += this.connections[i].sendStatistics[(int)s];
            }
            return res;
        }


        public int BroadcastBufferSegment(MessageHeader header, BufferSegment segment)
        {
            var nmsgs = 0;
            if (segment.Length > 0)
            {

                if (this.Controller.Configuration.Broadcast == Configuration.BroadcastProtocol.UdpOnly || this.Controller.Configuration.Broadcast == Configuration.BroadcastProtocol.TcpUdp)
                {
                    ArraySegment<byte> array = segment.ToArraySegment();
                    Debug.Assert(array.Offset == 0);
                    NaiadTracing.Trace.RegionStart(NaiadTracingRegion.BroadcastUDP);
                    this.udpClient.Send(array.Array, array.Count);
                    NaiadTracing.Trace.RegionStop(NaiadTracingRegion.BroadcastUDP);
                    nmsgs++;
                }

                if (this.Controller.Configuration.Broadcast == Configuration.BroadcastProtocol.TcpOnly || this.Controller.Configuration.Broadcast == Configuration.BroadcastProtocol.TcpUdp)
                {
                    NaiadTracing.Trace.RegionStart(NaiadTracingRegion.BroadcastTCP);
                    for (int i = 0; i < this.connections.Count; ++i)
                        if (i != this.localProcessID)
                        {
                            // Increment refcount for each destination process.
                            //segment.Copy();
                            this.SendBufferSegment(header, i, segment.DeepCopy(), true, !this.useBroadcastWakeup);
                            nmsgs++;
                        }
                    if (this.useBroadcastWakeup)
                        this.wakeUpEvent.Advance();
                    NaiadTracing.Trace.RegionStop(NaiadTracingRegion.BroadcastTCP);
                }
            }
            // Decrement refcount for the initial call to Consume().
            segment.Dispose();
            return nmsgs;
        }


        private Dictionary<int, Dictionary<int, BufferSegment>> deferredDisposalSegments = new Dictionary<int, Dictionary<int, BufferSegment>>(); 

        private void DeferDisposalUntilAcknowledged(int destProcessId, int sequenceNumber, BufferSegment segment)
        {
            lock (this.connections[destProcessId].DeferredDisposalSegments)
            {
                this.connections[destProcessId].DeferredDisposalSegments.Add(sequenceNumber, segment);
            }
        }

        private void AcknowledgeMessage(int fromProcessId, int sequenceNumber)
        {
            this.connections[fromProcessId].SequenceNumberToAcknowledge = sequenceNumber;
            this.connections[fromProcessId].SendEvent.Set();
        }

        private void HandleAcknowledgement(int fromProcessId, int sequenceNumber)
        {
            //Console.WriteLine("Handling ack for {0}", sequenceNumber);
            lock (this.connections[fromProcessId].DeferredDisposalSegments)
            {
                var deferredSegments = this.connections[fromProcessId].DeferredDisposalSegments;

                foreach (var x in deferredSegments.ToArray())
                {
                    if (x.Key <= sequenceNumber)
                    {
                        deferredSegments.Remove(x.Key);
                        x.Value.Dispose();
                    }
                }
            }
        }

    }

}
