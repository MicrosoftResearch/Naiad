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
using System.Net.Sockets;
using Naiad.Dataflow.Channels;
using Naiad.CodeGeneration;
using System.Diagnostics;
using System.Collections.Concurrent;
using System.Threading;
using System.Net;
using System.Collections.Specialized;
using System.ComponentModel;
using Naiad.DataStructures;
using Naiad.Scheduling;
using Naiad.Runtime.Controlling;
using System.IO;

namespace Naiad
{
    internal enum RemotingProtocol
    {
        OPEN_EPOCH_CHANNEL_ID = 1,
        CLOSE_EPOCH_CHANNEL_ID = 2,
        DATA_CHANNEL_ID = 3,
        CLOSE_CONNECTION_CHANNEL_ID = 4,
        SHUTDOWN_COMPUTATION_CHANNEL_ID = 5
    }

    internal static class RemotingHelpers
    {
        public static string RecvCollectionName(Socket socket)
        {
            byte[] collectionNameLengthBuffer = new byte[sizeof(int)];
            int n = socket.Receive(collectionNameLengthBuffer);
            Debug.Assert(n == sizeof(int));
            int collectionNameLength = BitConverter.ToInt32(collectionNameLengthBuffer, 0);
            byte[] collectionNameBuffer = new byte[collectionNameLength];
            n = socket.Receive(collectionNameBuffer);
            Debug.Assert(n == collectionNameLength);
            return new UTF8Encoding().GetString(collectionNameBuffer);
        }

        public static void SendCollectionName(Socket socket, string collectionName)
        {
            byte[] collectionNameBytes = new UTF8Encoding().GetBytes(collectionName);
            socket.Send(BitConverter.GetBytes(collectionNameBytes.Length));
            socket.Send(collectionNameBytes);
        }
    }

    /// <summary>
    /// A set of callback methods that should be implemented to receive a stream that follows the
    /// Naiad remote data protocol.
    /// 
    /// The sequence of calls will be as follows:
    /// 
    /// ((StartEpoch (Send*) EndEpoch)* Close)* (StartEpoch (Send*) EndEpoch)? Shutdown
    /// </summary>
    /// <typeparam name="T">The type of records in this data stream.</typeparam>
    public interface NaiadDataStreamProtocol<T>
    {
        /// <summary>
        /// Called before sending a batch of records in the same epoch.
        /// </summary>
        void StartEpoch();

        /// <summary>
        /// Called for each record in an epoch.
        /// </summary>
        /// <param name="record">The record being sent.</param>
        void Send(T record);

        /// <summary>
        /// Called after sending a batch of records in the same epoch.
        /// </summary>
        void EndEpoch();

        /// <summary>
        /// Called at the end of a remote Naiad session.
        /// </summary>
        void Close();

        /// <summary>
        /// Called at the end of a remote Naiad session, and the final message sent to this object.
        /// </summary>
        void Shutdown();
    }

    /// <summary>
    /// Represents a data stream protocol handler that will broadcast data to a group of zero or more Sockets.
    /// </summary>
    public interface DataStreamBroadcaster
    {
        /// <summary>
        /// Adds the given socket to the broadcast group.
        /// </summary>
        /// <param name="s">The socket to be added to the broadcast group.</param>
        void Add(Socket s);
    }
      
    /// <summary>
    /// A data stream protocol handler that will broadcast data of a particular type to a group of zero or more Sockets or other protocol handlers.
    /// </summary>
    /// <typeparam name="T">The type of records in this data stream.</typeparam>
    public class DataStreamBroadcaster<T> : DataStreamBroadcaster, NaiadDataStreamProtocol<T>
        where T : IEquatable<T>
    {
        private readonly ConcurrentBag<NaiadDataStreamProtocol<T>> senders;
        private List<NaiadDataStreamProtocol<T>> currentSnapshot;

        /// <summary>
        /// Constructs a new instance with an initially empty broadcast group.
        /// </summary>
        public DataStreamBroadcaster()
        {
            this.senders = new ConcurrentBag<NaiadDataStreamProtocol<T>>();
            this.currentSnapshot = new List<NaiadDataStreamProtocol<T>>(1);
        }

        /// <summary>
        /// Adds the given data stream handler to the broadcast group.
        /// </summary>
        /// <param name="handler">The handler to be added to the broadcast group.</param>
        public void Add(NaiadDataStreamProtocol<T> handler)
        {
            this.senders.Add(handler);
        }

        /// <summary>
        /// Adds the given socket to the broadcast group.
        /// </summary>
        /// <param name="s">The socket to be added to the broadcast group.</param>
        public void Add(Socket s)
        {
            this.senders.Add(new DataStreamSocketSender<T>(s));
        } 

        public void StartEpoch()
        {
            NaiadDataStreamProtocol<T> newSender;
            while (this.senders.TryTake(out newSender))
                this.currentSnapshot.Add(newSender);
        }

        public void Send(T record)
        {
            foreach (var sender in this.currentSnapshot)
                sender.Send(record);
        }

        public void EndEpoch()
        {
            foreach (var sender in this.currentSnapshot)
                sender.EndEpoch();
        }

        public void Close()
        {
            foreach (var sender in this.currentSnapshot)
                sender.Close();
        }

        public void Shutdown()
        {
            foreach (var sender in this.currentSnapshot)
                sender.Shutdown();
        }

    }

    /// <summary>
    /// A data stream protocol handler that transmits the stream over a network socket.
    /// </summary>
    /// <typeparam name="T">The type of records in this data stream.</typeparam>
    internal class DataStreamSocketSender<T> : NaiadDataStreamProtocol<T>
    {
        private const int PAGE_SIZE = 1 << 14;

        private Socket socket;
        private readonly SendBufferPage sendPage;
        private readonly NaiadSerialization<T> serializer;

        public DataStreamSocketSender(Socket socket)
        {
            this.socket = socket;
            this.sendPage = new SendBufferPage(GlobalBufferPool<byte>.pool, PAGE_SIZE);
            this.sendPage.WriteHeader(new MessageHeader(-1, 0, (int)RemotingProtocol.DATA_CHANNEL_ID, -1, SerializedMessageType.Data));
            this.serializer = AutoSerialization.GetSerializer<T>();
        }

        public void StartEpoch()
        {
            if (this.socket == null) return;
            byte[] header = new byte[MessageHeader.SizeOf];
            MessageHeader.WriteHeaderToBuffer(header, 0, new MessageHeader(-1, -1, (int)RemotingProtocol.OPEN_EPOCH_CHANNEL_ID, -1, 0, SerializedMessageType.Data));
            try
            {
                this.socket.Send(header);
            }
            catch (SocketException)
            {
                Logging.Info("[Socket remote sender] Failed sending to {0}, disabling...", this.socket.RemoteEndPoint);
                this.socket = null;
            }
        }

        public void Send(T record)
        {
            throw new NotImplementedException();
#if false
            if (this.socket == null) return;
            if (!this.sendPage.Write((int)RemotingProtocol.DATA_CHANNEL_ID, this.serializer, record))
            {
                this.FlushSendPage();

                bool success = this.sendPage.Write((int)RemotingProtocol.DATA_CHANNEL_ID, this.serializer, record);
                //Debug.Assert(success);
            }
#endif
        }

        public void EndEpoch()
        {
            throw new NotImplementedException();
#if false
            if (this.socket == null) return;
            this.FlushSendPage();
            byte[] header = new byte[MessageHeader.SizeOf];
            MessageHeader.WriteHeaderToBuffer(header, 0, new MessageHeader(-1, -1, (int)RemotingProtocol.CLOSE_EPOCH_CHANNEL_ID, -1, 0, SuperChannelMessageType.Data));
            try
            {
                this.socket.Send(header);
            }
            catch (SocketException)
            {
                Logging.Info("[Socket remote sender] Failed sending to {0}, disabling...", this.socket.RemoteEndPoint);
                this.socket = null;
            }
#endif
        }

        public void Close()
        {
            if (this.socket == null) return;
            byte[] header = new byte[MessageHeader.SizeOf];
            MessageHeader.WriteHeaderToBuffer(header, 0, new MessageHeader(-1, -1, (int)RemotingProtocol.CLOSE_CONNECTION_CHANNEL_ID, -1, -1, SerializedMessageType.Data));
            this.socket.Send(header);
            this.socket.Close();
        }

        public void Shutdown()
        {
            if (this.socket == null) return;
            byte[] header = new byte[MessageHeader.SizeOf];
            MessageHeader.WriteHeaderToBuffer(header, 0, new MessageHeader(-1, -1, (int)RemotingProtocol.SHUTDOWN_COMPUTATION_CHANNEL_ID, -1, -1, SerializedMessageType.Data));
            try
            {
                this.socket.Send(header);
            }
            catch (SocketException)
            {
                Logging.Info("[Socket remote sender] Failed sending to {0}, disabling...", this.socket.RemoteEndPoint);
                this.socket = null;
                return;
            }
            this.socket.Close();
        }


        private void FlushSendPage()
        {
            if (this.socket == null) return;
            this.sendPage.FinalizeLastMessage();
            ArraySegment<byte> segment = this.sendPage.Consume().ToArraySegment();
            try
            {
                this.socket.Send(segment.Array, segment.Offset, segment.Count, SocketFlags.None);
            }
            catch (SocketException)
            {
                Logging.Info("[Socket remote sender] Failed sending to {0}, disabling...", this.socket.RemoteEndPoint);
                this.socket = null;
                return;
            }
            this.sendPage.Reset();
            this.sendPage.WriteHeader(new MessageHeader(-1, 0, (int)RemotingProtocol.DATA_CHANNEL_ID, -1, SerializedMessageType.Data));
    
        }
    }

    /// <summary>
    /// A data stream protocol source that receives a data stream over a queue of network sockets (one after the other),
    /// and passes this to a protocol handler.
    /// </summary>
    /// <typeparam name="T">The type of records in this data stream.</typeparam>
    internal class SocketQueueRemoteRecordReceiver<T> : IDisposable
        where T : IEquatable<T>
    {
        private const int PAGE_SIZE = 1 << 14;

        private readonly BlockingCollection<Socket> socketQueue;
        private readonly NaiadDataStreamProtocol<T> recipient;
        private readonly Thread receiveThread;

        public SocketQueueRemoteRecordReceiver(NaiadDataStreamProtocol<T> recipient)
        {
            this.socketQueue = new BlockingCollection<Socket>();
            this.recipient = recipient;
            this.receiveThread = new Thread(this.IngressThread);
            this.receiveThread.Start();
        }

        public void Enqueue(Socket socket)
        {
            this.socketQueue.Add(socket);
        }

        public void Dispose()
        {
            lock (this.socketQueue)
            {
                if (!this.socketQueue.IsAddingCompleted)
                    this.socketQueue.CompleteAdding();
            }
        }

        private enum ConnectionState
        {
            // [Event type, Previous state] -> Next state.
            Disconnected, // (START) [CLOSE, Connected], [ERROR, *]     -> Disconnected
            Connected,    // [CONNECT, Disconnected], [END_EPOCH, Data] -> Connected
            Data,         // [START_EPOCH, Connected], [DATA, Data]     -> Data
            Shutdown      // [SHUTDOWN, Connected]                      -> Shutdown (FINAL)
        }

        private static void ReceiveAll(Socket socket, byte[] buffer, int count)
        {
            int start = 0;
            while (count > 0)
            {
                int bytesReceived = socket.Receive(buffer, start, count, SocketFlags.None);
                start += bytesReceived;
                count -= bytesReceived;
            }
        }

        private void IngressThread()
        {
            MessageHeader parsedHeader = default(MessageHeader);
            T currentRecord = default(T);
            NaiadSerialization<T> serializer = AutoSerialization.GetSerializer<T>();
            byte[] currentHeader = new byte[MessageHeader.SizeOf];
            byte[] currentBody = new byte[PAGE_SIZE];

            ConnectionState state = ConnectionState.Disconnected;

            foreach (Socket ingressSocket in this.socketQueue.GetConsumingEnumerable())
            {
                try
                {
                    state = ConnectionState.Connected;
                    do
                    {
                        ReceiveAll(ingressSocket, currentHeader, currentHeader.Length);
                        MessageHeader.ReadHeaderFromBuffer(currentHeader, 0, ref parsedHeader);
                        Logging.Info("State = {0}; Event = {1}", state, (RemotingProtocol)parsedHeader.ChannelID);
                        switch ((RemotingProtocol)parsedHeader.ChannelID)
                        {
                            case RemotingProtocol.OPEN_EPOCH_CHANNEL_ID:
                                if (state == ConnectionState.Connected)
                                {
                                    this.recipient.StartEpoch();
                                    state = ConnectionState.Data;
                                }
                                else
                                    goto default;
                                break;
                            case RemotingProtocol.CLOSE_EPOCH_CHANNEL_ID:
                                if (state == ConnectionState.Data)
                                {
                                    this.recipient.EndEpoch();
                                    state = ConnectionState.Connected;
                                }
                                else
                                    goto default;
                                break;
                            case RemotingProtocol.DATA_CHANNEL_ID:
                                if (state == ConnectionState.Data)
                                {
                                    ReceiveAll(ingressSocket, currentBody, parsedHeader.Length);
                                    RecvBuffer buffer = new RecvBuffer(currentBody, 0, parsedHeader.Length);
                                    while (serializer.TryDeserialize(ref buffer, out currentRecord))
                                    {
                                        this.recipient.Send(currentRecord);
                                    }
                                }
                                else
                                    goto default;
                                break;
                            case RemotingProtocol.CLOSE_CONNECTION_CHANNEL_ID:
                                if (state == ConnectionState.Connected)
                                {
                                    ingressSocket.Close();
                                    this.recipient.Close();
                                    state = ConnectionState.Disconnected;
                                }
                                else
                                    goto default;
                                break;
                            case RemotingProtocol.SHUTDOWN_COMPUTATION_CHANNEL_ID:
                                if (state == ConnectionState.Connected)
                                {
                                    ingressSocket.Close();
                                    this.recipient.Shutdown();
                                    state = ConnectionState.Shutdown;
                                }
                                else
                                    goto default;
                                return;
                            default:
                                // Error handling routine
                                Logging.Info("Error reading from socket: {0}", ingressSocket);
                                Debugger.Break();
                                if (this.recipient is Cancellable)
                                {
                                    Logging.Info("Cancelling current epoch");
                                    ((Cancellable)this.recipient).CancelEpoch();
                                }
                                else
                                {
                                    Logging.Info("Ending epoch prematurely: warning, data may be lost");
                                    this.recipient.EndEpoch();
                                }
                                state = ConnectionState.Disconnected; 
                                break;
                        }
                    } while (state != ConnectionState.Disconnected);
                    continue;

                }
                catch (Exception)
                {
                    Logging.Info("Error reading from socket: {0}", ingressSocket);
                    if (this.recipient is Cancellable)
                    {
                        Logging.Info("Cancelling current epoch");
                        ((Cancellable)this.recipient).CancelEpoch();
                    }
                    else
                    {
                        Logging.Info("Ending epoch prematurely: warning, data may be lost");
                        this.recipient.EndEpoch();
                    }
                    state = ConnectionState.Disconnected;
                }
            }
        }
    }

    public interface Cancellable
    {
        void CancelEpoch();
    }

#if false
    /// <summary>
    /// A simple data stream protocol handler that logs the protocol messages as they are received.
    /// </summary>
    /// <typeparam name="R"></typeparam>
    internal class NaiadDataStreamLogger<R> : NaiadDataStreamProtocol<R>
        where R : IEquatable<R>
    {
        public void StartEpoch()
        {
            Logging.Info("Starting epoch");
        }

        public void Send(R record)
        {
            Logging.Info("Received record: {0}", record);
        }

        public void EndEpoch()
        {
            Logging.Info("Ending epoch");
        }

        public void Close()
        {
            Logging.Info("Closing connection");
        }

        public void Shutdown()
        {
            Logging.Info("Shutting down computation");
        }
    }
#endif

#if false
    public interface RemoteCollection
    {
        void AttachIngressSocket(Socket s);
    }

    /// <summary>
    /// An input to a Naiad computation that consumes messages from a stream of Naiad data stream protocol
    /// messages, and ingests the records into the computation.
    /// </summary>
    /// <typeparam name="R">The type of records in the data stream.</typeparam>
    public class RemoteCollection<R> : TypedCollection<R, IntLattice>, RemoteCollection, NaiadDataStreamProtocol<WeightedNaiadable<R>>, Cancellable
        where R : IEquatable<R>
    {
        private readonly IncrementalCollection<R> input;
        private readonly SocketQueueRemoteRecordReceiver<WeightedNaiadable<R>> receiver;

        private readonly string name;

        private readonly List<Weighted<R>> bufferedInput;

        internal override InternalController Controller
        {
            get { return input.Controller; }
        }

        internal RemoteCollection(Placement placement, InternalController controller, string name)
            : base()
        {
            this.name = name;
            this.input = (IncrementalCollection<R>)this.Controller.NewInput<R>();
            this.bufferedInput = new List<Weighted<R>>();
            if (placement.Any(x => x.ProcessId == controller.Configuration.ProcessID))
                this.receiver = new SocketQueueRemoteRecordReceiver<WeightedNaiadable<R>>(this);
            else
            {
                Logging.Error("Warning: local parallelism was 0; receiver will be null and incoming connections will not be accepted.");
                this.receiver = null;
            }
            this.Controller.RegisterRemoteCollection(this, this.name);
        }

        public void AttachIngressSocket(Socket s)
        {
            Logging.Info("RemoteInput [{0}] Attaching socket from {1}", this.name, s.RemoteEndPoint);
            this.receiver.Enqueue(s);
        }

        /// <summary>
        /// Calling this method advances the epoch at which incoming records will be ingested.
        /// 
        /// This is typically done to ensure that some initial computation is carried out before
        /// clients connect.
        /// </summary>
        public void AdvanceEpoch()
        {
            this.input.OnNext(Enumerable.Empty<Weighted<R>>());
        }

        #region RemoteSender methods
        public void StartEpoch()
        {
            Logging.Info("RemoteInput [{0}] Starting epoch", this.name);
        }

        public void Send(WeightedNaiadable<R> record)
        {
            Logging.Info("RemoteInput [{0}] Sending record: {1}", this.name, record);
            this.bufferedInput.Add(new Weighted<R>(record.payload, record.weight));
        }

        public void EndEpoch()
        {
            Logging.Info("RemoteInput [{0}] Ending epoch", this.name);
            this.input.OnNext(this.bufferedInput);
            this.bufferedInput.Clear();
        }

        public void CancelEpoch()
        {
            this.bufferedInput.Clear();
        }

        public void Close()
        {
            Logging.Debug("RemoteInput [{0}] Closing connection", this.name);
        }

        public void Shutdown()
        {
            Logging.Debug("RemoteInput [{0}] Shutting down", this.name);
            this.input.OnCompleted();
        }
        #endregion

        #region Interface methods (wrapping the internal IncrementalCollection<R>)
        internal override BundleSendEndpoint<Weighted<R>, IntLattice> Output
        {
            get
            {
                return this.input.Output;
            }
        }

        #endregion
    }

    public static class RemotingExtensionMethods
    {
        /// <summary>
        /// Creates a new computation output to which other processes may connect using network sockets,
        /// over which the Naiad data stream protocol will be sent.
        /// </summary>
        /// <typeparam name="R">The type of records in this data stream.</typeparam>
        /// <param name="collection"></param>
        /// <param name="collectionName">The human-readable name of this collection, for use in a naiad:// URI.</param>
        public static void RemoteOutput<R>(this Collection<R, IntLattice> collection, string collectionName)
            where R : IEquatable<R>
        {
            BufferingDataStreamBroadcaster<R> multiSender = new BufferingDataStreamBroadcaster<R>(collectionName);
            
            ((TypedCollection<R, IntLattice>)collection).Controller.RegisterRemoteOutput(multiSender, collectionName);
            collection.Subscribe(xs =>
                {
                    multiSender.StartEpoch();
                    foreach (Weighted<R> record in xs)
                        multiSender.Send(record);
                    multiSender.EndEpoch();
                });
        }
    }
#endif

    public class StreamObserver<R> : IObserver<IEnumerable<R>>
    {
        private readonly NaiadWriter writer;
        private readonly Stream stream;
        private NaiadSerialization<R> serializer;

        public StreamObserver(Stream stream)
        {
            this.writer = new NaiadWriter(stream);
            this.stream = stream;
        }

        public void OnCompleted()
        {
            this.writer.Write<int>(-1, PrimitiveSerializers.Int32);
            this.writer.Flush();
            this.stream.Close();
        }

        public void OnError(Exception error)
        {
            throw new NotImplementedException();
        }

        public void OnNext(IEnumerable<R> value)
        {
            if (this.serializer == null)
                this.serializer = AutoSerialization.GetSerializer<R>();

            this.writer.Write<int>(value.Count(), PrimitiveSerializers.Int32);
            foreach (R element in value)
                this.writer.Write<R>(element, this.serializer);
        }

        public void Dispose()
        {
            writer.Dispose();
            stream.Close();
        }

    }

    public class ObservableStream<R> : IObservable<IEnumerable<R>>
    {
        private class Unsubscriber : IDisposable
        {
            private readonly List<IObserver<IEnumerable<R>>> observers;
            private readonly IObserver<IEnumerable<R>> observer;

            public Unsubscriber(List<IObserver<IEnumerable<R>>> observers, IObserver<IEnumerable<R>> observer)
            {
                this.observers = observers;
                this.observer = observer;
            }

            public void Dispose()
            {
                lock (this.observers)
                {
                    this.observers.Remove(this.observer);
                }
            }
        }

        private readonly Stream stream;
        private readonly List<IObserver<IEnumerable<R>>> observers;

        public IDisposable Subscribe(IObserver<IEnumerable<R>> observer)
        {
            lock (this.observers)
            {
                this.observers.Add(observer);
            }
            return new Unsubscriber(this.observers, observer);
        }

        public ObservableStream(Stream stream)
        {
            this.stream = stream;
            this.observers = new List<IObserver<IEnumerable<R>>>();
        }

        public void Play()
        {

        }
        
    }

}
