/*
 * Naiad ver. 0.5
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
using System.Net.Sockets;
using System.Net;
using System.Diagnostics;
using System.Threading;
using Microsoft.Research.Naiad.Dataflow.Channels;
using System.IO;
using System.Threading.Tasks;
using Microsoft.Research.Naiad.Scheduling;
using Microsoft.Research.Naiad.Runtime.Networking;

using Microsoft.Research.Naiad.Diagnostics;

namespace Microsoft.Research.Naiad.Runtime.Networking
{
    internal enum NaiadProtocolOpcode
    {
        InvalidOpcode = -1,
        PeerConnect = 0,
        ConstructGraph = 1,
        LoadDataFromFile = 2,
        GetIngressSocket = 3,
        GetEgressSocket = 4,
        GetComputationStats = 5,
        DoReport = 6,
        Kill = 7,
        List = 8,
        Complain = 9
    }

    internal class NaiadServer : IDisposable
    {
        private enum ServerState
        {
            Initalized,
            Started,
            Stopped
        }

        private ServerState state;

        private struct GuardedAction
        {
            public GuardedAction(Action<Socket> a, bool mustGuard)
            {
                Guard = new Task(() => { });
                if (!mustGuard)
                {
                    Guard.RunSynchronously();
                }
                Action = a;
            }

            public readonly Task Guard;
            public readonly Action<Socket> Action;
        }

        private readonly IPEndPoint endpoint;
        private readonly Socket listeningSocket;
        private readonly Dictionary<NaiadProtocolOpcode, GuardedAction> serverActions;
        private readonly Dictionary<int, TcpNetworkChannel> networkChannels;
        private readonly Thread servingThread;

        public NaiadServer(ref IPEndPoint endpoint)
        {
            this.serverActions = new Dictionary<NaiadProtocolOpcode, GuardedAction>();
            this.state = ServerState.Initalized;
            this.serverActions[NaiadProtocolOpcode.Kill] = new GuardedAction(s => { using (TextWriter writer = new StreamWriter(new NetworkStream(s))) writer.WriteLine("Killed"); s.Close(); System.Environment.Exit(-9); }, false);
            this.networkChannels = new Dictionary<int, TcpNetworkChannel>();
            this.serverActions[NaiadProtocolOpcode.PeerConnect] = new GuardedAction(s => { int channelId = ReceiveInt(s); this.networkChannels[channelId].PeerConnect(s); }, true);

            Socket socket = BindSocket(ref endpoint);
            
            this.endpoint = endpoint;
            this.listeningSocket = socket;

            this.servingThread = new Thread(this.ThreadStart);
        }

        public void RegisterNetworkChannel(TcpNetworkChannel channel)
        {
            if (channel != null)
                this.networkChannels[channel.Id] = channel;
        }

        /// <summary>
        /// Registers an Action that will be called when a connection is made with the given opcode.
        /// 
        /// The Action receives the socket for the accepted connection, and is responsible for managing that
        /// resource by e.g. closing it.
        /// 
        /// This method must be called before a call to Start();
        /// </summary>
        /// <param name="opcode">The opcode to handle.</param>
        /// <param name="serverAction">The Action to execute when this opcode is received.</param>
        /// <param name="mustGuard">If true, the execution will be guarded.</param>
        public void RegisterServerAction(NaiadProtocolOpcode opcode, Action<Socket> serverAction, bool mustGuard)
        {
            this.serverActions[opcode] = new GuardedAction(serverAction, mustGuard);
        }

        private Socket TryToBind(IPEndPoint endpoint)
        {
            Logging.Progress("Trying to bind Naiad server at {0}", endpoint);

            Socket s = new Socket(endpoint.AddressFamily, SocketType.Stream, ProtocolType.Tcp);

            try
            {
                s.Bind(endpoint);

                return s;
            }
            catch (SocketException e)
            {
                if (e.SocketErrorCode == SocketError.AddressAlreadyInUse)
                {
                    // another process has bound to this socket: we'll pick a different port and try
                    // again in the calling code
                    return null;
                }
                else
                {
                    // unexpected error so we'll just let someone else deal with it
                    throw;
                }
            }
        }

        /// <summary>
        /// Binds the server socket to an available port.
        /// </summary>
        /// <param name="endpoint">If this parameter is not null, try to listen on this endpoint, otherwise one will be picked arbitrarily.</param>
        /// <returns>The bound socket.</returns>
        private Socket BindSocket(ref IPEndPoint endpoint)
        {
            Socket socket = null;

            if (endpoint == null)
            {
                int port = 2101;
                for (int i = 0; socket == null && i < 1000; ++i)
                {
                    endpoint = new IPEndPoint(IPAddress.Any, port + i);
                    socket = TryToBind(endpoint);
                }
            }
            else
            {
                socket = TryToBind(endpoint);
            }

            if (socket == null)
            {
                throw new ApplicationException("Unable to find a socket to bind to");
            }

            Logging.Progress("Starting Naiad server at {0}", endpoint);

            return socket;
        }

        /// <summary>
        /// Starts the server accepting connections. To stop the server, call <see cref="Stop"/>.
        /// </summary>
        public void Start()
        {
            Debug.Assert(this.state == ServerState.Initalized);

            this.servingThread.Start();
        }

        /// <summary>
        /// Accept loop thread. Implements protocol operation demuxing, based on a 4-byte opcode, in the first 4 bytes received
        /// from the accept()'ed socket.
        /// </summary>
        private void ThreadStart()
        {
            this.state = ServerState.Started;

            this.listeningSocket.Listen(100);
            while (true)
            {
                Socket acceptedSocket;
                try
                {
                    acceptedSocket = this.listeningSocket.Accept();
                }
                catch (ObjectDisposedException)
                {
                    break;
                }
                catch (SocketException se)
                {
                    if (se.SocketErrorCode == SocketError.Interrupted)
                    {
                        break;
                    }
                    else
                    {
                        throw;
                    }
                }
                NaiadProtocolOpcode opcode = (NaiadProtocolOpcode) ReceiveInt(acceptedSocket);
                GuardedAction acceptAction;
                if (this.serverActions.TryGetValue(opcode, out acceptAction))
                {
                    // Ensures that the Action runs after the Guard task has run.
                    acceptAction.Guard.ContinueWith(
                        (task) => AcceptInternal(acceptAction.Action, acceptedSocket, opcode));
                }
                else
                {
                    Logging.Progress("Invalid/unhandled opcode received: {0}", opcode);
                    acceptedSocket.Close();
                }
            }
        }

        public void AcceptPeerConnections()
        {
            // Allows queued PeerConnect operations to continue.
            this.serverActions[NaiadProtocolOpcode.PeerConnect].Guard.RunSynchronously();
        }

        private void AcceptInternal(Action<Socket> action, Socket peerSocket, NaiadProtocolOpcode opcode)
        {
            try
            {
                action(peerSocket);
            }
            catch (Exception e)
            {
                Logging.Progress("Error handling a connection with opcode: {0}", opcode);
                Logging.Progress(e.ToString());
                peerSocket.Close();
            }
        }

        private static int ReceiveInt(Socket peerSocket)
        {
            byte[] intBuffer = new byte[4];
            int n = peerSocket.Receive(intBuffer);
            Debug.Assert(n == 4);
            return BitConverter.ToInt32(intBuffer, 0);
        }

        public void Stop()
        {
            Debug.Assert(this.state == ServerState.Started);
            this.listeningSocket.Close();
            this.servingThread.Join();
            this.state = ServerState.Stopped;
        }

        public void Dispose()
        {
            if (this.state == ServerState.Started)
                this.Stop();
            this.listeningSocket.Dispose();
        }
    }
}
