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
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Net.Sockets;
using System.Net;
using System.Diagnostics;
using Naiad.Dataflow.Channels;
using System.IO;
using Naiad.Scheduling;
using Naiad.Runtime.Networking;

namespace Naiad.Runtime.Networking
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

        private readonly IPEndPoint endpoint;
        private readonly Socket listeningSocket;
        private Dictionary<NaiadProtocolOpcode, Action<Socket>> serverActions;
        private Dictionary<int, TcpNetworkChannel> networkChannels;
        
        public NaiadServer(IPEndPoint endpoint)
        {
            Logging.Progress("Starting Naiad server at {0}", endpoint);
                
            this.endpoint = endpoint;
            this.listeningSocket = new Socket(this.endpoint.AddressFamily, SocketType.Stream, ProtocolType.Tcp);
            this.serverActions = new Dictionary<NaiadProtocolOpcode, Action<Socket>>();
            this.state = ServerState.Initalized;
            this.serverActions[NaiadProtocolOpcode.Kill] = s => { using (TextWriter writer = new StreamWriter(new NetworkStream(s))) writer.WriteLine("Killed"); s.Close(); System.Environment.Exit(-9); };
            //this.serverActions[NaiadProtocolOpcode.GetIngressSocket] = s => this.controller.AttachIngressSocketToRemoteCollection(s);
            //this.serverActions[NaiadProtocolOpcode.GetEgressSocket] = s => this.controller.AttachEgressSocketToRemoteOutput(s);
            this.networkChannels = new Dictionary<int, TcpNetworkChannel>();
            this.serverActions[NaiadProtocolOpcode.PeerConnect] = s => { int channelId = ReceiveInt(s); this.networkChannels[channelId].PeerConnect(s); };

        }

        public void RegisterNetworkChannel(TcpNetworkChannel channel)
        {
            if (channel != null)
                this.networkChannels[channel.Id] = channel;
        }

        /// <summary>
        /// Registers an action that will be called when a connection is made with the given opcode.
        /// 
        /// The action receives the socket for the accepted connection, and is responsible for managing that
        /// resource by e.g. closing it.
        /// 
        /// This method must be called before a call to Start();
        /// </summary>
        /// <param name="opcode">The opcode to handle.</param>
        /// <param name="serverAction"></param>
        public void RegisterServerAction(NaiadProtocolOpcode opcode, Action<Socket> serverAction)
        {
            this.serverActions[opcode] = serverAction;
        }

        public void Start()
        {
            Debug.Assert(this.state == ServerState.Initalized);
            this.state = ServerState.Started;

            this.listeningSocket.Bind(this.endpoint);

            this.listeningSocket.Listen(100);
            IAsyncResult result = this.listeningSocket.BeginAccept(4, this.AcceptCallback, null);
            while (result.CompletedSynchronously)
            {
                this.AcceptHandler(result);
                result = this.listeningSocket.BeginAccept(4, this.AcceptCallback, null);
            }
        }

        /// <summary>
        /// Asynchronous callback used in BeginAccept().
        /// Implements protocol operation demuxing, based on a 4-byte opcode, in the first 4 bytes received
        /// from the accept()'ed socket.
        /// </summary>
        /// <param name="result"></param>
        private void AcceptCallback(IAsyncResult result)
        {
            if (result.CompletedSynchronously)
                return;

            this.AcceptHandler(result);

            result = this.listeningSocket.BeginAccept(4, this.AcceptCallback, null);
            while (result.CompletedSynchronously)
            {
                this.AcceptHandler(result);
                result = this.listeningSocket.BeginAccept(4, this.AcceptCallback, null);
            }
        }

        private void AcceptHandler(IAsyncResult result)
        {
            Debug.Assert(this.state == ServerState.Started);
            Socket peerSocket;
            NaiadProtocolOpcode opcode = this.GetOpcode(result, out peerSocket);
            Action<Socket> acceptAction;
            if (this.serverActions.TryGetValue(opcode, out acceptAction))
            {
                try
                {
                    acceptAction(peerSocket);
                }
                catch (Exception e)
                {
                    Logging.Progress("Error handling a connection with opcode: {0}", opcode);
                    Logging.Progress(e.ToString());
                    peerSocket.Close();
                }
            }
            else
            {
                Logging.Progress("Invalid/unhandled opcode received: {0}", opcode);
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

        private NaiadProtocolOpcode GetOpcode(IAsyncResult acceptResult, out Socket peerSocket)
        {
            byte[] opcodeBuffer;
            int n;
            peerSocket = this.listeningSocket.EndAccept(out opcodeBuffer, out n, acceptResult);
            if (n != 4)
            {
                opcodeBuffer = new byte[4];
                n = peerSocket.Receive(opcodeBuffer);
                // If the opcode still hasn't shown up, something is wrong
                if (n != 4)
                {
                    return NaiadProtocolOpcode.InvalidOpcode;
                }
            }
            return (NaiadProtocolOpcode)BitConverter.ToInt32(opcodeBuffer, 0);
        }

        public void Stop()
        {
            Debug.Assert(this.state == ServerState.Started);
            this.listeningSocket.Close();
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
