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
using System.IO;
using System.Linq;
using System.Net.NetworkInformation;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace Naiad.Util
{
    internal class StateForStats
    {
        System.Net.IPAddress IPAddress;
        BaseController controller;
        StreamWriter writer;
        int sleepTime;

        public void MonitorMemFootprint(object obj)
        {
            CancellationToken ct = (CancellationToken)obj;
            writer.WriteLine("# Elapsed(ms)\tAllocated(bytes)");
            var mystopwatch = System.Diagnostics.Stopwatch.StartNew();

            while (!ct.IsCancellationRequested)
            {
                mystopwatch.Reset();
                writer.WriteLine("  {0}\t\t{1}\t{2}", controller.Stopwatch.ElapsedMilliseconds, GC.GetTotalMemory(false), mystopwatch.ElapsedMilliseconds);
                Thread.Sleep(sleepTime);
            }
            //Console.Error.WriteLine("Memory monitoring thread shutting down");
            writer.Close();
        }

        public void MonitorNetwork(object obj)
        {
            CancellationToken ct = (CancellationToken)obj;

            Thread.CurrentThread.Priority = ThreadPriority.Highest;

            NetworkInterface[] adapters = NetworkInterface.GetAllNetworkInterfaces();
            IPGlobalProperties ipprops = IPGlobalProperties.GetIPGlobalProperties();
            NetworkInterface ni = null;


            writer.WriteLine("# My address {0}", this.IPAddress);
            foreach (NetworkInterface a in adapters)
            {
                writer.WriteLine("# Adapter {0}", a.Name);
                var props = a.GetIPProperties();
                foreach (var ua in props.UnicastAddresses)
                {
                    writer.WriteLine("#   Address {0}", ua.Address);
                    if (ua.Address.Equals(this.IPAddress))
                    {
                        writer.WriteLine("# IP Address match");
                        ni = a;
                        goto done;
                    }
                }
            }
        done:

            var ifstats = ni.GetIPv4Statistics();
            var tcpstats = ipprops.GetTcpIPv4Statistics();

            var segsresent1 = tcpstats.SegmentsResent;
            var segssent1 = tcpstats.SegmentsSent;
            var segsrcvd1 = tcpstats.SegmentsReceived;
            var bsent1 = ifstats.BytesSent;
            var brcvd1 = ifstats.BytesReceived;
            var dis1 = ifstats.IncomingPacketsDiscarded;
            var pktssent1 = ifstats.UnicastPacketsSent;
            var pktsrcvd1 = ifstats.UnicastPacketsReceived;

            var lastifstats = ifstats;
            var lasttcpstats = tcpstats;
            long lastTime = 0;

            long totBWStartTime = 0;
            long totBWStartBytes = 0;
            long now;
            writer.WriteLine("# Elapsed(ms)   Retransmit   RetransDelta Segments-Tx   Segs-TxDelta  Segments-Rx   Segs-RxDelta BytesTx   BytesTxDelta     BytesRx    BytesRxDelta   Discards     Pkts-Tx     Pkts-Rx     Name  ElapsedDelta");
            do
            {
                Thread.Sleep(sleepTime);
                now = controller.Stopwatch.ElapsedMilliseconds;
                ifstats = ni.GetIPv4Statistics();
                tcpstats = ipprops.GetTcpIPv4Statistics();
                if (totBWStartTime == 0 && now > 5000)
                {
                    totBWStartTime = now;
                    totBWStartBytes = ifstats.BytesSent;
                }
                writer.WriteLine("{0,10} {1,12} {2,12} {3,12} {4,12} {5,12} {6,12} {7,12} {8,12} {9,12} {10,12} {11,12} {12,12} {13,12} {14} {15}",
                                  now,
                                  tcpstats.SegmentsResent - segsresent1, (tcpstats.SegmentsResent - segsresent1) - (lasttcpstats.SegmentsResent - segsresent1),
                                  tcpstats.SegmentsSent - segssent1, (tcpstats.SegmentsSent - segssent1) - (lasttcpstats.SegmentsSent - segssent1),
                                  tcpstats.SegmentsReceived - segsrcvd1, (tcpstats.SegmentsReceived - segsrcvd1) - (lasttcpstats.SegmentsReceived - segsrcvd1),
                                  ifstats.BytesSent - bsent1, (ifstats.BytesSent - bsent1) - (lastifstats.BytesSent - bsent1),
                                  ifstats.BytesReceived - brcvd1, (ifstats.BytesReceived - brcvd1) - (lastifstats.BytesReceived - brcvd1),
                                  ifstats.IncomingPacketsDiscarded - dis1,
                                  ifstats.UnicastPacketsSent - pktssent1,
                                  ifstats.UnicastPacketsReceived - pktsrcvd1,
                                  ni.Name, now - lastTime);
                lastifstats = ifstats;
                lasttcpstats = tcpstats;
                lastTime = now;
            }
            while (!ct.IsCancellationRequested);

            Console.WriteLine("{0} {1} {2} {3} {4} #AvgBw",
                8000.0 * (lastifstats.BytesSent - totBWStartBytes) / (now - totBWStartTime),
                lastifstats.BytesSent, totBWStartBytes, now, totBWStartTime);
            //Console.Error.WriteLine("Network monitoring thread shutting down");
            writer.Close();
        }

        internal StateForStats(BaseController controller, StreamWriter writer, int sleepTime, System.Net.IPAddress address)
        {
            this.controller = controller;
            this.writer = writer;
            this.sleepTime = sleepTime;
            this.IPAddress = address;
        }
    }
}
