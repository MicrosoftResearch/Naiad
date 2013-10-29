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
using System.Diagnostics;
//using System.Diagnostics.Tracing;
using System.IO;
using System.Linq;
using System.Runtime.InteropServices;
using System.Text;
using System.Threading;
using Naiad.Dataflow.Channels;
using Naiad.Scheduling;

namespace Naiad
{
    /// <summary>
    /// ETW provider for Naiad 
    /// </summary>
    /// 
#if false
    // Some stuff to remember about the EventSource class:
    //     - Anything returning void will be considered an Event unless given the NonEvent attribute
    //     - Events can only have primitive types as arguments
    [EventSource(Name = "Naiad")]
    internal class NaiadTracing : EventSource
    {
        public static NaiadTracing Trace = new NaiadTracing();

        /// <summary>
        /// Identifies the Naiad subsystem the event pertains to
        /// </summary>
        public class Tasks
        {
            public const EventTask Channels = (EventTask)1;
            public const EventTask Scheduling = (EventTask)2;
            public const EventTask Control = (EventTask)3;
        }

        /// <summary>
        /// Defines logical groups of events that can be turned on and off independently
        /// </summary>
        public class Keywords
        {
            public const EventKeywords Debug = (EventKeywords)0x0001;
            public const EventKeywords Measurement = (EventKeywords)0x0002;
            public const EventKeywords Network = (EventKeywords)0x0004;
            public const EventKeywords Viz = (EventKeywords)0x0008;
        }

        #region Channels
        /// <summary>
        /// Generates an ETW event for data message send.
        /// Does nothing if there is no listener for the Network or Viz keywords of the NaiadTracing provider.
        /// </summary>
        /// <param name="msg">Message header with fields correctly populated.</param>
        [NonEvent]
        public void DataSend(MessageHeader msg)
        {
            if (Trace.IsEnabled(EventLevel.LogAlways, (Keywords.Network | Keywords.Viz)))
                DataSend(msg.SequenceNumber, msg.Length, msg.FromShardID, msg.DestShardID);
        }

        [Event(1, Task = Tasks.Channels, Opcode = EventOpcode.Send, Keywords = (Keywords.Network | Keywords.Viz))]
        private void DataSend(int seqno, int len, int src, int dst)
        {
            WriteEvent(1, seqno, len, src, dst);
        }

        /// <summary>
        /// Generates an ETW event for data message receive.
        /// Does nothing if there is no listener for the Network or Viz keywords of the NaiadTracing provider.
        /// </summary>
        /// <param name="msg">Message header with fields correctly populated.</param>
        [NonEvent]
        public void DataRecv(MessageHeader msg)
        {
            if (Trace.IsEnabled(EventLevel.LogAlways, (Keywords.Network | Keywords.Viz)))
                DataRecv(msg.SequenceNumber, msg.Length, msg.FromShardID, msg.DestShardID);
        }

        [Event(2, Task = Tasks.Channels, Opcode = EventOpcode.Receive, Keywords = (Keywords.Network | Keywords.Viz))]
        private void DataRecv(int seqno, int len, int src, int dst)
        {
            WriteEvent(2, seqno, len, src, dst);
        }

        /// <summary>
        /// Generates an ETW event for progress message send.
        /// Does nothing if there is no listener for the Network or Viz keywords of the NaiadTracing provider.
        /// </summary>
        /// <param name="msg">Message header with fields correctly populated.</param>
        [NonEvent]
        public void ProgressSend(MessageHeader msg)
        {
            if (Trace.IsEnabled(EventLevel.LogAlways, (Keywords.Network | Keywords.Viz)))
                ProgressSend(msg.SequenceNumber, msg.Length, msg.FromShardID, msg.DestShardID);
        }

        [Event(3, Task = Tasks.Channels, Opcode = EventOpcode.Send, Keywords = (Keywords.Network | Keywords.Viz))]
        private void ProgressSend(int seqno, int len, int src, int dst)
        {
            WriteEvent(3, seqno, len, src, dst);
        }

        /// <summary>
        /// Generates an ETW event for progress message receive.
        /// Does nothing if there is no listener for the Network or Viz keywords of the NaiadTracing provider.
        /// </summary>
        /// <param name="msg">Message header with fields correctly populated.</param>
        [NonEvent]
        public void ProgressRecv(MessageHeader msg)
        {
            if (Trace.IsEnabled(EventLevel.LogAlways, (Keywords.Network | Keywords.Viz)))
                ProgressRecv(msg.SequenceNumber, msg.Length, msg.FromShardID, msg.DestShardID);
        }

        [Event(4, Task = Tasks.Channels, Opcode = EventOpcode.Receive, Keywords = (Keywords.Network | Keywords.Viz))]
        private void ProgressRecv(int seqno, int len, int src, int dst)
        {
            WriteEvent(4, seqno, len, src, dst);
        }

        #endregion Channels

        #region Scheduling
        /// <summary>
        /// Generates an ETW event for the scheduling of a work item.
        /// Does nothing if there is no listener for the Viz keyword of the NaiadTracing provider.
        /// </summary>
        /// <param name="id">Scheduler id</param>
        /// <param name="workitem">Item being scheduled</param>
        [NonEvent]
        internal void StartSched(int schedulerid, Scheduler.WorkItem workitem)
        {
            if (Trace.IsEnabled(EventLevel.LogAlways, Keywords.Viz))
                StartSched(schedulerid, workitem.Shard.Stage.StageId, workitem.Shard.Stage.Name, workitem.Requirement.Timestamp.ToString());
        }

        [Event(5, Task = Tasks.Scheduling, Opcode = EventOpcode.Start, Keywords = Keywords.Viz)]
        internal void StartSched(int schedulerid, int stageid, string stagename, string pointstamp)
        {
            WriteEvent(5, schedulerid, stageid, stagename, pointstamp, "<deprecated>");
        }

        /// <summary>
        /// Generates an ETW event for the end of a scheduling period of a work item.
        /// Does nothing if there is no listener for the Viz keyword of the NaiadTracing provider.
        /// </summary>
        /// <param name="id">Scheduler id</param>
        /// <param name="workitem">Item being descheduled</param>
        [NonEvent]
        internal void StopSched(int schedulerid, Scheduler.WorkItem workitem)
        {
            //Console.WriteLine("]Sched {0} {1}", id, workitem.ToString());
            if (Trace.IsEnabled(EventLevel.LogAlways, Keywords.Viz))
                StopSched(schedulerid, workitem.Shard.Stage.StageId, workitem.Shard.Stage.Name, workitem.Requirement.Timestamp.ToString());
            }

        [Event(6, Task = Tasks.Scheduling, Opcode = EventOpcode.Stop, Keywords = Keywords.Viz)]
        internal void StopSched(int schedulerid, int stageid, string stagename, string pointstamp)
        {
            WriteEvent(6, schedulerid, stageid, stagename, pointstamp, "<deprecated>");
        }

        #endregion Scheduling

        #region Metadata
        [Event(7, Task = Tasks.Control, Opcode = EventOpcode.Info, Keywords = Keywords.Viz)]
        public void RefAlignFrontier()
        {
            if (Trace.IsEnabled(EventLevel.LogAlways, Keywords.Viz))
                WriteEvent(7);
        }

        /// <summary>
        /// Generates an ETW event for frontier advance.
        /// Does nothing if there is no listener for the Viz keyword of the NaiadTracing provider.
        /// </summary>
        /// <param name="frontier">The new PCS frontier</param>
        [NonEvent]
        public void AdvanceFrontier(Pointstamp[] frontier)
        {
            if (Trace.IsEnabled(EventLevel.LogAlways, Keywords.Viz))
                AdvanceFrontier(frontier.Select(x => x.ToString()).Aggregate((x, y) => x + " " + y));
        }

        [Event(8, Task = Tasks.Control, Opcode = EventOpcode.Info, Keywords = Keywords.Viz)]
        private void AdvanceFrontier(string newFrontier)
        {
            WriteEvent(8, newFrontier);
        }

        #endregion Metadata

        #region Misc utilities
        /// <summary>
        /// Write the XML manifest of the Naiad ETW provider to a file
        /// </summary>
        /// <param name="filename">File to write to</param>
        [NonEvent]
        public void DumpManifestToFile(string filename)
        {
            using (var s = new StreamWriter(File.Open(filename, FileMode.Create)))
            {
                s.Write(NaiadTracing.GenerateManifest(typeof(NaiadTracing), "Naiad.dll"));
            }
        }
        #endregion Misc utilities
    }
#endif
    public class Tracing
    {
        private static object _lock = new object();
        private static bool _inited = false;

        [StructLayout(LayoutKind.Sequential, CharSet = CharSet.Ansi)]
        public struct ETW_SET_MARK_INFORMATION
        {
            public Int32 Flag;
            [MarshalAs(UnmanagedType.ByValTStr, SizeConst = 128)] public String Mark;
        };

        [StructLayout(LayoutKind.Sequential, Pack = 1, CharSet = CharSet.Ansi)]
        public struct ETW_SET_BINARY_MARK_INFORMATION
        {
            public Int32 Flag;
            public char Op;
            public UInt64 Key;
        };

        [DllImport("ntdll", ExactSpelling = true, SetLastError = false)]
        private static extern Int32 EtwSetMark(Int64 handle, ref ETW_SET_MARK_INFORMATION smi, Int32 size);

        // Another name for the same entry point so we can overload
        [DllImport("ntdll", ExactSpelling = true, EntryPoint = "EtwSetMark", SetLastError = false)]
        private static extern Int32 EtwSetBinaryMark(Int64 handle, ref ETW_SET_BINARY_MARK_INFORMATION smi, Int32 size);

        [DllImport("ntdll", ExactSpelling = true, SetLastError = false)]
        unsafe private static extern Int32 EtwSetMark(Int64 handle, byte* buf, Int32 size);

        private const UInt32 ETW_NT_FLAGS_TRACE_MARK = 6; // from sdpublic/internal/base/inc/ntwmi.h

        // Initialise ETW logging
        // Return true if initialization actually happened
        private static bool _initEtwMarks()
        {
            bool res = false;

            lock (_lock)
            {
                if (!_inited)
                {
                    _inited = true;
                    res = true;
                }
            }
            return res;
        }

        // Generate ETW Mark event using Naiad thread id, current operator name and message
        private static void _etwMark(ref Logging.LogInfo info, string msg)
        {
            if (!_inited)
            {
                _initEtwMarks();
            }

            String m = String.Format("{0}, {1}, {2}\n", info.ThreadId, info.OperatorName, msg);

            ETW_SET_MARK_INFORMATION smi = new ETW_SET_MARK_INFORMATION();
            smi.Flag = 0;
            smi.Mark = m;
            var s = Marshal.SizeOf(smi);
            var ret = EtwSetMark(0, ref smi, 4 + smi.Mark.Length);
        }

        // Generate raw ETW Mark event 
        private static void _etwMark(string msg)
        {
            if (!_inited)
            {
                _initEtwMarks();
            }

            ETW_SET_MARK_INFORMATION smi = new ETW_SET_MARK_INFORMATION();
            smi.Flag = 0;
            smi.Mark = msg;
            var s = Marshal.SizeOf(smi);
            var ret = EtwSetMark(0, ref smi, 4 + smi.Mark.Length);
        }

        // Generate raw binary ETW Mark event 
        private static void _etwBinaryMark(char op, UInt64 key)
        {
            ETW_SET_BINARY_MARK_INFORMATION smi = new ETW_SET_BINARY_MARK_INFORMATION();
            smi.Flag = 0;
            smi.Op = op;
            smi.Key = key;
            var ret = EtwSetBinaryMark(0, ref smi, 4 + 1 + 8);
        }

        /// <summary>
        /// Message producer ETW event.
        /// Generates mark event containing the character '+' followed by a 64-bit key constructed from the arguments.
        /// Post-processing (eg by ViewEtl) will use the key to match message producer and consumer events.
        /// </summary>
        /// <param name="args">Array of bytes, must be size 8</param>
        [Conditional("TRACING_ON")]
        public static void TraceProducer(params byte[] args)
        {
            ulong key = 0;
            for (int i = 0; i < args.Length; i++)
            {
                ulong foo = ((ulong)args[i] & 0xFF) << (i * 8);
                key = key | foo;
                //Console.Write("args[{0}]={1:x} ", i, args[i]);
            }
            //Console.WriteLine("key={0:x}", key);
            _etwBinaryMark('+', key);
        }

        /// <summary>
        /// Message consumer ETW event.
        /// Generates mark event containing the character '-' followed by a 64-bit key constructed from the arguments.
        /// Post-processing (eg by ViewEtl) will use the key to match message producer and consumer events.
        /// </summary>
        /// <param name="args">Array of bytes, must be size 8</param>
        [Conditional("TRACING_ON")]
        public static void TraceConsumer(params byte[] args)
        {
            ulong key = 0;
            for (int i = 0; i < args.Length; i++)
            {
                ulong foo = ((ulong)args[i] & 0xFF) << (i * 8);
                key = key | foo;
                //Console.Write("args[{0}]={1:x} ", i, args[i]);
            }
            //Console.WriteLine("key={0:x}", key);
            _etwBinaryMark('-', key);
        }

        // Wrap up a byte array and a stringbuilder which we will reuse frequently
        internal class TracingBuffer
        {
            public byte[] buf;
            public StringBuilder sb;
            
            public TracingBuffer()
            {
                buf = new byte[4 + 128];
                buf[0] = buf[1] = buf[2] = buf[3] = 0;
                sb = new StringBuilder(128);
            }
            unsafe public void Trace(string msg, params object[] args)
            {
                if (!_inited)
                {
                    _initEtwMarks();
                }

                sb.AppendFormat(msg, args);
                for (int i = 0; i < Math.Min(sb.Length, buf.Length - 4); i++)
                {
                    buf[i + 4] = (byte)sb[i];
                }
                fixed (byte* ptr = buf)
                {
                    EtwSetMark(0, ptr, 4 + sb.Length);
                }
                sb.Clear();
            }
            unsafe public void Trace(string msg)
            {
                if (!_inited)
                {
                    _initEtwMarks();
                }

                for (int i = 0; i < msg.Length; i++)
                {
                    buf[i + 4] = (byte)msg[i];
                }
                fixed (byte* ptr = buf)
                {
                    EtwSetMark(0, ptr, 4 + msg.Length);
                }
            }
        }

        // Per-thread instances of TracingBuffer;
        private static ThreadLocal<TracingBuffer> tracingBuffer = new ThreadLocal<TracingBuffer>(() => new TracingBuffer());

        /// <summary>
        /// Writes a freetext trace event using ETW
        /// </summary>
        /// <param name="msg">The format string to be logged, as in String.Format</param>
        /// <param name="args">Arguments to be formatted.</param>
        [Conditional("TRACING_ON")]
        public static void Trace(string msg, params object[] args)
        {
            var tb = tracingBuffer.Value;
            tb.Trace(msg, args);
        }

        [Conditional("TRACING_ON")]
        public static void Trace(string msg)
        {
            var tb = tracingBuffer.Value;
            tb.Trace(msg);
        }

        [Conditional("TRACING_ON")]
        public static void TracePreformatted(string msg)
        {
            var tb = tracingBuffer.Value;
            tb.Trace(msg);
        }

    }
}
