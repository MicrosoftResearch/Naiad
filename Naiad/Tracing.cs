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

#if true
using System;
using Microsoft.Research.Naiad.Scheduling;
using Microsoft.Research.Naiad.Runtime.Progress;
#else
using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Diagnostics.Tracing;
using System.IO;
using System.Linq;
using System.Runtime.InteropServices;
using System.Text;
using System.Threading;
using Microsoft.Research.Naiad.Dataflow;
using Microsoft.Research.Naiad.Dataflow.Channels;
using Microsoft.Research.Naiad.Scheduling;
using Microsoft.Research.Naiad.Serialization;
using Microsoft.Research.Naiad.Runtime.Progress;
#endif

namespace Microsoft.Research.Naiad.Diagnostics
{
    /// <summary>
    /// Enumeration describing which aspect of a Naiad computation a tracing region corresponds to
    /// </summary>
    public enum NaiadTracingRegion
    {
        /// <summary>
        /// Region corresponding to a flush
        /// </summary>
        Flush, 
        /// <summary>
        /// Region corresponding to a send
        /// </summary>
        Send,
        /// <summary>
        /// Region corresponding to a TCP message broadcast
        /// </summary>
        BroadcastTCP,
        /// <summary>
        /// Region corresponding to a UDP message broadcast
        /// </summary>
        BroadcastUDP,
        /// <summary>
        /// Region corresponding to a reachability computation
        /// </summary>
        Reachability,
        /// <summary>
        /// Region corresponding to codegen
        /// </summary>
        Compile,
        /// <summary>
        /// Region corresponding to waking up dormant workers
        /// </summary>
        Wakeup,
        /// <summary>
        /// Region corresponding to setting events to wake up dormant workers
        /// </summary>
        SetEvent,
        /// <summary>
        /// Unclassifed region
        /// </summary>
        Unspecified
    }

#if true
    internal class NaiadTracing
    {
        public static NaiadTracing Trace = new NaiadTracing();

        public void RegionStart(NaiadTracingRegion region) { }
        public void RegionStop(NaiadTracingRegion region) { }

        public void ChannelInfo(int channel, int src, int dst, bool isExchange, bool isProgress) { }
        public void ProcessInfo(int id, string name) { }
        public void StageInfo(int id, string name) { }
        public void VertexPlacement(int stageid, int vertexid, int proc, int worker) { }

        public void LockInfo(Object obj, string name) { }
        public void LockAcquire(Object obj) { }
        public void LockHeld(Object obj) { }
        public void LockRelease(Object obj) { }

        public void ThreadName(string name, params object[] args) { }
        public void SocketError(System.Net.Sockets.SocketError err) { }
        public void MsgSend(int channel, int seqno, int len, int src, int dst) { }
        public void MsgRecv(int channel, int seqno, int len, int src, int dst) { }
        internal void StartSched(Scheduler.WorkItem workitem) { }
        internal void StopSched(Scheduler.WorkItem workitem) { }

        public void RefAlignFrontier() { }
        public void AdvanceFrontier(Pointstamp[] frontier) { }
    }

    /// <summary>
    /// This class containes methods that allow Mark events to be posted in the ETW Kernel Logger session.
    /// </summary>
    public class KernelLoggerTracing
    {
        /// <summary>
        /// Formats and then writes a freetext Mark event into the ETW Kernel Logger trace session.
        /// This event is expensive and should be used sparingly.
        /// </summary>
        /// <param name="msg">The format string to be logged, as in String.Format</param>
        /// <param name="args">Arguments to be formatted.</param>
        public static void PostKernelLoggerMarkEvent(string msg, params object[] args) { }
    }
#else
    /// <summary>
    /// ETW provider for Naiad 
    /// GUID is 0ad7158e-b717-53ae-c71a-6f41ab15fe16
    /// </summary>
    /// 
    // Some stuff to remember about the EventSource class:
    //     - Anything returning void will be considered an Event unless given the NonEvent attribute
    //     - Events can only have primitive types as arguments
    [EventSource(Name = "Naiad")]
    internal class NaiadTracing : EventSource
    {
        public static NaiadTracing Trace = new NaiadTracing();

        public NaiadTracing()
            : base(true)
        {
            Logging.Progress("Naiad provider guid = {0}", this.Guid);
            Logging.Progress("Naiad provider enabled = {0}", this.IsEnabled());

            this.DumpManifestToFile("naiadprovider.xml");
        }

        protected override void OnEventCommand(EventCommandEventArgs command)
        {
            //Logging.Progress("OnEventCommand: {0} {1}", command.Command, command.Arguments);
            base.OnEventCommand(command);
        }

        /// <summary>
        /// Identifies the Naiad subsystem the event pertains to
        /// </summary>
        public class Tasks
        {
            public const EventTask Channels = (EventTask)1;
            public const EventTask Scheduling = (EventTask)2;
            public const EventTask Control = (EventTask)3;
            public const EventTask Locks = (EventTask)4;
            public const EventTask Graph = (EventTask)5;
        }

        /// <summary>
        /// Defines logical groups of events that can be turned on and off independently
        /// </summary>
        public class Keywords
        {
            public const EventKeywords Debug = (EventKeywords)0x0001;
            public const EventKeywords Measurement = (EventKeywords)0x0002;
            public const EventKeywords Network = (EventKeywords)0x0004;
            public const EventKeywords Scheduling = (EventKeywords)0x0008;
            public const EventKeywords Viz = (EventKeywords)0x0010;
            public const EventKeywords Locks = (EventKeywords)0x0020;
            public const EventKeywords GraphMetaData = (EventKeywords)0x0040;
        }

        #region Channels
        /// <summary>
        /// Generates an ETW event for message send.
        /// Does nothing if there is no listener for the Network or Viz keywords of the NaiadTracing provider.
        /// <param name="channel">Id of the channel the message is sent on</param>
        /// <param name="seqno">Sequence number in the message header</param>
        /// <param name="len">Length value in the message header</param>
        /// <param name="src">Source vertex id</param>
        /// <param name="dst">Destination vertex id</param>
        /// </summary>
        [Conditional("TRACING_ON")]
        [Event(104, Task = Tasks.Channels, Opcode = EventOpcode.Send, Keywords = Keywords.Network | Keywords.Viz)]
        public void MsgSend(int channel, int seqno, int len, int src, int dst)
        {
            if (Trace.IsEnabled(EventLevel.LogAlways, Keywords.Network | Keywords.Viz))
            {
                WriteEvent(104, channel, seqno, len, src, dst);
            }   
        }

        /// <summary>
        /// Generates an ETW event for message receive.
        /// Does nothing if there is no listener for the Network or Viz keywords of the NaiadTracing provider.
        /// <param name="channel">Id of the channel the message is received on</param>
        /// <param name="seqno">Sequence number in the message header</param>
        /// <param name="len">Length value in the message header</param>
        /// <param name="src">Source vertex id</param>
        /// <param name="dst">Destination vertex id</param>
        /// </summary>
        [Conditional("TRACING_ON")]
        [Event(105, Task = Tasks.Channels, Opcode = EventOpcode.Send, Keywords = Keywords.Network | Keywords.Viz)]
        public void MsgRecv(int channel, int seqno, int len, int src, int dst)
        {
            if (Trace.IsEnabled(EventLevel.LogAlways, Keywords.Network | Keywords.Viz))
            {
                WriteEvent(105, channel, seqno, len, src, dst);
            }
        }
        #endregion Channels

        #region Scheduling
        /// <summary>
        /// Generates an ETW event for the scheduling of a work item.
        /// Does nothing if there is no listener for the Viz keyword of the NaiadTracing provider.
        /// </summary>
        /// <param name="workitem">Item being scheduled</param>
        [NonEvent]
        [Conditional("TRACING_ON")]
        internal void StartSched(Scheduler.WorkItem workitem)
        {
            if (Trace.IsEnabled(EventLevel.LogAlways, Keywords.Scheduling | Keywords.Viz))
                StartSched(workitem.Vertex.Stage.StageId);
        }

        [Event(200, Task = Tasks.Scheduling, Opcode = EventOpcode.Start, Keywords = Keywords.Scheduling | Keywords.Viz)]
        private void StartSched(int stageid)
        {
            WriteEvent(200, stageid);
        }

        /// <summary>
        /// Generates an ETW event for the end of a scheduling period of a work item.
        /// Does nothing if there is no listener for the Viz keyword of the NaiadTracing provider.
        /// </summary>
        /// <param name="workitem">Item being descheduled</param>
        [NonEvent]
        [Conditional("TRACING_ON")]
        internal void StopSched(Scheduler.WorkItem workitem)
        {
            //Console.WriteLine("]Sched {0} {1}", id, workitem.ToString());
            if (Trace.IsEnabled(EventLevel.LogAlways, Keywords.Scheduling | Keywords.Viz))
                StopSched(workitem.Vertex.Stage.StageId);
            }

        [Event(201, Task = Tasks.Scheduling, Opcode = EventOpcode.Stop, Keywords = (Keywords.Scheduling | Keywords.Viz))]
        private void StopSched(int stageid)
        {
            WriteEvent(201, stageid);
        }

        #endregion Scheduling

        #region Control
        [Event(300, Task = Tasks.Control, Opcode = EventOpcode.Info, Keywords = Keywords.Viz)]
        public void RefAlignFrontier()
        {
            if (Trace.IsEnabled(EventLevel.LogAlways, Keywords.Viz))
                WriteEvent(300);
        }

        /// <summary>
        /// Generates an ETW event for frontier advance.
        /// Does nothing if there is no listener for the Viz keyword of the NaiadTracing provider.
        /// </summary>
        /// <param name="frontier">The new PCS frontier</param>
        [NonEvent]
        [Conditional("TRACING_ON")]
        public void AdvanceFrontier(Pointstamp[] frontier)
        {
            if (Trace.IsEnabled(EventLevel.LogAlways, Keywords.Viz))
                AdvanceFrontier(frontier.Select(x => x.ToString()).Aggregate((x, y) => x + " " + y));
        }
        [Event(301, Task = Tasks.Control, Opcode = EventOpcode.Info, Keywords = Keywords.Viz)]
        private void AdvanceFrontier(string newFrontier)
        {
            WriteEvent(301, newFrontier);
        }

        [NonEvent]
        [Conditional("TRACING_ON")]
        public void SocketError(System.Net.Sockets.SocketError err)
        {
            if (Trace.IsEnabled(EventLevel.Error, Keywords.Viz | Keywords.Network | Keywords.Debug))
            {
                SocketError(Enum.GetName(typeof(System.Net.Sockets.SocketError), err));
            }
        }
        [Event(302, Task = Tasks.Control, Opcode = EventOpcode.Info, Keywords = Keywords.Viz | Keywords.Network | Keywords.Debug)]
        private void SocketError(string msg)
        {
            WriteEvent(302, msg);
        }

        [NonEvent]
        [Conditional("TRACING_ON")]
        public void RegionStart(NaiadTracingRegion region)
        {
            if (Trace.IsEnabled(EventLevel.Error, Keywords.Viz | Keywords.Network | Keywords.Debug))
            {
                if (!regionInfoEventsPosted)
                {
                    RegionInfo();
                }
                RegionStart((int)region);
            }
        }
        [Event(303, Task = Tasks.Control, Opcode = EventOpcode.Info, Keywords = Keywords.Viz | Keywords.Debug)]
        private void RegionStart(int id)
        {
            WriteEvent(303, id);
        }

        [NonEvent]
        [Conditional("TRACING_ON")]
        public void RegionStop(NaiadTracingRegion region)
        {
            if (Trace.IsEnabled(EventLevel.Error, Keywords.Viz | Keywords.Network | Keywords.Debug))
            {
                if (!regionInfoEventsPosted)
                {
                    RegionInfo();
                }
                RegionStop((int)region);
            }
        }
        [Event(304, Task = Tasks.Control, Opcode = EventOpcode.Info, Keywords = Keywords.Viz | Keywords.Debug)]
        private void RegionStop(int id)
        {
            WriteEvent(304, id);
        }


        private bool regionInfoEventsPosted = false;
        [Event(305, Task = Tasks.Control, Opcode = EventOpcode.Info, Keywords = Keywords.Viz | Keywords.Debug)]
        [Conditional("TRACING_ON")]
        private void RegionInfo()
        {
            foreach (var val in Enum.GetValues(typeof(NaiadTracingRegion)))
            {
                WriteEvent(305, val, Enum.GetName(typeof(NaiadTracingRegion), val));
            }
            regionInfoEventsPosted = true;
        }


        #endregion Control

        #region Graph
        [Event(500, Task = Tasks.Graph, Opcode = EventOpcode.Info, Keywords = Keywords.Viz | Keywords.GraphMetaData)]
        [Conditional("TRACING_ON")]
        public void StageInfo(int id, string name)
        {
            if (Trace.IsEnabled(EventLevel.LogAlways, Keywords.Viz | Keywords.GraphMetaData))
                WriteEvent(500, id, name);
        }

        /// <summary>
        /// Posts an event describing the placement of a vertex
        /// </summary>
        /// <param name="stageid">Stage id</param>
        /// <param name="vertexid">Vertex id</param>
        /// <param name="proc">Naiad process id</param>
        /// <param name="worker">Worker thread id</param>
        [Event(501, Task = Tasks.Graph, Opcode = EventOpcode.Info, Keywords = Keywords.Viz | Keywords.GraphMetaData)]
        [Conditional("TRACING_ON")]
        public void VertexPlacement(int stageid, int vertexid, int proc, int worker)
        {
            if (Trace.IsEnabled(EventLevel.LogAlways, Keywords.Viz | Keywords.GraphMetaData))
                WriteEvent(501, stageid, vertexid, proc, worker);
        }

        /// <summary>
        /// Posts channel info metadata event
        /// </summary>
        /// <param name="channel">Channel id</param>
        /// <param name="src">Source stage id</param>
        /// <param name="dst">Destination stage id</param>
        /// <param name="isExchange">True if an exchange channel (otherwise a pipeline channel)</param>
        /// <param name="isProgress">True if a progress channel (otherwise a data channel)</param>
        [Event(502, Task = Tasks.Graph, Opcode = EventOpcode.Info, Keywords = Keywords.Viz | Keywords.GraphMetaData)]
        [Conditional("TRACING_ON")]
        public void ChannelInfo(int channel, int src, int dst, bool isExchange, bool isProgress)
        {
            if (Trace.IsEnabled(EventLevel.LogAlways, Keywords.Viz | Keywords.GraphMetaData))
                WriteEvent(502, channel, src, dst, isExchange, isProgress);
        }

        /// <summary>
        /// Posts a friendly name for the calling thread.
        /// Note that this tracing event is not conditionally compiled because we want these events to appear
        /// even when not tracing Naiad specifically.
        /// </summary>
        /// <param name="name">Name for this thread (usually to be displayed in a visualization of the trace)</param>
        /// <param name="args">Any formatting args</param>
        [NonEvent]
        public void ThreadName(string name, params object[] args)
        {
            StringBuilder sb = new StringBuilder();
            sb.AppendFormat(name, args);
            ThreadName(sb.ToString());
        }
        [Event(503, Task = Tasks.Graph, Opcode = EventOpcode.Info, Keywords = Keywords.Viz)]
        private void ThreadName(string name)
        {
            WriteEvent(503, name);
        }

        /// <summary>
        /// Posts the Naiad process id and the name of the local machine.
        /// </summary>
        /// <param name="id">Naiad process id</param>
        /// <param name="name">Local machine name</param>
        [Event(504, Task = Tasks.Graph, Opcode = EventOpcode.Info, Keywords = Keywords.Viz | Keywords.GraphMetaData)]
        [Conditional("TRACING_ON")]
        public void ProcessInfo(int id, string name)
        {
            WriteEvent(504, id, name);
        }
        #endregion Graph

        #region Locking

        [NonEvent]
        [Conditional("TRACE_LOCKS")]
        public void LockInfo(Object obj, string name)
        {
            if (Trace.IsEnabled(EventLevel.Verbose, Keywords.Locks))
                LockInfo(obj.GetHashCode(), name);
        }
        [Event(400, Task = Tasks.Locks, Opcode = EventOpcode.Info, Keywords = Keywords.Locks, Level = EventLevel.Verbose)]
        private void LockInfo(int id, string name)
        {
                WriteEvent(400, id, name);
        }

        [NonEvent]
        [Conditional("TRACE_LOCKS")]
        public void LockAcquire(Object obj)
        {
            if (Trace.IsEnabled(EventLevel.Verbose, Keywords.Locks))
                LockAcquire(obj.GetHashCode());
        }
        [Event(401, Task = Tasks.Locks, Opcode = EventOpcode.Info, Keywords = Keywords.Locks, Level = EventLevel.Verbose)]
        private void LockAcquire(int id)
        {
            WriteEvent(401, id);
        }

        [NonEvent]
        [Conditional("TRACE_LOCKS")]
        public void LockHeld(Object obj)
        {
            if (Trace.IsEnabled(EventLevel.Verbose, Keywords.Locks))
                LockHeld(obj.GetHashCode());
        }
        [Event(402, Task = Tasks.Locks, Opcode = EventOpcode.Info,
            Keywords = Keywords.Locks, Level = EventLevel.Verbose)]
        private void LockHeld(int id)
        {
            WriteEvent(402, id);
        }

        [NonEvent]
        [Conditional("TRACE_LOCKS")]
        public void LockRelease(Object obj)
        {
            if (Trace.IsEnabled(EventLevel.Verbose, Keywords.Locks))
                LockRelease(obj.GetHashCode());
        }
        [Event(403, Task = Tasks.Locks, Opcode = EventOpcode.Info, Keywords = Keywords.Locks, Level = EventLevel.Verbose)]
        private void LockRelease(int id)
        {
            WriteEvent(403, id);
        }
        #endregion

        #region Misc utilities
        /// <summary>
        /// Writes the XML manifest of the Naiad ETW provider to a file.
        /// This method is not thread-safe, but will catch exceptions and do nothing.
        /// </summary>
        /// <param name="filename">File to write to</param>
        [NonEvent]
        public void DumpManifestToFile(string filename)
        {
            try
            {
                using (var s = new StreamWriter(File.Open(filename, FileMode.Create)))
                {
                    s.Write(NaiadTracing.GenerateManifest(typeof(NaiadTracing), "Naiad.dll"));
                }
            }
            catch (Exception e)
            {
                Logging.Error("Error dumping ETW provider manifest: {0}", e.Message);
            }
        }

        /// <summary>
        /// DIY versions of WriteEvent to avoid the slow path.
        /// See http://msdn.microsoft.com/en-us/library/system.diagnostics.tracing.eventsource.writeeventcore(v=vs.110).aspx.
        /// </summary>
        /// <param name="eventId"></param>
        /// <param name="arg1"></param>
        /// <param name="arg2"></param>
        /// <param name="arg3"></param>
        /// <param name="arg4"></param>
        /// <param name="arg5"></param>
        protected unsafe void WriteEvent(int eventId, int arg1, int arg2, int arg3, int arg4, int arg5)
        {
               EventSource.EventData* dataDesc = stackalloc EventSource.EventData[1];
               int* data = stackalloc int[6];
               data[0] = arg1;
               data[1] = arg2;
               data[2] = arg3;
               data[3] = arg4;
               data[4] = arg5; 
               
               dataDesc[0].DataPointer = (IntPtr)data;
               dataDesc[0].Size = 4*5;
               WriteEventCore(eventId, 1, dataDesc);
        }

        #endregion Misc utilities
    }

    /// <summary>
    /// This class containes methods that allow Mark events to be posted in the ETW Kernel Logger session.
    /// </summary>
    public class KernelLoggerTracing
    {
        private static object _lock = new object();
        private static bool _inited = false;

        [StructLayout(LayoutKind.Sequential, CharSet = CharSet.Ansi)]
        private struct ETW_SET_MARK_INFORMATION
        {
            public Int32 Flag;
            [MarshalAs(UnmanagedType.ByValTStr, SizeConst = 128)] public String Mark;
        };

        [StructLayout(LayoutKind.Sequential, Pack = 1, CharSet = CharSet.Ansi)]
        private struct ETW_SET_BINARY_MARK_INFORMATION
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
                    EtwSetMark(0, ptr, Math.Min(4 + sb.Length, buf.Length));
                }
                sb.Clear();
            }
            unsafe public void Trace(string msg)
            {
                if (!_inited)
                {
                    _initEtwMarks();
                }

                for (int i = 0; i < msg.Length && i + 4 < buf.Length; i++)
                {
                    buf[i + 4] = (byte) msg[i];
                }
                fixed (byte* ptr = buf)
                {
                    EtwSetMark(0, ptr, Math.Min(4 + msg.Length, buf.Length));
                }
            }
        }

        // Per-thread instances of TracingBuffer;
        private static ThreadLocal<TracingBuffer> tracingBuffer = new ThreadLocal<TracingBuffer>(() => new TracingBuffer());

        /// <summary>
        /// Formats and then writes a freetext Mark event into the ETW Kernel Logger trace session.
        /// This event is expensive and should be used sparingly.
        /// </summary>
        /// <param name="msg">The format string to be logged, as in String.Format</param>
        /// <param name="args">Arguments to be formatted.</param>
        [Conditional("TRACING_ON")]
        public static void PostKernelLoggerMarkEvent(string msg, params object[] args)
        {
            var tb = tracingBuffer.Value;
            tb.Trace(msg, args);
        }

        /// <summary>
        /// Writes a freetext Mark event into the ETW Kernel Logger trace session.
        /// This event is expensive and should be used sparingly.
        /// </summary>
        /// <param name="msg">message</param>
        [Conditional("TRACING_ON")]
        public static void PostKernelLoggerMarkEvent(string msg)
        {
            var tb = tracingBuffer.Value;
            tb.Trace(msg);
        }
    }
#endif
}
