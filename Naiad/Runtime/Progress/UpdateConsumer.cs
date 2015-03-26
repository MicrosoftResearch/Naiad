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

using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading;
using Microsoft.Research.Naiad.Dataflow.Channels;
using Microsoft.Research.Naiad.Serialization;
using Microsoft.Research.Naiad.DataStructures;
using Microsoft.Research.Naiad.Scheduling;
using Microsoft.Research.Naiad.Dataflow;
using Microsoft.Research.Naiad.Frameworks;

using Microsoft.Research.Naiad.Diagnostics;
using Microsoft.Research.Naiad.Runtime.Progress;
using Microsoft.Research.Naiad.Runtime.FaultTolerance;

namespace Microsoft.Research.Naiad.Diagnostics
{
    /// <summary>
    /// Container for Frontier events
    /// </summary>
    public class FrontierChangedEventArgs : System.EventArgs
    {
        /// <summary>
        /// The minimal antichain of <see cref="Pointstamp"/>s in the new frontier.
        /// </summary>
        public readonly Pointstamp[] NewFrontier;

        /// <summary>
        /// Constructs new event arguments with the given <paramref name="newFrontier"/>.
        /// </summary>
        /// <param name="newFrontier">The new frontier.</param>
        public FrontierChangedEventArgs(Pointstamp[] newFrontier) { this.NewFrontier = newFrontier; }
    }

    /// <summary>
    /// Container for Graph Materialized events
    /// </summary>
    public class GraphMaterializedEventArgs : System.EventArgs
    {
        /// <summary>
        /// The stages in the computation
        /// </summary>
        public readonly IEnumerable<Dataflow.Stage> stages;

        /// <summary>
        /// For each stage and vertex, the fault-tolerance manager vertexID that receives updates
        /// </summary>
        public readonly IEnumerable<Pair<Pair<int, int>, int>> ftmanager;

        /// <summary>
        /// The directed edges in the computation: each edge is a pair of source and destination. Each source 
        /// or destination is a pair of StageId,VertexId
        /// </summary>
        public readonly IEnumerable<Pair<Pair<int,int>, Pair<int,int>>> edges;

        /// <summary>
        /// Constructs new event arguments with the given <paramref name="stages"/> and <paramref name="edges"/>.
        /// </summary>
        /// <param name="stages">The stages.</param>
        /// <param name="ftManager">The vertex ID of the fault-tolerance vertex that manages each stage+vertex.</param>
        /// <param name="edges">The edges.</param>
        public GraphMaterializedEventArgs(
            IEnumerable<Dataflow.Stage> stages,
            IEnumerable<Pair<Pair<int, int>, int>> ftManager,
            IEnumerable<Pair<Pair<int, int>, Pair<int, int>>> edges)
        {
            this.stages = stages;
            this.ftmanager = ftManager;
            this.edges = edges;
        }
    }

    /// <summary>
    /// Container for Checkpoint Persisted events
    /// </summary>
    public class CheckpointPersistedEventArgs : System.EventArgs
    {
        /// <summary>
        /// The new checkpoint
        /// </summary>
        public readonly CheckpointUpdate checkpoint;

        /// <summary>
        /// Constructs new event arguments with the given <paramref name="checkpoint"/>.
        /// </summary>
        /// <param name="checkpoint">The checkpoint.</param>
        public CheckpointPersistedEventArgs(CheckpointUpdate checkpoint)
        {
            this.checkpoint = checkpoint;
        }
    }

    /// <summary>
    /// Container for Stage Stable events
    /// </summary>
    public class StageStableEventArgs : System.EventArgs
    {
        /// <summary>
        /// The stage that has advanced
        /// </summary>
        public readonly int stageId;
        /// <summary>
        /// The new frontier
        /// </summary>
        public readonly Pointstamp[] frontier;

        /// <summary>
        /// Constructs new event arguments with the given <paramref name="stageId"/> and <paramref name="frontier"/>.
        /// </summary>
        /// <param name="stageId">The stage</param>
        /// <param name="frontier">The frontier</param>
        public StageStableEventArgs(int stageId, Pointstamp[] frontier)
        {
            this.stageId = stageId;
            this.frontier = frontier;
        }
    }

    /// <summary>
    /// Container for Process Restart events
    /// </summary>
    public class ProcessRestartedEventArgs : System.EventArgs
    {
        /// <summary>
        /// The process that has restarted
        /// </summary>
        public readonly int processId;

        /// <summary>
        /// Constructs new event arguments with the given <paramref name="processId"/>.
        /// </summary>
        /// <param name="processId">The process Id.</param>
        public ProcessRestartedEventArgs(int processId)
        {
            this.processId = processId;
        }
    }

    /// <summary>
    /// Container for Log events
    /// </summary>
    public class LogEventArgs : System.EventArgs
    {
        /// <summary>
        /// the sender of the message, or null for a notification
        /// </summary>
        public readonly ReturnAddress From;

        /// <summary>
        /// the index of the vertex in a stage logging the message or notification
        /// </summary>
        public readonly int VertexId;

        /// <summary>
        /// the stage containing the vertex logging the message or notification
        /// </summary>
        public readonly int StageId;

        /// <summary>
        /// the time of the message or notification
        /// </summary>
        public readonly string Time;

        /// <summary>
        /// the number of records being logged
        /// </summary>
        public readonly int count;

        /// <summary>
        /// Construct a log event for a message
        /// </summary>
        /// <param name="stageId">the stage receiving the message</param>
        /// <param name="vertexId">the vertex receiving the message</param>
        /// <param name="from">the message sender</param>
        /// <param name="time">the time of the message</param>
        /// <param name="count">the number of records in the message</param>
        public LogEventArgs(int stageId, int vertexId, ReturnAddress from, string time, int count)
        {
            this.StageId = stageId;
            this.VertexId = vertexId;
            this.From = from;
            this.Time = time;
            this.count = count;
        }

        /// <summary>
        /// Construct a log event for a notification
        /// </summary>
        /// <param name="stageId">the stage receiving the notification</param>
        /// <param name="vertexId">the vertex receiving the notification</param>
        /// <param name="time">the time of the notification</param>
        public LogEventArgs(int stageId, int vertexId, string time)
        {
            this.StageId = stageId;
            this.VertexId = vertexId;
            this.From = new ReturnAddress();
            this.Time = time;
            this.count = 0;
        }
    }
}

namespace Microsoft.Research.Naiad.Runtime.Progress
{
    /// <summary>
    /// Supports adding FrontierChanged events
    /// </summary>
    internal interface Frontier
    {
        /// <summary>
        /// Collection of events for frontier changes
        /// </summary>
        event EventHandler<FrontierChangedEventArgs> OnFrontierChanged;
    }

    // Consumes information about outstanding records in the system, and reports the least causal time known to exist.
    internal class ProgressUpdateConsumer : Dataflow.Vertex<Empty>, Frontier, LocalProgressInfo
    {
        private class VertexInput : Dataflow.VertexInput<Update, Empty>
        {
            private readonly ProgressUpdateConsumer op;
            public Dataflow.Vertex<Empty> Vertex { get { return this.op; } }
            
            public VertexInput(ProgressUpdateConsumer op) { this.op = op; } 

            // none of these are implemented, or ever used.
            public void Flush() { throw new NotImplementedException(); }
            //public void RecordReceived(Pair<Int64, Pointstamp> record, RemotePostbox sender) { throw new NotImplementedException(); }
            public void OnReceive(Message<Update, Empty> message, ReturnAddress sender) { throw new NotImplementedException(); }
            public void SerializedMessageReceived(SerializedMessage message, ReturnAddress from) { throw new NotImplementedException(); }
            public void SetCheckpointer(Checkpointer<Empty> checkpointer) { throw new NotImplementedException(); }
            public bool LoggingEnabled { get { return false; } }
            public int SenderStageId { get; set; }
            public int ChannelId { get; set; }
            public int AvailableEntrancy { get { throw new NotImplementedException(); } set { throw new NotImplementedException(); } }
        }

        public Dataflow.VertexInput<Update, Empty> Input { get { return new VertexInput(this); } }

        public override string ToString() { return "ProgressUpdateConsumer"; }

        internal void InjectElement(Pointstamp time, Int64 update)
        {
            // by directly modifying the PCS, we don't risk sending anything from centralizer. Used only for initializing inputs.
            NaiadTracing.Trace.LockAcquire(this.PCS);
            Monitor.Enter(this.PCS);
            NaiadTracing.Trace.LockHeld(this.PCS);

            var progressChanged = PCS.UpdatePointstampCount(time, update);

            Monitor.Exit(this.PCS);
            NaiadTracing.Trace.LockRelease(this.PCS);
        }

        public readonly PointstampCountSet PCS;
        public PointstampCountSet PointstampCountSet { get { return this.PCS; } }

        public ManualResetEvent FrontierEmpty = new ManualResetEvent(false);

        public void ProcessCountChange(Pointstamp time, Int64 weight)
        {
            // the PCS should not be touched outside this lock, other than by capturing PCS.Frontier.
            NaiadTracing.Trace.LockAcquire(this.PCS);
            Monitor.Enter(this.PCS);
            NaiadTracing.Trace.LockHeld(this.PCS);

            var oldFrontier = PCS.Frontier;
            var frontierChanged = PCS.UpdatePointstampCount(time, weight);
            var newFrontier = PCS.Frontier;

            Monitor.Exit(this.PCS);
            NaiadTracing.Trace.LockRelease(this.PCS);

            if (frontierChanged)
            {
                // aggregation may need to flush
                this.Aggregator.ConsiderFlushingBufferedUpdates(false);

                // fire any frontier changed events
                if (this.OnFrontierChanged != null)
                    this.OnFrontierChanged(this.Stage.Computation, new FrontierChangedEventArgs(newFrontier));

                // no elements means done.
                if (newFrontier.Length == 0)
                {
                    //Tracing.Trace("Frontier advanced to <empty>");
                    NaiadTracing.Trace.RefAlignFrontier();
                    this.FrontierEmpty.Set();
                }
                else
                {
                    NaiadTracing.Trace.AdvanceFrontier(newFrontier);
                }

                // Wake up schedulers to run shutdown actions for the graph.
                this.Stage.InternalComputation.Controller.Workers.WakeUp();
            }
        }

        public void ProcessCountChange(Message<Update, Empty> updates)
        {
            // the PCS should not be touched outside this lock, other than by capturing PCS.Frontier.
            NaiadTracing.Trace.LockAcquire(this.PCS);
            Monitor.Enter(this.PCS);
            NaiadTracing.Trace.LockHeld(this.PCS);

            var oldFrontier = PCS.Frontier;

            var frontierChanged = false;
            for (int i = 0; i < updates.length; i++)
                frontierChanged = PCS.UpdatePointstampCount(updates.payload[i].Pointstamp, updates.payload[i].Delta) || frontierChanged;

            var newFrontier = PCS.Frontier;

            Monitor.Exit(this.PCS);
            NaiadTracing.Trace.LockRelease(this.PCS);

            if (frontierChanged)
            {
                // aggregation may need to flush
                this.Aggregator.ConsiderFlushingBufferedUpdates(false);

                // fire any frontier changed events
                if (this.OnFrontierChanged != null)
                    this.OnFrontierChanged(this.Stage.Computation, new FrontierChangedEventArgs(newFrontier));

                // no elements means done.
                if (newFrontier.Length == 0)
                {
                    //Tracing.Trace("Frontier advanced to <empty>");
                    NaiadTracing.Trace.RefAlignFrontier();
                    this.FrontierEmpty.Set();
                }
                else
                {
                    NaiadTracing.Trace.AdvanceFrontier(newFrontier);
                }

                // Wake up schedulers to run shutdown actions for the graph.
                this.Stage.InternalComputation.Controller.Workers.WakeUp();
            }
        }

        internal ProgressUpdateAggregator Aggregator;

        public event EventHandler<FrontierChangedEventArgs> OnFrontierChanged;

        #region Checkpoint and Restore

        /* Checkpoint format:
         * PointstampCountSet               PCS
         */

        internal void Reset()
        {
            this.PCS.Reset();
        }

        protected override void Checkpoint(NaiadWriter writer)
        {
            this.PCS.Checkpoint(writer, this.SerializationFormat.GetSerializer<long>(), this.SerializationFormat.GetSerializer<Pointstamp>(), this.SerializationFormat.GetSerializer<int>());
        }

        protected override void Restore(NaiadReader reader)
        {
            this.PCS.Restore(reader, this.SerializationFormat.GetSerializer<long>(), this.SerializationFormat.GetSerializer<Pointstamp>(), this.SerializationFormat.GetSerializer<int>());
        }

        #endregion 

        internal ProgressUpdateConsumer(int index, Stage<Empty> stage, ProgressUpdateAggregator aggregator)
            : base(index, stage)
        {
            this.Aggregator = aggregator;

            this.PCS = new PointstampCountSet(this.Stage.InternalComputation.Reachability);
        }

        internal override int Replay(ReplayMode mode)
        {
            // don't do anything for replay
            return 0;
        }

        internal override void PerformAction(Scheduler.WorkItem workItem)
        {
            throw new NotImplementedException();
        }
    }

    internal class ProgressUpdateCentralizer : Dataflow.Vertex<Empty>, Frontier, LocalProgressInfo
    {
        private class VertexInput : Dataflow.VertexInput<Update, Empty>
        {
            private readonly ProgressUpdateCentralizer op;
            public Dataflow.Vertex<Empty> Vertex { get { return this.op; } }

            public VertexInput(ProgressUpdateCentralizer op)
            {
                this.op = op;
            }

            // none of these are implemented, or ever called.
            public void Flush() { throw new NotImplementedException(); }            
            public void OnReceive(Message<Update, Empty> message, ReturnAddress sender) { throw new NotImplementedException(); }
            public void SerializedMessageReceived(SerializedMessage message, ReturnAddress from) { throw new NotImplementedException(); }
            public void SetCheckpointer(Checkpointer<Empty> checkpointer) { throw new NotImplementedException(); }
            public bool LoggingEnabled { get { return false; } }
            public int SenderStageId { get; set; }
            public int ChannelId { get; set; }
            public int AvailableEntrancy { get { throw new NotImplementedException(); } set { throw new NotImplementedException(); } }
        }

        public Dataflow.VertexInput<Update, Empty> Input { get { return new VertexInput(this); } }

        public override string ToString() { return "ProgressUpdateCentralizer"; }

        internal void InjectElement(Pointstamp time, Int64 update)
        {
            // by directly modifying the PCS, we don't risk sending anything from the centralizer. Used only for initializing inputs.
            NaiadTracing.Trace.LockAcquire(this.PCS);
            Monitor.Enter(this.PCS);
            NaiadTracing.Trace.LockHeld(this.PCS);

            var frontierChanged = PCS.UpdatePointstampCount(time, update);

            Monitor.Exit(this.PCS);
            NaiadTracing.Trace.LockRelease(this.PCS);
        }

        public readonly PointstampCountSet PCS;
        public PointstampCountSet PointstampCountSet { get { return this.PCS; } }

        internal VertexOutputBuffer<Update, Empty> Output;

        public ManualResetEvent FrontierEmpty = new ManualResetEvent(false);

        private bool preparingForRollback = false;

        internal void PrepareCentralizerForRollback(bool preparing)
        {
            // we are starting to roll back, and we need to get a precise estimate of global progress, so stop pushing updates
            // in order to ensure the queues get drained
            NaiadTracing.Trace.LockAcquire(this.PCS);
            Monitor.Enter(this.PCS);
            NaiadTracing.Trace.LockHeld(this.PCS);

            this.preparingForRollback = preparing;

            Monitor.Exit(this.PCS);
            NaiadTracing.Trace.LockRelease(this.PCS);
        }

        internal void Reset()
        {
            this.PCS.Reset();
        }

        public void ProcessCountChange(Message<Update, Empty> updates)
        {
            bool localPreparingForRollback;

            // the PCS should not be touched outside this lock, other than by capturing PCS.Frontier.
            NaiadTracing.Trace.LockAcquire(this.PCS);
            Monitor.Enter(this.PCS);
            NaiadTracing.Trace.LockHeld(this.PCS);

            var oldfrontier = PCS.Frontier;

            var frontierChanged = false;
            for (int i = 0; i < updates.length; i++)
                frontierChanged = PCS.UpdatePointstampCount(updates.payload[i].Pointstamp, updates.payload[i].Delta) || frontierChanged;

            var newfrontier = PCS.Frontier;

            localPreparingForRollback = this.preparingForRollback;

            Monitor.Exit(this.PCS);
            NaiadTracing.Trace.LockRelease(this.PCS);

            if (frontierChanged && !localPreparingForRollback)
            {
                // get an exclusive lock, as this.Output.Send is not threadsafe.
                NaiadTracing.Trace.LockAcquire(this.scheduler.Controller.GlobalLock);
                lock (this.scheduler.Controller.GlobalLock)
                {
                    NaiadTracing.Trace.LockHeld(this.scheduler.Controller.GlobalLock);

                    var output = this.Output.GetBufferForTime(new Empty());
                    foreach (var pointstamp in newfrontier.Except(oldfrontier))
                    {
                        output.Send(new Update(pointstamp, +1));
                    }

                    foreach (var pointstamp in oldfrontier.Except(newfrontier))
                        output.Send(new Update(pointstamp, -1));

                    this.Output.Flush();
                }
                NaiadTracing.Trace.LockRelease(this.scheduler.Controller.GlobalLock);

                if (this.OnFrontierChanged != null)
                    this.OnFrontierChanged(this, new FrontierChangedEventArgs(newfrontier));
            }
        }

        internal ProgressUpdateAggregator Aggregator;

        public event EventHandler<FrontierChangedEventArgs> OnFrontierChanged;

        protected override void Checkpoint(NaiadWriter writer) { this.PCS.Checkpoint(writer, this.SerializationFormat.GetSerializer<long>(), this.SerializationFormat.GetSerializer<Pointstamp>(), this.SerializationFormat.GetSerializer<int>()); }
        protected override void Restore(NaiadReader reader) { this.PCS.Restore(reader, this.SerializationFormat.GetSerializer<long>(), this.SerializationFormat.GetSerializer<Pointstamp>(), this.SerializationFormat.GetSerializer<int>()); }

        internal ProgressUpdateCentralizer(int index, Stage<Empty> stage, ProgressUpdateAggregator aggregator)
            : base(index, stage)
        {
            this.Aggregator = aggregator;

            this.Output = new VertexOutputBuffer<Update, Empty>(this);

            this.PushEventTime(new Empty());

            this.PCS = new PointstampCountSet(this.Stage.InternalComputation.Reachability);
            NaiadTracing.Trace.LockInfo(this.PCS, "PCS lock");
        }

        internal override int Replay(ReplayMode mode)
        {
            // don't do anything for replay
            return 0;
        }

        internal override void PerformAction(Scheduler.WorkItem workItem) { throw new NotImplementedException(); }
    }
}
