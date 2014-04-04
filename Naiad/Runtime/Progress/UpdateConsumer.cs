/*
 * Naiad ver. 0.3
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
using Microsoft.Research.Naiad.CodeGeneration;
using Microsoft.Research.Naiad.DataStructures;
using Microsoft.Research.Naiad.Scheduling;
using Microsoft.Research.Naiad.FaultTolerance;
using Microsoft.Research.Naiad.Dataflow;
using Microsoft.Research.Naiad.Frameworks;


namespace Microsoft.Research.Naiad.Runtime.Progress
{
    public class FrontierChangedEventArgs : System.EventArgs
    {
        public readonly Pointstamp[] NewFrontier;

        public FrontierChangedEventArgs(Pointstamp[] newFrontier) { this.NewFrontier = newFrontier; }
    }

    public interface Frontier
    {
        event EventHandler<FrontierChangedEventArgs> OnFrontierChanged;
    }

    // Consumes information about outstanding records in the system, and reports the least causal time known to exist.
    internal class ProgressUpdateConsumer : Dataflow.Vertex<Empty>, Frontier, LocalProgressInfo
    {
        private class VertexInput : Dataflow.VertexInput<Update, Empty>
        {
            private readonly ProgressUpdateConsumer op;
            public Dataflow.Vertex Vertex { get { return this.op; } }
            
            public VertexInput(ProgressUpdateConsumer op) { this.op = op; } 

            // none of these are implemented, or ever used.
            public void Flush() { throw new NotImplementedException(); }
            //public void RecordReceived(Pair<Int64, Pointstamp> record, RemotePostbox sender) { throw new NotImplementedException(); }
            public void OnReceive(Message<Update, Empty> message, RemotePostbox sender) { throw new NotImplementedException(); }
            public void SerializedMessageReceived(SerializedMessage message, RemotePostbox sender) { throw new NotImplementedException(); }
            public bool LoggingEnabled { get { return false; } set { throw new NotImplementedException("Logging for RecvFiberBank"); } }
            public int AvailableEntrancy { get { throw new NotImplementedException(); } set { throw new NotImplementedException(); } }
        }

        public Dataflow.VertexInput<Update, Empty> Input { get { return new VertexInput(this); } }

        public override string ToString() { return "ProgressUpdateConsumer"; }

        internal void InjectElement(Pointstamp time, Int64 update)
        {
            // by directly modifying the PCS, we don't risk sending anything from centralizer. Used only for initializing inputs.
            Tracing.Trace("(PCSLock");
            Monitor.Enter(this.PCS);

            var progressChanged = PCS.UpdatePointstampCount(time, update);

            Monitor.Exit(this.PCS);
            Tracing.Trace(")PCSLock");
        }

        public readonly PointstampCountSet PCS;
        public PointstampCountSet PointstampCountSet { get { return this.PCS; } }

        public ManualResetEvent FrontierEmpty = new ManualResetEvent(false);

        public void ProcessCountChange(Pointstamp time, Int64 weight)
        {
            // the PCS should not be touched outside this lock, other than by capturing PCS.Frontier.
            Tracing.Trace("(PCSLock");
            Monitor.Enter(this.PCS);

            var oldFrontier = PCS.Frontier;
            var frontierChanged = PCS.UpdatePointstampCount(time, weight);
            var newFrontier = PCS.Frontier;

            Monitor.Exit(this.PCS);
            Tracing.Trace(")PCSLock");

            if (frontierChanged)
            {
                // aggregation may need to flush
                this.Aggregator.ConsiderFlushingBufferedUpdates();

                // fire any frontier changed events
                if (this.OnFrontierChanged != null)
                    this.OnFrontierChanged(this, new FrontierChangedEventArgs(newFrontier));

                // no elements means done.
                if (newFrontier.Length == 0)
                {
                    Tracing.Trace("Frontier advanced to <empty>");
                    this.FrontierEmpty.Set();
                }
                else
                {
                    Tracing.Trace("Frontier advanced to " + string.Join(" ", newFrontier.Select(x => x.ToString())));
                }

                // Wake up schedulers to run shutdown actions for the graph.
                this.Stage.InternalGraphManager.Controller.Workers.WakeUp();
            }
        }

        public void ProcessCountChange(Message<Update, Empty> updates)
        {
            // the PCS should not be touched outside this lock, other than by capturing PCS.Frontier.
            Tracing.Trace("(PCSLock");
            Monitor.Enter(this.PCS);

            var oldFrontier = PCS.Frontier;

            var frontierChanged = false;
            for (int i = 0; i < updates.length; i++)
                frontierChanged = PCS.UpdatePointstampCount(updates.payload[i].Pointstamp, updates.payload[i].Delta) || frontierChanged;

            var newFrontier = PCS.Frontier;

            Monitor.Exit(this.PCS);
            Tracing.Trace(")PCSLock");

            if (frontierChanged)
            {
                // aggregation may need to flush
                this.Aggregator.ConsiderFlushingBufferedUpdates();

                // fire any frontier changed events
                if (this.OnFrontierChanged != null)
                    this.OnFrontierChanged(this, new FrontierChangedEventArgs(newFrontier));

                // no elements means done.
                if (newFrontier.Length == 0)
                {
                    Tracing.Trace("Frontier advanced to <empty>");
                    this.FrontierEmpty.Set();
                }
                else
                {
                    Tracing.Trace("Frontier advanced to " + string.Join(" ", newFrontier.Select(x => x.ToString())));
                }

                // Wake up schedulers to run shutdown actions for the graph.
                this.Stage.InternalGraphManager.Controller.Workers.WakeUp();
            }
        }

        internal ProgressUpdateAggregator Aggregator;

        public event EventHandler<FrontierChangedEventArgs> OnFrontierChanged;

        #region Checkpoint and Restore

        /* Checkpoint format:
         * PointstampCountSet               PCS
         */

        public override void Checkpoint(NaiadWriter writer)
        {
            this.PCS.Checkpoint(writer, this.CodeGenerator.GetSerializer<long>(), this.CodeGenerator.GetSerializer<Pointstamp>(), this.CodeGenerator.GetSerializer<int>());
        }

        public override void Restore(NaiadReader reader)
        {
            this.PCS.Restore(reader, this.CodeGenerator.GetSerializer<long>(), this.CodeGenerator.GetSerializer<Pointstamp>(), this.CodeGenerator.GetSerializer<int>());
        }

        #endregion 

        internal ProgressUpdateConsumer(int index, Stage<Empty> stage, ProgressUpdateAggregator aggregator)
            : base(index, stage)
        {
            this.Aggregator = aggregator;

            this.PCS = new PointstampCountSet(this.Stage.InternalGraphManager.Reachability);
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
            public Dataflow.Vertex Vertex { get { return this.op; } }

            public VertexInput(ProgressUpdateCentralizer op)
            {
                this.op = op;
            }

            // none of these are implemented, or ever called.
            public void Flush() { throw new NotImplementedException(); }            
            public void OnReceive(Message<Update, Empty> message, RemotePostbox sender) { throw new NotImplementedException(); }
            public void SerializedMessageReceived(SerializedMessage message, RemotePostbox sender) { throw new NotImplementedException(); }
            public bool LoggingEnabled { get { return false; } set { throw new NotImplementedException("Logging for RecvFiberBank"); } }
            public int AvailableEntrancy { get { throw new NotImplementedException(); } set { throw new NotImplementedException(); } }
        }

        public Dataflow.VertexInput<Update, Empty> Input { get { return new VertexInput(this); } }

        public override string ToString() { return "ProgressUpdateCentralizer"; }

        internal void InjectElement(Pointstamp time, Int64 update)
        {
            // by directly modifying the PCS, we don't risk sending anything from the centralizer. Used only for initializing inputs.
            Tracing.Trace("(PCSLock");
            Monitor.Enter(this.PCS);

            var frontierChanged = PCS.UpdatePointstampCount(time, update);

            Monitor.Exit(this.PCS);
            Tracing.Trace(")PCSLock");
        }

        public readonly PointstampCountSet PCS;
        public PointstampCountSet PointstampCountSet { get { return this.PCS; } }

        internal VertexOutputBuffer<Update, Empty> Output;

        public ManualResetEvent FrontierEmpty = new ManualResetEvent(false);

        public void ProcessCountChange(Message<Update, Empty> updates)
        {
            // the PCS should not be touched outside this lock, other than by capturing PCS.Frontier.
            Tracing.Trace("(PCSLock");
            Monitor.Enter(this.PCS);

            var oldfrontier = PCS.Frontier;

            var frontierChanged = false;
            for (int i = 0; i < updates.length; i++)
                frontierChanged = PCS.UpdatePointstampCount(updates.payload[i].Pointstamp, updates.payload[i].Delta) || frontierChanged; ;

            var newfrontier = PCS.Frontier;

            Monitor.Exit(this.PCS);
            Tracing.Trace(")PCSLock");

            if (frontierChanged)
            {
                // get an exclusive lock, as this.Output.Send is not threadsafe.
                Tracing.Trace("(GlobalLock");
                lock (this.scheduler.Controller.GlobalLock)
                {
                    var output = this.Output.GetBufferForTime(new Empty());
                    foreach (var pointstamp in newfrontier.Except(oldfrontier))
                        output.Send(new Update(pointstamp, +1));

                    foreach (var pointstamp in oldfrontier.Except(newfrontier))
                        output.Send(new Update(pointstamp, -1));

                    this.Output.Flush();
                }
                Tracing.Trace(")GlobalLock");

                if (this.OnFrontierChanged != null)
                    this.OnFrontierChanged(this, new FrontierChangedEventArgs(newfrontier));
            }
        }

        internal ProgressUpdateAggregator Aggregator;

        public event EventHandler<FrontierChangedEventArgs> OnFrontierChanged;

        public override void Checkpoint(NaiadWriter writer) { this.PCS.Checkpoint(writer, this.CodeGenerator.GetSerializer<long>(), this.CodeGenerator.GetSerializer<Pointstamp>(), this.CodeGenerator.GetSerializer<int>()); }
        public override void Restore(NaiadReader reader) { this.PCS.Restore(reader, this.CodeGenerator.GetSerializer<long>(), this.CodeGenerator.GetSerializer<Pointstamp>(), this.CodeGenerator.GetSerializer<int>()); }

        internal ProgressUpdateCentralizer(int index, Stage<Empty> stage, ProgressUpdateAggregator aggregator)
            : base(index, stage)
        {
            this.Aggregator = aggregator;

            this.Output = new VertexOutputBuffer<Update, Empty>(this);

            this.PCS = new PointstampCountSet(this.Stage.InternalGraphManager.Reachability);
        }

        internal override void PerformAction(Scheduler.WorkItem workItem) { throw new NotImplementedException(); }
    }
}
