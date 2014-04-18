/*
 * Naiad ver. 0.4
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

using Microsoft.Research.Naiad.Serialization;
using Microsoft.Research.Naiad.Dataflow.Channels;
using Microsoft.Research.Naiad.Dataflow.Reporting;
using Microsoft.Research.Naiad.DataStructures;
using Microsoft.Research.Naiad.Runtime.Progress;
using Microsoft.Research.Naiad.Scheduling;
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

using Microsoft.Research.Naiad.Diagnostics;

namespace Microsoft.Research.Naiad.Dataflow
{
    /// <summary>
    /// Represents a single abstract vertex in a dataflow graph. 
    /// (In Naiad, most concrete vertices extend <see cref="Vertex{TTime}"/>.)
    /// </summary>
    /// <remarks>
    /// This class provides common functionality to dataflow vertices that do not depend
    /// on a specific <see cref="Time{TTime}"/> type. When implementing a new vertex for use
    /// in Naiad, use <see cref="Vertex{TTime}"/> as the base class.
    /// </remarks>
    /// <seealso cref="Vertex{TTime}"/>
    public abstract class Vertex
    {
        /// <summary>
        /// Indicates number of additional times the vertex may be entered
        /// </summary>
        protected internal int Entrancy { get { return this.entrancy; } set { this.entrancy = value; } }
        private int entrancy;

        internal SerializationFormat SerializationFormat { get { return this.Stage.InternalComputation.SerializationFormat; } }

        private readonly List<Action> OnFlushActions;

        internal abstract void PerformAction(Scheduling.Scheduler.WorkItem workItem);


        /// <summary>
        /// The worker will invoke this method periodically to indicate progress through
        /// the computation, and enable (for example) garbage-collection code to run.
        /// </summary>
        /// <param name="pointstamps">List of the minimal pointstamps at which this
        /// vertex could be reached.</param>
        /// <remarks>
        /// Classes overriding this method must call <c>base.UpdateReachability(pointstamps)</c>.
        /// </remarks>
        protected internal virtual void UpdateReachability(List<Pointstamp> pointstamps)
        {
            if (pointstamps == null && !isShutdown)
                this.ShutDown();
        }

        /// <summary>
        /// Flushes all buffered state at this vertex.
        /// </summary>
        /// <remarks>
        /// To add user-defined buffering, provide a corresponding flush action using the
        /// <see cref="AddOnFlushAction"/> method.
        /// </remarks>
        protected internal void Flush()
        {
            for (int i = 0; i < this.OnFlushActions.Count; i++)
                this.OnFlushActions[i]();
        }

        /// <summary>
        /// Adds a <see cref="System.Action"/> to be called each time the vertex <see cref="Flush"/> method
        /// is called.
        /// </summary>
        /// <param name="onFlush">The flush action.</param>
        public void AddOnFlushAction(Action onFlush)
        {
            this.OnFlushActions.Add(onFlush);
        }

        /// <summary>
        /// Indicates whether the vertex has been shut down
        /// </summary>
        public bool IsShutDown { get { return this.isShutdown; } }

        /// <summary>
        /// Currently available for checkpoint restore
        /// </summary>
        protected bool isShutdown = false;
        internal void ShutDown()
        {
            if (!this.isShutdown)
            {
                Logging.Info("Shutting down {0}", this);
                this.OnShutdown();
                isShutdown = true;
            }
        }

        /// <summary>
        /// Called when the vertex is shut down
        /// </summary>
        protected virtual void OnShutdown()
        {
            this.Flush();
            this.OnFlushActions.Clear();
        }

        /// <summary>
        /// The stage to which this vertex belongs.
        /// </summary>
        public readonly Stage Stage;

        /// <summary>
        /// Vertex identifier (unique within the same <see cref="Stage"/>).
        /// </summary>
        public readonly int VertexId;

        internal Scheduler scheduler = null;
        internal Scheduler Scheduler { set { scheduler = value; scheduler.Register(this, this.Stage.InternalComputation); } get { return this.scheduler; } }

        /// <summary>
        /// Indicates that the vertex has state to save
        /// </summary>
        internal virtual bool Stateful { get { return true; } }

        internal long currentCheckpointStart = 0;

        /// <summary>
        /// Checkpoints the vertex
        /// </summary>
        /// <param name="isMajor"></param>
        /// <returns></returns>
        internal virtual Pair<Stream, Pair<long, long>> Checkpoint(bool isMajor)
        {
            Stream stream = this.LoggingOutput;
            if (isMajor)
            {
                currentCheckpointStart = stream.Position;
                using (NaiadWriter writer = new NaiadWriter(stream, this.SerializationFormat))
                {
                    this.Checkpoint(writer);
                }
            }
            if (stream is FileStream)
                ((FileStream)stream).Flush(true);
            else
                stream.Flush();
            return new Pair<Stream, Pair<long, long>>(stream, new Pair<long, long>(currentCheckpointStart, stream.Position));
        }

        private Stream loggingOutput;

        /// <summary>
        /// The stream that will be used for logging updates to the vertex state.
        /// </summary>
        public Stream LoggingOutput
        {
            get
            {
                if (this.loggingOutput == null)
                {
                    this.loggingOutput = this.Stage.InternalComputation.Controller.GetLoggingOutputStream(this);
                    // Must start with a complete checkpoint of the stage, which should be cheap if we're empty.
                    this.Checkpoint(false);
                }
                return this.loggingOutput;
            }
        }

        /// <summary>
        /// Writes the state of this vertex to the given writer.
        /// </summary>
        /// <param name="writer">The writer.</param>
        protected abstract void Checkpoint(NaiadWriter writer);

        /// <summary>
        /// Restores the state of this vertex from the given reader.
        /// </summary>
        /// <param name="reader">The reader.</param>
        protected abstract void Restore(NaiadReader reader);

        private readonly string MyName;

        /// <summary>
        /// Returns a string representation of this vertex.
        /// </summary>
        /// <returns>A string representation of this vertex.</returns>
        public override string ToString()
        {
            return MyName;
        }

        /// <summary>
        /// Constructs a new Vertex with the given index in the given stage.
        /// </summary>
        /// <param name="index">Unique index of this vertex within the stage.</param>
        /// <param name="stage">Stage to which this vertex will belong.</param>
        internal Vertex(int index, Stage stage)
        {
            this.VertexId = index;

            this.Stage = stage;
            this.Scheduler = this.Stage.InternalComputation.Controller.Workers[this.Stage.Placement[this.VertexId].ThreadId];

            this.MyName = String.Format("{0}.[{1}]", stage.ToString().Remove(stage.ToString().Length - 1), this.VertexId);
            this.OnFlushActions = new List<Action>();
        }
    }

    /// <summary>
    /// Represents a single abstract vertex in a dataflow graph, with a time type that
    /// indicates its level of nesting in the graph.
    /// </summary>
    /// <remarks>
    /// Derived classes may override the <see cref="OnNotify"/> method to handle synchronous
    /// progress notifications for times in TTime, and simulate batch execution.
    /// </remarks>
    /// <typeparam name="TTime">The type of timestamps on messages that this vertex processes.</typeparam>
    public abstract class Vertex<TTime> : Dataflow.Vertex
        where TTime : Time<TTime>
    {
        private IVertexContext<TTime> vertexContext;

        internal IVertexContext<TTime> Context
        {
            get { return vertexContext; }
            set { this.vertexContext = value; }
        }

        private HashSet<TTime> OutstandingResumes = new HashSet<TTime>();
        private Microsoft.Research.Naiad.Runtime.Progress.ProgressUpdateBuffer<TTime> progressBuffer;

        /// <summary>
        /// Called after all messages and notifications have been delivered to this vertex.
        /// </summary>
        /// <remarks>
        /// Classes overriding this method must call <c>base.OnShutdown()</c>.
        /// </remarks>
        protected override void OnShutdown()
        {
            base.OnShutdown();
            OutstandingResumes = null;
            progressBuffer.Flush();    // should be unnecessary
            progressBuffer = null;
        }

        /// <summary>
        /// Requests notification after all messages bearing the given time or earlier have been delivered.
        /// </summary>
        /// <param name="time">The time.</param>
        public void NotifyAt(TTime time)
        {
            this.NotifyAt(time, time);
        }

        /// <summary>
        /// Requests a notification once all messages bearing the requirement time have been delivered, 
        /// with the capability to send messages at a different (potentially later) time.
        /// </summary>
        /// <param name="requirement">The requirement time.</param>
        /// <param name="capability">The capability time.</param>
        public void NotifyAt(TTime requirement, TTime capability)
        {
            if (!requirement.LessThan(capability))
                Console.Error.WriteLine("Requesting a notification with a requirement not less than the capability");
            else
            {
                System.Diagnostics.Debug.Assert(!this.IsShutDown);
                if (this.IsShutDown)
                    Console.Error.WriteLine("Scheduling {0} at {1} but already shut down", this, capability);

                if (!OutstandingResumes.Contains(capability))
                {
                    OutstandingResumes.Add(capability);

                    // do some progress magic 
                    progressBuffer.Update(capability, 1);
                    progressBuffer.Flush();

                    // inform the scheduler
                    this.Scheduler.EnqueueNotify(this, requirement, capability, true);
                }
            }
        }

        internal override void PerformAction(Scheduling.Scheduler.WorkItem workItem)
        {
            if (this.IsShutDown)
                Console.Error.WriteLine("Scheduling {0} at {1} but already shut down", this, workItem);
            else
            {
                var time = default(TTime).InitializeFrom(workItem.Requirement, workItem.Requirement.Timestamp.Length);

                OutstandingResumes.Remove(time);

                progressBuffer.Update(time, -1);

                this.Entrancy = this.Entrancy - 1;

                this.OnNotify(time);

                this.Entrancy = this.Entrancy + 1;

                this.Flush();
            }
        }

        /// <summary>
        /// Indicates that all messages bearing the given time (or earlier) have been delivered.
        /// </summary>
        /// <param name="time">The timestamp of the notification.</param>
        public virtual void OnNotify(TTime time)
        {
        }

        /// <summary>
        /// Writes the state of this vertex to the given writer.
        /// </summary>
        /// <param name="writer">The writer.</param>
        protected override void Checkpoint(NaiadWriter writer)
        {
            IList<Scheduler.WorkItem> workItems = this.Scheduler.GetWorkItemsForVertex(this);
            writer.Write(workItems.Count, this.SerializationFormat.GetSerializer<Int32>());
            foreach (Scheduler.WorkItem workItem in workItems)
                writer.Write(workItem.Requirement,  this.SerializationFormat.GetSerializer<Pointstamp>());
        }

        /// <summary>
        /// Restores the state of this vertex from the given reader.
        /// </summary>
        /// <param name="reader">The reader.</param>
        protected override void Restore(NaiadReader reader)
        {
            int workItemsCount = reader.Read<int>(this.SerializationFormat.GetSerializer<Int32>());
            for (int i = 0; i < workItemsCount; ++i)
            {
                Pointstamp pointstamp = reader.Read<Pointstamp>(this.SerializationFormat.GetSerializer<Pointstamp>());

                var time = default(TTime).InitializeFrom(pointstamp, pointstamp.Timestamp.Length);

                this.Scheduler.EnqueueNotify(this, time, time, false);    // could be set to true if we are sure this executes under the worker
            }
        }

        /// <summary>
        /// Constructs a new Vertex with the given index in the given stage.
        /// </summary>
        /// <param name="index">Unique index of this vertex within the stage.</param>
        /// <param name="stage">Stage to which this vertex will belong.</param>
        public Vertex(int index, Stage<TTime> stage)
            : base(index, stage)
        {
            this.progressBuffer = new Runtime.Progress.ProgressUpdateBuffer<TTime>(stage.StageId, this.scheduler.State(this.Stage.InternalComputation).Producer);
            this.AddOnFlushAction(() => { if (this.progressBuffer != null) this.progressBuffer.Flush(); });
        }
    }
}
