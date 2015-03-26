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

using Microsoft.Research.Naiad.Serialization;
using Microsoft.Research.Naiad.Dataflow.Channels;
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
using Microsoft.Research.Naiad.Runtime.FaultTolerance;

namespace Microsoft.Research.Naiad.Dataflow
{
    internal interface IReachabilityListener
    {
        void UpdateReachability(List<Pointstamp> pointstamps);
    }

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

        internal abstract void ConsiderCheckpointing(List<Pointstamp> reachabilityPointstamps);

        internal abstract void SendInstantaneousFaultToleranceFrontier();

        internal enum ReplayMode
        {
            RollbackVertex = -1,
            ReplayVertex = -2,
            SendDeferredMessages = -3,
        }

        internal abstract int Replay(ReplayMode mode);

        internal abstract void SetRollbackFrontier();

        internal abstract void RestoreProgressUpdates(bool fromCheckpoint);

        private List<IReachabilityListener> reachabilityListeners = new List<IReachabilityListener>();
        internal void AddReachabilityListener(IReachabilityListener listener)
        {
            this.reachabilityListeners.Add(listener);
        }
    
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
            foreach (IReachabilityListener listener in this.reachabilityListeners)
            {
                listener.UpdateReachability(pointstamps);
            }

            this.ConsiderCheckpointing(pointstamps);

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
        public abstract Stage Stage { get; }

        /// <summary>
        /// Vertex identifier (unique within the same <see cref="Stage"/>).
        /// </summary>
        public readonly int VertexId;

        internal Scheduler scheduler = null;
        internal Scheduler Scheduler { private set { scheduler = value; } get { return this.scheduler; } }

        /// <summary>
        /// Indicates that the vertex has state to save
        /// </summary>
        internal virtual bool Stateful { get { return true; } }

        internal long currentCheckpointStart = 0;

        /// <summary>
        /// Checkpoints the vertex
        /// </summary>
        /// <param name="isMajor"></param>
        /// <param name="stream"></param>
        /// <returns></returns>
        internal virtual Pair<Stream, Pair<long, long>> Checkpoint(bool isMajor, Stream stream)
        {
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
        /// <param name="stage">Stage the vertex belongs to</param>
        internal Vertex(int index, Stage stage)
        {
            this.VertexId = index;

            this.Scheduler = stage.InternalComputation.Controller.Workers[stage.Placement[this.VertexId].ThreadId];
            this.Scheduler.Register(this, stage);

            this.MyName = String.Format("{0}.[{1}]]", stage.ToString().Remove(stage.ToString().Length - 1), this.VertexId);
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
        private HashSet<TTime> OutstandingResumes = new HashSet<TTime>();
        private Microsoft.Research.Naiad.Runtime.Progress.ProgressUpdateBuffer<TTime> progressBuffer;
        internal Checkpointer<TTime> Checkpointer;
        internal INotificationLogger<TTime> notificationLogger;
        internal readonly Stage<TTime> TypedStage;

        /// <summary>
        /// The stage to which this vertex belongs.
        /// </summary>
        public override Stage Stage
        {
            get { return this.TypedStage; }
        }

        private HashSet<FlushableChannel> deferredSendChannels = new HashSet<FlushableChannel>();
        private List<Action> deferredSendActions = new List<Action>();
        internal void AddDeferredSendAction(FlushableChannel channel, Action sendMessage)
        {
            if (!this.deferredSendChannels.Contains(channel))
            {
                // we are adding the first deferred action: put a time 'hold' on the vertex that won't be released until after
                // the action is performed
                this.deferredSendChannels.Add(channel);
            }
            this.deferredSendActions.Add(sendMessage);
        }

        /// <summary>
        /// Turn on checkpointing/logging for this vertex
        /// </summary>
        /// <param name="streamSequenceFactory">a factory to create streams for logging</param>
        internal virtual void EnableLogging(Func<string, IStreamSequence> streamSequenceFactory)
        {
            this.Checkpointer = Checkpointer<TTime>.Create(this, streamSequenceFactory);
            this.notificationLogger = this.Checkpointer.CreateNotificationLogger(this.DeliverLoggedNotification);

            foreach (StageOutputForVertex<TTime> stageOutput in this.TypedStage.StageOutputs)
            {
                stageOutput.EnableLogging(this.VertexId);
            }
        }
        internal bool LoggingEnabled { get { return (this.notificationLogger != null); } }

        internal virtual void TransferLogging(Vertex<TTime> oldVertex)
        {
            this.Checkpointer = oldVertex.Checkpointer;
            this.notificationLogger = this.Checkpointer.CreateNotificationLogger(this.DeliverLoggedNotification);
            this.Checkpointer.UpdateVertexForRollback(this);
            this.Checkpointer.UpdateNotifier(this.DeliverLoggedNotification);
        }

        /// <summary>
        /// Called by the logging mechanism whenever a new frontier has passed: the vertex will
        /// never roll back beyond this frontier
        /// </summary>
        /// <param name="frontier">low watermark frontier</param>
        public virtual void NotifyGarbageCollectionFrontier(Pointstamp[] frontier)
        {
        }

        private readonly Stack<TTime> eventTimeStack = new Stack<TTime>();
        internal TTime CurrentEventTime
        {
            get { return this.eventTimeStack.Peek(); }
        }
        internal void PushEventTime(TTime time)
        {
            this.eventTimeStack.Push(time);
        }
        internal TTime PopEventTime()
        {
            return this.eventTimeStack.Pop();
        }

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
            if (this.Checkpointer != null)
            {
                this.Checkpointer.ShutDown();
            }
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

                if (this.Stage.InternalComputation.IsRestoring &&
                    this.TypedStage.CurrentCheckpoint(this.VertexId).RestorationContainsTime(requirement))
                {
                    //Console.WriteLine("Suppressing logged notification request at " + this.Stage.StageId + "." + this.VertexId + " for " + requirement.ToString() + "," + capability.ToString());
                    return;
                }

                if (!OutstandingResumes.Contains(capability))
                {
                    OutstandingResumes.Add(capability);

                    if (this.LoggingEnabled && !this.Stage.InternalComputation.IsRestoring)
                    {
                        this.notificationLogger.LogNotificationRequest(this.CurrentEventTime, requirement, capability);
                    }

                    // do some progress magic 
                    progressBuffer.Update(capability, 1);
                    progressBuffer.Flush();

                    // inform the scheduler
                    this.Scheduler.EnqueueNotify(this, this.CurrentEventTime, requirement, capability, true, true);
                }
            }
        }

        internal virtual void DeliverLoggedNotification(TTime time)
        {
            this.PushEventTime(time);

            this.OnNotify(time);

            TTime poppedTime = this.PopEventTime();
            if (poppedTime.CompareTo(time) != 0)
            {
                throw new ApplicationException("Time stack mismatch");
            }
        }

        internal override void PerformAction(Scheduling.Scheduler.WorkItem workItem)
        {
            var time = default(TTime).InitializeFrom(workItem.Requirement, workItem.Requirement.Timestamp.Length);
            bool considerCheckpointing = false;

            if (!default(TTime).LessThan(time))
            {
                this.Entrancy = this.Entrancy - 1;

                // this is a fake work item triggering replay
                ReplayMode mode = (ReplayMode)workItem.Requirement.Timestamp.a;
                this.PerformReplayAction(mode);

                this.Entrancy = this.Entrancy + 1;
            }
            else
            {
                if (this.IsShutDown)
                {
                    Console.Error.WriteLine("Scheduling {0} at {1} but already shut down", this, workItem.Requirement);
                }
                else
                {
                    this.Entrancy = this.Entrancy - 1;

                    OutstandingResumes.Remove(time);

                    progressBuffer.Update(time, -1);

                    this.PushEventTime(time);

                    if (this.LoggingEnabled && !this.Stage.InternalComputation.IsRestoring)
                    {
                        // if we are restoring then by definition we are replaying from a log entry and don't need to re-log it
                        this.notificationLogger.LogNotification(time);
                    }

                    this.OnNotify(time);

                    // if the notification didn't request another notification for the same time, then the time is complete and we
                    // may want to checkpoint
                    if (this.LoggingEnabled && !OutstandingResumes.Contains(time))
                    {
                        considerCheckpointing = true;
                    }

                    TTime poppedTime = this.PopEventTime();
                    if (poppedTime.CompareTo(time) != 0)
                    {
                        throw new ApplicationException("Time stack mismatch");
                    }

                    this.Entrancy = this.Entrancy + 1;
                }
            }

            this.Flush();

            if (considerCheckpointing)
            {
                // do this after flush so we correctly record any discarded messages
                this.Checkpointer.ConsiderCheckpointing(time);
            }
        }

        internal override int Replay(ReplayMode mode)
        {
            Pointstamp fakePointStamp = default(TTime).ToPointstamp(-1);
            fakePointStamp.Timestamp[0] = (int)mode;
            TTime fakeTime = default(TTime).InitializeFrom(fakePointStamp, fakePointStamp.Timestamp.Length);
            return this.Scheduler.EnqueueNotify(this, fakeTime, fakeTime, fakeTime, false, false);
        }

        internal override void SetRollbackFrontier()
        {
            this.Checkpointer.SetRetentionFrontier();
        }

        internal override void RestoreProgressUpdates(bool fromCheckpoint)
        {
            if (fromCheckpoint)
            {
                this.Checkpointer.RepairProgress();
            }
            else
            {
                throw new ApplicationException("Can't restore vertex without checkpoint");
            }
        }

        internal void PerformReplayAction(ReplayMode mode)
        {
            switch (mode)
            {
                case ReplayMode.RollbackVertex:
                    this.Checkpointer.RestoreToFrontier();
                    break;

                case ReplayMode.ReplayVertex:
                    this.Checkpointer.ReplayVertex();
                    break;

                case ReplayMode.SendDeferredMessages:
                    foreach (Action action in this.deferredSendActions)
                    {
                        action();
                    }
                    this.deferredSendActions.Clear();
                    foreach (FlushableChannel channel in this.deferredSendChannels)
                    {
                        channel.Flush();
                    }
                    this.deferredSendChannels.Clear();
                    if (this.Checkpointer != null)
                    {
                        this.Checkpointer.ResendLoggedMessages();
                    }
                    break;
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

                this.Scheduler.EnqueueNotify(this, time, time, time, true, false);    // could be set to true if we are sure this executes under the worker
            }
        }

        internal override void ConsiderCheckpointing(List<Pointstamp> reachablePointstamps)
        {
            if (this.LoggingEnabled)
            {
                this.Checkpointer.ConsiderCheckpointing(reachablePointstamps);
            }
        }

        internal override void SendInstantaneousFaultToleranceFrontier()
        {
            if (this.Stage.CheckpointType != CheckpointType.None)
            {
                this.Checkpointer.SendInstantaneousFaultToleranceFrontier();
            }
        }

        internal bool CanRollBackInsteadOfRestarting(Pointstamp[] frontier)
        {
            return this.CanRollBackPreservingState(frontier);
        }

        internal bool MustRollBackInsteadOfRestarting(Pointstamp[] frontier)
        {
            return this.MustRollBackPreservingState(frontier);
        }

        internal void NotifyPotentialRollbackRange(ICheckpoint<TTime> stableRange, ICheckpoint<TTime> rollbackRange)
        {
            this.SetPotentialRollbackRange(stableRange, rollbackRange);
        }

        internal void WriteCheckpoint(NaiadWriter writer, ICheckpoint<TTime> checkpoint)
        {
            this.Checkpoint(writer, checkpoint);
        }

        internal void StartRestoration(Pointstamp[] frontier)
        {
            this.InitializeRestoration(frontier);
        }

        internal void ReadPartialCheckpoint(NaiadReader reader, ICheckpoint<TTime> checkpoint)
        {
            this.RestorePartialCheckpoint(reader, checkpoint);
        }

        internal bool MustStartFullCheckpoint(ICheckpoint<TTime> candidateIncrementalCheckpoint)
        {
            return !this.NextCheckpointIsIncremental(candidateIncrementalCheckpoint);
        }

        /// <summary>
        /// Returns true if the vertex can roll back to the specified frontier by deleting internal state, rather
        /// than restoring from a checkpoint. For stateless vertices this returns false by default, for simplicity,
        /// since restoring from empty checkpoints is assumed to be easy
        /// </summary>
        /// <param name="frontier">proposed rollback frontier</param>
        /// <returns>true if the vertex can roll back without being recreated</returns>
        protected virtual bool CanRollBackPreservingState(Pointstamp[] frontier)
        {
            return false;
        }

        /// <summary>
        /// Returns true if the vertex must roll back to the specified frontier by deleting internal state, rather
        /// than restoring from a checkpoint, even when the controller has failed. This is true for inputs and outputs
        /// which need to preserve their state related to the external caller
        /// </summary>
        /// <param name="frontier">proposed rollback frontier</param>
        /// <returns>true if the vertex can roll back without being recreated</returns>
        protected virtual bool MustRollBackPreservingState(Pointstamp[] frontier)
        {
            return false;
        }

        /// <summary>
        /// Prune internal state to include only events within a rollback frontier
        /// </summary>
        /// <param name="frontier">the frontier to roll back to</param>
        /// <param name="lastFullCheckpoint">the last full checkpoint contained within frontier</param>
        /// <param name="lastIncrementalCheckpoint">the last incremental checkpoint contained within frontier</param>
        public virtual void RollBackPreservingState(Pointstamp[] frontier, ICheckpoint<TTime> lastFullCheckpoint, ICheckpoint<TTime> lastIncrementalCheckpoint)
        {
            this.OutstandingResumes.Clear();
        }

        /// <summary>
        /// this is called by the checkpointing machinery to tell the vertex about what times may be rolled back.
        /// In general it contains all the times that are in the most recent checkpoint but are not in the most
        /// recent 'low watermark' checkpoint beyond which we will not roll back
        /// </summary>
        /// <param name="stableRange">the range of times that will never be rolled back</param>
        /// <param name="rollbackRange">the range of times that have been checkpointed but may be rolled back</param>
        protected virtual void SetPotentialRollbackRange(ICheckpoint<TTime> stableRange, ICheckpoint<TTime> rollbackRange)
        {
        }

        /// <summary>
        /// Return true if the vertex is happy with the next checkpoint being incremental instead of a
        /// full checkpoint
        /// </summary>
        /// <param name="candidateIncrementalCheckpoint">checkpoint that will be made</param>
        /// <returns>true if the vertex plans to make an incremental checkpoint</returns>
        protected virtual bool NextCheckpointIsIncremental(ICheckpoint<TTime> candidateIncrementalCheckpoint)
        {
            throw new NotImplementedException();
        }

        /// <summary>
        /// Writes the state of this vertex to the given writer.
        /// </summary>
        /// <param name="writer">The writer.</param>
        /// <param name="checkpoint">Object describing what subset of state should be checkpointed</param>
        protected virtual void Checkpoint(NaiadWriter writer, ICheckpoint<TTime> checkpoint)
        {
            throw new NotImplementedException();
        }

        /// <summary>
        /// Notifies the vertex that it is being rolled back to the frontier. If the vertex belongs to a stateful-checkpointing
        /// stage, this is followed by a sequence of calls to RestoreCompacted, each of which restores
        /// a partial checkpoint
        /// </summary>
        /// <param name="frontier">the restoration frontier</param>
        protected virtual void InitializeRestoration(Pointstamp[] frontier)
        {
        }

        /// <summary>
        /// Restores the state of this vertex from the given reader after checkpointing a subset.
        /// </summary>
        /// <param name="reader">The reader.</param>
        /// <param name="checkpoint">Object describing the subset of the state that is in the checkpoint</param>
        protected virtual void RestorePartialCheckpoint(NaiadReader reader, ICheckpoint<TTime> checkpoint)
        {
            throw new NotImplementedException();
        }

        /// <summary>
        /// Constructs a new Vertex with the given index in the given stage.
        /// </summary>
        /// <param name="index">Unique index of this vertex within the stage.</param>
        /// <param name="stage">Stage to which this vertex will belong.</param>
        public Vertex(int index, Stage<TTime> stage) : base(index, stage)
        {
            this.TypedStage = stage;
            this.progressBuffer = new Runtime.Progress.ProgressUpdateBuffer<TTime>(stage.StageId, this.scheduler.State(this.Stage.InternalComputation).Producer);
            this.AddOnFlushAction(() => { if (this.progressBuffer != null) this.progressBuffer.Flush(); });
        }
    }
}
