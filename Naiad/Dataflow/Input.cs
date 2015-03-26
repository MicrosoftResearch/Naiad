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
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using Microsoft.Research.Naiad.Runtime.FaultTolerance;
using Microsoft.Research.Naiad.Runtime.Progress;
using Microsoft.Research.Naiad.Dataflow.Channels;
using Microsoft.Research.Naiad.Serialization;
using Microsoft.Research.Naiad.Dataflow.StandardVertices;
using Microsoft.Research.Naiad.Frameworks;
using Microsoft.Research.Naiad.Scheduling;
using System.Diagnostics;

using Microsoft.Research.Naiad.Diagnostics;
using Microsoft.Research.Naiad.Input;

namespace Microsoft.Research.Naiad.Dataflow
{
    internal interface InputStage
    {
        /// <summary>
        /// Returns true if OnCompleted() has been called on the input.
        /// </summary>
        bool IsCompleted { get; }

        /// <summary>
        /// Returns the number of the pending epoch of input data.
        /// </summary>
        int CurrentEpoch { get; }

        /// <summary>
        /// Return the largest epoch known to be valid.
        /// </summary>
        int MaximumValidEpoch { get; }

        /// <summary>
        /// Returns the stage identifier.
        /// </summary>
        int InputId { get; }

        void BlockExternalCalls();
        void ReleaseExternalCalls();
        void ReMaterializeForRollback();
        IEnumerable<Pointstamp> InitialTimes { get; }
    }

    /// <summary>
    /// Represents a streaming input to a Naiad computation.
    /// </summary>
    /// <typeparam name="TRecord">The type of records accepted by this input.</typeparam>
    /// <typeparam name="TTime">The time of records emitted by this input.</typeparam>
    public interface StreamingInput<TRecord, TTime> where TTime : Time<TTime>
    {
        /// <summary>
        /// Indicates the local worker identifier.
        /// </summary>
        int WorkerId { get; }

        /// <summary>
        /// Introduces a batch of records at the same time.
        /// </summary>
        /// <param name="batch">records</param>
        /// <param name="time">time</param>
        /// <param name="fromThreadIndex">the sending thread, allowing cut-through</param>
        void OnStreamingRecv(TRecord[] batch, TTime time, int fromThreadIndex = -1);

        /// <summary>
        /// Indicates that no further records will appear at or before epoch.
        /// </summary>
        /// <param name="time">time</param>
        void OnStreamingNotify(TTime time);

        /// <summary>
        /// Increment a time
        /// </summary>
        /// <param name="time">time to increment</param>
        /// <returns>incremented time</returns>
        TTime IncrementOuter(TTime time);

        /// <summary>
        /// Indicates that no further records will appear.
        /// </summary>
        void OnCompleted();
    }

    /// <summary>
    /// Action for a streaming input vertex. This is public so it can be serialized for logging
    /// </summary>
    /// <typeparam name="S">record type</typeparam>
    /// <typeparam name="T">time type</typeparam>
    public struct Instruction<S, T> where T : Time<T>
    {
        /// <summary>
        /// time of input, where epoch is Int32.Max for completion
        /// </summary>
        public T Time;
        /// <summary>
        /// data being input, or null for a notification
        /// </summary>
        public S[] Payload;

        /// <summary>
        /// construct an Instruction action
        /// </summary>
        /// <param name="time">epoch of input, or Int32.Max for completion</param>
        /// <param name="payload">data being input, or null for a notification</param>
        public Instruction(T time, S[] payload)
        {
            this.Time = time;
            this.Payload = payload;
        }
    }

    internal class StreamingInputVertex<S, T> : Dataflow.Vertex<T>, StreamingInput<S, T> where T : Time<T>
    {
        private T currentVertexHold = default(T);
        internal int CurrentEpoch { get { return this.EpochOf(this.currentVertexHold); } }

        internal T maximumValidEpoch;
        internal int MaximumValidEpoch { get { return this.EpochOf(this.maximumValidEpoch); } }

        internal int EpochOf(T time)
        {
            return time.ToPointstamp(this.Stage.StageId).Timestamp[0];
        }

        public bool LexThan(T a, T b)
        {
            var aStamp = a.ToPointstamp(0);
            var bStamp = b.ToPointstamp(0);
            return FTFrontier.IsLessThanOrEqualTo(aStamp, bStamp);
        }

        public T IncrementOuter(T time)
        {
            Pointstamp newStamp = time.ToPointstamp(this.Stage.StageId);
            int position = newStamp.Timestamp.Length-1;
            while (newStamp.Timestamp[position] == int.MaxValue)
            {
                if (position == 0)
                {
                    throw new ApplicationException("Can't increment past completed");
                }
                newStamp.Timestamp[position] = 0;
                --position;
            }
            ++newStamp.Timestamp[position];
            return default(T).InitializeFrom(newStamp, newStamp.Timestamp.Length);
        }

        internal T InvalidTime
        {
            get
            {
                Pointstamp stamp = default(T).ToPointstamp(this.Stage.StageId);
                stamp.Timestamp[0] = -1;
                return default(T).InitializeFrom(stamp, stamp.Timestamp.Length);
            }
        }

        internal T CompletedTime
        {
            get
            {
                Pointstamp stamp = default(T).ToPointstamp(this.Stage.StageId);
                stamp.Timestamp[0] = int.MaxValue;
                return default(T).InitializeFrom(stamp, stamp.Timestamp.Length);
            }
        }

        private bool isCompleted = false;
        internal bool IsCompleted { get { return this.isCompleted; } }

        public int WorkerId { get { return this.Stage.Placement.Single(x => x.VertexId == this.VertexId).ThreadId; } }

        /// <summary>
        /// Turn on checkpointing/logging for this vertex
        /// </summary>
        /// <param name="streamSequenceFactory">a factory to create streams for logging</param>
        internal override void EnableLogging(Func<string, IStreamSequence> streamSequenceFactory)
        {
            this.Checkpointer = Checkpointer<T>.Create(this, streamSequenceFactory);
            this.notificationLogger = this.Checkpointer.CreateNotificationLogger(
                (e) => { throw new ApplicationException("Input must use stateless logger"); });
            this.inputLogger = this.Checkpointer.CreateInputLogger<S>(this.ReplayInput);

            foreach (StageOutputForVertex<T> stageOutput in this.TypedStage.StageOutputs)
            {
                stageOutput.EnableLogging(this.VertexId);
            }
        }

        private System.Collections.Concurrent.ConcurrentQueue<Instruction<S, T>> inputQueue;
        private IInputLogger<S, T> inputLogger;

        internal readonly VertexOutputBuffer<S, T> output;

        internal void UpdateLexicalRecordCounts(T time, long count)
        {
            Pointstamp stamp = time.ToPointstamp(this.Stage.StageId);
            for (int i=stamp.Timestamp.Length; i>0; --i)
            {
                if (i < stamp.Timestamp.Length)
                {
                    if (stamp.Timestamp[i-1] < int.MaxValue)
                    {
                        ++stamp.Timestamp[i - 1];
                    }
                    stamp.Timestamp[i] = 0;
                }
                this.scheduler.State(this.Stage.InternalComputation).Producer.UpdateRecordCounts(stamp, count);
            }
        }

        private void PerformInstruction(Instruction<S, T> instruction)
        {
            if (this.LoggingEnabled)
            {
                this.inputLogger.LogInputProcessing(instruction.Time, instruction.Payload);
            }

            bool considerCheckpointing = false;

            if (instruction.Time.Equals(this.CompletedTime))
            {
                Logging.Progress("[{0}] Performing OnCompleted", this.Stage.ToString());

                // OnCompleted logic.
                lock (this)
                {
                    if (!this.isCompleted)
                    {
                        this.UpdateLexicalRecordCounts(this.currentVertexHold, -1);
                        this.isCompleted = true;
                    }
                    else
                    {
                        Logging.Error("WARNING: input ignored redundant shutdown when already shutdown.");
                    }
                }

                considerCheckpointing = this.LoggingEnabled;
            }
            else if (instruction.Payload == null)
            {
                Logging.Progress("[{1}] Performing OnNotify({0})", instruction.Time, this.Stage.ToString());

                // OnNotify logic.
                lock (this)
                {
                    if (this.LexThan(this.maximumValidEpoch, instruction.Time))
                    {
                        this.maximumValidEpoch = instruction.Time;
                    }

                    if (this.LexThan(this.currentVertexHold, instruction.Time))
                    {
                        T newHold = this.IncrementOuter(instruction.Time);
                        this.UpdateLexicalRecordCounts(newHold, +1);
                        this.UpdateLexicalRecordCounts(this.currentVertexHold, -1);
                        this.currentVertexHold = newHold;
                    }
                    else
                    {
                        Logging.Error("WARNING: input ignored redundant notification for epoch {0} when current epoch was {1}.", instruction.Time, this.currentVertexHold);
                    }
                }

                considerCheckpointing = this.LoggingEnabled;
            }
            else
            {
                // XXX : Getting called a lot, resulting in lots of object allocations
                // Logging.Progress("[{0}] Performing OnRecv", this.Stage.ToString());

                // OnRecv logic.
                lock (this)
                {
                    this.SendBatch(instruction);
                }
            }

            if (considerCheckpointing)
            {
                // do this after flush so we correctly record any discarded messages
                this.Checkpointer.ConsiderCheckpointing(instruction.Time);
            }
        }

        private void SendBatch(Instruction<S, T> instruction)
        {
            if (this.LexThan(this.maximumValidEpoch, instruction.Time))
            {
                this.maximumValidEpoch = instruction.Time;
            }

            if (this.LexThan(this.currentVertexHold, instruction.Time))
            {
                this.PushEventTime(instruction.Time);

                var output = this.output.GetBufferForTime(instruction.Time);
                for (int i = 0; i < instruction.Payload.Length; ++i)
                {
                    output.Send(instruction.Payload[i]);
                }
                this.Flush();

                this.PopEventTime();
            }
            else
            {
                Logging.Error("WARNING: input ignored invalid data for epoch {0} when current epoch was {1}", instruction.Time, this.currentVertexHold);
            }
        }

        internal override void RestoreProgressUpdates(bool fromCheckpoint)
        {
            if (fromCheckpoint)
            {
                Pair<bool, T> maxSendTime =
                    this.Checkpointer.RepairInputProgress(this.scheduler.State(this.Stage.InternalComputation).Producer);

                T maxSendEpoch = this.InvalidTime;
                if (maxSendTime.First)
                {
                    maxSendEpoch = maxSendTime.Second;
                }

                Pointstamp? maxCompleted = this.inputLogger.MaxCompleted();
                if (maxCompleted.HasValue)
                {
                    T maxCollectedTime = default(T).InitializeFrom(maxCompleted.Value, maxCompleted.Value.Timestamp.Length);
                    if (maxCollectedTime.Equals(this.CompletedTime))
                    {
                        this.currentVertexHold = maxCollectedTime;
                    }
                    else
                    {
                        this.currentVertexHold = this.IncrementOuter(maxCollectedTime);
                    }

                    if (this.LexThan(maxCollectedTime, maxSendEpoch))
                    {
                        this.maximumValidEpoch = maxSendEpoch;
                    }
                    else
                    {
                        this.maximumValidEpoch = maxCollectedTime;
                    }
                }
                else
                {
                    this.currentVertexHold = default(T);
                    this.maximumValidEpoch = maxSendEpoch;
                }

                this.isCompleted = (this.currentVertexHold.Equals(this.CompletedTime));

                // just put back the hold at maxCollected since the replay will move it forward again
                this.UpdateLexicalRecordCounts(this.currentVertexHold, 1);
            }
            else
            {
                if (!this.isCompleted)
                {
                    this.UpdateLexicalRecordCounts(this.currentVertexHold, 1);
                }
            }
        }

        internal override void PerformAction(Scheduling.Scheduler.WorkItem workItem)
        {
            var time = default(T).InitializeFrom(workItem.Requirement, workItem.Requirement.Timestamp.Length);

            if (!default(T).LessThan(time))
            {
                ReplayMode mode = (ReplayMode)workItem.Requirement.Timestamp.a;
                this.PerformReplayAction(mode);
                return;
            }

            Instruction<S, T> nextInstruction;
            bool success = inputQueue.TryDequeue(out nextInstruction);
            Debug.Assert(success);

            this.PerformInstruction(nextInstruction);
        }

        internal override int Replay(ReplayMode mode)
        {
            lock (this)
            {
                Pointstamp fakePointStamp = default(T).ToPointstamp(-1);
                fakePointStamp.Timestamp[0] = (int)mode;
                T fakeTime = default(T).InitializeFrom(fakePointStamp, fakePointStamp.Timestamp.Length);
                return this.Scheduler.EnqueueNotify(this, fakeTime, fakeTime, fakeTime, false, false);
            }
        }

        public void OnStreamingRecv(S[] batch, T time, int fromThreadIndex)
        {
            Pointstamp stamp = time.ToPointstamp(0);
            if (stamp.Timestamp[0] < 0)
                throw new ApplicationException("?");
            lock (this)     // this is probably already under a lock, but just to be safe...
            {
                if (fromThreadIndex == this.Scheduler.Index)
                {
                    this.SendBatch(new Instruction<S, T>(time, batch));
                }
                else
                {
                    this.inputQueue.Enqueue(new Instruction<S, T>(time, batch));
                    if (this.LoggingEnabled)
                    {
                        this.inputLogger.LogInput(time, batch);
                    }
                    scheduler.EnqueueNotify(this, this.currentVertexHold, this.currentVertexHold, false, false);
                }
            }
        }

        public void OnStreamingNotify(T time)
        {
            lock (this)
            {
                this.inputQueue.Enqueue(new Instruction<S, T>(time, null));
                if (this.LoggingEnabled)
                {
                    this.inputLogger.LogInput(time, null);
                }
                scheduler.EnqueueNotify(this, this.currentVertexHold, this.currentVertexHold, false, false);
            }
        }

        public void ReplayInput(T time, S[] batch)
        {
            lock (this)
            {
                this.inputQueue.Enqueue(new Instruction<S, T>(time, batch));
                scheduler.EnqueueNotify(this, this.currentVertexHold, this.currentVertexHold, false, false);
            }
        }

        public void OnCompleted()
        {
            this.OnStreamingNotify(this.CompletedTime);
        }

        internal override void TransferLogging(Vertex<T> oldVertex)
        {
            StreamingInputVertex<S, T> oldInput = oldVertex as StreamingInputVertex<S, T>;

            this.Checkpointer = oldInput.Checkpointer;
            this.notificationLogger = this.Checkpointer.CreateNotificationLogger(
                (e) => { throw new ApplicationException("Input must use stateless logger"); });
            this.Checkpointer.UpdateVertexForRollback(this);
            this.inputLogger = oldInput.inputLogger;
            this.inputLogger.UpdateReplayAction(this.ReplayInput);
        }

        internal StreamingInputVertex(int index, Stage<T> stage)
            : base(index, stage)
        {
            this.maximumValidEpoch = this.InvalidTime;
            this.inputQueue = new System.Collections.Concurrent.ConcurrentQueue<Instruction<S, T>>();
            this.output = new VertexOutputBuffer<S,T>(this);
        }

        internal static Stream<S, T> MakeStage(DataSource<S, T> source, InternalComputation internalComputation, Placement placement, string inputName)
        {
            var stage = new StreamingInputStage<S, T>(source, placement, internalComputation, inputName);

            return stage;
        }
    }

    internal class StreamingInputStage<R, T> : InputStage where T : Time<T>
    {
        private readonly Placement placement;
        private readonly DataSource<R, T> source;
        private StreamingInputVertex<R, T>[] localVertices;

        public void BlockExternalCalls()
        {
            this.source.BlockExternalCalls();
        }

        public void ReleaseExternalCalls()
        {
            this.source.ReleaseExternalCalls();
        }

        internal StreamingInputVertex<R, T> GetInputVertex(int vertexId)
        {
            foreach (var vertex in localVertices)
                if (vertexId == vertex.VertexId)
                    return vertex;

            throw new Exception(String.Format("Vertex {0} not found in Input {1} on Process {2}", vertexId, stage.StageId, stage.InternalComputation.Controller.Configuration.ProcessID));
        }

        public int InputId { get { return this.stage.StageId; } }

        private bool completedCalled;
        public bool Completed { get { return this.completedCalled; } }

        private bool hasActivatedProgressTracker;

        private readonly Stream<R, T> output;
        public Stream<R, T> Output { get { return output; } }

        public static implicit operator Stream<R, T>(StreamingInputStage<R, T> stage) { return stage.Output; }

        internal InternalComputation InternalComputation { get { return this.stage.InternalComputation; } }
        public Placement Placement { get { return this.stage.Placement; } }

        private readonly string inputName;
        public string InputName { get { return this.inputName; } }

        public override string ToString()
        {
            return this.InputName;
        }

        private readonly Stage<StreamingInputVertex<R, T>, T> stage;

        public int CurrentEpoch { get { return this.localVertices.Min(x => x.CurrentEpoch); } }
        public int MaximumValidEpoch { get { return this.localVertices.Max(x => x.MaximumValidEpoch); } }

        public bool IsCompleted { get { return this.localVertices.All(x => x.IsCompleted); } }

        public Type RecordType { get { return typeof(R); } }

        public bool Stateful { get { return false; } }
        public void Checkpoint(NaiadWriter writer) { throw new NotImplementedException(); }
        public void Restore(NaiadReader reader) { throw new NotImplementedException(); } 

        internal StreamingInputStage(DataSource<R, T> source, Placement placement, InternalComputation internalComputation, string inputName)
        {
            this.inputName = inputName;

            this.stage = Foundry.NewStage<StreamingInputVertex<R, T>, T>(new StreamContext(internalComputation), (i, v) => new StreamingInputVertex<R, T>(i, v), this.inputName);
            this.stage.SetCheckpointType(CheckpointType.Stateless);

            this.output = stage.NewOutput(vertex => vertex.output);

            this.stage.Materialize();

            this.placement = placement;
            this.localVertices = this.placement.Where(x => x.ProcessId == internalComputation.Controller.Configuration.ProcessID)
                                        .Select(x => this.stage.GetVertex(x.VertexId) as StreamingInputVertex<R, T>)
                                        .ToArray();

            this.source = source;
            this.source.RegisterInputs(this.localVertices);

            this.completedCalled = false;
            this.hasActivatedProgressTracker = false;

            // results in pointstamp comparisons which assert w/o this.
            this.InternalComputation.Reachability.UpdateReachabilityPartialOrder(internalComputation);
            this.InternalComputation.Reachability.DoNotImpersonate(stage.StageId);

            var initialVersions = this.InitialTimes.ToArray();

            internalComputation.ProgressTracker.BroadcastProgressUpdate(initialVersions[0], placement.Count);
            foreach (var vertex in this.localVertices)
            {
                for (int i = 1; i < initialVersions.Length; ++i)
                {
                    vertex.scheduler.State(this.InternalComputation).Producer.UpdateRecordCounts(initialVersions[i], 1);
                }
            }
        }

        public IEnumerable<Pointstamp> InitialTimes
        {
            get
            {
                Pointstamp stamp = default(T).ToPointstamp(stage.StageId);
                for (int i = stamp.Timestamp.Length; i > 0; --i)
                {
                    if (i < stamp.Timestamp.Length)
                    {
                        if (stamp.Timestamp[i - 1] < int.MaxValue)
                        {
                            ++stamp.Timestamp[i - 1];
                        }
                        stamp.Timestamp[i] = 0;
                    }
                    yield return stamp;
                }
            }
        }

        public void ReMaterializeForRollback()
        {
            this.localVertices = this.placement.Where(x => x.ProcessId == this.stage.InternalComputation.Controller.Configuration.ProcessID)
                                        .Select(x => this.stage.GetVertex(x.VertexId) as StreamingInputVertex<R, T>)
                                        .ToArray();

            this.source.RegisterInputs(this.localVertices);
        }

        private void EnsureProgressTrackerActivated()
        {
            if (!this.hasActivatedProgressTracker)
            {
                stage.InternalComputation.Activate();
                this.hasActivatedProgressTracker = true;
            }
        }

    }
}
