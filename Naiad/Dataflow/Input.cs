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
    }

    /// <summary>
    /// TODO this class is deprecated by virtue of StreamingInputStage. Still used by Reporting, but that can be fixed.
    /// </summary>
    internal class InputStage<TRecord> : InputStage, IObserver<IEnumerable<TRecord>>, IObserver<TRecord>
    {
        private readonly KeyValuePair<int,InputVertex<TRecord>>[] localVertices;

        internal InputVertex<TRecord> GetInputVertex(int vertexId) 
        {
            foreach (var vertex in localVertices)
                if (vertexId == vertex.Key)
                    return vertex.Value;

            throw new Exception(String.Format("Vertex {0} not found in Input {1} on Process {2}", vertexId, stage.StageId, stage.InternalComputation.Controller.Configuration.ProcessID));
        }

        public int InputId { get { return this.stage.StageId; } }

        private int currentEpoch;

        private bool completedCalled;
        public bool Completed { get { return this.completedCalled; } }

        private bool hasActivatedProgressTracker;

        private readonly Stream<TRecord, Epoch> output;
        public Stream<TRecord, Epoch> Stream { get { return output; } }

        public static implicit operator Stream<TRecord, Epoch>(InputStage<TRecord> stage) { return stage.Stream; }

        public TimeContext<Epoch> Context { get { return this.stage.Context; } }
        internal InternalComputation InternalComputation { get { return this.stage.InternalComputation; } }
        public Placement Placement { get { return this.stage.Placement; } }

        private readonly string inputName;
        public string InputName { get { return this.inputName; } }

        private readonly Stage<InputVertex<TRecord>, Epoch> stage;

        internal InputStage(Placement placement, InternalComputation internalComputation, string inputName)
        {
            this.inputName = inputName;

            stage = Foundry.NewStage(new TimeContext<Epoch>(internalComputation.ContextManager.RootContext), (i, v) => new InputVertex<TRecord>(i, v), this.inputName);

            this.output = stage.NewOutput(vertex => vertex.Output);

            stage.Materialize();

            this.localVertices = placement.Where(x => x.ProcessId == internalComputation.Controller.Configuration.ProcessID)
                                        .Select(x => new KeyValuePair<int, InputVertex<TRecord>>(x.VertexId, stage.GetVertex(x.VertexId) as InputVertex<TRecord>))
                                        .ToArray();

            this.completedCalled = false;
            this.hasActivatedProgressTracker = false;
            this.currentEpoch = 0;

            // results in pointstamps comparisons which assert w/o this.
            this.InternalComputation.Reachability.UpdateReachabilityPartialOrder(internalComputation);
            this.InternalComputation.Reachability.DoNotImpersonate(stage.StageId);

            var initialVersion = new Runtime.Progress.Pointstamp(stage.StageId, new int[] { 0 });

            //if (this.Controller.Configuration.Impersonation)
            //{
            //    foreach (var version in Reachability.EnumerateImpersonations(initialVersion))
            //        controller.BroadcastUpdate(version, placement.Count);
            //}
            //else
                
            internalComputation.ProgressTracker.BroadcastProgressUpdate(initialVersion, placement.Count);
        }

        private void EnsureProgressTrackerActivated()
        {
            if (!this.hasActivatedProgressTracker)
            {
                stage.InternalComputation.Activate();
                this.hasActivatedProgressTracker = true;
            }
        }

        public void OnNext()
        {
            this.OnNext(new TRecord[] { });
        }

        public void OnNext(TRecord record)
        {
            this.OnNext(new[] { record });
        }

        public void OnNext(IEnumerable<TRecord> batch)
        {
            //Debug.Assert(!this.completedCalled);
            this.EnsureProgressTrackerActivated();

            var array = batch == null ? new TRecord[] { } : batch.ToArray();
            lock (this)
            {
                var arrayCursor = 0;
                for (int i = 0; i < this.localVertices.Length; i++)
                {
                    var toEat = (array.Length / this.localVertices.Length) + ((i < (array.Length % this.localVertices.Length)) ? 1 : 0);
                    var chunk = new TRecord[toEat];

                    Array.Copy(array, arrayCursor, chunk, 0, toEat);
                    arrayCursor += toEat;

                    this.localVertices[i].Value.OnNext(chunk);
                }
                ++this.currentEpoch;
            }
        }

        public void OnCompleted()
        {
            //Debug.Assert(!this.completedCalled);
            if (!this.completedCalled)
            {
                this.EnsureProgressTrackerActivated();
                this.completedCalled = true;
                for (int i = 0; i < this.localVertices.Length; i++)
                    this.localVertices[i].Value.OnCompleted();
            }
        }

        public void OnCompleted(TRecord record)
        {
            this.OnCompleted(new[] { record });
        }
        public void OnCompleted(IEnumerable<TRecord> batch)
        {
            if (!this.completedCalled)
            {
                this.EnsureProgressTrackerActivated();
                this.completedCalled = true;

                var array = batch == null ? new TRecord[] { } : batch.ToArray();
                lock (this)
                {
                    ++this.currentEpoch;

                    var arrayCursor = 0;
                    for (int i = 0; i < this.localVertices.Length; i++)
                    {
                        var toEat = (array.Length / this.localVertices.Length) + ((i < (array.Length % this.localVertices.Length)) ? 1 : 0);
                        var chunk = new TRecord[toEat];

                        Array.Copy(array, arrayCursor, chunk, 0, toEat);
                        arrayCursor += toEat;

                        this.localVertices[i].Value.OnCompleted(chunk);
                    }
                }
            }
        }

        public void OnError(Exception error) { throw error; }

        public bool IsCompleted { get { return this.completedCalled; } }

        public int CurrentEpoch { get { return this.currentEpoch; } }
        public int MaximumValidEpoch { get { return this.currentEpoch - 1; } }

        public void Checkpoint(NaiadWriter writer)
        {
            writer.Write(currentEpoch, this.InternalComputation.SerializationFormat.GetSerializer<int>());
            writer.Write(completedCalled, this.InternalComputation.SerializationFormat.GetSerializer<bool>());
            writer.Write(hasActivatedProgressTracker, this.InternalComputation.SerializationFormat.GetSerializer<bool>());
        }

        public void Restore(NaiadReader reader)
        {
            this.currentEpoch = reader.Read<int>(this.InternalComputation.SerializationFormat.GetSerializer<int>());
            this.completedCalled = reader.Read<bool>(this.InternalComputation.SerializationFormat.GetSerializer<bool>());
            this.hasActivatedProgressTracker = reader.Read<bool>(this.InternalComputation.SerializationFormat.GetSerializer<bool>());
        }

        public bool Stateful { get { return true; } }

        public override string ToString()
        {
            return string.Format("{0} (current epoch = {1})", base.ToString(), this.currentEpoch);
        }
    }

    /// <summary>
    /// TODO this class is deprecated by virtue of StreamingInputVertex. Still used by Reporting, but that can be fixed.
    /// </summary>
    /// <typeparam name="S"></typeparam>
    internal class InputVertex<S> : Dataflow.Vertex<Epoch>
    {
        private struct Instruction
        {
            public S[] payload;
            public bool isLast;

            public Instruction(S[] p, bool il)
            {
                this.payload = p;
                this.isLast = il;
            }
        }

        private System.Collections.Concurrent.ConcurrentQueue<Instruction> inputQueue;

        internal VertexOutputBuffer<S, Epoch> Output;

        private int nextAvailableEpoch;
        private int nextSendEpoch;


        internal override void PerformAction(Scheduling.Scheduler.WorkItem workItem)
        {
            var epoch = new Epoch().InitializeFrom(workItem.Requirement, 1).epoch;

            for (int i = nextSendEpoch; i <= epoch; i++)
            {
                var sendTime = new Epoch(i);

                var output = this.Output.GetBufferForTime(sendTime);

                Instruction nextInstruction;
                inputQueue.TryDequeue(out nextInstruction);

                if (nextInstruction.payload != null)
                {
                    for (int j = 0; j < nextInstruction.payload.Length; j++)
                        output.Send(nextInstruction.payload[j]);

                    Flush();
                }

                if (!nextInstruction.isLast)
                    this.Scheduler.State(this.Stage.InternalComputation).Producer.UpdateRecordCounts(new Runtime.Progress.Pointstamp(this.Stage.StageId, new int[] { i + 1 }), +1);
                else
                    Logging.Progress("Completing input {0}", this.VertexId);

                this.scheduler.State(this.Stage.InternalComputation).Producer.UpdateRecordCounts(new Runtime.Progress.Pointstamp(this.Stage.StageId, new int[] { i }), -1);
            }

            nextSendEpoch = epoch + 1;
        }

        public void OnNext(S[] batch)
        {
            lock (this)     // this is probably already under a lock, but just to be safe...
            {
                this.inputQueue.Enqueue(new Instruction(batch, false));
                scheduler.EnqueueNotify(this, new Epoch(nextAvailableEpoch++), false);
            }
        }

        public void OnCompleted()
        {
            lock (this)
            {
                this.inputQueue.Enqueue(new Instruction(null, true));
                scheduler.EnqueueNotify(this, new Epoch(nextAvailableEpoch++), false);
                nextAvailableEpoch++;
            }
        }

        public void OnCompleted(S[] batch)
        {
            lock (this)     // this is probably already under a lock, but just to be safe...
            {
                this.inputQueue.Enqueue(new Instruction(batch, true));
                scheduler.EnqueueNotify(this, new Epoch(nextAvailableEpoch++), false);
            }
        }

        public override string ToString()
        {
            return "Input";
        }

        /* Checkpoint format:
         * int                           nextAvailableEpoch
         * int                           nextSendEpoch
         * int                           inputQueueCount
         * Weighted<S>[]*inputQueueCount inputQueue
         */

        private static NaiadSerialization<S> weightedSSerializer = null;

        protected override void Checkpoint(NaiadWriter writer)
        {
            if (weightedSSerializer == null)
                weightedSSerializer = this.SerializationFormat.GetSerializer<S>();

            var intSerializer = this.SerializationFormat.GetSerializer<Int32>();
            var boolSerializer = this.SerializationFormat.GetSerializer<bool>();

            writer.Write(this.nextAvailableEpoch, intSerializer);
            writer.Write(this.nextSendEpoch, intSerializer);
            writer.Write(this.inputQueue.Count, intSerializer);
            foreach (Instruction batch in this.inputQueue)
            {
                if (batch.payload != null) batch.payload.Checkpoint(batch.payload.Length, writer);
                writer.Write(batch.isLast, boolSerializer);
            }
        }

        protected override void Restore(NaiadReader reader)
        {
            this.nextAvailableEpoch = reader.Read<int>();
            this.nextSendEpoch = reader.Read<int>();
            int inputQueueCount = reader.Read<int>();
            for (int i = 0; i < inputQueueCount; ++i)
            {
                S[] array = CheckpointRestoreExtensionMethods.RestoreArray<S>(reader, n => new S[n]);
                bool isLast = reader.Read<bool>();
                this.inputQueue.Enqueue(new Instruction(array, isLast));
            }
        }

        public InputVertex(int index, Stage<Epoch> stage)
            : base(index, stage)
        {
            this.inputQueue = new System.Collections.Concurrent.ConcurrentQueue<Instruction>();
            this.Output = new VertexOutputBuffer<S, Epoch>(this);
        }
    }

    /// <summary>
    /// Represents a streaming input to a Naiad computation.
    /// </summary>
    /// <typeparam name="TRecord">The type of records accepted by this input.</typeparam>
    public interface StreamingInput<TRecord>
    {
        /// <summary>
        /// Indicates the local worker identifier.
        /// </summary>
        int WorkerId { get; }

        /// <summary>
        /// Introduces a batch of records at the same epoch.
        /// </summary>
        /// <param name="batch">records</param>
        /// <param name="epoch">epoch</param>
        void OnStreamingRecv(TRecord[] batch, int epoch);

        /// <summary>
        /// Indicates that no further records will appear at or before epoch.
        /// </summary>
        /// <param name="epoch">epoch</param>
        void OnStreamingNotify(int epoch);

        /// <summary>
        /// Indicates that no further records will appear.
        /// </summary>
        void OnCompleted();
    }

    internal class StreamingInputVertex<S> : Dataflow.Vertex<Epoch>, StreamingInput<S>
    {
        internal int CurrentEpoch { get { return this.currentVertexHold; } }
        private bool isCompleted = false;
        internal bool IsCompleted { get { return this.isCompleted; } }

        public int WorkerId { get { return this.Stage.Placement.Single(x => x.VertexId == this.VertexId).ThreadId; } }

        private struct Instruction
        {
            public readonly int Epoch;
            public readonly S[] Payload;

            public Instruction(int epoch, S[] payload)
            {
                this.Epoch = epoch;
                this.Payload = payload;
            }
        }

        private System.Collections.Concurrent.ConcurrentQueue<Instruction> inputQueue;

        internal readonly VertexOutputBuffer<S, Epoch> output;

        private int currentVertexHold = 0;

        private int maximumValidEpoch = -1;
        internal int MaximumValidEpoch { get { return this.maximumValidEpoch; } }

        internal override void PerformAction(Scheduling.Scheduler.WorkItem workItem)
        {
            var epoch = new Epoch().InitializeFrom(workItem.Requirement, 1).epoch;

            Instruction nextInstruction;
            bool success = inputQueue.TryDequeue(out nextInstruction);
            Debug.Assert(success);

            if (nextInstruction.Epoch == int.MaxValue)
            {
                Logging.Progress("[{0}] Performing OnCompleted", this.Stage.ToString());

                // OnCompleted logic.
                lock (this)
                {
                    if (!this.isCompleted)
                    {
                        this.scheduler.State(this.Stage.InternalComputation).Producer.UpdateRecordCounts(new Runtime.Progress.Pointstamp(this.Stage.StageId, new int[] { this.currentVertexHold }), -1);
                        this.isCompleted = true;
                    }
                    else
                    {
                        Logging.Error("WARNING: input ignored redundant shutdown when already shutdown.");
                    }
                }
            }
            else if (nextInstruction.Payload == null)
            {
                Logging.Progress("[{1}] Performing OnNotify({0})", nextInstruction.Epoch, this.Stage.ToString());

                // OnNotify logic.
                lock (this)
                {
                    this.maximumValidEpoch = Math.Max(this.maximumValidEpoch, nextInstruction.Epoch);

                    if (nextInstruction.Epoch >= this.currentVertexHold)
                    {
                        this.scheduler.State(this.Stage.InternalComputation).Producer.UpdateRecordCounts(new Runtime.Progress.Pointstamp(this.Stage.StageId, new int[] { nextInstruction.Epoch + 1 }), +1);
                        this.scheduler.State(this.Stage.InternalComputation).Producer.UpdateRecordCounts(new Runtime.Progress.Pointstamp(this.Stage.StageId, new int[] { this.currentVertexHold }), -1);
                        this.currentVertexHold = nextInstruction.Epoch + 1;
                    }
                    else
                    {
                        Logging.Error("WARNING: input ignored redundant notification for epoch {0} when current epoch was {1}.", nextInstruction.Epoch, this.currentVertexHold);
                    }
                }
            }
            else
            {
                // XXX : Getting called a lot, resulting in lots of object allocations
                // Logging.Progress("[{0}] Performing OnRecv", this.Stage.ToString());

                // OnRecv logic.
                lock (this)
                {
                    this.maximumValidEpoch = Math.Max(this.maximumValidEpoch, nextInstruction.Epoch);

                    if (nextInstruction.Epoch >= this.currentVertexHold)
                    {
                        var sendTime = new Epoch(nextInstruction.Epoch);
                        var output = this.output.GetBufferForTime(sendTime);
                        for (int i = 0; i < nextInstruction.Payload.Length; ++i)
                        {
                            output.Send(nextInstruction.Payload[i]);
                        }
                        this.Flush();
                    }
                    else
                    {
                        Logging.Error("WARNING: input ignored invalid data for epoch {0} when current epoch was {1}", nextInstruction.Epoch, this.currentVertexHold);
                    }
                }
            }
        }
    
        public void OnStreamingRecv(S[] batch, int epoch)
        {
            lock (this)     // this is probably already under a lock, but just to be safe...
            {
                this.inputQueue.Enqueue(new Instruction(epoch, batch));
                scheduler.EnqueueNotify(this, new Epoch(this.currentVertexHold), false);
            }
        }

        public void OnStreamingNotify(int epoch)
        {
            lock (this)
            {
                this.inputQueue.Enqueue(new Instruction(epoch, null));
                scheduler.EnqueueNotify(this, new Epoch(this.currentVertexHold), false);
            }
        }

        public void OnCompleted()
        {
            this.OnStreamingNotify(int.MaxValue);
        }

        public override string ToString()
        {
            return "FromSourceInput";
        }

        internal StreamingInputVertex(int index, Stage<Epoch> stage)
            : base(index, stage)
        {
            this.inputQueue = new System.Collections.Concurrent.ConcurrentQueue<Instruction>();
            this.output = new VertexOutputBuffer<S,Epoch>(this);
        }

        internal static Stream<S, Epoch> MakeStage(DataSource<S> source, InternalComputation internalComputation, Placement placement, string inputName)
        {
            var stage = new StreamingInputStage<S>(source, placement, internalComputation, inputName);
         
            return stage;
        }
    }

    internal class StreamingInputStage<R> : InputStage
    {
        private readonly StreamingInputVertex<R>[] localVertices;

        internal StreamingInputVertex<R> GetInputVertex(int vertexId)
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

        private readonly Stream<R, Epoch> output;
        public Stream<R, Epoch> Output { get { return output; } }

        public static implicit operator Stream<R, Epoch>(StreamingInputStage<R> stage) { return stage.Output; }

        public TimeContext<Epoch> Context { get { return this.stage.Context; } }
        internal InternalComputation InternalComputation { get { return this.stage.InternalComputation; } }
        public Placement Placement { get { return this.stage.Placement; } }

        private readonly string inputName;
        public string InputName { get { return this.inputName; } }

        public override string ToString()
        {
            return this.InputName;
        }

        private readonly Stage<StreamingInputVertex<R>, Epoch> stage;

        public int CurrentEpoch { get { return this.localVertices.Min(x => x.CurrentEpoch); } }
        public int MaximumValidEpoch { get { return this.localVertices.Max(x => x.MaximumValidEpoch); } }

        public bool IsCompleted { get { return this.localVertices.All(x => x.IsCompleted); } }

        public Type RecordType { get { return typeof(R); } }

        public bool Stateful { get { return true; } }
        public void Checkpoint(NaiadWriter writer) { throw new NotImplementedException(); }
        public void Restore(NaiadReader reader) { throw new NotImplementedException(); } 

        internal StreamingInputStage(DataSource<R> source, Placement placement, InternalComputation internalComputation, string inputName)
        {
            this.inputName = inputName;

            this.stage = Foundry.NewStage(new TimeContext<Epoch>(internalComputation.ContextManager.RootContext), (i, v) => new StreamingInputVertex<R>(i, v), this.inputName);

            this.output = stage.NewOutput(vertex => vertex.output);

            this.stage.Materialize();

            this.localVertices = placement.Where(x => x.ProcessId == internalComputation.Controller.Configuration.ProcessID)
                                        .Select(x => this.stage.GetVertex(x.VertexId) as StreamingInputVertex<R>)
                                        .ToArray();

            source.RegisterInputs(this.localVertices);

            this.completedCalled = false;
            this.hasActivatedProgressTracker = false;

            // results in pointstamp comparisons which assert w/o this.
            this.InternalComputation.Reachability.UpdateReachabilityPartialOrder(internalComputation);
            this.InternalComputation.Reachability.DoNotImpersonate(stage.StageId);

            var initialVersion = new Runtime.Progress.Pointstamp(stage.StageId, new int[] { 0 });

            internalComputation.ProgressTracker.BroadcastProgressUpdate(initialVersion, placement.Count);
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
