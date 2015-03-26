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
using System.Linq;
using System.Linq.Expressions;
using System.Text;
using Microsoft.Research.Naiad.Dataflow.Channels;
using Microsoft.Research.Naiad.Serialization;
using Microsoft.Research.Naiad.DataStructures;
using Microsoft.Research.Naiad.Frameworks;
using Microsoft.Research.Naiad.Runtime.FaultTolerance;
using Microsoft.Research.Naiad.Runtime.Progress;
using Microsoft.Research.Naiad.Scheduling;
using System.IO;

namespace Microsoft.Research.Naiad.Dataflow
{
    /// <summary>
    /// Represents an abstract stage in a dataflow graph, which comprises one or more dataflow vertices that each
    /// handle a partition of the data received by the stage.
    /// </summary>
    /// <remarks>
    /// This class cannot be instantiated directly: instead use the <see cref="Stage{TVertex,TTime}(StreamContext,Func{int,Stage{TTime},TVertex},string)"/> constructor, or
    /// the static factory and extension methods in <see cref="StandardVertices.Foundry"/>.
    /// </remarks>
    public abstract class Stage
    {
        private readonly InternalComputation internalComputation;
        internal InternalComputation InternalComputation { get { return this.internalComputation; } }

        internal abstract List<UntypedStageOutput> UntypedStageOutputs { get; }

        /// <summary>
        /// the graph manager associated with the stage
        /// </summary>
        public Computation Computation { get { return this.internalComputation.ExternalComputation; } }

        /// <summary>
        /// The ratio of new entries to existing entries beyond which a full checkpoint is taken instead
        /// of an incremental one
        /// </summary>
        public double IncrementalCheckpointRatio { get; set; }

        /// <summary>
        /// the unique identifier associated with the stage
        /// </summary>
        public readonly int StageId;
        internal abstract Pointstamp DefaultVersion { get; }

        private List<Edge> targets;
        internal List<Edge> Targets { get { return targets; } }

        private List<Edge> sources;
        internal List<Edge> Sources { get { return sources; } }

        internal abstract void EnableLogging(Func<string, IStreamSequence> streamSequenceFactoryy, CheckpointTracker tracker, bool deleteOldCheckpoints);

        /// <summary>
        /// the placement used for the stage
        /// </summary>
        public readonly Placement Placement;

        internal enum OperatorType { Unknown, Default, IterationAdvance, IterationIngress, IterationEgress };
        private readonly OperatorType collectionType;

        internal bool IsIterationAdvance { get { return collectionType == OperatorType.IterationAdvance; } }
        internal bool IsIterationIngress { get { return collectionType == OperatorType.IterationIngress; } }
        internal bool IsIterationEgress { get { return collectionType == OperatorType.IterationEgress; } }

        internal abstract IEnumerable<Vertex> Vertices { get; }

        /// <summary>
        /// The type of checkpointing/logging used for the stage
        /// </summary>
        public CheckpointType CheckpointType { get; private set; }
        /// <summary>
        /// Set the type of checkpointing/logging used for the stage
        /// </summary>
        /// <param name="checkpointType">type of checkpointing/logging</param>
        public void SetCheckpointType(CheckpointType checkpointType)
        {
            this.CheckpointType = checkpointType;
        }

        /// <summary>
        /// The policy of when to checkpoint
        /// </summary>
        public Func<int, ICheckpointPolicy> CheckpointPolicyFactory { get; private set; }
        /// <summary>
        /// Set the policy of when to checkpoint
        /// </summary>
        /// <param name="shouldCheckpointFactory">function to create a checkpoint policy object for a vertex</param>
        /// <returns></returns>
        public void SetCheckpointPolicy(Func<int, ICheckpointPolicy> shouldCheckpointFactory)
        {
            this.CheckpointPolicyFactory = shouldCheckpointFactory;
        }
        internal abstract bool IsRollingBack(int vertexId);
        internal abstract void SetRollbackFrontier(CheckpointLowWatermark frontier);
        internal abstract void SetDiscardFrontiers(DiscardManager discardManager);
        internal abstract ICheckpointCollection[] InitializeCheckpointCollections(Func<string, IStreamSequence> streamSequenceFactory);

        internal abstract void Materialize();

        internal abstract Dictionary<int, Vertex> ReMaterializeForRollback();

        #region Input/Output creation
        #endregion

        #region Constructor

        private readonly string MyName;
        /// <summary>
        /// Returns the stage name decorated with the stage ID.
        /// </summary>
        /// <returns>The stage name decorated with the stage ID.</returns>
        public override string ToString()
        {
            return MyName;
        }

        private readonly string name;
        /// <summary>
        /// Returns the stage name undecorated by stage id
        /// </summary>
        public string Name { get { return name; } }

        internal Stage(Placement placement, InternalComputation internalComputation, OperatorType operatorType, string name)
        {
            // start a new checkpoint when the number of entries since the last checkpoint is more than 20%
            // the number of entries in the last checkpoint
            this.IncrementalCheckpointRatio = 0.2;
            this.CheckpointType = CheckpointType.Logging;
            this.internalComputation = internalComputation;
            this.targets = new List<Edge>();
            this.sources = new List<Edge>();
            this.StageId = this.InternalComputation.Register(this);
            this.collectionType = operatorType;
            this.Placement = placement;
            this.name = name;

            MyName = string.Format("{0}[{1}]", name, this.StageId);
        }

        #endregion Constructor
    }

    /// <summary>
    /// Represents an abstract stage in a dataflow graph, which comprises one or more dataflow vertices that each
    /// handle a partition of the data received by the stage, with a time type that
    /// indicates its level of nesting in the graph.
    /// </summary>
    /// <remarks>
    /// This class cannot be instantiated directly: instead use the <see cref="Stage{TVertex,TTime}(StreamContext,Func{int,Stage{TTime},TVertex},string)"/> constructor, or
    /// the static factory and extension methods in <see cref="StandardVertices.Foundry"/>.
    /// </remarks>
    /// <typeparam name="TTime">The type of timestamps on messages that this stage processes.</typeparam>
    public abstract class Stage<TTime> : Stage
        where TTime : Time<TTime>
    {
        internal abstract Vertex<TTime> Vertex(int vertexId);

        internal override Pointstamp DefaultVersion
        {
            get { return default(TTime).ToPointstamp(this.StageId); }
        }

        internal Checkpoint<TTime>[] currentCheckpoints;
        internal Checkpoint<TTime> CurrentCheckpoint(int vertexId)
        {
            return this.currentCheckpoints[vertexId];
        }
        internal void SetCurrentCheckpoint(int vertexId, Checkpoint<TTime> checkpoint)
        {
            this.currentCheckpoints[vertexId] = checkpoint;
        }

        internal override void SetRollbackFrontier(CheckpointLowWatermark frontier)
        {
            this.SetCurrentCheckpoint(frontier.vertexId, new FrontierCheckpoint<TTime>(this, frontier.frontier));
        }

        internal override void SetDiscardFrontiers(DiscardManager discardManager)
        {
            discardManager.SetDiscardFrontier(this);
        }

        internal long CheckpointStreamThreshold { get; private set; }

        internal override ICheckpointCollection[] InitializeCheckpointCollections(Func<string, IStreamSequence> streamSequenceFactory)
        {
            ICheckpointCollection[] collections = new ICheckpointCollection[this.Placement.Count];
            for (int i=0; i<this.Placement.Count; ++i)
            {
                collections[i] = CheckpointCollection<TTime>.InitializeFrom(streamSequenceFactory, this, i);
            }
            return collections;
        }

        private bool inputsSealed = false;
        internal bool InputsSealed { get { return this.inputsSealed; } }

        internal readonly List<StageOutputForVertex<TTime>> StageOutputs = new List<StageOutputForVertex<TTime>>();

        internal override List<UntypedStageOutput> UntypedStageOutputs
        {
            get { return this.StageOutputs.Select<StageOutputForVertex<TTime>, UntypedStageOutput>(so => so).ToList(); }
        }

        internal int NewStageOutput(StageOutputForVertex<TTime> stageOutput)
        {
            this.StageOutputs.Add(stageOutput);
            return this.StageOutputs.Count - 1;
        }

        internal FullyTypedStageOutput<TTime, R, T> NewOutput<R, T>(Expression<Func<R, int>> partitionedBy, Func<TTime, T> sendTimeProjection)
            where T : Time<T>
        {
            inputsSealed = true;

            var result = new FullyTypedStageOutput<TTime, R, T>(this, partitionedBy, sendTimeProjection);

            return result;
        }

        // creates a new send socket for the stage to send records into.
        internal FullyTypedStageOutput<TTime, R, T> NewOutput<R, T>(Func<TTime, T> sendTimeProjection)
            where T : Time<T>
        {
            return this.NewOutput<R, T>(null, sendTimeProjection);
        }

        /// <summary>
        /// Creates a new input for this stage, with the given <paramref name="partitioning"/> requirement.
        /// </summary>
        /// <typeparam name="TRecord">The record type.</typeparam>
        /// <param name="stream">The stream from which this input will receive records.</param>
        /// <param name="partitioning">Function that maps records to integers, implying the requirement that all records
        /// mapping to the same integer must be processed by the same <see cref="Vertex"/>.</param>
        /// <returns>An object that represents the stage input.</returns>
        public StageInput<TRecord, TTime> NewInput<TRecord>(Stream<TRecord, TTime> stream, Expression<Func<TRecord, int>> partitioning)
        {
            if (InputsSealed)
                throw new Exception("Inputs for a stage may not be added after outputs");

            if (stream == null)
                throw new ArgumentNullException("stream");

            var result = new StageInput<TRecord, TTime>(this, partitioning);

            if (partitioning != null)
            {
                var compiled = partitioning.Compile();
                Action<TRecord[], int[], int> vectoredPartitioning = (data, dsts, len) => { for (int i = 0; i < len; i++) dsts[i] = compiled(data[i]); };

                this.InternalComputation.Connect(stream.StageOutput, result, vectoredPartitioning, Channel.Flags.None);
            }
            else
                this.InternalComputation.Connect(stream.StageOutput, result, null, Channel.Flags.None);

            return result;
        }

        /// <summary>
        /// Creates a new input for this stage, with the given <paramref name="partitioning"/> requirement.
        /// </summary>
        /// <typeparam name="TRecord">The record type.</typeparam>
        /// <param name="stream">The stream from which this input will receive records.</param>
        /// <param name="partitioning">Function that maps records to integers, implying the requirement that all records
        /// mapping to the same integer must be processed by the same <see cref="Vertex"/>.</param>
        /// <param name="vectoredPartitioning">Action that maps an array of records to an array of integers, implying the requirement that all records
        /// mapping to the same integer must be processed by the same <see cref="Vertex"/>. The third argument is the number of valid records in the input array</param>
        /// <returns>An object that represents the stage input.</returns>
        public StageInput<TRecord, TTime> NewInput<TRecord>(Stream<TRecord, TTime> stream, Expression<Func<TRecord, int>> partitioning, Action<TRecord[], int[], int> vectoredPartitioning)
        {
            if (InputsSealed)
                throw new Exception("Inputs for a stage may not be added after outputs");

            if (stream == null)
                throw new ArgumentNullException("stream");

            var result = new StageInput<TRecord, TTime>(this, partitioning);

            this.InternalComputation.Connect(stream.StageOutput, result, vectoredPartitioning, Channel.Flags.None);

            return result;
        }


        internal StageInput<R, TTime> NewUnconnectedInput<R>(Expression<Func<R, int>> partitioning)
        {
            if (InputsSealed)
                throw new Exception("Inputs for a stage may not be added after outputs");

            var result = new StageInput<R, TTime>(this, partitioning);

            return result;
        }

        internal Stage(Placement placement, InternalComputation computation, OperatorType opType, string name)
            : base(placement, computation, opType, name)
        {
            // by default write incremental checkpoints
            this.CheckpointStreamThreshold = long.MaxValue;
        }
    }

    /// <summary>
    /// Represents a stage in a dataflow graph, which comprises one or more dataflow vertices of a particular type that each
    /// handle a partition of the data received by the stage.
    /// </summary>
    /// <remarks>
    /// This class can be instantiated directly using the <see cref="Stage{TVertex,TTime}(StreamContext,Func{int,Stage{TTime},TVertex},string)"/> constructor, or indirectly
    /// using the static factory and extension methods in <see cref="StandardVertices.Foundry"/>.
    /// </remarks>
    /// <typeparam name="TVertex">The type of dataflow vertices in this stage.</typeparam>
    /// <typeparam name="TTime">The type of timestamps on messages that this stage processes.</typeparam>
    public class Stage<TVertex, TTime> : Stage<TTime>
        where TVertex : Vertex<TTime>
        where TTime : Time<TTime>
    {
        private readonly Dictionary<int, TVertex> vertices;

        internal override IEnumerable<Vertex> Vertices { get { return this.vertices.Values; } }

        internal override Vertex<TTime> Vertex(int vertexId)
        {
            return this.vertices[vertexId];
        }

        internal override bool IsRollingBack(int vertexId)
        {
            return
                this.CheckpointType != CheckpointType.None &&
                !this.CurrentCheckpoint(vertexId).GetRestorationFrontier().Complete;
        }

        /// <summary>
        /// Turn on checkpointing/logging
        /// </summary>
        internal override void EnableLogging(Func<string, IStreamSequence> streamSequenceFactory, CheckpointTracker tracker, bool deleteOldCheckpoints)
        {
            this.currentCheckpoints = new Checkpoint<TTime>[this.Placement.Count];

            if (this.CheckpointType == CheckpointType.None)
            {
                for (int i = 0; i < this.Placement.Count; ++i)
                {
                    this.currentCheckpoints[i] = new NullCheckpoint<TTime>();
                }
                return;
            }

            for (int i = 0; i < this.Placement.Count; ++i)
            {
                this.currentCheckpoints[i] = new FrontierCheckpoint<TTime>(this);
            }

            foreach (Vertex<TTime> v in this.vertices.Values)
            {
                v.EnableLogging(streamSequenceFactory);
                v.Checkpointer.Initialize(tracker, deleteOldCheckpoints);
            }
        }

        internal TVertex GetVertex(int vertexIndex)
        {
            return vertices[vertexIndex];
        }

        internal void AddVertex(TVertex vertex) { vertices.Add(vertex.VertexId, vertex); }

        internal Stage(Placement placement, InternalComputation computation, OperatorType optype, Func<int, Stage<TTime>, TVertex> factory, string name)
            : base(placement, computation, optype, name)
        {
            vertices = new Dictionary<int, TVertex>();
            this.factory = factory;

            if (factory == null)
                throw new ArgumentNullException("factory");
        }

        internal Stage(InternalComputation computation, OperatorType optype, Func<int, Stage<TTime>, TVertex> factory, string name)
            : this(computation.DefaultPlacement, computation, optype, factory, name)
        {
        }

        /// <summary>
        /// Constructs a new stage in the given time context, using the given vertex factory to construct the constituent vertices.
        /// </summary>
        /// <param name="context">The time context.</param>
        /// <param name="factory">A factory for vertices in this stage.</param>
        /// <param name="name">A human-readable name for this stage.</param>
        /// <example>
        /// To use this constructor, the programmer must pass a vertex factory, which is a function from an integer ID and stage to the vertex type (TVertex). The factory
        /// arguments must be passed through to the <see cref="Microsoft.Research.Naiad.Dataflow.Vertex{TTime}(int,Stage{TTime})"/> constructor. For example:
        /// <code>
        /// class MyVertex : Vertex&lt;TTime&gt; where TTime : Time&lt;TTime&gt;
        /// {
        ///     public MyVertex(int id, Stage&lt;TTime&gt; stage, ...)
        ///         : base(id, stage)
        ///     {
        ///         /* Other initialization. */
        ///     }
        /// }
        /// 
        /// var stage = new Stage&lt;TTime, MyVertex&gt;(context, (i, s) => new MyVertex(i, s, ...), "MyStage");
        /// </code>
        /// </example>
        public Stage(StreamContext context, Func<int, Stage<TTime>, TVertex> factory, string name)
            : this(context.Computation.DefaultPlacement, context.Computation, OperatorType.Default, factory, name)
        {
        }

        private readonly List<Action<TVertex>> materializeActions = new List<Action<TVertex>>();

        #region Input creation

        /// <summary>
        /// Creates a new input from a stream, a VertexInput selector, and partitioning information.
        /// </summary>
        /// <typeparam name="TRecord">Record type</typeparam>
        /// <param name="stream">source stream</param>
        /// <param name="vertexInput">VertexInput selector</param>
        /// <param name="partitionedBy">partitioning expression, or null</param>
        /// <returns>StageInput</returns>
        internal StageInput<TRecord, TTime> NewInput<TRecord>(Stream<TRecord, TTime> stream, Func<TVertex, VertexInput<TRecord, TTime>> vertexInput, Expression<Func<TRecord, int>> partitionedBy)
        {
            var ret = this.NewInput(stream, partitionedBy);

            this.materializeActions.Add(vertex => { ret.Register(vertexInput(vertex)); });

            return ret;
        }

        /// <summary>
        /// Creates a new input from a stream, a VertexInput selector, and partitioning information.
        /// </summary>
        /// <typeparam name="TRecord">Record type</typeparam>
        /// <param name="stream">source stream</param>
        /// <param name="vertexInput">VertexInput selector</param>
        /// <param name="partitionedBy">partitioning expression, or null</param>
        /// <param name="vectoredPartitionedBy">Action that maps an array of records to an array of integers, or none, implying the requirement that all records
        /// mapping to the same integer must be processed by the same <see cref="Vertex"/>. The third argument is the number of valid records in the input array</param>
        /// <returns>StageInput</returns>
        internal StageInput<TRecord, TTime> NewInput<TRecord>(Stream<TRecord, TTime> stream, Func<TVertex, VertexInput<TRecord, TTime>> vertexInput, Expression<Func<TRecord, int>> partitionedBy, Action<TRecord[], int[], int> vectoredPartitionedBy)
        {
            var ret = this.NewInput(stream, partitionedBy, vectoredPartitionedBy);

            this.materializeActions.Add(vertex => { ret.Register(vertexInput(vertex)); });

            return ret;
        }

        /// <summary>
        /// Creates a new input that consumes records from the given stream, partitioned by the given partitioning function,
        /// and delivers them to a vertex through the given onReceive callback.
        /// </summary>
        /// <typeparam name="TRecord">Record type</typeparam>
        /// <param name="stream">The stream from which records will be consumed.</param>
        /// <param name="onReceive">A callback that will be invoked on a message and vertex when that message is to be delivered to that vertex.</param>
        /// <param name="partitionedBy">A partitioning expression, or <c>null</c> if the records need not be repartitioned.</param>
        /// <returns>A handle to the input.</returns>
        public StageInput<TRecord, TTime> NewInput<TRecord>(Stream<TRecord, TTime> stream, Action<Message<TRecord, TTime>, TVertex> onReceive, Expression<Func<TRecord, int>> partitionedBy)
        {
            return this.NewInput<TRecord>(stream, s => new ActionReceiver<TRecord, TTime>(s, m => onReceive(m, s)), partitionedBy);
        }

        /// <summary>
        /// Creates a new input that consumes records from the given stream, partitioned by the given partitioning function,
        /// and delivers them to a vertex through the given onReceive callback.
        /// </summary>
        /// <typeparam name="TRecord">Record type</typeparam>
        /// <param name="stream">The stream from which records will be consumed.</param>
        /// <param name="onReceive">A callback that will be invoked on a message and vertex when that message is to be delivered to that vertex.</param>
        /// <param name="partitionedBy">A partitioning expression, or <c>null</c> if the records need not be repartitioned.</param>
        /// <param name="vectoredPartitionedBy">Action that maps an array of records to an array of integers, or none, implying the requirement that all records
        /// mapping to the same integer must be processed by the same <see cref="Vertex"/>. The third argument is the number of valid records in the input array</param>
        /// <returns>A handle to the input.</returns>
        public StageInput<TRecord, TTime> NewInput<TRecord>(Stream<TRecord, TTime> stream, Action<Message<TRecord, TTime>, TVertex> onReceive, Expression<Func<TRecord, int>> partitionedBy, Action<TRecord[], int[], int> vectoredPartitionedBy)
        {
            return this.NewInput<TRecord>(stream, s => new ActionReceiver<TRecord, TTime>(s, m => onReceive(m, s)), partitionedBy, vectoredPartitionedBy);
        }

        /// <summary>
        /// Ok, listen. This is used in very few places when we need to violate the "inputs before outputs" rule. We create the input anyhow, and assign the stream later on.
        /// </summary>
        internal StageInput<S, TTime> NewUnconnectedInput<S>(Func<TVertex, VertexInput<S, TTime>> vertexInput, Expression<Func<S, int>> partitionedBy)
        {
            var ret = this.NewUnconnectedInput<S>(partitionedBy);

            this.materializeActions.Add(vertex => { ret.Register(vertexInput(vertex)); });

            return ret;
        }

        internal StageInput<S, TTime> NewUnconnectedInput<S>(Action<Message<S, TTime>, TVertex> onRecv, Expression<Func<S, int>> partitionedBy)
        {
            return this.NewUnconnectedInput<S>(vertex => new ActionReceiver<S, TTime>(vertex, message => onRecv(message, vertex)), partitionedBy);
        }


        #endregion

        #region Output creation

        /// <summary>
        /// Creates a new output with no partitioning guarantee.
        /// </summary>
        /// <typeparam name="R">record type</typeparam>
        /// <param name="vertexOutput">A function that, given a vertex in this stage, returns the corresponding vertex-level output.</param>
        /// <returns>A handle to the output stream.</returns>
        public Stream<R, TTime> NewOutput<R>(Func<TVertex, VertexOutput<TTime, R, TTime>> vertexOutput) 
        { 
            return this.NewOutput(vertexOutput, null); 
        }

        /// <summary>
        /// Creates a new output with no partitioning guarantee and a time type different to that of the stage.
        /// </summary>
        /// <typeparam name="R">record type</typeparam>
        /// <typeparam name="T">output time type</typeparam>
        /// <param name="vertexOutput">A function that, given a vertex in this stage, returns the corresponding vertex-level output.</param>
        /// <param name="sendTimeProjection">A function that, given an event time at the vertex, returns the lowest legal time of a message on the output.</param>
        /// <returns>A handle to the output stream.</returns>
        internal Stream<R, T> NewSurprisingTimeTypeOutput<R, T>(Func<TVertex, VertexOutput<TTime, R, T>> vertexOutput, Func<TTime, T> sendTimeProjection)
            where T : Time<T>
        {
            return this.NewSurprisingTimeTypeOutput(vertexOutput, null, sendTimeProjection);
        }

        /// <summary>
        /// Creates a new output with a partitioning guarantee
        /// </summary>
        /// <typeparam name="R">record type</typeparam>
        /// <param name="vertexOutput">Given a vertex in this stage, returns the corresponding vertex-level output.</param>
        /// <param name="partitionedBy">A partitioning guarantee, or null if there is no known partitioning guarantee.</param>
        /// <returns>A handle to the output stream.</returns>
        public Stream<R, TTime> NewOutput<R>(Func<TVertex, VertexOutput<TTime, R, TTime>> vertexOutput, Expression<Func<R, int>> partitionedBy) 
        {
            return this.NewTypedOutput<R>(vertexOutput, partitionedBy);
        }

        internal FullyTypedStream<TTime, R, TTime> NewTypedOutput<R>(Func<TVertex, VertexOutput<TTime, R, TTime>> vertexOutput, Expression<Func<R, int>> partitionedBy)
        {
            var ret = this.NewOutput<R, TTime>(partitionedBy, t => t);

            this.materializeActions.Add(vertex => { ret.Register(vertexOutput(vertex)); });

            return new FullyTypedStream<TTime, R, TTime>(ret);
        }

        /// <summary>
        /// Creates a new output with a partitioning guarantee and a time type different to that of the stage
        /// </summary>
        /// <typeparam name="R">record type</typeparam>
        /// <typeparam name="T">output time type</typeparam>
        /// <param name="vertexOutput">Given a vertex in this stage, returns the corresponding vertex-level output.</param>
        /// <param name="partitionedBy">A partitioning guarantee, or null if there is no known partitioning guarantee.</param>
        /// <param name="sendTimeProjection">A function that, given an event time at the vertex, returns the lowest legal time of a message on the output.</param>
        /// <returns>A handle to the output stream.</returns>
        internal Stream<R, T> NewSurprisingTimeTypeOutput<R, T>(
            Func<TVertex, VertexOutput<TTime, R, T>> vertexOutput, Expression<Func<R, int>> partitionedBy, Func<TTime, T> sendTimeProjection)
            where T : Time<T>
        {
            var ret = this.NewOutput<R, T>(partitionedBy, sendTimeProjection);

            this.materializeActions.Add(vertex => { ret.Register(vertexOutput(vertex)); });

            return new FullyTypedStream<TTime, R, T>(ret);
        }

        /// <summary>
        /// Adds an output with a partitioning guarantee
        /// </summary>
        /// <typeparam name="R">record type</typeparam>
        /// <param name="newListener">new listener callback</param>
        /// <param name="partitionedBy">partitoining guarantee</param>
        /// <returns>output stream</returns>
        internal Stream<R, TTime> NewOutput<R>(Action<SendChannel<TTime, R, TTime>, TVertex> newListener, Expression<Func<R, int>> partitionedBy)
        {
            return this.NewOutput<R>(vertex => new ActionSubscriber<R, TTime>(vertex, listener => { newListener(listener, vertex); vertex.AddOnFlushAction(() => listener.Flush()); }), partitionedBy);
        }

        internal Stream<R, TTime> NewOutputWithoutSealing<R>(Func<TVertex, VertexOutput<TTime, R, TTime>> vertexOutput, Expression<Func<R, int>> partitionedBy)
        {
            var result = new FullyTypedStageOutput<TTime, R, TTime>(this, partitionedBy, t => t);

            this.materializeActions.Add(vertex => { result.Register(vertexOutput(vertex)); });

            return new FullyTypedStream<TTime, R, TTime>(result);
        }

        #endregion

        private bool materialized = false;
        internal override void Materialize()
        {
            if (materialized)
                return; // progress stages get re-materialized because it is a pain not to.

            materialized = true;
            Diagnostics.NaiadTracing.Trace.StageInfo(this.StageId, this.Name);

            foreach (var loc in Placement)
            {
                Diagnostics.NaiadTracing.Trace.VertexPlacement(this.StageId, loc.VertexId, loc.ProcessId, loc.ThreadId);
                if (loc.ProcessId == this.InternalComputation.Controller.Configuration.ProcessID)
                {
                    var vertex = this.factory(loc.VertexId, this);
                    AddVertex(vertex);

                    foreach (var action in this.materializeActions)
                        action(vertex);
                }
            }
        }

        internal override Dictionary<int,Vertex> ReMaterializeForRollback()
        {
            Dictionary<int, Vertex> oldVertices = new Dictionary<int, Vertex>();
            foreach (var loc in Placement
                .Where(l => 
                    l.ProcessId == this.InternalComputation.Controller.Configuration.ProcessID &&
                    this.IsRollingBack(l.VertexId)))
            {
                Pointstamp[] frontier = this.CurrentCheckpoint(loc.VertexId).GetRestorationFrontier().ToPointstamps(this.DefaultVersion);
                var oldVertex = this.vertices[loc.VertexId];
                if (oldVertex.MustRollBackInsteadOfRestarting(frontier) ||
                    (oldVertex.CanRollBackInsteadOfRestarting(frontier) &&
                     !this.InternalComputation.Controller.HasFailed))
                {
                    Pair<ICheckpoint<TTime>, ICheckpoint<TTime>> checkpoints = 
                        oldVertex.Checkpointer.checkpointWriter.LastCheckpointsInRestoration(
                            this.CurrentCheckpoint(loc.VertexId).GetRestorationFrontier());
                    oldVertex.RollBackPreservingState(frontier, checkpoints.First, checkpoints.Second);
                }
                else
                {
                    oldVertices.Add(loc.VertexId, oldVertex);

                    var newVertex = this.factory(loc.VertexId, this);
                    this.vertices[loc.VertexId] = newVertex;
                    newVertex.TransferLogging(oldVertex);

                    foreach (var action in this.materializeActions)
                    {
                        action(newVertex);
                    }
                }
            }

            return oldVertices;
        }

        private readonly Func<int, Stage<TTime>, TVertex> factory;
    }

}
