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
using Microsoft.Research.Naiad.Dataflow;
using Microsoft.Research.Naiad.Dataflow.Channels;
using Microsoft.Research.Naiad.Runtime.FaultTolerance;
using Microsoft.Research.Naiad.Runtime.Progress;
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Linq.Expressions;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

using Microsoft.Research.Naiad.Diagnostics;
using Microsoft.Research.Naiad.Input;

namespace Microsoft.Research.Naiad
{
    /// <summary>
    /// A Computation with an internal Controller which cannot be re-used for other Computations.
    /// </summary>
    public interface OneOffComputation : Computation
    {
        /// <summary>
        /// The configuration used by this controller.
        /// </summary>
        Configuration Configuration { get; }

        /// <summary>
        /// The workers associated with this controller.
        /// </summary>
        WorkerGroup WorkerGroup { get; }

        /// <summary>
        /// The default placement of new stages.
        /// </summary>
        Placement DefaultPlacement { get; }
    }

    internal class InternalOneOffComputation : OneOffComputation
    {
        private readonly Controller controller;

        private readonly Computation computation;

        public InternalOneOffComputation(Configuration configuration)
        {
            this.controller = NewController.FromConfig(configuration);
            this.computation = controller.NewComputation();
        }

        public void Join()
        {
            this.controller.Join();
        }

        public void Dispose()
        {
            this.computation.Dispose();
            this.controller.Dispose();
        }

        public Configuration Configuration
        {
            get { return this.controller.Configuration; }
        }

        public WorkerGroup WorkerGroup
        {
            get { return this.controller.WorkerGroup; }
        }

        public Placement DefaultPlacement
        {
            get { return this.controller.DefaultPlacement; }
        }

        public Stream<TRecord, TTime> NewInput<TRecord, TTime>(DataSource<TRecord, TTime> source) where TTime : Time<TTime>
        {
            return this.computation.NewInput<TRecord, TTime>(source);
        }

        public Stream<TRecord, TTime> NewInput<TRecord, TTime>(DataSource<TRecord, TTime> source, string name) where TTime : Time<TTime>
        {
            return this.computation.NewInput<TRecord, TTime>(source, name);
        }

        public event EventHandler<FrontierChangedEventArgs> OnFrontierChange { add { this.computation.OnFrontierChange += value; } remove { this.computation.OnFrontierChange -= value; } }

        public event EventHandler<GraphMaterializedEventArgs> OnMaterialized { add { this.computation.OnMaterialized += value; } remove { this.computation.OnMaterialized -= value; } }

        public event EventHandler<CheckpointPersistedEventArgs> OnCheckpointPersisted { add { this.computation.OnCheckpointPersisted += value; } remove { this.computation.OnCheckpointPersisted -= value; } }

        public event EventHandler<StageStableEventArgs> OnStageStable { add { this.computation.OnStageStable += value; } remove { this.computation.OnStageStable -= value; } }

        public event EventHandler<ProcessRestartedEventArgs> OnProcessRestarted { add { this.computation.OnProcessRestarted += value; } remove { this.computation.OnProcessRestarted -= value; } }

        public void PausePeerProcesses(IEnumerable<int> processors)
        {
            this.computation.PausePeerProcesses(processors);
        }

        public void StartRollback()
        {
            this.computation.StartRollback();
        }

        public void SimulateFailure(int processId, int restartDelay)
        {
            this.computation.SimulateFailure(processId, restartDelay);
        }

        public void RestoreToFrontiers(IEnumerable<CheckpointLowWatermark> frontiers)
        {
            this.computation.RestoreToFrontiers(frontiers);
        }

        public void ReceiveCheckpointUpdates(IEnumerable<CheckpointLowWatermark> updates)
        {
            this.computation.ReceiveCheckpointUpdates(updates);
        }

        public event EventHandler OnStartup { add { this.computation.OnStartup += value; } remove { this.computation.OnStartup -= value; } }

        public event EventHandler OnShutdown { add { this.computation.OnShutdown += value; } remove { this.computation.OnShutdown -= value; } }

        public void NotifyLog(int stageId, int vertexId, ReturnAddress from, string time, int count)
        {
            this.computation.NotifyLog(stageId, vertexId, from, time, count);
        }
        public void NotifyLog(int stageId, int vertexId, string time)
        {
            this.computation.NotifyLog(stageId, vertexId, time);
        }
        public event EventHandler<LogEventArgs> OnLog { add { this.computation.OnLog += value; } remove { this.computation.OnLog -= value; } }

        public void Sync(int epoch)
        {
            this.computation.Sync(epoch);
        }

        public void Activate()
        {
            this.computation.Activate();
        }

        public void Restore()
        {
            this.computation.Restore();
        }

        public long TicksSinceStartup { get { return this.computation.TicksSinceStartup; } }

        public TemporaryPlacement Placement(Placement scopedPlacement)
        {
            return this.computation.Placement(scopedPlacement);
        }

        public StreamContext Context { get { return this.computation.Context; } }

        public Controller Controller
        {
            get { return this.controller; }
        }
    }

    /// <summary>
    /// Object used to temporarily override the default placement for a computation
    /// </summary>
    public interface TemporaryPlacement : IDisposable
    {
    }

    /// <summary>
    /// Manages the construction and execution of an individual dataflow computation.
    /// </summary>
    /// <remarks>
    /// A Computation manages the execution of a single Naiad computation.
    /// To construct an instance of this interface, use the
    /// <see cref="Microsoft.Research.Naiad.Controller.NewComputation"/> method.
    /// </remarks>
    public interface Computation : IDisposable
    {
        /// <summary>
        /// Creates a new input stage from the given <see cref="DataSource"/>.
        /// </summary>
        /// <typeparam name="TRecord">record type</typeparam>
        /// <typeparam name="TTime">time type</typeparam>
        /// <param name="source">data source</param>
        /// <returns>A new input stage</returns>
        Stream<TRecord, TTime> NewInput<TRecord, TTime>(DataSource<TRecord, TTime> source) where TTime : Time<TTime>;

        /// <summary>
        /// Creates a new input stage from the given <see cref="DataSource"/>.
        /// </summary>
        /// <typeparam name="TRecord">record type</typeparam>
        /// <typeparam name="TTime">time type</typeparam>
        /// <param name="source">data source</param>
        /// <param name="name">name for the input</param>
        /// <returns>A new input stage</returns>
        Stream<TRecord, TTime> NewInput<TRecord, TTime>(DataSource<TRecord, TTime> source, string name) where TTime : Time<TTime>;

        /// <summary>
        /// An event that is raised each time the frontier changes.
        /// </summary>
        /// <remarks>
        /// This event provides a hook for debugging statements that track the progress of
        /// a computation.
        /// </remarks>
        /// <example>
        /// computation.OnFrontierChange += (c, f) =>
        ///     {
        ///         Console.WriteLine("New frontier: {0}", string.Join(", ", f.NewFrontier));
        ///     };
        /// </example>
        event EventHandler<FrontierChangedEventArgs> OnFrontierChange;

        /// <summary>
        /// An event that is raised when the graph is materialized, if logging is enabled
        /// </summary>
        /// <remarks>
        /// This event provides a hook for determining the tolopogy of the graph
        /// </remarks>
        event EventHandler<GraphMaterializedEventArgs> OnMaterialized;

        /// <summary>
        /// An event that is raised when a vertex checkpoint has been persisted, if logging is enabled
        /// </summary>
        event EventHandler<CheckpointPersistedEventArgs> OnCheckpointPersisted;

        /// <summary>
        /// An event that is raised when an entire vertex stage is stable, if logging is enabled
        /// </summary>
        event EventHandler<StageStableEventArgs> OnStageStable;

        /// <summary>
        /// An event that is raised when a failed process has restarted, if logging is enabled
        /// </summary>
        event EventHandler<ProcessRestartedEventArgs> OnProcessRestarted;

        /// <summary>
        /// the number of ticks since the first barrier sychronization with the other processes
        /// </summary>
        long TicksSinceStartup { get; }

        /// <summary>
        /// Tell the computation about new checkpoint low watermarks, to allow checkpointers to
        /// garbage-collect state
        /// </summary>
        /// <param name="updates">the new low watermarks</param>
        void ReceiveCheckpointUpdates(IEnumerable<CheckpointLowWatermark> updates);

        /// <summary>
        /// Tell a process to act as if it has failed and discard all computation state, taking
        /// a specified time before reporting that it has restarted
        /// </summary>
        /// <param name="processId">process to fail</param>
        /// <param name="restartDelay">number of milliseconds to wait before reporting restart</param>
        void SimulateFailure(int processId, int restartDelay);

        /// <summary>
        /// Tell the computation to pause a subset of peer processes in preparation for rollback
        /// </summary>
        /// <param name="processors">processes to pause</param>
        void PausePeerProcesses(IEnumerable<int> processors);

        /// <summary>
        /// Tell the computation to initiate roll back, pausing all the workers
        /// </summary>
        void StartRollback();

        /// <summary>
        /// Tell the computation to roll back to the supplied frontiers, then restart
        /// </summary>
        /// <param name="frontiers">the new frontiers</param>
        void RestoreToFrontiers(IEnumerable<CheckpointLowWatermark> frontiers);

        /// <summary>
        /// An event that is raised once the graph is started.
        /// </summary>
        event EventHandler OnStartup;

        /// <summary>
        /// An event that is raised once the graph is shut down.
        /// </summary>
        event EventHandler OnShutdown;

        /// <summary>
        /// Signal the logging event that a message has been logged
        /// </summary>
        /// <param name="stageId">the stage receiving the message</param>
        /// <param name="vertexId">the vertex receiving the message</param>
        /// <param name="from">the message sender</param>
        /// <param name="time">the time of the message</param>
        /// <param name="count">the number of records in the message</param>
        void NotifyLog(int stageId, int vertexId, ReturnAddress from, string time, int count);
        /// <summary>
        /// Signal the logging event that a notification has been logged
        /// </summary>
        /// <param name="stageId">the stage receiving the notification</param>
        /// <param name="vertexId">the vertex receiving the notification</param>
        /// <param name="time">the time of the notification</param>
        void NotifyLog(int stageId, int vertexId, string time);
        /// <summary>
        /// An event that is raised whenever a message or notification is logged
        /// </summary>
        event EventHandler<LogEventArgs> OnLog;

        /// <summary>
        /// Blocks until all subscriptions have processed all inputs up to the supplied epoch.
        /// If the computation has no subscriptions, no synchronization occurs.
        /// </summary>
        /// <param name="epoch">The epoch.</param>
        /// <remarks>
        /// This method is commonly used along with <see cref="Input.BatchedDataSource{TRecord}"/> to
        /// process epochs of input data in batches, or with a bounded number of outstanding
        /// epochs.
        /// 
        /// If the computation contains many inputs and outputs that are stimulated asynchronously,
        /// the <see cref="Subscription.Sync"/> method provides a mechanism to synchronize on an individual 
        /// subscription.
        /// </remarks>
        /// <example>
        /// var source = new BatchedDataSource&lt;int&gt;();
        /// var subscription = computation.NewInput(source)
        ///                               /* ... */
        ///                               .Subscribe();
        /// 
        /// for (int i = 0; i &lt; numEpochs; ++i)
        /// {
        ///     source.OnNext(i);
        ///     computation.Sync(i); // Alternatively subscription.Sync(i);
        /// }
        /// </example>
        /// <seealso cref="Input.BatchedDataSource{TRecord}"/>
        /// <seealso cref="Subscription.Sync"/>
        void Sync(int epoch);

        /// <summary>
        /// Blocks until all computation in this graph has termintaed.
        /// </summary>        
        /// <remarks>
        /// This method must be called after calling <see cref="Activate"/> and before calling Dispose()
        /// or an error will be raised.
        /// </remarks>
        /// <example>
        /// The typical usage of Join is before the end of the <c>using</c> block for a
        /// Computation:
        /// <code>
        /// using (Computation computation = controller.NewComputation())
        /// {
        ///     /* Dataflow graph defined here. */
        ///     
        ///     computation.Activate();
        /// 
        ///     /* Inputs supplied here. */
        /// 
        ///     computation.Join();
        /// }
        /// </code>
        /// </example>
        /// <seealso cref="Microsoft.Research.Naiad.Controller.NewComputation"/>
        /// <seealso cref="Join"/>
        void Join();

        /// <summary>
        /// Starts computation in this graph.
        /// </summary>
        /// <remarks>
        /// This method must be called after the entire dataflow graph has been constructed, and before calling <see cref="Join"/>,
        /// or an error will be raised.
        /// </remarks>
        /// <example>
        /// The typical usage of Activate is between the definition of the dataflow graph and
        /// before inputs are supplied to the graph:
        /// <code>
        /// using (Computation computation = controller.NewComputation())
        /// {
        ///     /* Dataflow graph defined here. */
        ///     
        ///     computation.Activate();
        /// 
        ///     /* Inputs supplied here. */
        /// 
        ///     computation.Join();
        /// }
        /// </code>
        /// </example>
        /// <see cref="Microsoft.Research.Naiad.Controller.NewComputation"/>
        /// <seealso cref="Activate"/>
        void Activate();

        /// <summary>
        /// Restore the state of a computation from a set of checkpoints and logs
        /// </summary>
        void Restore();

        /// <summary>
        /// Adopt a different placement until the returned object is disposed
        /// </summary>
        /// <param name="scopedPlacement">the placement to use in the scope</param>
        /// <returns>object that will revert to the previous placement on disposal</returns>
        TemporaryPlacement Placement(Placement scopedPlacement);

        /// <summary>
        /// A base context for the computation
        /// </summary>
        StreamContext Context { get; }

        /// <summary>
        /// The <see cref="Controller"/> that hosts this graph.
        /// </summary>
        Controller Controller { get; }
    }

    /// <summary>
    /// The subgraph manager holds on to data related to a specific executable graph.
    /// Much of this functionality used to exist in the controller, but we extract out
    /// the 
    /// </summary>
    internal enum InternalComputationState { Inactive, Active, Complete, Failed }

    internal interface InternalComputation
    {
        int Index { get; }

        InternalComputationState CurrentState { get; }

        void Cancel(Exception dueTo);

        Exception Exception { get; }

        InternalController Controller { get; }

        SerializationFormat SerializationFormat { get; }

        Runtime.Progress.ProgressTracker ProgressTracker { get; }

        Runtime.Progress.Reachability Reachability { get; }

        IEnumerable<Dataflow.InputStage> Inputs { get; }
        IEnumerable<Subscription> Outputs { get; }
        IEnumerable<KeyValuePair<int, Dataflow.Stage>> Stages { get; }
        Dataflow.Stage Stage(int stageId);
        IEnumerable<KeyValuePair<int, Dataflow.Edge>> Edges { get; }

        void Register(Subscription sub);
        int Register(Dataflow.Stage stage);
        int Register(Dataflow.Edge edge);

        Placement DefaultPlacement { get; }

        void SignalShutdown();

        int AllocateNewGraphIdentifier();

        CheckpointTracker CheckpointTracker { get; }

        bool IsRestoring { get; }
        void ReceiveCheckpointFrontiersAndRepairProgress(IEnumerable<CheckpointLowWatermark> frontiers);

        void Activate();
        void MaterializeAll(bool deleteOldCheckpoints); // used by Controller.Restore(); perhaps can hide.

        void Connect<S, T>(Dataflow.StageOutput<S, T> stream, Dataflow.StageInput<S, T> recvPort, Action<S[], int[], int> key, Channel.Flags flags) where T : Time<T>;
        void Connect<S, T>(Dataflow.StageOutput<S, T> stream, Dataflow.StageInput<S, T> recvPort, Action<S[], int[], int> key) where T : Time<T>;
        void Connect<S, T>(Dataflow.StageOutput<S, T> stream, Dataflow.StageInput<S, T> recvPort) where T : Time<T>;

        Computation ExternalComputation { get; }
    }

    internal class BaseComputation : Computation, InternalComputation, IDisposable
    {
        private readonly int index;
        public int Index { get { return this.index; } }

        public long TicksSinceStartup { get { return this.controller.TicksSinceStartup; } }

        public SerializationFormat SerializationFormat { get { return this.Controller.SerializationFormat; } }

        public Computation ExternalComputation { get { return this; } }

        public void Cancel(Exception e)
        {
            Logging.Error("Cancelling execution of graph {0}, due to exception:\n{1}", this.Index, e);

            lock (this)
            {
                if (this.currentState == InternalComputationState.Failed)
                    return;

                this.currentState = InternalComputationState.Failed;
                this.Exception = e;


                if (this.Controller.NetworkChannel != null)
                {
                    MessageHeader header = MessageHeader.GraphFailure(this.index);
                    SendBufferPage page = SendBufferPage.CreateSpecialPage(header, 0);
                    BufferSegment segment = page.Consume();

                    Logging.Error("Broadcasting graph failure message");

                    this.Controller.NetworkChannel.BroadcastBufferSegment(header, segment);
                }

                this.ProgressTracker.Cancel();

            }
        }

        private InternalComputationState currentState = InternalComputationState.Inactive;
        public InternalComputationState CurrentState { get { return this.currentState; } }

        private Exception exception = null;
        public Exception Exception
        {
            get
            {
                return this.exception;
            }
            private set
            {
                lock (this)
                {
                    if (this.exception == null)
                        this.exception = value;
                    else if (!(this.exception is AggregateException))
                        this.exception = new AggregateException(this.exception, value);
                    else
                    {
                        List<Exception> innerExceptions = new List<Exception>((this.exception as AggregateException).InnerExceptions);
                        innerExceptions.Add(value);
                        this.exception = new AggregateException(innerExceptions);
                    }
                }
            }
        }

        private readonly InternalController controller;
        public InternalController Controller { get { return this.controller; } }

        Controller Computation.Controller { get { return this.controller.ExternalController; } }

        private readonly ProgressTracker progressTracker;

        internal readonly CheckpointTracker checkpointTracker;

        public ProgressTracker ProgressTracker { get { return this.progressTracker; } }

        private readonly Reachability reachability = new Reachability();
        public Reachability Reachability { get { return this.reachability; } }

        public event EventHandler<FrontierChangedEventArgs> OnFrontierChange 
        { 
            add { this.progressTracker.OnFrontierChanged += value; } 
            remove { this.progressTracker.OnFrontierChanged -= value; } 
        }

        public event EventHandler OnStartup;

        protected void NotifyOnStartup()
        {
            if (this.OnStartup != null)
                this.OnStartup(this, new EventArgs());
        }

        public event EventHandler OnShutdown;

        protected void NotifyOnShutdown()
        {
            if (this.OnShutdown != null)
                this.OnShutdown(this, new EventArgs());
        }

        public event EventHandler<GraphMaterializedEventArgs> OnMaterialized;

        private void NotifyMaterialized(
            IEnumerable<Stage> stages,
            IEnumerable<Pair<Pair<int, int>, int>> ftManager,
            IEnumerable<Pair<Pair<int,int>,Pair<int,int>>> edges)
        {
            if (this.OnMaterialized != null)
            {
                this.OnMaterialized(this, new GraphMaterializedEventArgs(stages, ftManager, edges));
            }
        }

        public event EventHandler<CheckpointPersistedEventArgs> OnCheckpointPersisted;

        internal void NotifyCheckpointPersisted(CheckpointUpdate checkpoint)
        {
            if (this.OnCheckpointPersisted != null)
            {
                this.OnCheckpointPersisted(this, new CheckpointPersistedEventArgs(checkpoint));
            }
        }

        public event EventHandler<StageStableEventArgs> OnStageStable;

        internal void NotifyStageStable(int stageId, Pointstamp[] frontier)
        {
            if (this.OnStageStable != null)
            {
                this.OnStageStable(this, new StageStableEventArgs(stageId, frontier));
            }
        }

        public event EventHandler<ProcessRestartedEventArgs> OnProcessRestarted;

        public void SimulateFailure(int processId, int restartDelay)
        {
            this.controller.TriggerSimulatedFailure(processId, restartDelay);
        }

        public void ReportSimulatedFailureRestart(int processId)
        {
            if (this.OnProcessRestarted != null)
            {
                this.OnProcessRestarted(this, new ProcessRestartedEventArgs(processId));
            }
        }

        public void PausePeerProcesses(IEnumerable<int> processors)
        {
            this.controller.PausePeerProcesses(processors);
        }

        public void StartRollback()
        {
            this.controller.StartRollback();
        }

        public void RestoreToFrontiers(IEnumerable<CheckpointLowWatermark> frontiers)
        {
            this.controller.RestoreToFrontiers(this.Index, frontiers);
        }

        public void ReceiveCheckpointUpdates(IEnumerable<CheckpointLowWatermark> updates)
        {
            this.checkpointTracker.ReceiveGCUpdates(updates);
        }

        public event EventHandler<LogEventArgs> OnLog;

        public void NotifyLog(int stageId, int vertexId, ReturnAddress from, string time, int count)
        {
            if (this.OnLog != null)
                this.OnLog(this, new LogEventArgs(stageId, vertexId, from, time, count));
        }

        public void NotifyLog(int stageId, int vertexId, string time)
        {
            if (this.OnLog != null)
                this.OnLog(this, new LogEventArgs(stageId, vertexId, time));
        }

        protected readonly List<Dataflow.InputStage> inputs = new List<Dataflow.InputStage>();
        public IEnumerable<Dataflow.InputStage> Inputs
        {
            get { return this.inputs; }
        }

        protected readonly List<Subscription> outputs = new List<Subscription>();
        public IEnumerable<Subscription> Outputs
        {
            get { return this.outputs; }
        }

        public void Register(Subscription sub) { outputs.Add(sub); }

        protected readonly Dictionary<int, Dataflow.Stage> stages = new Dictionary<int, Dataflow.Stage>();
        public IEnumerable<KeyValuePair<int, Dataflow.Stage>> Stages
        {
            get { return this.stages; }
        }
        public Dataflow.Stage Stage(int stageId)
        {
            return this.stages[stageId];
        }

        public bool IsRestoring { get; private set; }

        protected readonly Dictionary<int, Dataflow.Edge> edges = new Dictionary<int, Dataflow.Edge>();
        public IEnumerable<KeyValuePair<int, Dataflow.Edge>> Edges
        {
            get { return this.edges; }
        }

        private readonly List<DataSource> streamingInputs = new List<DataSource>();

        public Stream<R, TTime> NewInput<R, TTime>(DataSource<R, TTime> source) where TTime : Time<TTime>
        {
            string generatedName = string.Format("__Input{0}", this.inputs.Count);

            return this.NewInput(source, generatedName);
        }

        public Stream<R, TTime> NewInput<R, TTime>(DataSource<R, TTime> source, string name) where TTime : Time<TTime>
        {
            Dataflow.StreamingInputStage<R, TTime> ret = new Dataflow.StreamingInputStage<R, TTime>(source, this.DefaultPlacement, this, name);
            this.inputs.Add(ret);
            this.streamingInputs.Add(source);
            return ret;
        }

        public int Register(Dataflow.Stage stage)
        {
            int ret = AllocateNewGraphIdentifier();
            this.stages.Add(ret, stage);
            return ret;
        }

        public int Register(Dataflow.Edge edge)
        {
            int ret = AllocateNewGraphIdentifier();

            this.edges.Add(ret, edge);
            return ret;
        }

        private int nextGraphIdentifier;
        public int AllocateNewGraphIdentifier()
        {
            return nextGraphIdentifier++;
        }

        public int AllocatedGraphIdentifiers
        {
            get { return nextGraphIdentifier; }
        }


        public void Connect<S, T>(Dataflow.StageOutput<S, T> stream, Dataflow.StageInput<S, T> recvPort, Action<S[], int[], int> key, Channel.Flags flags)
            where T : Time<T>
        {
            Dataflow.Edge edge = stream.NewEdge(recvPort, key, flags);
            stream.ForStage.Targets.Add(edge);
            recvPort.ForStage.Sources.Add(edge);
            recvPort.SetChannelId(edge.ChannelId, stream.ForStage.StageId);
        }
        public void Connect<S, T>(Dataflow.StageOutput<S, T> stream, Dataflow.StageInput<S, T> recvPort, Action<S[], int[], int> key)
            where T : Time<T>
        {
            this.Connect(stream, recvPort, key, Channel.Flags.None);
        }
        public void Connect<S, T>(Dataflow.StageOutput<S, T> stream, Dataflow.StageInput<S, T> recvPort)
            where T : Time<T>
        {
            this.Connect(stream, recvPort, null, Channel.Flags.None);
        }

        private bool isJoined = false;

        /// <summary>
        /// Blocks until all computation is complete and resources are released.
        /// </summary>
        public void Join()
        {
            if (this.CurrentState == InternalComputationState.Inactive)
            {
                throw new Exception("Joining graph manager before calling Activate()");
            }
            else
            {
                foreach (var input in this.streamingInputs)
                    input.Join();

                // Wait for all progress updates to drain (or an exception to occur).
                this.ProgressTracker.BlockUntilComplete();

                // The shutdown counter will not be signalled in an exceptional case,
                // so test the exception here.
                if (this.exception != null)
                {
                    this.currentState = InternalComputationState.Failed;
                    throw new Exception("Error during Naiad execution", this.exception);
                }

                // We terminated successfully, so wait until all thread/vertex shutdown routines have
                // finished.
                this.ShutdownCounter.Wait();

                NotifyOnShutdown();

                this.isJoined = true;
                this.currentState = InternalComputationState.Complete;
            }
        }

        private Placement defaultPlacement;
        public Placement DefaultPlacement { get { return this.defaultPlacement; } }

        private class OverridePlacement : TemporaryPlacement
        {
            private readonly Placement previousPlacement;
            private readonly BaseComputation parent;

            public void Dispose()
            {
                this.parent.defaultPlacement = this.previousPlacement;
            }

            public OverridePlacement(Placement previousPlacement, BaseComputation parent)
            {
                this.previousPlacement = previousPlacement;
                this.parent = parent;
            }
        }

        public TemporaryPlacement Placement(Placement scopedPlacement)
        {
            Placement previous = this.defaultPlacement;
            this.defaultPlacement = scopedPlacement;
            return new OverridePlacement(previous, this);
        }

        public StreamContext Context { get { return new StreamContext(this); } }

        public BaseComputation(InternalController controller, int index)
        {
            this.controller = controller;
            this.defaultPlacement = this.controller.DefaultPlacement;
            this.index = index;

            this.ShutdownCounter = new CountdownEvent(controller.Workers.Count);

            if (this.controller.Configuration.DistributedProgressTracker)
                this.progressTracker = new DistributedProgressTracker(this);
            else
                this.progressTracker = new CentralizedProgressTracker(this);

            if (this.Controller.Configuration.LoggingEnabled)
            {
                this.checkpointTracker = new CheckpointTracker(this);
            }
            else
            {
                this.checkpointTracker = null;
            }
        }

        /// <summary>
        /// Blocks until all computation associated with the supplied epoch have been retired.
        /// </summary>
        /// <param name="epoch">Epoch to wait for</param>
        public void Sync(int epoch)
        {
            foreach (Dataflow.InputStage input in this.inputs)
            {
                if (!input.IsCompleted && input.CurrentEpoch <= epoch)
                {
                    Logging.Debug("Syncing at epoch ({0}) in the future of {1}.", epoch, input);
                }
            }

            if (this.outputs.Count == 0)
                Logging.Debug("Syncing a computation with no subscriptions; no synchronization performed.");

            foreach (var subscription in this.Outputs)
                subscription.Sync(epoch);
        }

        bool activated = false;
        bool materialized = false;
        public void Activate()
        {
            if (activated)
                return;

            Logging.Progress("Activating Computation");

            activated = true;

            this.Reachability.UpdateReachabilityPartialOrder(this);

            this.MaterializeAll(true);

            this.Controller.DoStartupBarrier();

            this.currentState = InternalComputationState.Active;

            this.Controller.Workers.WakeUp();

            foreach (var streamingInput in this.streamingInputs)
                streamingInput.Activate();

            this.NotifyOnStartup();
        }

        private IEnumerable<Pair<Pair<int,int>,Pair<int,int>>> EdgeTopology()
        {
            foreach (Stage stage in this.stages.Values.Where(s => s.CheckpointType != CheckpointType.None))
            {
                foreach (Edge edge in stage.Targets)
                {
                    if (edge.Exchanges)
                    {
                        foreach (int src in Enumerable.Range(0, edge.SourceStage.Placement.Count))
                        {
                            foreach (int dst in Enumerable.Range(0, edge.TargetStage.Placement.Count))
                            {
                                yield return
                                    (edge.SourceStage.StageId.PairWith(src))
                                    .PairWith(edge.TargetStage.StageId.PairWith(dst));
                            }
                        }
                    }
                    else
                    {
                        foreach (int src in Enumerable.Range(0, edge.SourceStage.Placement.Count))
                        {
                            yield return
                                (edge.SourceStage.StageId.PairWith(src))
                                .PairWith(edge.TargetStage.StageId.PairWith(src));
                        }
                    }
                }
            }
        }

        public void ReMaterializeForRollback()
        {
            Dictionary<int, Dictionary<int, Vertex>> newVertices = new Dictionary<int, Dictionary<int, Vertex>>();

            foreach (var stage in this.stages.Values)
            {
                newVertices.Add(stage.StageId, stage.ReMaterializeForRollback());
            }

            foreach (var input in this.inputs)
            {
                input.ReMaterializeForRollback();
            }

            foreach (var edge in this.edges.Values)
            {
                edge.ReMaterializeForRollback(newVertices[edge.SourceStage.StageId], newVertices[edge.TargetStage.StageId]);
                edge.EnableReceiveLogging();
            }
        }

        public void MaterializeAll(bool deleteOldCheckpoints)
        {
            if (this.materialized)
                return;

            foreach (var stage in this.stages.Values)
            {
                stage.Materialize();
            }

            foreach (var edge in this.edges.Values)
            {
                edge.Materialize();
            }

            if (this.Controller.Configuration.LoggingEnabled)
            {
                foreach (var stage in this.stages.Values)
                {
                    stage.EnableLogging(this.Controller.Configuration.CheckpointingFactory, this.checkpointTracker, deleteOldCheckpoints);
                }

                foreach (var edge in this.edges.Values)
                {
                    edge.EnableReceiveLogging();
                }

                this.NotifyMaterialized(
                    this.stages.Values.Where(s => s.CheckpointType != CheckpointType.None),
                    this.stages.Values
                        .Where(s => s.CheckpointType != CheckpointType.None)
                        .SelectMany(s => s.Placement.Select(v =>
                            (s.StageId.PairWith(v.VertexId).PairWith(this.checkpointTracker.GetUpdateReceiverVertex(v))))),
                    this.EdgeTopology());

                this.Controller.Workers.Sleeping += (x, y) => this.ReactToWorkerSleeping(y.QueueHighWaterMark);

                this.checkpointTracker.RegisterStageStableCallbacks(this);
            }

            this.materialized = true;
        }

        private int quiescentHighWaterMark = 0;
        private ManualResetEventSlim sleepingWorkersEvent = new ManualResetEventSlim(false);

        private void ReactToWorkerSleeping(int highWaterMark)
        {
            lock (this)
            {
                if (highWaterMark > quiescentHighWaterMark)
                {
                    quiescentHighWaterMark = highWaterMark;
                    sleepingWorkersEvent.Set();
                }
            }
        }

        private void WaitForQuiescence(int maxQueued)
        {
            while (true)
            {
                lock (this)
                {
                    if (this.quiescentHighWaterMark >= maxQueued)
                    {
                        break;
                    }
                    this.sleepingWorkersEvent.Reset();
                }

                this.sleepingWorkersEvent.Wait();
            }
        }

        public void ReceiveCheckpointFrontiersAndRepairProgress(IEnumerable<CheckpointLowWatermark> frontiers)
        {
            this.IsRestoring = true;

            System.Diagnostics.Stopwatch stopwatch = this.controller.Stopwatch;

            long startTicks = stopwatch.ElapsedTicks;

            foreach (CheckpointLowWatermark frontier in frontiers)
            {
                this.stages[frontier.stageId].SetRollbackFrontier(frontier);
            }

            long frontierTicks = stopwatch.ElapsedTicks;

            long blockTicks = stopwatch.ElapsedTicks;

            // for each scheduler in this process, install the scheduler-local copy of the initial discard frontiers
            // these will be abandoned by each scheduler after its vertices have replayed their deferred and logged
            // messages, and reachability reports that the frontiers are no longer reachable. There is a separate copy
            // per scheduler to avoid concurrency issues
            foreach (var scheduler in this.stages.Values
                .SelectMany(s => s.Vertices)
                .Select(v => v.Stage.InternalComputation.PairWith(v.Scheduler))
                .Distinct()
                .Select(s => s.Second.PairWith(s.Second.State(s.First))))
            {
                scheduler.Second.DiscardManager.Reset();
                foreach (var stage in this.stages.Where(s => s.Value.CheckpointType != CheckpointType.None))
                {
                    stage.Value.SetDiscardFrontiers(scheduler.Second.DiscardManager);
                }
                scheduler.Second.PruneNotificationsAndRepairProgress(scheduler.First);
                scheduler.Second.PostOffice.PruneMessagesAndRepairProgress(this.stages);
            }

            long pruneTicks = stopwatch.ElapsedTicks;

            // reset the state of any vertex that needs to roll back. Do this after removing the notifications,
            // so that subscribe vertices put theirs back in
            this.ReMaterializeForRollback();

            long materializeTicks = stopwatch.ElapsedTicks;

            Dictionary<Pointstamp, long> inputHolds = new Dictionary<Pointstamp,long>();
            foreach (InputStage input in this.inputs)
            {
                int numberOfLocalInputs = this.stages[input.InputId].Vertices.Count();
                foreach (var time in input.InitialTimes)
                {
                    inputHolds.Add(time, numberOfLocalInputs);
                }
            }
            // don't let anything shut down until we have finished repairing all the vertices' progress
            this.progressTracker.Aggregator.OnRecv(inputHolds);

            this.checkpointTracker.RestoreProgress();

            long progressTicks = stopwatch.ElapsedTicks;

            foreach (var stage in this.inputs.Select(i => this.stages[i.InputId]).Where(s => s.CheckpointType == CheckpointType.None))
            {
                foreach (var v in stage.Vertices)
                {
                    v.RestoreProgressUpdates(false);
                }
            }

            foreach (var stage in this.stages.Where(s => s.Value.CheckpointType != CheckpointType.None))
            {
                foreach (var v in stage.Value.Vertices)
                {
                    v.SetRollbackFrontier();
                    v.RestoreProgressUpdates(true);
                }
            }

            long restoreTicks = stopwatch.ElapsedTicks;

            // start the worker threads so we can do restoration on the correct threads
            this.controller.Workers.Resume();

            long resumeTicks = stopwatch.ElapsedTicks;

            int maxQueued = 0;

            // logging vertices actually have to be replayed in order to repair their progress traffic
            foreach (var stage in this.stages.Where(s => s.Value.CheckpointType == CheckpointType.Logging))
            {
                foreach (var v in stage.Value.Vertices)
                {
                    maxQueued = Math.Max(maxQueued, v.Replay(Vertex.ReplayMode.ReplayVertex));
                }
            }

            // wait until all the replays have completed
            this.WaitForQuiescence(maxQueued);

            long replayTicks = stopwatch.ElapsedTicks;

            this.IsRestoring = false;

            long unblockTicks = stopwatch.ElapsedTicks;

            // remove the discard frontiers. The outgoing message loggers have taken a private copy, so
            // their messages will still be filtered correctly
            foreach (var scheduler in this.stages.Values
                .Where(s => s.CheckpointType != CheckpointType.None)
                .SelectMany(s => s.Vertices)
                .Select(v => v.Stage.InternalComputation.PairWith(v.Scheduler))
                .Distinct()
                .Select(s => s.Second.State(s.First)))
            {
                scheduler.DiscardManager.Reset();
            }

            // there may have been some checkpoint updates generated during restoration, which we need to send now
            maxQueued = this.checkpointTracker.SendDeferredMessages(maxQueued);

            // wait until the updates have been sent
            this.WaitForQuiescence(maxQueued);

            this.progressTracker.ForceFlush();

            long totalTicks = stopwatch.ElapsedTicks;

            long frontierMicroSeconds = ((frontierTicks - startTicks) * 1000000L) / System.Diagnostics.Stopwatch.Frequency;
            long blockMicroSeconds = ((blockTicks - startTicks) * 1000000L) / System.Diagnostics.Stopwatch.Frequency;
            long pruneMicroSeconds = ((pruneTicks - startTicks) * 1000000L) / System.Diagnostics.Stopwatch.Frequency;
            long materializeMicroSeconds = ((materializeTicks - startTicks) * 1000000L) / System.Diagnostics.Stopwatch.Frequency;
            long progressMicroSeconds = ((progressTicks - startTicks) * 1000000L) / System.Diagnostics.Stopwatch.Frequency;
            long restoreMicroSeconds = ((restoreTicks - startTicks) * 1000000L) / System.Diagnostics.Stopwatch.Frequency;
            long resumeMicroSeconds = ((resumeTicks - startTicks) * 1000000L) / System.Diagnostics.Stopwatch.Frequency;
            long replayMicroSeconds = ((replayTicks - startTicks) * 1000000L) / System.Diagnostics.Stopwatch.Frequency;
            long unblockMicroSeconds = ((unblockTicks - startTicks) * 1000000L) / System.Diagnostics.Stopwatch.Frequency;
            long flushMicroSeconds = ((totalTicks - startTicks) * 1000000L) / System.Diagnostics.Stopwatch.Frequency;
            long totalMicroSeconds = (totalTicks * 1000000L) / System.Diagnostics.Stopwatch.Frequency;
            this.Controller.WriteLog(
                String.Format("{0:D3} REP {1:D7} {2:D7} {3:D7} {4:D7} {5:D7} {6:D7} {7:D7} {8:D7} {9:D7} {10:D7} {11:D11}",
                this.controller.Configuration.ProcessID,
                frontierMicroSeconds, blockMicroSeconds, pruneMicroSeconds, materializeMicroSeconds, progressMicroSeconds,
                restoreMicroSeconds, resumeMicroSeconds, replayMicroSeconds, unblockMicroSeconds, flushMicroSeconds,
                totalMicroSeconds));
        }

        public void Rollback()
        {
            // when we enter here, all the receive threads are paused but the progress has been repaired
            // and other workers are starting up. We need to roll everyone back, send out any needed messages,
            // then restart the receive threads

            int maxQueued = 0;

            // first roll everybody back: this does nothing for logging checkpoints which have already
            // been replayed in order to recreate their state
            foreach (var stage in this.stages.Where(s => s.Value.CheckpointType != CheckpointType.None))
            {
                foreach (var v in stage.Value.Vertices)
                {
                    maxQueued = Math.Max(maxQueued, v.Replay(Vertex.ReplayMode.RollbackVertex));
                }
            }

            // wait until all the replays have completed
            this.WaitForQuiescence(maxQueued);

            // it's ok to add new inputs again
            foreach (InputStage input in this.inputs)
            {
                input.ReleaseExternalCalls();
            }

            foreach (var scheduler in this.stages.Values
                .SelectMany(s => s.Vertices)
                .Select(v => v.Scheduler)
                .Distinct())
            {
                scheduler.StopRestoring();
            }

            Dictionary<Pointstamp, long> inputHolds = new Dictionary<Pointstamp, long>();
            // remove the fake holds from the inputs now that progress has been repaired
            foreach (InputStage input in this.inputs)
            {
                int numberOfLocalInputs = this.stages[input.InputId].Vertices.Count();
                foreach (var time in input.InitialTimes)
                {
                    inputHolds.Add(time, -numberOfLocalInputs);
                }
            }
            this.progressTracker.Aggregator.OnRecv(inputHolds);

            this.progressTracker.ForceFlush();

            StringWriter w = new StringWriter();
            this.progressTracker.Complain(w);
            Console.WriteLine(w);
        }

        public void RestartAfterRollback()
        {
            foreach (var kvp in this.Stages.Where(s => s.Value.CheckpointType != CheckpointType.None))
            {
                foreach (var v in kvp.Value.Vertices)
                {
                    v.Replay(Vertex.ReplayMode.SendDeferredMessages);
                }
            }
        }

        public void Restore()
        {
#if false
            if (activated)
                return;

            Logging.Progress("Restoring Computation");

            activated = true;

            this.Reachability.UpdateReachabilityPartialOrder(this);

            this.MaterializeAll(false);

            HashSet<int> inputStages = new HashSet<int>();
            foreach (InputStage input in this.inputs)
            {
                inputStages.Add(input.InputId);
            }

            // for each scheduler in this process, install the scheduler-local copy of the initial discard frontiers
            // these will be abandoned by each scheduler after its vertices have replayed their deferred and logged
            // messages, and reachability reports that the frontiers are no longer reachable. There is a separate copy
            // per scheduler to avoid concurrency issues
            foreach (var discardManager in this.stages.Values
                .Where(s => s.CheckpointType != CheckpointType.None)
                .SelectMany(s => s.Vertices)
                .Select(v => v.Stage.InternalComputation.PairWith(v.Scheduler))
                .Distinct()
                .Select(s => s.Second.State(s.First).DiscardManager))
            {
                foreach (var stage in this.stages.Where(s => s.Value.CheckpointType != CheckpointType.None).OrderBy(s => s.Key))
                {
                    stage.Value.SetDiscardFrontiers(discardManager);
                }
            }

            this.checkpointTracker.AddReplayHoldsForRestoration();

            foreach (var stage in this.stages.Where(s => s.Value.CheckpointType != CheckpointType.None).OrderBy(s => s.Key))
            {
                foreach (var v in stage.Value.Vertices)
                {
                    v.AddReplayHolds();
                }
            }

            this.currentState = InternalComputationState.Active;

            this.Controller.Workers.Sleeping += (x, y) => this.ReactToWorkerSleeping(y.QueueHighWaterMark);

            this.Controller.Workers.WakeUp();

            int maxQueued = 0;

            // first roll everybody back
            foreach (var stage in this.stages.Where(s => s.Value.CheckpointType != CheckpointType.None).OrderBy(s => s.Key))
            {
                foreach (var v in stage.Value.Vertices)
                {
                    maxQueued = Math.Max(maxQueued, v.Replay(Vertex.ReplayMode.RollbackVertex));
                }
            }

            this.WaitForQuiescence(maxQueued);

            // first do all the non-inputs
            foreach (var kvp in this.Stages.Where(s => s.Value.CheckpointType != CheckpointType.None).Where(s => !inputStages.Contains(s.Key)).OrderBy(x => x.Key))
            {
                foreach (var v in kvp.Value.Vertices)
                {
                    maxQueued = Math.Max(maxQueued, v.Replay(Vertex.ReplayMode.ReplayVertex));
                }
            }

            this.WaitForQuiescence(maxQueued);

            // then the inputs, which may shut down some vertices
            foreach (var kvp in this.Stages.Where(s => s.Value.CheckpointType != CheckpointType.None).Where(s => inputStages.Contains(s.Key)).OrderBy(x => x.Key))
            {
                foreach (var v in kvp.Value.Vertices)
                {
                    maxQueued = Math.Max(maxQueued, v.Replay(Vertex.ReplayMode.ReplayVertex));
                }
            }

            this.WaitForQuiescence(maxQueued);

            // wait for all the processes to finish restoring before starting to send any messages
            this.Controller.DoStartupBarrier();

            // there may have been some checkpoint updates generated during restoration, which we need to send now
            maxQueued = this.checkpointTracker.SendDeferredMessages(maxQueued);

            this.WaitForQuiescence(maxQueued);

            foreach (var kvp in this.Stages.Where(s => s.Value.CheckpointType != CheckpointType.None).OrderBy(x => x.Key))
            {
                foreach (var v in kvp.Value.Vertices)
                {
                    maxQueued = Math.Max(maxQueued, v.Replay(Vertex.ReplayMode.SendDeferredMessages));
                }
            }

            foreach (var kvp in this.Stages.Where(s => s.Value.CheckpointType != CheckpointType.None).OrderBy(x => x.Key))
            {
                foreach (var v in kvp.Value.Vertices)
                {
                    maxQueued = Math.Max(maxQueued, v.Replay(Vertex.ReplayMode.SendLoggedMessages));
                }
            }
#endif
        }

        private CountdownEvent ShutdownCounter;

        public void SignalShutdown()
        {
            this.ShutdownCounter.Signal();
        }

        public CheckpointTracker CheckpointTracker { get { return this.checkpointTracker; } }

        public void Dispose()
        {
            if (this.activated && !this.isJoined)
            {
                Logging.Error("Attempted to dispose Computation before joining.");
                Logging.Error("You must call Computation.Join() before disposing/exiting the using block.");
                //System.Environment.Exit(-1);
            }
        }
    }
}
