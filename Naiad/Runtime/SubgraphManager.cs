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

using Microsoft.Research.Naiad.Serialization;
using Microsoft.Research.Naiad.Dataflow;
using Microsoft.Research.Naiad.Dataflow.Channels;
using Microsoft.Research.Naiad.Runtime.Progress;
using System;
using System.Collections.Generic;
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
            this.computation.Join();
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

        public Stream<TRecord, Epoch> NewInput<TRecord>(DataSource<TRecord> source)
        {
            return this.computation.NewInput<TRecord>(source);
        }

        public Stream<TRecord, Epoch> NewInput<TRecord>(DataSource<TRecord> source, string name)
        {
            return this.computation.NewInput<TRecord>(source, name);
        }

        public event EventHandler<FrontierChangedEventArgs> OnFrontierChange { add { this.computation.OnFrontierChange += value; } remove { this.computation.OnFrontierChange -= value; } }

        public event EventHandler OnStartup { add { this.computation.OnStartup += value; } remove { this.computation.OnStartup -= value; } }

        public event EventHandler OnShutdown { add { this.computation.OnShutdown += value; } remove { this.computation.OnShutdown -= value; } }

        public void Sync(int epoch)
        {
            this.computation.Sync(epoch);
        }

        public void Activate()
        {
            this.computation.Activate();
        }

        public Controller Controller
        {
            get { return this.controller; }
        }
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
        /// <param name="source">data source</param>
        /// <returns>A new input stage</returns>
        Stream<TRecord, Epoch> NewInput<TRecord>(DataSource<TRecord> source);

        /// <summary>
        /// Creates a new input stage from the given <see cref="DataSource"/>.
        /// </summary>
        /// <typeparam name="TRecord">record type</typeparam>
        /// <param name="source">data source</param>
        /// <param name="name">name for the input</param>
        /// <returns>A new input stage</returns>
        Stream<TRecord, Epoch> NewInput<TRecord>(DataSource<TRecord> source, string name);

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
        /// An event that is raised once the graph is started.
        /// </summary>
        event EventHandler OnStartup;

        /// <summary>
        /// An event that is raised once the graph is shut down.
        /// </summary>
        event EventHandler OnShutdown;

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

        // TODO these are only used for reporting; could swap over to new DataSource-based approach.
        Dataflow.InputStage<R> NewInput<R>();
        Dataflow.InputStage<R> NewInput<R>(string name);

        IEnumerable<Dataflow.InputStage> Inputs { get; }
        IEnumerable<Subscription> Outputs { get; }
        IEnumerable<KeyValuePair<int, Dataflow.Stage>> Stages { get; }
        IEnumerable<KeyValuePair<int, Dataflow.Edge>> Edges { get; }

        void Register(Subscription sub);
        int Register(Dataflow.Stage stage);
        int Register(Dataflow.Edge edge);

        Placement DefaultPlacement { get; }

        Dataflow.ITimeContextManager ContextManager { get; }

        void SignalShutdown();

        int AllocateNewGraphIdentifier();

        void Activate();
        void MaterializeAll(); // used by Controller.Restore(); perhaps can hide.

        void Connect<S, T>(Dataflow.StageOutput<S, T> stream, Dataflow.StageInput<S, T> recvPort, Action<S[], int[], int> key, Channel.Flags flags) where T : Time<T>;
        void Connect<S, T>(Dataflow.StageOutput<S, T> stream, Dataflow.StageInput<S, T> recvPort, Action<S[], int[], int> key) where T : Time<T>;
        void Connect<S, T>(Dataflow.StageOutput<S, T> stream, Dataflow.StageInput<S, T> recvPort) where T : Time<T>;

        Computation ExternalComputation { get; }
    }

    internal class BaseComputation : Computation, InternalComputation, IDisposable
    {
        private readonly int index;
        public int Index { get { return this.index; } }

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

        protected readonly Dictionary<int, Dataflow.Edge> edges = new Dictionary<int, Dataflow.Edge>();
        public IEnumerable<KeyValuePair<int, Dataflow.Edge>> Edges
        {
            get { return this.edges; }
        }

        /// <summary>
        /// Constructs and registers a new input collection of type R.
        /// </summary>
        /// <typeparam name="R">Record</typeparam>
        /// <returns>New input collection</returns>
        public Dataflow.InputStage<R> NewInput<R>()
        {
            string generatedName = string.Format("__Input{0}", this.inputs.Count);
            return this.NewInput<R>(generatedName);
        }

        public Dataflow.InputStage<R> NewInput<R>(string name)
        {
            Dataflow.InputStage<R> ret = new Dataflow.InputStage<R>(this.DefaultPlacement, this, name);
            this.inputs.Add(ret);
            return ret;
        }

        private readonly List<DataSource> streamingInputs = new List<DataSource>();

        public Stream<R, Epoch> NewInput<R>(DataSource<R> source)
        {
            string generatedName = string.Format("__Input{0}", this.inputs.Count);

            return this.NewInput(source, generatedName);
        }

        public Stream<R, Epoch> NewInput<R>(DataSource<R> source, string name)
        {
            Dataflow.StreamingInputStage<R> ret = new Dataflow.StreamingInputStage<R>(source, this.DefaultPlacement, this, name);
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
            stream.ForStage.Targets.Add(new Dataflow.Edge<S, T>(stream, recvPort, key, flags));
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

        private readonly Dataflow.TimeContextManager contextManager;

        internal void InitializeReporting(bool makeDomain, bool makeInline, bool doAggregate)
        {
            this.contextManager.InitializeReporting(makeDomain, makeInline, doAggregate);
        }

        private bool isJoined = false;


        private readonly bool DomainReporting = false;

        protected Dataflow.InputStage<string> RootDomainStatisticsStage
        {
            get { return this.contextManager.Reporting.domainReportingIngress; }
        }
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
                if (this.DomainReporting)
                {
                    foreach (var input in this.streamingInputs)
                        input.Join();

                    var largestRealInputEpoch = this.inputs.Max(x => x.CurrentEpoch);
                    Logging.Info("Largest real epoch " + largestRealInputEpoch + " current stats " + RootDomainStatisticsStage.CurrentEpoch);
                    while (this.RootDomainStatisticsStage.CurrentEpoch < largestRealInputEpoch)
                    {
                        this.RootDomainStatisticsStage.OnNext(new string[] { });
                    }
                    Logging.Info("New stats " + RootDomainStatisticsStage.CurrentEpoch);
                    // wait until all real inputs have drained (possibly generating new logging)
                    if (largestRealInputEpoch > 0)
                    {
                        Logging.Info("Syncing stats " + (largestRealInputEpoch - 1));
                        this.Sync(largestRealInputEpoch - 1);
                    }
                    // now shut down the reporting
                    Console.WriteLine("Calling reporting completed");
                    this.RootDomainStatisticsStage.OnCompleted();
                }
                else
                {
                    foreach (var input in this.streamingInputs)
                        input.Join();
                }

                // Wait for all progress updates to drain (or an exception to occur).
                this.ProgressTracker.BlockUntilComplete();

                // The shutdown counter will not be signalled in an exceptional case,
                // so test the exception here.
                if (this.exception != null)
                {
                    this.currentState = InternalComputationState.Failed;
                    throw new Exception("Error during Naiad execution", this.exception);
                }

                // We terminated successfully, so wait until all shutdown routines have
                // finished.
                this.ShutdownCounter.Wait();

                NotifyOnShutdown();

                this.isJoined = true;
                this.currentState = InternalComputationState.Complete;
            }
        }

        private Placement defaultPlacement;
        public Placement DefaultPlacement { get { return this.defaultPlacement; } }

        public BaseComputation(InternalController controller, int index)
        {
            this.controller = controller;
            this.defaultPlacement = this.controller.DefaultPlacement;
            this.index = index;

            this.ShutdownCounter = new CountdownEvent(controller.Workers.Count);

            this.contextManager = new Microsoft.Research.Naiad.Dataflow.TimeContextManager(this);

            if (this.controller.Configuration.DistributedProgressTracker)
                this.progressTracker = new DistributedProgressTracker(this);
            else
                this.progressTracker = new CentralizedProgressTracker(this);

            this.InitializeReporting(this.controller.Configuration.DomainReporting, this.controller.Configuration.InlineReporting, this.controller.Configuration.AggregateReporting);
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

            this.MaterializeAll();

            this.Controller.DoStartupBarrier();

            this.currentState = InternalComputationState.Active;

            this.Controller.Workers.WakeUp();

            foreach (var streamingInput in this.streamingInputs)
                streamingInput.Activate();

            this.NotifyOnStartup();
        }

        public void MaterializeAll()
        {
            if (this.materialized)
                return;

            foreach (var stage in this.Stages)
                stage.Value.Materialize();

            foreach (var edge in this.Edges)
                edge.Value.Materialize();

            this.materialized = true;
        }

        private CountdownEvent ShutdownCounter;

        public void SignalShutdown()
        {
            this.ShutdownCounter.Signal();
        }

        public Dataflow.ITimeContextManager ContextManager
        {
            get { return this.contextManager; }
        }

        public void Dispose()
        {
            if (this.activated && !this.isJoined)
            {
                Logging.Error("Attempted to dispose Computation before joining.");
                Logging.Error("You must call Computation.Join() before disposing/exiting the using block.");
                //System.Environment.Exit(-1);
            }

            this.ContextManager.ShutDown();
        }
    }
}
