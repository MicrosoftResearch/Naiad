/*
 * Naiad ver. 0.2
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

using Naiad.Dataflow;
using Naiad.Dataflow.Channels;
using Naiad.Runtime.Progress;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;
using System.Text;
using System.Threading.Tasks;

namespace Naiad.Runtime
{
    /// <summary>
    /// Manages the construction and execution of individual dataflow graphs.
    /// </summary>
    public interface GraphManager : IDisposable
    {
        /// <summary>
        /// Creates a new input stage from a typed DataSource.
        /// </summary>
        /// <typeparam name="TRecord">record type</typeparam>
        /// <param name="source">data source</param>
        /// <returns>A new input stage</returns>
        Dataflow.Stream<TRecord, Epoch> NewInput<TRecord>(DataSource<TRecord> source);

        /// <summary>
        /// Creates a new input stage from a typed DataSource.
        /// </summary>
        /// <typeparam name="TRecord">record type</typeparam>
        /// <param name="source">data source</param>
        /// <param name="name">name for the input</param>
        /// <returns>A new input stage</returns>
        Dataflow.Stream<TRecord, Epoch> NewInput<TRecord>(DataSource<TRecord> source, string name);

        /// <summary>
        /// Returns a frontier with which one can register frontier change events.
        /// </summary>
        Naiad.Runtime.Progress.Frontier Frontier { get; }

        /// <summary>
        /// An event that is called once the graph is started.
        /// </summary>
        event EventHandler OnStartup;

        /// <summary>
        /// An event that is called once the graph is shut down.
        /// </summary>
        event EventHandler OnShutdown;

        /// <summary>
        /// Blocks until all subscriptions have processed all inputs up to the supplied epoch.
        /// </summary>
        /// <param name="epoch"></param>
        void Sync(int epoch);

        /// <summary>
        /// Blocks until all computation complete for the graph.
        /// </summary>
        void Join();

        /// <summary>
        /// Enables the graph for execution, and disables further stage construction.
        /// </summary>
        void Activate();
    }

    /// <summary>
    /// The subgraph manager holds on to data related to a specific executable graph.
    /// Much of this functionality used to exist in the controller, but we extract out
    /// the 
    /// </summary>
    internal enum InternalGraphManagerState { Inactive, Active, Complete, Failed }

    internal interface InternalGraphManager
    {
        int Index { get; }

        InternalGraphManagerState CurrentState { get; }

        void Cancel(Exception dueTo);

        Exception Exception { get; }

        InternalController Controller { get; }

        Runtime.Progress.ProgressTracker ProgressTracker { get; }
        
        Scheduling.Reachability Reachability { get; }

        // TODO these are only used for reporting; could swap over to new DataSource-based approach.
        Dataflow.InputStage<R> NewInput<R>();
        Dataflow.InputStage<R> NewInput<R>(string name);

        IEnumerable<Dataflow.InputStage> Inputs { get; }
        IEnumerable<Dataflow.Subscription> Outputs { get; }
        IEnumerable<KeyValuePair<int, Dataflow.Stage>> Stages { get; }
        IEnumerable<KeyValuePair<int, Dataflow.Edge>> Edges { get; }

        void Register(Dataflow.Subscription sub);
        int Register(Dataflow.Stage stage);
        int Register(Dataflow.Edge edge);
        
        Scheduling.Placement DefaultPlacement { get; }        

        Dataflow.ITimeContextManager ContextManager { get; }

        int AllocateNewGraphIdentifier();

        void Activate();
        void MaterializeAll(); // used by Controller.Restore(); perhaps can hide.

        void Connect<S, T>(Dataflow.StageOutput<S, T> stream, Dataflow.StageInput<S, T> recvPort, Expression<Func<S, int>> key, Channel.Flags flags) where T : Time<T>;
        void Connect<S, T>(Dataflow.StageOutput<S, T> stream, Dataflow.StageInput<S, T> recvPort, Expression<Func<S, int>> key) where T : Time<T>;
        void Connect<S, T>(Dataflow.StageOutput<S, T> stream, Dataflow.StageInput<S, T> recvPort) where T : Time<T>;
    }

    internal class BaseGraphManager : GraphManager, InternalGraphManager, IDisposable
    {
        private readonly int index;
        public int Index { get { return this.index; } }

        public void Cancel(Exception e)
        {
            Logging.Error("Cancelling execution of graph {0}, due to exception:\n{1}", this.Index, e);

            lock (this)
            {
                if (this.currentState == InternalGraphManagerState.Failed)
                    return;

                this.currentState = InternalGraphManagerState.Failed;
                this.Exception = e;

                MessageHeader header = MessageHeader.GraphFailure(this.index);
                SendBufferPage page = SendBufferPage.CreateSpecialPage(header, 0);
                BufferSegment segment = page.Consume();
                
                Logging.Error("Broadcasting graph failure message");
                            
                this.Controller.NetworkChannel.BroadcastBufferSegment(header, segment);

                this.ProgressTracker.Cancel();
            }
        }

        private InternalGraphManagerState currentState = InternalGraphManagerState.Inactive;
        public InternalGraphManagerState CurrentState { get { return this.currentState; } }

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

        private readonly ProgressTracker progressTracker;

        public Naiad.Runtime.Progress.ProgressTracker ProgressTracker { get { return this.progressTracker; } }

        public Naiad.Runtime.Progress.Frontier Frontier { get { return this.progressTracker; } }

        private readonly Scheduling.Reachability reachability = new Scheduling.Reachability();
        public Scheduling.Reachability
            Reachability { get { return this.reachability; } }


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

        protected readonly List<Dataflow.Subscription> outputs = new List<Dataflow.Subscription>();
        public IEnumerable<Dataflow.Subscription> Outputs
        {
            get { return this.outputs; }
        }

        public void Register(Naiad.Dataflow.Subscription sub) { outputs.Add(sub); }

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

        public Dataflow.Stream<R, Epoch> NewInput<R>(DataSource<R> source)
        {
            string generatedName = string.Format("__Input{0}", this.inputs.Count);

            return this.NewInput(source, generatedName);
        }

        public Dataflow.Stream<R, Epoch> NewInput<R>(DataSource<R> source, string name)
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


        public void Connect<S, T>(Dataflow.StageOutput<S, T> stream, Dataflow.StageInput<S, T> recvPort, Expression<Func<S, int>> key, Channel.Flags flags)
            where T : Time<T>
        {
            stream.ForStage.Targets.Add(new Dataflow.Edge<S, T>(stream, recvPort, key, flags));
        }
        public void Connect<S, T>(Dataflow.StageOutput<S, T> stream, Dataflow.StageInput<S, T> recvPort, Expression<Func<S, int>> key)
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


        private readonly bool DomainReporting;
        
        protected Dataflow.InputStage<string> RootDomainStatisticsStage
        {
            get { return this.contextManager.Reporting.domainReportingIngress; }
        }
        /// <summary>
        /// Blocks until all computation is complete and resources are released.
        /// </summary>
        public void Join()
        {
            if (this.CurrentState == InternalGraphManagerState.Inactive)
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

                // wait for all progress updates to drain.
                this.ProgressTracker.BlockUntilComplete();

                if (this.exception != null)
                    throw new Exception("Error during Naiad execution", this.exception);

                NotifyOnShutdown();

                this.isJoined = true;
                this.currentState = InternalGraphManagerState.Complete;
            }
        }


        private Scheduling.Placement defaultPlacement;
        public Scheduling.Placement DefaultPlacement { get { return this.defaultPlacement; } }

        public BaseGraphManager(InternalController controller, int index)
        {
            this.controller = controller;
            this.defaultPlacement = this.controller.DefaultPlacement;
            this.index = index;

            this.contextManager = new Naiad.Dataflow.TimeContextManager(this);

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
                    Logging.Error("Syncing at epoch ({0}) that is in the future of {1}", epoch, input);
                }
            }

            foreach (var subscription in this.Outputs)
                subscription.Sync(epoch);
        }

        bool activated = false;
        bool materialized = false;
        public void Activate()
        {
            if (activated)
                return;

            Logging.Progress("Activating GraphManager");

            activated = true;

            this.Reachability.UpdateReachabilityPartialOrder(this);

            this.MaterializeAll();

            this.Controller.DoStartupBarrier();

            this.currentState = InternalGraphManagerState.Active;

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

        public Dataflow.ITimeContextManager ContextManager
        {
            get { return this.contextManager; }
        }

        public void Dispose()
        {
            if (!this.isJoined)
            {
                Logging.Error("Attempted to dispose GraphManager before joining.");
                Logging.Error("You must call manager.Join() before disposing/exiting the using block.");
                //System.Environment.Exit(-1);
            }

            this.ContextManager.ShutDown();
        }
    }
}
