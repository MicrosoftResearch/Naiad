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
using System.Threading.Tasks;
using Microsoft.Research.Naiad.DataStructures;
using System.Diagnostics;
using Microsoft.Research.Naiad.Frameworks;
using System.IO;
using System.Runtime.InteropServices;
using Microsoft.Research.Naiad.Utilities;
using Microsoft.Research.Naiad.Dataflow;
using Microsoft.Research.Naiad.Dataflow.Channels;
//using Microsoft.Research.Naiad.Runtime.Controlling;

using Microsoft.Research.Naiad.Diagnostics;
using Microsoft.Research.Naiad.Runtime.Progress;
using Microsoft.Research.Naiad.Runtime.FaultTolerance;

namespace Microsoft.Research.Naiad.Scheduling
{
    internal class Scheduler : IDisposable
    {
        internal class ComputationState
        {
            public readonly InternalComputation InternalComputation;

            public readonly PostOffice PostOffice;
            public readonly List<WorkItem> WorkItems;
            public readonly Queue<DrainItem> DrainItems;
            public readonly DiscardManager DiscardManager;
            private readonly int index;

            public readonly List<Dataflow.Vertex> Vertices;

            public Runtime.Progress.ProgressUpdateProducer Producer 
            { 
                get 
                {
                    if (this.InternalComputation.ProgressTracker == null)
                        return null;
                    else
                    {
                        if (this.producer == null)
                            this.producer = new Runtime.Progress.ProgressUpdateProducer(this.InternalComputation, this.InternalComputation.ProgressTracker.Aggregator);

                        return this.producer;
                    }
                }
            }

            private Runtime.Progress.ProgressUpdateProducer producer;

            internal void PruneNotificationsAndRepairProgress(Scheduler parent)
            {
                WorkItem[] keepItems = this.WorkItems.Where(w =>
                    w.Vertex.Stage.CheckpointType == CheckpointType.None ||
                    (!w.Vertex.Stage.IsRollingBack(w.Vertex.VertexId) &&
                     // don't keep anything that was scheduled by a previous rollback
                     w.EnqueueTime.Timestamp[0] >= 0)).ToArray();
                this.WorkItems.Clear();
                this.WorkItems.AddRange(keepItems);

                foreach (WorkItem item in keepItems)
                {
                    Console.WriteLine("Work " + item.Vertex + " " + item.Capability);
                    if (item.ShouldRestoreProgress)
                    {
                        this.producer.UpdateRecordCounts(item.Capability, 1);
                    }
                }

                this.producer.Start();
            }

            public ComputationState(InternalComputation manager, Scheduler scheduler)
            {
                this.InternalComputation = manager;
                this.PostOffice = new PostOffice(scheduler, this.InternalComputation.Index);
                this.WorkItems = new List<WorkItem>();
                this.DrainItems = new Queue<DrainItem>();
                this.DiscardManager = new DiscardManager();
                this.index = scheduler.Index;
                this.Vertices = new List<Dataflow.Vertex>();

                this.producer = null;
            }
            public ComputationState()
            {
            }
        }

        internal ComputationState State(InternalComputation internalComputation)
        {
            if (this.computationStates.Count <= internalComputation.Index || this.computationStates[internalComputation.Index].InternalComputation == null)
                this.RegisterGraph(internalComputation);

            return this.computationStates[internalComputation.Index];
        }

        private List<ComputationState> computationStates = new List<ComputationState>();
        internal void RegisterGraph(InternalComputation internalComputation)
        {
            var success = false;

            do
            {
                var oldList = this.computationStates;

                var newList = this.computationStates.ToList();

                while (newList.Count < internalComputation.Index + 1)
                    newList.Add(new ComputationState());
                
                if (newList[internalComputation.Index].InternalComputation != internalComputation)
                {
                    newList[internalComputation.Index] = new ComputationState(internalComputation, this);
                }

                success = oldList == Interlocked.CompareExchange(ref this.computationStates, newList, oldList);
            }
            while (!success);
        }

        public BufferPool<byte> SendPool { get { return this.sendPool; } }
        private readonly BufferPool<byte> sendPool;

        public struct WorkItem : IEquatable<WorkItem>
        {
            public Pointstamp EnqueueTime;  // time the notification was enqueued
            public Pointstamp Requirement;  // should not be run until this time (scheduled at).
            public Pointstamp Capability;   // may produce records at this time (prioritize by).
            public bool ShouldRestoreProgress;
            public Dataflow.Vertex Vertex;

            public void Run()
            {
                Vertex.PerformAction(this);
            }

            public override int GetHashCode()
            {
                return Requirement.GetHashCode() + Vertex.GetHashCode();
            }

            public bool Equals(WorkItem that)
            {
                return this.Requirement.Equals(that.Requirement) && this.Vertex == that.Vertex;
            }

            public override string ToString()
            {
                return String.Format("{0}\t{1}", Requirement, Vertex);
            }

            public WorkItem(Pointstamp enq, Pointstamp req, Pointstamp cap, bool shouldRestoreProgress, Dataflow.Vertex o)
            {
                EnqueueTime = enq;
                Requirement = req;
                Capability = cap;
                ShouldRestoreProgress = shouldRestoreProgress;
                Vertex = o;
            }
        }

        public struct DrainItem 
        {
            public readonly Pointstamp Capability;
            public readonly LocalMailbox Mailbox;

            public void PerformDrain()
            {
                this.Mailbox.Drain(Capability);
            }

            public DrainItem(Pointstamp capability, LocalMailbox mailbox) { this.Capability = capability; this.Mailbox = mailbox; }
        }

        private volatile CountdownEvent pauseEvent = null;
        private volatile CountdownEvent simulatedFailureEvent = null;
        private readonly AutoResetEvent resumeEvent = new AutoResetEvent(false);
        internal void Pause(CountdownEvent pauseEvent)
        {
            this.pauseEvent = pauseEvent;
            this.Signal();
        }

        internal void SimulateFailure(CountdownEvent simulatedFailureEvent)
        {
            this.simulatedFailureEvent = simulatedFailureEvent;
            this.Signal();
        }

        internal void Resume()
        {
            this.resumeEvent.Set();
        }

        private readonly ConcurrentQueue<CheckpointPersistedAction> persistedCheckpointQueue = new ConcurrentQueue<CheckpointPersistedAction>();
        private readonly ConcurrentQueue<Task> persistenceFailedQueue = new ConcurrentQueue<Task>();

        internal void NotifyCheckpointPersisted(CheckpointPersistedAction persistence)
        {
            this.persistedCheckpointQueue.Enqueue(persistence);
            this.Signal();
        }

        internal void NotifyCheckpointPersistenceFailed(Task persistence)
        {
            this.persistenceFailedQueue.Enqueue(persistence);
            this.Signal();
        }

        private bool CheckPersistenceQueue()
        {
            bool didAnything = false;

            Task failure;
            while (this.persistenceFailedQueue.TryDequeue(out failure))
            {
                // throw the exception here
                failure.Wait();
            }

            CheckpointPersistedAction action;
            while (this.persistedCheckpointQueue.TryDequeue(out action))
            {
                action.Execute();
            }

            return didAnything;
        }

        private readonly int deadlockTimeout;

        public readonly string Name;
        public readonly int Index;

        AutoResetEvent ev = new AutoResetEvent(true);  // set when there is work to do

        ManualResetEvent allChannelsInitializedEvent = new ManualResetEvent(false);

        internal readonly InternalController Controller;

        volatile bool aborted = false;

        internal long[] statistics;

        private readonly Thread thread;

        internal void Schedule(WorkItem workItem)
        {
            if (Logging.LogLevel <= LoggingLevel.Info) Logging.Info("Vertex {2}: Running @ {1}:\t{0}", workItem.Vertex, workItem.Requirement, this.Index);
            workItem.Run();
            if (Logging.LogLevel <= LoggingLevel.Info) Logging.Info("Vertex {2}: Finishing @ {1}:\t{0}", workItem.Vertex, workItem.Requirement, this.Index);
        }

        internal void Register(Dataflow.Vertex vertex, Stage stage)
        {
            InternalComputation manager = stage.InternalComputation;

            for (int i = 0; i < this.computationStates.Count; i++)
                if (this.computationStates[i].InternalComputation == manager)
                {
                    for (int v = 0; v < this.computationStates[i].Vertices.Count; ++v)
                    {
                        if (this.computationStates[i].Vertices[v].Stage.StageId == stage.StageId &&
                            this.computationStates[i].Vertices[v].VertexId == vertex.VertexId)
                        {
                            // we are being re-materialized
                            this.computationStates[i].Vertices[v] = vertex;
                            return;
                        }
                    }

                    this.computationStates[i].Vertices.Add(vertex);
                }
        }

        internal IList<WorkItem> GetWorkItemsForVertex(Dataflow.Vertex vertex)
        {
            IEnumerable<WorkItem> items = new WorkItem[] { };
            for (int i = 0; i < this.computationStates.Count; ++i)
            {
                items = items.Concat(this.computationStates[i].WorkItems.Where(x => x.Vertex == vertex));
            }
            return items.ToList();
        }

        protected System.Collections.Concurrent.ConcurrentQueue<WorkItem> sharedQueue = new System.Collections.Concurrent.ConcurrentQueue<WorkItem>();

        private int Enqueue(WorkItem item, bool fromThisScheduler = true)
        {
            this.Controller.Workers.NotifyVertexEnqueued(this, item);

            if (fromThisScheduler)
            {
                computationStates[item.Vertex.Stage.InternalComputation.Index].WorkItems.Add(item);
                return 0;
            }
            else
            {
                int newTotal = this.Controller.Workers.IncrementSharedQueueCount(1);
                sharedQueue.Enqueue(item);
                this.Signal();
                return newTotal;
            }
        }

        public int EnqueueNotify<T>(Dataflow.Vertex op, T enqueueTime, T time, bool shouldRestoreProgress, bool local)
            where T : Time<T>
        {
            return EnqueueNotify(op, enqueueTime, time, time, shouldRestoreProgress, local);
        }

        public int EnqueueNotify<T>(Dataflow.Vertex op, T enqueueTime, T requirement, T capability, bool shouldRestoreProgress, bool local)
            where T : Time<T>
        {
            var enq = enqueueTime.ToPointstamp(op.Stage.StageId);
            var req = requirement.ToPointstamp(op.Stage.StageId);
            var cap = capability.ToPointstamp(op.Stage.StageId);

            return Enqueue(new WorkItem(enq, req, cap, shouldRestoreProgress, op), local);
        }


        public void RequestDrain<T>(int channelId, T capability, LocalMailbox mailbox, int computationIndex, bool requestCutThrough)
            where T : Time<T>
        {
            // eager cut-through
            if (requestCutThrough)
                mailbox.Drain(capability.ToPointstamp(channelId));
            else
               this.computationStates[computationIndex].DrainItems.Enqueue(new DrainItem(capability.ToPointstamp(channelId), mailbox));
        }

        internal void Start()
        {
            this.thread.Start();
        }

        protected Dictionary<Type, BufferPool> BufferPools = new Dictionary<Type, BufferPool>();

        internal BufferPool<T> GetBufferPool<T>()
        {
            lock (this)
            {
                var type = typeof(T);
                if (!this.BufferPools.ContainsKey(type))
                    this.BufferPools.Add(type, new MessageBufferPool<T>());

                return this.BufferPools[type] as BufferPool<T>;
            }
        }

        private bool isRestoring = false;

        public void StopRestoring()
        {
            this.isRestoring = false;
        }

        private FileStream checkpointLogFile = null;
        private StreamWriter checkpointLog = null;
        private StreamWriter CheckpointLog
        {
            get
            {
                if (checkpointLog == null)
                {
                    string fileName = String.Format("checkpoint.{0:D3}.{1:D3}.log",
                        this.Controller.Configuration.ProcessID, this.Index);
                    this.checkpointLogFile = new FileStream(fileName, FileMode.Create, FileAccess.Write, FileShare.ReadWrite);
                    this.checkpointLog = new StreamWriter(this.checkpointLogFile);
                }
                return checkpointLog;
            }
        }

        internal void WriteLogEntry(string format, params object[] args)
        {
            lock (this)
            {
                this.CheckpointLog.WriteLine(format, args);
            }
        }

        private long logFlushTime = 0;
        private void ConsiderFlushingLogs()
        {
            if (this.checkpointLog != null && this.Controller.Stopwatch.ElapsedMilliseconds - this.logFlushTime > 1000)
            {
                this.checkpointLog.Flush();
                this.checkpointLogFile.Flush(true);
                this.logFlushTime = this.Controller.Stopwatch.ElapsedMilliseconds;
            }
        }

        /// <summary>
        /// Starts the ThreadScheduler into an infinite scheduling loop.
        /// </summary>
        protected virtual void InternalStart()
        {
            this.Controller.Workers.NotifyWorkerStarting(this);

            // the time of the most recent reachability computation. 
            this.reachabilityTime = this.Controller.Stopwatch.ElapsedMilliseconds - this.Controller.Configuration.CompactionInterval;

            int dequeuedSharedItems = 0;
            // perform work until the scheduler is aborted
            for (int iteration = 0; !aborted; iteration++)
            {
                // set to true if messages or or notifications delivered.
                var didAnything = false;

                this.ConsiderFlushingLogs();

                // test pause event.
                this.ConsiderPausing();

                if (!this.isRestoring)
                {
                    // check to see if any checkpoints finished being saved to stable storage
                    didAnything = this.CheckPersistenceQueue() || didAnything;

                    // check for computations that have empty frontiers: these can be shutdown.
                    for (int computationIndex = 0; computationIndex < this.computationStates.Count; computationIndex++)
                        this.TestComputationsForShutdown(computationIndex);

                    // check all mailboxes with undifferentiated messages.
                    for (int computationIndex = 0; computationIndex < this.computationStates.Count; computationIndex++)
                        this.CheckMailboxesForComputation(computationIndex);

                    // push any pending messages to recipients, so that work-to-do is as current as possible.
                    for (int computationIndex = 0; computationIndex < this.computationStates.Count; computationIndex++)
                        didAnything = this.DrainMailboxesForComputation(computationIndex) || didAnything;

                    // periodically assesses global reachability.
                    didAnything = this.ConsiderAssessingGlobalReachability() || didAnything;

                    // flush all computations and push progress tracking traffic out to other workers.
                    for (int computationIndex = 0; computationIndex < this.computationStates.Count; computationIndex++)
                        this.FlushProgressUpdatesForComputation(computationIndex);
                }

                // accept work items from the shared queue.
                dequeuedSharedItems += this.AcceptWorkItemsFromOthers();

                // deliver notifications.
                for (int computationIndex = 0; computationIndex < computationStates.Count; computationIndex++)
                    didAnything = this.RunNotification(computationIndex) || didAnything;

                // if nothing ran, consider sleeping until more work arrives
                if (!didAnything)
                {
                    int highWaterMark = this.Controller.Workers.DecrementSharedQueueCount(dequeuedSharedItems);
                    dequeuedSharedItems = 0;
                    this.ConsiderSleeping(highWaterMark);
                }
            }

            this.Controller.Workers.NotifySchedulerTerminating(this);            
        }

        protected bool ComputationActive(int computationIndex)
        {
            return this.computationStates.Count > computationIndex &&
                   this.computationStates[computationIndex].InternalComputation != null &&
                   this.computationStates[computationIndex].InternalComputation.CurrentState == InternalComputationState.Active;
        }

        internal int AcceptWorkItemsFromOthers()
        {
            int dequeuedItems = 0;
            // drain the shared queue.
            var item = default(WorkItem);
            while (sharedQueue.TryDequeue(out item))
            {
                ++dequeuedItems;
                Enqueue(item);
            }
            return dequeuedItems;
        }

        private void ConsiderPausing()
        {
            if (this.pauseEvent != null)
            {
                Logging.Info("Starting to pause worker {0}", this.Index);

                foreach (var computation in this.computationStates)
                {
                    // make sure nobody messes with inputs while we are modifying the work item queues
                    foreach (InputStage input in computation.InternalComputation.Inputs)
                    {
                        input.BlockExternalCalls();
                    }

                    foreach (var vertex in computation.Vertices)
                    {
                        vertex.SendInstantaneousFaultToleranceFrontier();
                    }
                    computation.InternalComputation.CheckpointTracker.FlushUpdates(this.Index);
                    computation.Producer.Start();
                    this.FlushProgressUpdatesForComputation(computation.InternalComputation.Index);
                }

                for (int computationIndex = 0; computationIndex < this.computationStates.Count; computationIndex++)
                    this.FlushProgressUpdatesForComputation(computationIndex);

                this.isRestoring = true;

                CountdownEvent signalEvent = this.pauseEvent;
                this.pauseEvent = null;
                signalEvent.Signal();
                Logging.Info("Finished pausing worker {0}", this.Index);

                this.resumeEvent.WaitOne();
                Logging.Info("Resumed worker {0}", this.Index);
            }

            if (this.simulatedFailureEvent != null)
            {
                Logging.Info("Starting to fail worker {0}", this.Index);

                // don't do any real work after we wake up
                this.isRestoring = true;

                CountdownEvent signalEvent = this.simulatedFailureEvent;
                this.simulatedFailureEvent = null;
                signalEvent.Signal();
                Logging.Info("Finished failing worker {0}", this.Index);

                this.resumeEvent.WaitOne();
                Logging.Info("Resumed worker {0} after failure", this.Index);
            }
        }

        private void TestComputationsForShutdown(int computationIndex)
        {
            if (this.ComputationActive(computationIndex) && this.computationStates[computationIndex].InternalComputation.ProgressTracker.GetInfoForWorker(this.Index).PointstampCountSet.Frontier.Length == 0)
            {
                foreach (Dataflow.Vertex vertex in this.computationStates[computationIndex].Vertices)
                    vertex.ShutDown();

                this.computationStates[computationIndex].InternalComputation.SignalShutdown();

                this.computationStates[computationIndex] = new ComputationState();
            }
        }

        private void CheckMailboxesForComputation(int computationIndex)
        {
            if (this.ComputationActive(computationIndex))
            {
                //Tracing.Trace("(Accept {0}", this.Index);

                this.computationStates[computationIndex].PostOffice.CheckAllMailboxes();

                //Tracing.Trace(")Accept {0}", this.Index);
            }
        }

        private bool DrainMailboxesForComputation(int computationIndex)
        {
            var drainedSomething = false;

            if (this.ComputationActive(computationIndex))
            {
                //Tracing.Trace("(Drain {0}", this.Index);
                try
                {
                    var drainItems = this.computationStates[computationIndex].DrainItems;

                    if (drainItems.Count > 0)
                        drainedSomething = true;

                    while (this.computationStates[computationIndex].DrainItems.Count > 0)
                        this.computationStates[computationIndex].DrainItems.Dequeue().PerformDrain();

                    this.computationStates[computationIndex].Producer.Start();   // tell everyone about records produced and consumed.
                }
                catch (Exception e)
                {
                    Logging.Error("Graph {0} failed on scheduler {1} with exception:\n{2}", computationIndex, this.Index, e);
                    this.computationStates[computationIndex].InternalComputation.Cancel(e);
                }
                //Tracing.Trace(")Drain {0}", this.Index);
            }

            return drainedSomething;
        }

        public void FlushFaultToleranceTraffic()
        {
            for (int computationIndex = 0; computationIndex < this.computationStates.Count; ++computationIndex)
            {
                if (this.ComputationActive(computationIndex))
                {
                    ComputationState state = this.computationStates[computationIndex];

                    Vertex receiver = state.InternalComputation.CheckpointTracker.CentralReceiverVertex;

                    state.PostOffice.CheckVertexMailboxes(receiver);

                    foreach (var drainItem in state.DrainItems.Where(i => i.Mailbox.Vertex == receiver))
                    {
                        drainItem.PerformDrain();
                    }

                    state.Producer.Start();

                    // just leave the drain items in the queue: they will get drained again, but that's ok since
                    // it is idempotent
                }

                this.FlushProgressUpdatesForComputation(computationIndex);
            }
        }

        public void ResetProgress()
        {
            for (int computationIndex = 0; computationIndex < this.computationStates.Count; ++computationIndex)
            {
                if (this.ComputationActive(computationIndex))
                {
                    this.computationStates[computationIndex].Producer.Reset();
                }
            }
        }

        public void FlushProgressUpdatesForComputation(int computationIndex)
        {
            if (this.ComputationActive(computationIndex))
            {
                NaiadTracing.Trace.RegionStart(NaiadTracingRegion.Flush);
                try
                {
                    this.computationStates[computationIndex].PostOffice.FlushAllMailboxes();
                    this.computationStates[computationIndex].Producer.Start();   // tell everyone about records produced and consumed.
                }
                catch (Exception e)
                {
                    Logging.Error("Graph {0} failed on scheduler {1} with exception:\n{2}", computationIndex, this.Index, e);
                    this.computationStates[computationIndex].InternalComputation.Cancel(e);
                }
                NaiadTracing.Trace.RegionStop(NaiadTracingRegion.Flush);
            }
        }

        #region Related to global reachability computation
        private bool ConsiderAssessingGlobalReachability()
        {
            if (this.Controller.Configuration.CompactionInterval > 0 && this.Controller.Stopwatch.ElapsedMilliseconds - this.reachabilityTime > this.Controller.Configuration.CompactionInterval)
            {
                NaiadTracing.Trace.RegionStart(NaiadTracingRegion.Reachability);
                for (int i = 0; i < this.computationStates.Count; i++)
                    this.AssessAndNotifyGlobalReachability(i);

                this.reachabilityTime = this.Controller.Stopwatch.ElapsedMilliseconds;
                NaiadTracing.Trace.RegionStop(NaiadTracingRegion.Reachability);

                return true;
            }
            else
            {
                return false;
            }
        }

        long reachabilityTime;

        private void AssessAndNotifyGlobalReachability(int computationIndex)
        {
            if (this.ComputationActive(computationIndex))
            {
                var frontiers = this.computationStates[computationIndex].InternalComputation.ProgressTracker.GetInfoForWorker(0).PointstampCountSet.Frontier.Concat(this.computationStates[computationIndex].Producer.LocalPCS.Frontier).ToArray();
                this.computationStates[computationIndex].InternalComputation.Reachability
                    .UpdateReachability(
                        this.Controller, frontiers, this.computationStates[computationIndex].Vertices,
                        this.computationStates[computationIndex].DiscardManager);
                CheckpointTracker tracker = this.computationStates[computationIndex].InternalComputation.CheckpointTracker;
                if (tracker != null)
                {
                    tracker.FlushUpdates(this.Index);
                }
                
                this.computationStates[computationIndex].Producer.Start();
            }
        }
        #endregion

        private bool RunNotification(int computationIndex)
        {
            if (this.ComputationActive(computationIndex))
            {
                try
                {
                    return RunWorkItem(computationIndex);
                }
                catch (Exception e)
                {
                    Logging.Error("Graph {0} failed on scheduler {1} with exception:\n{2}", computationIndex, this.Index, e);
                    this.computationStates[computationIndex].InternalComputation.Cancel(e);
                }
            }

            return false;
        }


        protected bool RunWorkItem(int graphId)
        {
            var computation = this.computationStates[graphId].InternalComputation;
            var workItems = this.computationStates[graphId].WorkItems;
            var itemToRun = workItems.Count;

            if (this.isRestoring)
            {
                for (int i = 0; itemToRun == workItems.Count && i < workItems.Count; i++)
                {
                    if (workItems[i].Capability.Timestamp.a < 0)
                    {
                        itemToRun = i;
                    }
                }
            }
            else
            {
                // determine which item to run
                for (int i = 0; i < workItems.Count; i++)
                {
                    if (workItems[i].Capability.Timestamp.a < 0)
                    {
                        itemToRun = i;
                        break;
                    }

                    if (itemToRun == workItems.Count || computation.Reachability.CompareTo(workItems[itemToRun].Capability, workItems[i].Capability) > 0)
                    {
                        var valid = false;

                        // update the frontier, to keep things fresh-ish!
                        var frontier = computation.ProgressTracker.GetInfoForWorker(this.Index).PointstampCountSet.Frontier;
                        var local = this.computationStates[graphId].Producer.LocalPCS.Frontier;

                        var v = workItems[i].Requirement;

                        var dominated = false;
                        for (int j = 0; j < frontier.Length && !dominated; j++)
                            if (computation.Reachability.LessThan(frontier[j], v) && !frontier[j].Equals(v))
                                dominated = true;

                        for (int j = 0; j < local.Length && !dominated; j++)
                            if (computation.Reachability.LessThan(local[j], v) && !local[j].Equals(v))
                                dominated = true;

                        valid = !dominated;

                        if (valid)
                            itemToRun = i;
                    }
                }
            }

            // execute identified work item.
            if (itemToRun < workItems.Count)
            {
                var item = workItems[itemToRun];

                workItems[itemToRun] = workItems[workItems.Count - 1];
                workItems.RemoveAt(workItems.Count - 1);

                this.Controller.Workers.NotifyVertexStarting(this, item);
                NaiadTracing.Trace.StartSched(item);
                //Tracing.Trace("[Sched " + this.Index + " " + item.ToString());

                Schedule(item);

                CheckpointTracker checkpointTracker = this.computationStates[graphId].InternalComputation.CheckpointTracker;
                if (checkpointTracker != null)
                {
                    checkpointTracker.FlushUpdates(this.Index);
                }

                //Tracing.Trace("]Sched " + this.Index + " " + item.ToString());
                NaiadTracing.Trace.StopSched(item);
                this.Controller.Workers.NotifyVertexEnding(this, item);

                this.computationStates[graphId].Producer.Start();   // tell everyone about records produced and consumed.

                return true;
            }
            else
                return false;
        }


        private void ConsiderSleeping(int highWaterMark)
        {
            this.Controller.Workers.NotifySchedulerSleeping(this, highWaterMark);

            if (this.Controller.Configuration.UseBroadcastWakeup)
            {
                wakeupCount = this.Controller.Workers.BlockScheduler(this.ev, wakeupCount + 1);
            }
            else
            {
                this.ev.WaitOne(this.deadlockTimeout);
                //if (!ev.WaitOne(this.deadlockTimeout))
                //{
                //    Complain();
                //    while (!ev.WaitOne(1000)) ;
                //}
            }

            this.Controller.Workers.NotifyWorkerWaking(this);
        }

        long wakeupCount = 0;

        private void Complain()
        {
#if true
            Console.Error.WriteLine(ComplainObject);
#else
            // XXX : Currently races and can crash due to null data structures.
            for (int i = 0; i < computationStates.Count; i++)
            {
                var computationState = this.computationStates[i];
                if (computationState != null)
                {
                    var internalComputation = computationState.InternalComputation;
                    if (internalComputation != null)
                    {
                        var frontier = internalComputation.ProgressTracker.GetInfoForWorker(this.Index).PointstampCountSet.Frontier;
                        Console.WriteLine("Computation[{0}].Frontier.Length = {1}", i, frontier.Length);
                    }
                }
            }
#endif
        }

        private static string ComplainObject = "Moan moan moan";

        public void AllChannelsInitialized()
        {
            this.allChannelsInitializedEvent.Set();
        }

        public void Signal() 
        {
            ev.Set();
        }

        public void Abort()
        {
            Logging.Info("Aborting scheduler {0}", this.Index);
            aborted = true;
            this.Signal();
        }

        public void Report(int maxReport, TextWriter writer, bool reset)
        {
#if false
            var collections = Enumerable.Range(0, Stopwatches.Count)
                                        .OrderByDescending(x => Stopwatches.Array[x].Elapsed)
                                        .Take(maxReport)
                                        .ToArray();

            writer.WriteLine("{0}\t{1}\t\t{2}", this.Index, totalStopwatch.Elapsed, "TOTAL");

            var workElapsed = Stopwatches.Array.Take(Stopwatches.Count).Select(x => x.Elapsed).Aggregate((x,y) => x + y);

            writer.WriteLine("{0}\t{1}\t\t{2}", this.Index, workElapsed, "WORK");

            for (int i = 0; i < Math.Min(collections.Length, maxReport); i++)
                if (this.Controller.InternalGraphManager.Stages.ContainsKey(collections[i]))
                    writer.WriteLine("{0}\t{1}\t{3}\t{2}", this.Index, Stopwatches.Array[collections[i]].Elapsed, this.Controller.InternalGraphManager.Stages[collections[i]], ScheduleCount.Array[collections[i]]);
                //else
                //    writer.WriteLine("Collections[collections[{0}]] not found", i);

            if (reset)
            {
                Stopwatches.Count = 0;
                ScheduleCount.Count = 0;
            }

            if (this.Controller.Configuration.Processes > 1)
            {
                Console.Error.WriteLine("Postoffice report:");
                postOffice.Report();
            }
#endif
        }

        /// <summary>
        /// Creates a new thread scheduler
        /// </summary>
        /// <param name="n">The friendly name of this scheduler</param>
        /// <param name="i">The id of the core this thread is affinitized to</param>
        /// <param name="c">The internal controller reference</param>
        internal Scheduler(string n, int i, InternalController c)
        {
            Name = n;
            Index = i;
            Controller = c;

            //this.postOffice = new PostOffice(this);

            this.thread = new Thread(this.InternalStart);
            this.thread.Name = string.Format("Naiad Worker {0}", i);
            Logging.Info("Scheduler {0} created", i);

            this.deadlockTimeout = c.Configuration.DeadlockTimeout;

            this.statistics = new long[(int)RuntimeStatistic.NUM_STATISTICS];

            this.sendPool = c.Configuration.SendBufferPolicy == Configuration.SendBufferMode.PerWorker
                ? new BoundedBufferPool2<byte>(c.Configuration.SendPageSize, c.Configuration.SendPageCount)
                : null;
        }

        internal void Join()
        {
            this.thread.Join();
        }

        public void Dispose()
        {
            this.Abort();
            this.thread.Join();
            this.ev.Dispose();
            this.allChannelsInitializedEvent.Dispose();
            this.resumeEvent.Dispose();
        }
    }

    internal class PinnedScheduler : Scheduler
    {
        public PinnedScheduler(string n, int i, InternalController c)
            : base(n, i, c)
        {

        }

        protected override void InternalStart()
        {
            // Pin the thread to a cpu
            int CPUIndex = this.Controller.Configuration.MultipleLocalProcesses ? this.Index + this.Controller.Configuration.ProcessID * this.Controller.Workers.Count : this.Index;
            using (var thrd = new PinnedThread(CPUIndex, true))
            {
                NaiadTracing.Trace.ThreadName("Scheduler[{0}]", this.Index);
                Logging.Info("Starting scheduler {0} on CPU {1}, .NET thread {2} mapped to Windows thread {3}", this.Name, CPUIndex, thrd.runtimeThreadId, thrd.OSThreadId);
                //Console.Error.WriteLine("Starting scheduler {0}({4}) on CPU {1}, .NET thread {2} mapped to Windows thread {3}", this.Name, CPUIndex, thrd.runtimeThreadId, thrd.OSThreadId, this.Index);
                base.InternalStart();
            }
        }
    }
}
