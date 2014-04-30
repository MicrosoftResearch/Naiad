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

using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading;
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

namespace Microsoft.Research.Naiad.Scheduling
{
    internal class Scheduler : IDisposable
    {
        internal class ComputationState
        {
            public readonly InternalComputation InternalComputation;

            public readonly PostOffice PostOffice;
            public readonly List<WorkItem> WorkItems;
            private readonly int index;

            public readonly List<Dataflow.Vertex> Vertices;

            public Runtime.Progress.ProgressUpdateProducer Producer 
            { 
#if false
                get 
                { 
                    if (this.Manager.ProgressTracker == null)
                        return null; 
                    else
                        return this.Manager.ProgressTracker.GetProducerForScheduler(this.index);
                }
#else
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
#endif
            }

            private Runtime.Progress.ProgressUpdateProducer producer;

            public ComputationState(InternalComputation manager, Scheduler scheduler)
            {
                this.InternalComputation = manager;
                this.PostOffice = new PostOffice(scheduler);
                this.WorkItems = new List<WorkItem>();
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
                
                newList[internalComputation.Index] = new ComputationState(internalComputation, this);

                success = oldList == Interlocked.CompareExchange(ref this.computationStates, newList, oldList);
            }
            while (!success);
        }

        private readonly BufferPool<byte> sendPool;
        public BufferPool<byte> SendPool { get { return this.sendPool; } }

        public struct WorkItem : IEquatable<WorkItem>
        {
            public Pointstamp Requirement;  // should not be run until this time (scheduled at).
            public Pointstamp Capability;   // may produce records at this time (prioritize by).
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

            public WorkItem(Pointstamp req, Pointstamp cap, Dataflow.Vertex o)
            {
                Requirement = req;
                Capability = cap;
                Vertex = o;
            }
        }

        private volatile CountdownEvent pauseEvent = null;
        private readonly AutoResetEvent resumeEvent = new AutoResetEvent(false);
        internal void Pause(CountdownEvent pauseEvent)
        {
            this.pauseEvent = pauseEvent;
            this.Signal();
        }

        internal void Resume()
        {
            this.resumeEvent.Set();
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

        internal void Register(Dataflow.Vertex vertex, InternalComputation manager)
        {
            for (int i = 0; i < this.computationStates.Count; i++)
                if (this.computationStates[i].InternalComputation == manager)
                    this.computationStates[i].Vertices.Add(vertex);
        }

        internal IList<WorkItem> GetWorkItemsForVertex(Dataflow.Vertex vertex)
        {
            throw new NotImplementedException();
            //return workItems.Where(x => x.Vertex == vertex).ToList();
        }

        protected System.Collections.Concurrent.ConcurrentQueue<WorkItem> sharedQueue = new System.Collections.Concurrent.ConcurrentQueue<WorkItem>();

        private void Enqueue(WorkItem item, bool local = true)
        {
            this.Controller.Workers.NotifyVertexEnqueued(this, item);

            if (local)
            {
                computationStates[item.Vertex.Stage.InternalComputation.Index].WorkItems.Add(item);
            }
            else
            {
                sharedQueue.Enqueue(item);
                this.Signal();
            }
        }

        public void EnqueueNotify<T>(Dataflow.Vertex op, T time, bool local)
            where T : Time<T>
        {
            EnqueueNotify(op, time, time, local);
        }

        public void EnqueueNotify<T>(Dataflow.Vertex op, T requirement, T capability, bool local)
            where T : Time<T>
        {
            var req = requirement.ToPointstamp(op.Stage.StageId);
            var cap = capability.ToPointstamp(op.Stage.StageId);

            Enqueue(new WorkItem(req, cap, op), local);
        }

        internal void Start()
        {
            this.thread.Start();
        }

        internal void DrainPostOffice()
        {
            //throw new NotImplementedException();

            // drain the shared queue.
            var item = default(WorkItem);
            while (sharedQueue.TryDequeue(out item))
                Enqueue(item);
        }

        /// <summary>
        /// Starts the ThreadScheduler into an infinite scheduling loop.
        /// </summary>
        protected virtual void InternalStart()
        {
            this.Controller.Workers.NotifyWorkerStarting(this);

            // the time of the most recent reachability computation. 
            var reachabilityTime = this.Controller.Stopwatch.ElapsedMilliseconds - this.Controller.Configuration.CompactionInterval;

            long wakeupCount = 0;

            for (int iteration = 0; !aborted; iteration++)
            {
                #region related to pausing
                if (this.pauseEvent != null)
                {
                    Logging.Info("Starting to pause worker {0}", this.Index);

                    CountdownEvent signalEvent = this.pauseEvent;
                    this.pauseEvent = null;
                    signalEvent.Signal();
                    Logging.Info("Finished pausing worker {0}", this.Index);

                    this.resumeEvent.WaitOne();
                    Logging.Info("Resumed worker {0}", this.Index);
                    for (int i = 0; i < this.computationStates.Count; i++)
                        if (this.computationStates[i].InternalComputation != null)
                            this.computationStates[i].Producer.Start(); // In case any outstanding records were caught in the checkpoint.
                }
                #endregion

                // drain the shared queue.
                var item = default(WorkItem);
                while (sharedQueue.TryDequeue(out item))
                    Enqueue(item);

                #region related to shutting down finished computations
                // check for computations that have empty frontiers: these can be shutdown
                for (int i = 0; i < this.computationStates.Count; i++)
                {
                    if (this.computationStates[i].InternalComputation != null && this.computationStates[i].InternalComputation.CurrentState == InternalComputationState.Active && this.computationStates[i].InternalComputation.ProgressTracker.GetInfoForWorker(this.Index).PointstampCountSet.Frontier.Length == 0)
                    {
                        foreach (Dataflow.Vertex vertex in this.computationStates[i].Vertices)
                            vertex.ShutDown();

                        this.computationStates[i].InternalComputation.SignalShutdown();

                        this.computationStates[i] = new ComputationState();
                    }
                }
                #endregion

                #region related to flushing messages for each computations
                // push any pending messages to recipients, so that work-to-do is as current as possible
                for (int i = 0; i < this.computationStates.Count; i++)
                {
                    if (this.computationStates[i].InternalComputation != null && this.computationStates[i].InternalComputation.CurrentState == InternalComputationState.Active)
                    {
                        Tracing.Trace("(Flush {0}", this.Index);
                        try
                        {
                            this.computationStates[i].PostOffice.DrainAllQueues();
                            this.computationStates[i].Producer.Start();   // tell everyone about records produced and consumed.
                        }
                        catch (Exception e)
                        {
                            Logging.Error("Graph {0} failed on scheduler {1} with exception:\n{2}", i, this.Index, e);
                            this.computationStates[i].InternalComputation.Cancel(e);
                        }
                        Tracing.Trace(")Flush {0}", this.Index);
                    }
                }
                #endregion

                #region related to assessing reachability of vertices based on the current frontier
                // periodically assesses reachability of versions based on the frontier. alerts operator vertices to results, allowing them to compact state.
                if (this.Controller.Configuration.CompactionInterval > 0 && this.Controller.Stopwatch.ElapsedMilliseconds - reachabilityTime > this.Controller.Configuration.CompactionInterval)
                {
                    Tracing.Trace("(reachability {0}", this.Index);
                    for (int i = 0; i < this.computationStates.Count; i++)
                    {
                        if (this.computationStates[i].InternalComputation != null && this.computationStates[i].InternalComputation.CurrentState == InternalComputationState.Active)
                        {
                            var frontiers = this.computationStates[i].InternalComputation.ProgressTracker.GetInfoForWorker(0).PointstampCountSet.Frontier.Concat(this.computationStates[i].Producer.LocalPCS.Frontier).ToArray();
                            this.computationStates[i].InternalComputation.Reachability.UpdateReachability(this.Controller, frontiers, this.computationStates[i].Vertices);
                        }
                    }
                    reachabilityTime = this.Controller.Stopwatch.ElapsedMilliseconds;
                    Tracing.Trace(")reachability {0}", this.Index);
                }
                #endregion

                var ranAnything = false;
                for (int i = 0; i < computationStates.Count; i++)
                {
                    if (computationStates[i].InternalComputation != null && this.computationStates[i].InternalComputation.CurrentState == InternalComputationState.Active)
                    {
                        try
                        {
                            var ranSomething = RunWorkItem(i);
                            ranAnything = ranSomething || ranAnything;
                        }
                        catch (Exception e)
                        {
                            Logging.Error("Graph {0} failed on scheduler {1} with exception:\n{2}", i, this.Index, e);
                            this.computationStates[i].InternalComputation.Cancel(e);
                        }
                    }
                }

                // try and run some work. if we can't, go to sleep. if we sleep too long, complain.
                if (!ranAnything)
                {
                    this.Controller.Workers.NotifySchedulerSleeping(this);

                    if (this.Controller.Configuration.UseBroadcastWakeup)
                    {
                        wakeupCount = this.Controller.Workers.BlockScheduler(this.ev, wakeupCount + 1);
                    }
                    else
                    {
                        if (!ev.WaitOne(this.deadlockTimeout))
                        {
                            Complain();
                            while (!ev.WaitOne(1000)) ;
                        }
                    }

                    this.Controller.Workers.NotifyWorkerWaking(this);
                }
            }

            this.Controller.Workers.NotifySchedulerTerminating(this);
            
        }

        protected bool RunWorkItem(int graphId)
        {
            var computation = this.computationStates[graphId].InternalComputation;
            var workItems = this.computationStates[graphId].WorkItems;
            var itemToRun = workItems.Count;

            for (int i = 0; i < workItems.Count; i++)
            {
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

            if (itemToRun < workItems.Count)
            {
                var item = workItems[itemToRun];

                workItems[itemToRun] = workItems[workItems.Count - 1];
                workItems.RemoveAt(workItems.Count - 1);

                this.Controller.Workers.NotifyVertexStarting(this, item);
                Tracing.Trace("[Sched " + this.Index + " " + item.ToString());
                
                Schedule(item);

                Tracing.Trace("]Sched " + this.Index + " " + item.ToString());
                this.Controller.Workers.NotifyVertexEnding(this, item);

                this.computationStates[graphId].Producer.Start();   // tell everyone about records produced and consumed.

                return true;
            }
            else
                return false;            
        }

        private void Complain()
        {
            Console.Error.WriteLine(ComplainObject);
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
                Tracing.Trace("@Scheduler[{0}]", this.Index);
                Logging.Info("Starting scheduler {0} on CPU {1}, .NET thread {2} mapped to Windows thread {3}", this.Name, CPUIndex, thrd.runtimeThreadId, thrd.OSThreadId);
                //Console.Error.WriteLine("Starting scheduler {0}({4}) on CPU {1}, .NET thread {2} mapped to Windows thread {3}", this.Name, CPUIndex, thrd.runtimeThreadId, thrd.OSThreadId, this.Index);
                base.InternalStart();
            }
        }
    }
}
