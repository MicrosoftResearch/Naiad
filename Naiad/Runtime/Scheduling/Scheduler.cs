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

using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading;
using Naiad.DataStructures;
using System.Diagnostics;
using Naiad.Frameworks;
using System.IO;
using System.Runtime.InteropServices;
using Naiad.Util;
using Naiad.Dataflow.Channels;
using Naiad.Runtime.Controlling;

namespace Naiad.Scheduling
{
    internal class Scheduler : IDisposable
    {
        internal class GraphState
        {
            public readonly InternalGraphManager Manager;

            public readonly PostOffice PostOffice;
            public readonly NaiadList<WorkItem> WorkItems;
            private readonly int index;

            public readonly List<Dataflow.Vertex> Shards;

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
                    if (this.Manager.ProgressTracker == null)
                        return null;
                    else
                    {
                        if (this.producer == null)
                            this.producer = new Runtime.Progress.ProgressUpdateProducer(this.Manager, this.Manager.ProgressTracker.Aggregator);

                        return this.producer;
                    }
                }
#endif
            }

            private Runtime.Progress.ProgressUpdateProducer producer;

            public GraphState(InternalGraphManager manager, Scheduler scheduler)
            {
                this.Manager = manager;
                this.PostOffice = new PostOffice(scheduler);
                this.WorkItems = new NaiadList<WorkItem>();
                this.index = scheduler.Index;
                this.Shards = new List<Dataflow.Vertex>();

                this.producer = null;
            }
            public GraphState()
            {
            }
        }

        internal GraphState State(InternalGraphManager graphManager)
        {
            if (this.graphStates.Count <= graphManager.Index || this.graphStates[graphManager.Index].Manager == null)
                this.RegisterGraph(graphManager);

            return this.graphStates[graphManager.Index];
        }

        private volatile List<GraphState> graphStates = new List<GraphState>();
        internal void RegisterGraph(InternalGraphManager graphManager)
        {
            var success = false;

            do
            {
                var oldList = this.graphStates;

                var newList = this.graphStates.ToList();

                while (newList.Count < graphManager.Index + 1)
                    newList.Add(new GraphState());
                
                newList[graphManager.Index] = new GraphState(graphManager, this);

                success = oldList == Interlocked.CompareExchange(ref this.graphStates, newList, oldList);
            }
            while (!success);
        }

        private readonly BufferPool<byte> sendPool;
        public BufferPool<byte> SendPool { get { return this.sendPool; } }

        public struct WorkItem : IEquatable<WorkItem>
        {
            public Pointstamp Requirement;  // should not be run until this time (scheduled at).
            public Pointstamp Capability;   // may produce records at this time (prioritize by).
            public Dataflow.Vertex Shard;

            public void Run()
            {
                Shard.PerformAction(this);
            }

            public override int GetHashCode()
            {
                return Requirement.GetHashCode() + Shard.GetHashCode();
            }

            public bool Equals(WorkItem that)
            {
                return this.Requirement.Equals(that.Requirement) && this.Shard == that.Shard;
            }

            public override string ToString()
            {
                return String.Format("{0}\t{1}", Requirement, Shard);
            }

            public WorkItem(Pointstamp req, Pointstamp cap, Dataflow.Vertex o)
            {
                Requirement = req;
                Capability = cap;
                Shard = o;
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
            Logging.Info("Shard {2}: Running @ {1}:\t{0}", workItem.Shard, workItem.Requirement, this.Index);
            workItem.Run();
            Logging.Info("Shard {2}: Finishing @ {1}:\t{0}", workItem.Shard, workItem.Requirement, this.Index);
        }

        internal void Register(Dataflow.Vertex shard, InternalGraphManager manager)
        {
            for (int i = 0; i < this.graphStates.Count; i++)
                if (this.graphStates[i].Manager == manager)
                    this.graphStates[i].Shards.Add(shard);
        }

        internal IList<WorkItem> GetWorkItemsForShard(Dataflow.Vertex shard)
        {
            throw new NotImplementedException();
            //return workItems.Where(x => x.Shard == shard).ToList();
        }

        protected System.Collections.Concurrent.ConcurrentQueue<WorkItem> sharedQueue = new System.Collections.Concurrent.ConcurrentQueue<WorkItem>();

        private void Enqueue(WorkItem item, bool local = true)
        {
            this.Controller.Workers.NotifyOperatorEnqueued(this, item);

            if (local)
            {
                // workItems.Add(item);
                graphStates[item.Shard.Stage.InternalGraphManager.Index].WorkItems.Add(item);
            }
            else
            {
                sharedQueue.Enqueue(item);
                this.Signal();
            }
        }

        public void EnqueueNotify(Pointstamp requirement, Pointstamp capability, Dataflow.Vertex op, bool local = true)
        {
            Enqueue(new WorkItem(requirement, capability, op), local);
        }

        public void EnqueueNotify<T>(Dataflow.Vertex op, T time, bool local)
            where T : Time<T>
        {
            var pointstamp = time.ToPointstamp(op.Stage.StageId);
            EnqueueNotify(pointstamp, pointstamp, op, local);
        }

        public void EnqueueNotify<T>(Dataflow.Vertex op, T requirement, T capability, bool local = true)
            where T : Time<T>
        {
            var req = requirement.ToPointstamp(op.Stage.StageId);
            var cap = capability.ToPointstamp(op.Stage.StageId);

            EnqueueNotify(req, cap, op, local);
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
            this.Controller.Workers.NotifySchedulerStarting(this);

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
                    for (int i = 0; i < this.graphStates.Count; i++)
                        if (this.graphStates[i].Manager != null)
                            this.graphStates[i].Producer.Start(); // In case any outstanding records were caught in the checkpoint.
                }
                #endregion

                // drain the shared queue.
                var item = default(WorkItem);
                while (sharedQueue.TryDequeue(out item))
                    Enqueue(item);

                // check for graphs that have empty frontiers: these can be shutdown
                for (int i = 0; i < this.graphStates.Count; i++)
                {
                    if (this.graphStates[i].Manager != null && this.graphStates[i].Manager.CurrentState == InternalGraphManagerState.Active && this.graphStates[i].Manager.ProgressTracker.GetInfoForWorker(this.Index).PointstampCountSet.Frontier.Length == 0)
                    {
                        foreach (Dataflow.Vertex shard in this.graphStates[i].Shards)
                            shard.ShutDown();

                        this.graphStates[i] = new GraphState();
                    }
                }

                // push any pending messages to recipients, so that work-to-do is as current as possible
                for (int i = 0; i < this.graphStates.Count; i++)
                {
                    if (this.graphStates[i].Manager != null && this.graphStates[i].Manager.CurrentState == InternalGraphManagerState.Active)
                    {
                        Tracing.Trace("(Flush {0}", this.Index);
                        try
                        {
                            this.graphStates[i].PostOffice.DrainAllQueues();
                            this.graphStates[i].Producer.Start();   // tell everyone about records produced and consumed.
                        }
                        catch (Exception e)
                        {
                            Logging.Error("Graph {0} failed on scheduler {1} with exception:\n{2}", i, this.Index, e);
                            this.graphStates[i].Manager.Cancel(e);
                        }
                        Tracing.Trace(")Flush {0}", this.Index);
                    }
                }

#if true
                // periodically assesses reachability of versions based on the frontier. alerts operator shards to results, allowing them to compact state.
                if (this.Controller.Configuration.CompactionInterval > 0 && this.Controller.Stopwatch.ElapsedMilliseconds - reachabilityTime > this.Controller.Configuration.CompactionInterval)
                {
                    Tracing.Trace("(reachability {0}", this.Index);
                    for (int i = 0; i < this.graphStates.Count; i++)
                    {
                        if (this.graphStates[i].Manager != null && this.graphStates[i].Manager.CurrentState == InternalGraphManagerState.Active)
                        {
                            var frontiers = this.graphStates[i].Manager.ProgressTracker.GetInfoForWorker(0).PointstampCountSet.Frontier.Concat(this.graphStates[i].Producer.LocalPCS.Frontier).ToArray();
                            this.graphStates[i].Manager.Reachability.UpdateReachability(this.Controller, frontiers, this.graphStates[i].Shards);
                        }
                    }
                    reachabilityTime = this.Controller.Stopwatch.ElapsedMilliseconds;
                    Tracing.Trace(")reachability {0}", this.Index);
                }
#endif

                var ranAnything = false;
                for (int i = 0; i < graphStates.Count; i++)
                {
                    if (graphStates[i].Manager != null && this.graphStates[i].Manager.CurrentState == InternalGraphManagerState.Active)
                    {
                        try
                        {
                            var ranSomething = RunWorkItem(i);
                            ranAnything = ranSomething || ranAnything;
                        }
                        catch (Exception e)
                        {
                            Logging.Error("Graph {0} failed on scheduler {1} with exception:\n{2}", i, this.Index, e);
                            this.graphStates[i].Manager.Cancel(e);
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

                    this.Controller.Workers.NotifySchedulerWaking(this);
                }
            }

            this.Controller.Workers.NotifySchedulerTerminating(this);
            
        }

        protected bool RunWorkItem(int graphId)
        {
            var graphManager = this.graphStates[graphId].Manager;
            var workItems = this.graphStates[graphId].WorkItems;
            var itemToRun = workItems.Count;

            for (int i = 0; i < workItems.Count; i++)
            {
                if (itemToRun == workItems.Count || graphManager.Reachability.CompareTo(workItems.Array[itemToRun].Capability, workItems.Array[i].Capability) > 0)
                {
                    var valid = false;

                    // update the frontier, to keep things fresh-ish!
                    var frontier = graphManager.ProgressTracker.GetInfoForWorker(this.Index).PointstampCountSet.Frontier;
                    var local = this.graphStates[graphId].Producer.LocalPCS.Frontier;

                    var v = workItems.Array[i].Requirement;

                    var dominated = false;
                    for (int j = 0; j < frontier.Length && !dominated; j++)
                        if (graphManager.Reachability.LessThan(frontier[j], v) && !frontier[j].Equals(v))
                            dominated = true;

                    for (int j = 0; j < local.Length && !dominated; j++)
                        if (graphManager.Reachability.LessThan(local[j], v) && !local[j].Equals(v))
                            dominated = true;

                    valid = !dominated;

                    if (valid)
                        itemToRun = i;
                }
            }

            if (itemToRun < workItems.Count)
            {
                var item = workItems.RemoveAtAndReturn(itemToRun--);

                this.Controller.Workers.NotifyOperatorStarting(this, item);
                Tracing.Trace("[Sched " + this.Index + " " + item.ToString());
                
                Schedule(item);

                Tracing.Trace("]Sched " + this.Index + " " + item.ToString());
                this.Controller.Workers.NotifyOperatorEnding(this, item);

                this.graphStates[graphId].Producer.Start();   // tell everyone about records produced and consumed.

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
