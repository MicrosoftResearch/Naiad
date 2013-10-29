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

﻿using System;
using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;
using System.Text;
using System.Threading.Tasks;

using System.Threading;
using Naiad.DataStructures;
using Naiad.Dataflow.Channels;
using Naiad.CodeGeneration;
using Naiad.Frameworks;
using System.IO;
using System.Collections.Concurrent;
using System.Net.Sockets;
using System.Net;
using Naiad.Util;
using Naiad.Scheduling;
using Naiad.Runtime.Controlling;
using Naiad.FaultTolerance;
using Naiad.Runtime.Networking;
using Naiad.Runtime.Progress;
using Naiad.Runtime;

using System.Diagnostics;
using Naiad.Dataflow;
using System.Net.NetworkInformation;

namespace Naiad
{
    /// <summary>
    /// Manages the execution of a Naiad runtime
    /// </summary>
    public interface Controller : IDisposable //, GraphManager
    {
        /// <summary>
        /// The configuration used by the controller.
        /// </summary>
        Configuration Configuration { get; }

        /// <summary>
        /// Allocates a new inactive GraphManager.
        /// </summary>
        /// <returns>The new GraphManager</returns>
        GraphManager NewGraph();

        string QueryStatistic(RuntimeStatistic stat);

        #region Checkpoint / Restore

        //void Checkpoint(bool major);
        //void Checkpoint(string path, int epoch);
        
        //void Restore(string path, int epoch);
        //void Restore(NaiadReader reader);

        //void Pause();
        //void Resume();

        #endregion

        /// <summary>
        /// A WorkerGroup allowing worker event registration
        /// </summary>
        WorkerGroup WorkerGroup { get; }

        /// <summary>
        /// The default placement of new stages.
        /// </summary>
        Placement DefaultPlacement { get; }

        void Join();
    }

    internal interface InternalController
    {
        Configuration Configuration { get; }

        InternalWorkerGroup Workers { get; }

        Stopwatch Stopwatch { get; }

        int ProcessID { get; }
        int Processes { get; }

        Placement DefaultPlacement { get; }

        Object GlobalLock { get; }

        Stream GetLoggingOutputStream(Dataflow.Vertex shard);

        NetworkChannel NetworkChannel { get; }

        void DoStartupBarrier();
    }

    /// <summary>
    /// Static class for allocating Controllers.
    /// </summary>
    public static class NewController
    {
        /// <summary>
        /// Extracts arguments from args and constructs a Controller from them.
        /// </summary>
        /// <param name="args">arguments</param>
        /// <returns>A Controller derived from args.</returns>
        public static Controller FromArgs(ref string[] args)
        {
            return FromConfig(Configuration.FromArgs(ref args));
        }

        /// <summary>
        /// Returns a new Controller based on the supplied configuration.
        /// </summary>
        /// <param name="conf">configuration</param>
        /// <returns>A Controller derived from conf.</returns>
        public static Controller FromConfig(Configuration conf)
        {
            return new BaseController(conf);
        }
    }

    /// <summary>
    /// Responsible for managing the execution of multiple worker threads within a process.
    /// </summary>
    internal class BaseController : IDisposable, InternalController, Controller
    {
        private readonly List<BaseGraphManager> graphManagers;

        public string QueryStatistic(RuntimeStatistic stat)
        {
            var result = this.QueryStatisticAsLong(stat);
            if (result == null)
                return "-";
            else
                return Convert.ToString(result);
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
            if (this.OnShutdown!= null)
                this.OnShutdown(this, new EventArgs());
        }


        int streamCounter = 0;
        public Stream GetLoggingOutputStream(Dataflow.Vertex shard)
        {
            int streamNumber = Interlocked.Increment(ref this.streamCounter);

            return File.OpenWrite(string.Format("log_{0}_{1}-{2}.nad", streamNumber, shard.Stage.StageId, shard.VertexId));
        }

        private readonly Configuration configuration;
        public Configuration Configuration
        {
            get { return this.configuration; }
        }

        private readonly System.Diagnostics.Stopwatch stopwatch = System.Diagnostics.Stopwatch.StartNew();
        public System.Diagnostics.Stopwatch Stopwatch { get { return this.stopwatch; } }

        public virtual int ProcessID
        {
            get { return this.configuration.ProcessID; }
        }

        public virtual int Processes
        {
            get { return this.configuration.Endpoints == null ? 1 : this.configuration.Endpoints.Length; }
        }
        
        private readonly Object globalLock = new object();
        public Object GlobalLock { get { return this.globalLock; } }
        
        #region Checkpoint / Restore

        public void Checkpoint(bool major)
        {
            throw new NotImplementedException();

            Stopwatch checkpointWatch = Stopwatch.StartNew();

#if false
            foreach (var shard in this.currentGraphManager.Stages.Values.SelectMany(x => x.Shards.Where(s => s.Stateful)))
            {
                shard.Checkpoint(major);
            }
#endif
            Console.Error.WriteLine("!! Total checkpoint took time = {0}", checkpointWatch.Elapsed);
        }

        public void Checkpoint(string path, int epoch)
        {
            throw new NotImplementedException();

            Stopwatch checkpointWatch = Stopwatch.StartNew();

#if false
            foreach (var input in this.currentGraphManager.Inputs)
            {
                using (FileStream collectionFile = File.OpenWrite(Path.Combine(path, string.Format("input_{0}_{1}.shard", input.InputId, epoch))))
                using (NaiadWriter collectionWriter = new NaiadWriter(collectionFile))
                {
                    input.Checkpoint(collectionWriter);
                    Console.Error.WriteLine("Read  {0}: {1} objects", input.ToString(), collectionWriter.objectsWritten);
                }
            }
            foreach (var shard in this.currentGraphManager.Stages.Values.SelectMany(x => x.Shards.Where(s => s.Stateful)))
            {
                shard.Checkpoint(false);
                using (FileStream shardFile = File.OpenWrite(Path.Combine(path, string.Format("{0}_{1}_{2}.shard", shard.Stage.StageId, shard.VertexId, epoch))))
                using (NaiadWriter shardWriter = new NaiadWriter(shardFile))
                {
                    shard.Checkpoint(shardWriter);
                    Console.Error.WriteLine("Wrote {0}: {1} objects", shard.ToString(), shardWriter.objectsWritten);
                }
            }
#endif

            Console.Error.WriteLine("!! Total checkpoint took time = {0}", checkpointWatch.Elapsed);
        }

        public void Restore(string path, int epoch)
        {
            throw new NotImplementedException();

            Stopwatch checkpointWatch = Stopwatch.StartNew();
            
#if false
            // Need to do this to ensure that all stages exist.
            this.currentGraphManager.MaterializeAll();

            foreach (var input in this.currentGraphManager.Inputs)
            {
                using (FileStream collectionFile = File.OpenRead(Path.Combine(path, string.Format("input_{0}_{1}.shard", input.InputId, epoch))))
                using (NaiadReader collectionReader = new NaiadReader(collectionFile))
                {
                    input.Restore(collectionReader);
                    Console.Error.WriteLine("Read  {0}: {1} objects", input.ToString(), collectionReader.objectsRead);
                }
            }
            foreach (var shard in this.currentGraphManager.Stages.Values.SelectMany(x => x.Shards.Where(s => s.Stateful)))
            {
                using (FileStream shardFile = File.OpenRead(Path.Combine(path, string.Format("{0}_{1}_{2}.shard", shard.Stage.StageId, shard.VertexId, epoch))))
                using (NaiadReader shardReader = new NaiadReader(shardFile))
                {
                    shard.Restore(shardReader);
                    Console.Error.WriteLine("Read  {0}: {1} objects", shard.ToString(), shardReader.objectsRead);
                }
            }
            this.Workers.Activate();
            this.currentGraphManager.Activate();
#endif
            Console.Error.WriteLine("!! Total restore took time = {0}", checkpointWatch.Elapsed);
            Logging.Info("! Reactivated the controller");
        }

        public void Restore(NaiadReader reader)
        {
            throw new NotImplementedException();

#if false
            foreach (var kvp in this.currentGraphManager.Stages.OrderBy(x => x.Key))
            {
                int before = reader.objectsRead;
                kvp.Value.Restore(reader);
                int after = reader.objectsRead;
                Logging.Info("! Restored collection {0}, objects = {1}", kvp.Value, after - before);
            }
            this.Workers.Activate();
            this.currentGraphManager.Activate();
#endif
            Logging.Info("! Reactivated the controller");
        }

        #endregion

 

        /// <summary>
        /// Represents a groupb of Naiad workers that are controlled by a single Controller.
        /// </summary>
        public class BaseWorkerGroup : InternalWorkerGroup
        {
            private readonly int numWorkers;
            /// <summary>
            /// Returns the number of workers in this group.
            /// </summary>
            public int Count { get { return this.numWorkers; } }

            // Optional support for broadcast scheduler wakeup
            internal bool useBroadcastWakeup;
            internal EventCount wakeUpEvent;

            internal readonly Scheduler[] schedulers;

            public Scheduler this[int index] { get { return this.schedulers[index]; } }

            public void Start()
            {
                foreach (Scheduler scheduler in this.schedulers)
                    scheduler.Start();
            }

            public void Activate()
            {
                foreach (Scheduler scheduler in this.schedulers)
                    scheduler.AllChannelsInitialized();
            }

            public long BlockScheduler(AutoResetEvent selectiveEvent, long val)
            {
                this.wakeUpEvent.Await(selectiveEvent, val);
                return this.wakeUpEvent.Read(); // likely not necessary
            }

            public void WakeUp()
            {
                Tracing.Trace("{WakeUp");
                if (this.useBroadcastWakeup)
                {
                    this.wakeUpEvent.Advance();
                }
                else
                {
                foreach (Scheduler scheduler in this.schedulers)
                    scheduler.Signal();
                }
                Tracing.Trace("}WakeUp");
            }

            public void Abort()
            {
                foreach (Scheduler scheduler in this.schedulers)
                    scheduler.Abort();
            }

            public void Pause()
            {
                using (CountdownEvent pauseCountdown = new CountdownEvent(this.schedulers.Length))
                {
                    lock (this)
                    {
                        foreach (Scheduler scheduler in this.schedulers)
                            scheduler.Pause(pauseCountdown);
                    }
                    pauseCountdown.Wait();
                }
            }

            public void Resume()
            {
                foreach (Scheduler scheduler in this.schedulers)
                    scheduler.Resume();
            }

            internal void DrainAllQueuedMessages()
            {
                foreach (Scheduler scheduler in this.schedulers)
                    scheduler.DrainPostOffice();
            }

            #region Scheduler events
            /// <summary>
            /// This event is fired by each worker when it initially starts.
            /// </summary>
            public event EventHandler<SchedulerStartArgs> Starting;
            public void NotifySchedulerStarting(Scheduler scheduler)
            {
                if (this.Starting != null)
                    this.Starting(this, new SchedulerStartArgs(scheduler.Index));
            }

            /// <summary>
            /// This event is fired by each worker when it wakes from sleeping.
            /// </summary>
            public event EventHandler<SchedulerWakeArgs> Waking;
            public void NotifySchedulerWaking(Scheduler scheduler)
            {
                if (this.Waking != null)
                    this.Waking(this, new SchedulerWakeArgs(scheduler.Index));
            }

            /// <summary>
            /// This event is fired by a worker immediately before executing a work item.
            /// </summary>
            public event EventHandler<OperatorStartArgs> WorkItemStarting;
            public void NotifyOperatorStarting(Scheduler scheduler, Scheduler.WorkItem work)
            {
                if (this.WorkItemStarting != null)
                    this.WorkItemStarting(this, new OperatorStartArgs(scheduler.Index, work.Shard.Stage, work.Shard.VertexId, work.Requirement));
            }

            /// <summary>
            /// This event is fired by a worker immediately after executing a work item.
            /// </summary>
            public event EventHandler<OperatorEndArgs> WorkItemEnding;
            public void NotifyOperatorEnding(Scheduler scheduler, Scheduler.WorkItem work)
            {
                if (this.WorkItemEnding != null)
                    this.WorkItemEnding(this, new OperatorEndArgs(scheduler.Index, work.Shard.Stage, work.Shard.VertexId, work.Requirement));
            }

            /// <summary>
            /// This event is fired by a worker immediately after enqueueing a work item.
            /// </summary>
            public event EventHandler<OperatorEnqArgs> WorkItemEnqueued;
            public void NotifyOperatorEnqueued(Scheduler scheduler, Scheduler.WorkItem work)
            {
                if (this.WorkItemEnqueued != null)
                    this.WorkItemEnqueued(this, new OperatorEnqArgs(scheduler.Index, work.Shard.Stage, work.Shard.VertexId, work.Requirement));
            }

            /// <summary>
            /// This event is fired by a worker when it becomes idle, because it has no work to execute.
            /// </summary>
            public event EventHandler<SchedulerSleepArgs> Sleeping;
            public void NotifySchedulerSleeping(Scheduler scheduler)
            {
                if (this.Sleeping != null)
                    this.Sleeping(this, new SchedulerSleepArgs(scheduler.Index));
            }

            /// <summary>
            /// This event is fired by a worker when it has finished all work, and the computation has terminated.
            /// </summary>
            public event EventHandler<SchedulerTerminateArgs> Terminating;
            public void NotifySchedulerTerminating(Scheduler scheduler)
            {
                if (this.Terminating != null)
                    this.Terminating(this, new SchedulerTerminateArgs(scheduler.Index));
            }

            /// <summary>
            /// This event is fired by a worker when a batch of records is delivered to an operator.
            /// </summary>
            public event EventHandler<OperatorReceiveArgs> ReceivedRecords;
            public void NotifyOperatorReceivedRecords(Dataflow.Vertex op, int channelId, int recordsReceived)
            {
                if (this.ReceivedRecords != null)
                    this.ReceivedRecords(this, new OperatorReceiveArgs(op.Stage, op.VertexId, channelId, recordsReceived));
            }

            /// <summary>
            /// This event is fired by a worker when a batch of records is sent by an operator.
            /// (N.B. This event is currently not used.)
            /// </summary>
            public event EventHandler<OperatorSendArgs> SentRecords;
            public void NotifyOperatorSentRecords(Dataflow.Vertex op, int channelId, int recordsSent)
            {
                if (this.SentRecords != null)
                    this.SentRecords(this, new OperatorSendArgs(op.Stage, op.VertexId, channelId, recordsSent));
            }
            #endregion Scheduler events

            internal BaseWorkerGroup(InternalController controller, int numWorkers)
            {
                this.numWorkers = numWorkers;
                this.schedulers = new Scheduler[numWorkers];

                if (controller.Configuration.UseBroadcastWakeup)
                {
                    this.useBroadcastWakeup = true;
                    this.wakeUpEvent = new EventCount();
                }
                else
                    this.useBroadcastWakeup = false;

                for (int i = 0; i < numWorkers; ++i)
                {
                    switch (System.Environment.OSVersion.Platform)
                    {
                        case PlatformID.Win32NT:
                            this.schedulers[i] = new PinnedScheduler(string.Format("Naiad worker {0}", i), i, controller);
                            break;
                        default:
                            this.schedulers[i] = new Scheduler(string.Format("Naiad worker {0}", i), i, controller);
                            break;
                    }
                }
            }
        }

        private readonly BaseWorkerGroup workerGroup;

        /// <summary>
        /// Returns information about the local workers controlled by this controller.
        /// </summary>
        public InternalWorkerGroup Workers { get { return this.workerGroup; } }
        
        public WorkerGroup WorkerGroup { get { return this.workerGroup; } }

        private bool isJoined = false;

        /// <summary>
        /// Blocks until all computation is complete and resources are released.
        /// </summary>
        public void Join()
        {
            foreach (var manager in this.graphManagers.Where(x => x.CurrentState != InternalGraphManagerState.Inactive))
                manager.Join();

            this.workerGroup.Abort();
            
            foreach (Scheduler scheduler in this.workerGroup.schedulers)
                scheduler.Join();

            NotifyOnShutdown();

            this.isJoined = true;
        }

        public long? QueryStatisticAsLong(RuntimeStatistic s)
        {
            long res = 0;
            switch (s)
            {
                // Per scheduler statistics
                case RuntimeStatistic.ProgressLocalRecords:
                case RuntimeStatistic.RxProgressBytes:
                case RuntimeStatistic.RxProgressMessages:
                case RuntimeStatistic.TxProgressBytes:
                case RuntimeStatistic.TxProgressMessages:    
                {
                    for (int i = 0; i < this.workerGroup.schedulers.Length; i++)
                    {
                        res += this.workerGroup.schedulers[i].statistics[(int)s];
                    }
                    break;
                }
                // Network channel receive statistics
                case RuntimeStatistic.RxNetBytes:
                case RuntimeStatistic.RxNetMessages:
                case RuntimeStatistic.TxHighPriorityBytes:
                case RuntimeStatistic.TxHighPriorityMessages:
                case RuntimeStatistic.TxNormalPriorityBytes:
                case RuntimeStatistic.TxNormalPriorityMessages:
                {
                    if (this.NetworkChannel != null)
                    {
                        res = this.NetworkChannel.QueryStatistic(s);
                    }
                    else
                    {
                        return null;
                    }
                    break;
                }                    
                default:
                    return null;
            }
            return res;
        }

        CancellationTokenSource cancelStatsToken;

        

        public NetworkChannel NetworkChannel { get { return this.networkChannel; } }

        private readonly NetworkChannel networkChannel;
        private readonly IPEndPoint localEndpoint;

        private Placement defaultPlacement;
        public Placement DefaultPlacement { get { return this.defaultPlacement; } }


        /// <summary>
        /// Constructs a controller for a new computation.
        /// </summary>
        /// <param name="config">Controller configuration</param>
        public BaseController(Configuration config)
        {
            this.configuration = config;

            this.localEndpoint = config.Endpoints == null ? new IPEndPoint(IPAddress.Any, 2101) : config.Endpoints[this.ProcessID];
            this.workerGroup = new BaseWorkerGroup(this, config.WorkerCount);

            this.workerGroup.Start();
            this.workerGroup.Activate();

            if (this.Processes > 1)
            {
                this.defaultPlacement = new RoundRobinPlacement(this.Processes, this.workerGroup.Count);

                this.networkChannel = new TcpNetworkChannel(0, this, config);

                this.EnsureServerRunning();
                this.networkChannel.WaitForAllConnections();

                Logging.Info("Network channel activated");
            }
            else
            {
                this.defaultPlacement = new RoundRobinPlacement(1, this.workerGroup.Count);
            }

#if DEBUG
            Logging.Progress("Warning: DEBUG build. Not for performance measurements.");
#endif

            if (this.workerGroup.Count < Environment.ProcessorCount)
                Logging.Progress("Warning: Using fewer threads than available processors (use -t to set number of threads).");

            Logging.Progress("Initializing {0} {1}", this.workerGroup.Count, this.workerGroup.Count == 1 ? "thread" : "threads");
            Logging.Progress("Server GC = {0}", System.Runtime.GCSettings.IsServerGC);
            Logging.Progress("GC settings latencymode={0}", System.Runtime.GCSettings.LatencyMode);
            Logging.Progress("Using CLR {0}", System.Environment.Version);

            if (Configuration.CollectNetStats)
            {
                int sleepTime = 1000;
                Logging.Progress("Monitoring network stats on interface {0} every {1}ms", Configuration.NetStatsInterfaceName, sleepTime);
                StreamWriter sw = new StreamWriter(File.Create("stats.txt"));
                var st = new StateForStats(this, sw, sleepTime, this.localEndpoint.Address);
                //Thread statsThrd = new Thread(new ParameterizedThreadStart(st.MonitorMemFootprint));
                Thread statsThrd = new Thread(new ParameterizedThreadStart(st.MonitorNetwork));
                cancelStatsToken = new CancellationTokenSource();
                statsThrd.Start(cancelStatsToken.Token);
            }

            if (this.NetworkChannel != null)
                this.NetworkChannel.StartMessageDelivery();

            this.graphsManaged = 0;
            this.graphManagers = new List<BaseGraphManager>();
        }

        public GraphManager NewGraph()
        {
            var result = new BaseGraphManager(this, this.graphsManaged++);

            this.graphManagers.Add(result);

            for (int i = 0; i < workerGroup.Count; i++)
                workerGroup[i].RegisterGraph(result);

            return result;
        }

        private int graphsManaged;

        /// <summary>
        /// Blocks until all computation associated with the supplied epoch have been retired.
        /// </summary>
        /// <param name="epoch">Epoch to wait for</param>
        public void Sync(int epoch)
        {
            foreach (var manager in this.graphManagers)
                manager.Sync(epoch);
        }

        public void Dispose()
        {
            if (!this.isJoined)
            {
                Logging.Error("Attempted to dispose controller before joining.");
                Logging.Error("You must call controller.Join() before disposing/exiting the using block.");
                System.Environment.Exit(-1);
            }

            foreach (Scheduler scheduler in this.workerGroup.schedulers)
                scheduler.Dispose();

            if (this.networkChannel != null)
                this.networkChannel.Dispose();

            if (Configuration.CollectNetStats)
                cancelStatsToken.Cancel();

            Logging.Stop();
        }

        private NaiadServer server;

        internal void EnsureServerRunning()
        {
            if (this.server == null)
            {
                this.server = new NaiadServer(this.localEndpoint);
                this.server.RegisterNetworkChannel((TcpNetworkChannel)this.networkChannel);

                this.server.Start();
            }
        }

        public void DoStartupBarrier()
        {
            if (this.networkChannel != null)
            {
                this.networkChannel.DoStartupBarrier();
                Logging.Progress("Did startup barrier for graph");
            }
        }

        public void Pause()
        {
            this.Workers.Pause();
            if (this.networkChannel != null && this.networkChannel is Snapshottable)
            {
                ((Snapshottable)this.networkChannel).AnnounceCheckpoint();
                ((Snapshottable)this.networkChannel).WaitForAllCheckpointMessages();
            }
            this.workerGroup.DrainAllQueuedMessages();
        }

        public void Resume()
        {
            this.Workers.Resume();
            if (this.networkChannel != null && this.networkChannel is Snapshottable)
            {
                ((Snapshottable)this.networkChannel).ResumeAfterCheckpoint();
            }
        }
    }
}
