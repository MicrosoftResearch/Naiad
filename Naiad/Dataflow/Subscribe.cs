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
using Naiad.Dataflow.Channels;
using Naiad.Runtime.Controlling;
using Naiad.Scheduling;
using Naiad.Dataflow;
using Naiad.Frameworks;

namespace Naiad.Dataflow
{
    /// <summary>
    /// Returned by Subscribe(), allowing threads to block until epochs have completed.
    /// </summary>
    public interface Subscription : IDisposable
    {
        /// <summary>
        /// Blocks until each local subscribe has been notified for epoch.
        /// </summary>
        /// <param name="time">epoch</param>
        void Sync(Epoch time);
    }

    public static class SubscribeExtensionMethods
    {
        /// <summary>
        /// Subscribes to a stream with no callback.
        /// </summary>
        /// <typeparam name="R">record type</typeparam>
        /// <param name="stream">input stream</param>
        /// <returns>subscription for synchronization</returns>
        public static Subscription Subscribe<R>(this Dataflow.Stream<R, Epoch> stream)
        {
            return stream.Subscribe(x => { });
        }

        /// <summary>
        /// Subscribes to a stream with a per-epoch callback applied by one worker.
        /// </summary>
        /// <typeparam name="R">record type</typeparam>
        /// <param name="stream">input stream</param>
        /// <param name="action">callback</param>
        /// <returns>subscription for synchronization</returns>
        public static Subscription Subscribe<R>(this Dataflow.Stream<R, Epoch> stream, Action<IEnumerable<R>> action)
        {
            return new Subscription<R>(stream, new SingleVertexPlacement(0, 0), stream.Context, (j, t, l) => action(l));
        }

        /// <summary>
        /// Subscribes to a stream with a per-epoch callback applied at each worker.
        /// </summary>
        /// <typeparam name="R">record type</typeparam>
        /// <param name="stream">input stream</param>
        /// <param name="action">callback on worker id and records</param>
        /// <returns>subscription for synchronization</returns>
        public static Subscription Subscribe<R>(this Dataflow.Stream<R, Epoch> stream, Action<int, IEnumerable<R>> action)
        {
            return stream.Subscribe((j, t, l) => action(j, l));
        }

        /// <summary>
        /// Subscribes to a stream with a callback parameterized by worker id, epoch, and records.
        /// </summary>
        /// <typeparam name="R">record type</typeparam>
        /// <param name="stream">input stream</param>
        /// <param name="action">callback on worker id, epoch id, and records</param>
        /// <returns>subscription for synchronization</returns>
        public static Subscription Subscribe<R>(this Dataflow.Stream<R, Epoch> stream, Action<int, int, IEnumerable<R>> action)
        {
            return new Subscription<R>(stream, stream.ForStage.Placement, stream.Context, action);
        }

        /// <summary>
        /// Subscribes to a stream with callbacks for record receipt, epoch completion notification, and stream completion notification.
        /// </summary>
        /// <typeparam name="R">record type</typeparam>
        /// <param name="stream">input stream</param>
        /// <param name="onRecv">receipt callback</param>
        /// <param name="onNotify">notification callback</param>
        /// <param name="onComplete">completion callback</param>
        /// <returns>subscription for synchronization</returns>
        public static Subscription Subscribe<R>(this Dataflow.Stream<R, Epoch> stream, Action<Message<Pair<R, Epoch>>, int> onRecv, Action<Epoch, int> onNotify, Action<int> onComplete)
        {
            return new Subscription<R>(stream, stream.ForStage.Placement, stream.Context, onRecv, onNotify, onComplete);
        }
    }

    /// <summary>
    /// Manages several subscribe shards, and allows another thread to block until all have completed a specified epoch
    /// </summary>
    internal class Subscription<R> : IDisposable, Subscription
    {
        private readonly Dictionary<int, CountdownEvent> Countdowns;        
        private int LocalShardCount;

        private int CompleteThrough;

        private bool disposed = false;
        internal bool Disposed { get { return this.disposed; } }

        public void Dispose()
        {
            disposed = true;
        }

        internal readonly InputStage[] SourceInputs;


        /// <summary>
        /// Called by shards, indicates the receipt of an OnDone(time)
        /// </summary>
        /// <param name="time">Time that has completed for the shard</param>
        internal void Signal(Epoch time)
        {
            lock (this.Countdowns)
            {
                // if this is the first mention of time.t, create a new countdown
                if (!this.Countdowns.ContainsKey(time.t))
                    this.Countdowns[time.t] = new CountdownEvent(this.LocalShardCount);

                if (this.Countdowns[time.t].CurrentCount > 0)
                    this.Countdowns[time.t].Signal();
                else
                    Console.Error.WriteLine("Too many Signal({0})", time.t);

                // if the last signal, clean up a bit
                if (this.Countdowns[time.t].CurrentCount == 0)
                {
                    this.CompleteThrough = time.t; // bump completethrough int
                    this.Countdowns.Remove(time.t); // remove countdown object
                }
            }
        }

        /// <summary>
        /// Called by other threads, and should block until all local shards have received OnDone(time)
        /// </summary>
        /// <param name="time">Time to wait until locally complete</param>
        public void Sync(Epoch time)
        {
            CountdownEvent countdown;
            lock (this.Countdowns)
            {
                // if we have already completed it, don't wait
                if (time.t <= this.CompleteThrough)
                    return;

                // if we haven't heard about it, create a new countdown
                if (!this.Countdowns.ContainsKey(time.t))
                    this.Countdowns[time.t] = new CountdownEvent(this.LocalShardCount);

                countdown = this.Countdowns[time.t];
            }

            // having released the lock, wait.
            countdown.Wait();
        }

        internal Subscription(Stream<R, Epoch> input, Placement placement, OpaqueTimeContext<Epoch> context, Action<Message<Pair<R, Epoch>>, int> onRecv, Action<Epoch, int> onNotify, Action<int> onComplete)
        {
            foreach (var entry in placement)
                if (entry.ProcessId == context.Context.Manager.GraphManager.Controller.Configuration.ProcessID)
                    this.LocalShardCount++;

            var stage = new Stage<SubscribeStreamingVertex<R>, Epoch>(placement, context, Stage.OperatorType.Default, (i, v) => new SubscribeStreamingVertex<R>(i, v, this, onRecv, onNotify, onComplete), "Subscribe");

            stage.NewInput(input, (message, shard) => shard.MessageReceived(message), null);

            this.Countdowns = new Dictionary<int, CountdownEvent>();
            this.CompleteThrough = -1;

            // important for reachability to be defined for the next test
            stage.InternalGraphManager.Reachability.UpdateReachabilityPartialOrder(stage.InternalGraphManager);

            // should only schedule next epoch if at least one input who can reach this stage will have data for this.
            this.SourceInputs = stage.InternalGraphManager.Inputs.Where(i => stage.InternalGraphManager.Reachability.ComparisonDepth[i.InputId][stage.StageId] != 0).ToArray();

            // add this subscription to the list of outputs.
            stage.InternalGraphManager.Register(this);
        }

        internal Subscription(Stream<R, Epoch> input, Placement placement, OpaqueTimeContext<Epoch> context, Action<int, int, IEnumerable<R>> action)
        {
            foreach (var entry in placement)
                if (entry.ProcessId == context.Context.Manager.GraphManager.Controller.Configuration.ProcessID)
                    this.LocalShardCount++;

            var stage = new Stage<SubscribeBufferingVertex<R>, Epoch>(placement, context, Stage.OperatorType.Default, (i, v) => new SubscribeBufferingVertex<R>(i, v, this, action), "Subscribe");

            stage.NewInput(input, (message, shard) => shard.MessageReceived(message), null);

            this.Countdowns = new Dictionary<int, CountdownEvent>();
            this.CompleteThrough = -1;

            // important for reachability to be defined for the next test
            stage.InternalGraphManager.Reachability.UpdateReachabilityPartialOrder(stage.InternalGraphManager);

            // should only schedule next epoch if at least one input who can reach this stage will have data for this.
            this.SourceInputs = stage.InternalGraphManager.Inputs.Where(i => stage.InternalGraphManager.Reachability.ComparisonDepth[i.InputId][stage.StageId] != 0).ToArray();

            // add this subscription to the list of outputs.
            stage.InternalGraphManager.Register(this);
        }
    }

    /// <summary>
    /// Individual subscription shard, invokes actions and notifies parent stage.
    /// </summary>
    /// <typeparam name="R">Record type</typeparam>
    internal class SubscribeStreamingVertex<R> : SinkVertex<R, Epoch>
    {
        Action<Message<Pair<R, Epoch>>, int> OnRecv;
        Action<Epoch, int> OnNotify;
        Action<int> OnCompleted;

        Subscription<R> Parent;

        protected override void OnShutdown()
        {
            Console.WriteLine("About to invoke OnCompleted on scheduler {0}", this.scheduler.Index);
            this.OnCompleted(this.Scheduler.Index);

            base.ShutDown();
        }

        public override void MessageReceived(Message<Pair<R, Epoch>> record)
        {
            this.OnRecv(record, this.Scheduler.Index);
            for (int i = 0; i < record.length; i++)
                this.NotifyAt(record.payload[i].v2);
        }

        /// <summary>
        /// When a time completes, invokes an action on received data, signals parent stage, and schedules OnDone for next expoch.
        /// </summary>
        /// <param name="time"></param>
        public override void OnDone(Epoch time)
        {
            // test to see if inputs supplied data for this epoch, or terminated instead
            var validEpoch = false;
            for (int i = 0; i < this.Parent.SourceInputs.Length; i++)
                if (this.Parent.SourceInputs[i].MaximumValidEpoch >= time.t)
                    validEpoch = true;
            
            if (validEpoch)
                this.OnNotify(time, this.Scheduler.Index);

            this.Parent.Signal(time);

            if (!this.Parent.Disposed && validEpoch)
                this.NotifyAt(new Epoch(time.t + 1));         
        }

        public SubscribeStreamingVertex(int index, Stage<Epoch> stage, Subscription<R> parent, Action<Message<Pair<R, Epoch>>, int> onrecv, Action<Epoch, int> onnotify, Action<int> oncomplete)
            : base(index, stage)
        {
            this.Parent = parent;

            this.OnRecv = onrecv;
            this.OnNotify = onnotify;
            this.OnCompleted = oncomplete;

            this.NotifyAt(new Epoch(0));
        }
    }

    /// <summary>
    /// Individual subscription shard, invokes actions and notifies parent stage.
    /// </summary>
    /// <typeparam name="R">Record type</typeparam>
    internal class SubscribeBufferingVertex<R> : SinkBufferingVertex<R, Epoch>
    {
        Action<int, int, IEnumerable<R>> Action;        // (shardid, epoch, data) => ()
        Subscription<R> Parent;
        
        /// <summary>
        /// When a time completes, invokes an action on received data, signals parent stage, and schedules OnDone for next expoch.
        /// </summary>
        /// <param name="time"></param>
        public override void OnDone(Epoch time)
        {
            var validEpoch = false;
            for (int i = 0; i < this.Parent.SourceInputs.Length; i++)
                if (this.Parent.SourceInputs[i].MaximumValidEpoch >= time.t)
                    validEpoch = true;

            if (validEpoch)
                Action(this.VertexId, time.t, Input.GetRecordsAt(time));
            
            this.Parent.Signal(time);

            if (!this.Parent.Disposed && validEpoch)
                this.NotifyAt(new Epoch(time.t + 1));
        }

        public SubscribeBufferingVertex(int index, Stage<Epoch> stage, Subscription<R> parent, Action<int, int, IEnumerable<R>> action)
            : base(index, stage, null)
        {
            this.Parent = parent;
            this.Action = action;
            this.Input = new Frameworks.VertexInputBuffer<R, Epoch>(this);
            this.NotifyAt(new Epoch(0));
        }
    }
}