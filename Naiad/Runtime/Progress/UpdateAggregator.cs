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
using System.Text;
using System.Threading;
using Microsoft.Research.Naiad.Dataflow.Channels;
using Microsoft.Research.Naiad.Dataflow;
using Microsoft.Research.Naiad.Frameworks;
using Microsoft.Research.Naiad.Scheduling;

using Microsoft.Research.Naiad.Diagnostics;

namespace Microsoft.Research.Naiad.Runtime.Progress
{
    /// <summary>
    /// The aggregator takes progress updates (pointstamp, delta) and accumulates them until it is required to flush the accumulation.
    /// The requirement is based on a liveness constraint: the possibility than any delta in the accumulation might advance the frontier.
    /// </summary>
    internal class ProgressUpdateAggregator : Dataflow.Vertex<Empty>
    {
        internal readonly VertexOutputBuffer<Update, Empty> Output; //Int64, Pointstamp> Output;

        internal override void PerformAction(Scheduler.WorkItem workItem)
        {
            this.ConsiderFlushingBufferedUpdates(false);
        }
        
        // this should only be accessed under this.Lock, where we may swap these two things and flush one or the other.
        private Dictionary<Pointstamp, Int64> BufferedUpdates = new Dictionary<Pointstamp, long>();
        //private Dictionary<Pointstamp, Int64> BufferedUpdates2 = new Dictionary<Pointstamp, long>();

        // protects this.BufferedUpdates manipulation. It should not be consulted or manipulated without this lock.
        private readonly Object Lock = new object();

        // records transmitted notifications, for use in determining if buffered updates should be flushed.
        private readonly Dictionary<Pointstamp, Int64> Notifications = new Dictionary<Pointstamp, long>();

        internal void OnRecv(Dictionary<Pointstamp, Int64> deltas)
        {
            NaiadTracing.Trace.LockAcquire(this.Lock);
            lock(this.Lock)
            {
                NaiadTracing.Trace.LockHeld(this.Lock);
                foreach (var pair in deltas)
                {
                    if (!BufferedUpdates.ContainsKey(pair.Key))
                        BufferedUpdates.Add(pair.Key, 0);

                    BufferedUpdates[pair.Key] += pair.Value;

                    if (BufferedUpdates[pair.Key] == 0)
                        BufferedUpdates.Remove(pair.Key);
                }
            }
            NaiadTracing.Trace.LockRelease(this.Lock);

            ConsiderFlushingBufferedUpdates(false);
        }


        internal void Reset()
        {
            NaiadTracing.Trace.LockAcquire(this.Lock);
            lock (this.Lock)
            {
                if (this.BufferedUpdates.Count > 0)
                {
                    throw new ApplicationException("Resetting aggregator with unflushed updates");
                }
                this.Notifications.Clear();
            }
            NaiadTracing.Trace.LockRelease(this.Lock);
        }

        internal void ConsiderFlushingBufferedUpdates(bool force)
        {
            // set if a flush is required.
            var mustFlushBuffer = force;

            if (!force)
            {
                // consult the buffered updates under a lock.
                NaiadTracing.Trace.LockAcquire(this.Lock);
                lock (this.Lock)
                {
                    NaiadTracing.Trace.LockHeld(this.Lock);
                    if (this.BufferedUpdates.Count > 0)
                    {
                        var frontier = this.Stage.InternalComputation.ProgressTracker.GetInfoForWorker(0).PointstampCountSet.Frontier;

                        for (int i = 0; i < frontier.Length && !mustFlushBuffer; i++)
                        {
                            // flush if we find something in the buffered deltas, and is not a still outstanding notification.
                            // TODO ContainsKey test is overly conservative; absent key and positive update is no reason to flush.
                            if (this.BufferedUpdates.ContainsKey(frontier[i]))
                            {
                                if (!this.Stage.InternalComputation.Reachability.Graph[frontier[i].Location].IsStage
                                    || !this.Notifications.ContainsKey(frontier[i])
                                    || this.Notifications[frontier[i]] + this.BufferedUpdates[frontier[i]] <= 0)
                                    mustFlushBuffer = true;
                            }
                        }
                    }
                }
                NaiadTracing.Trace.LockRelease(this.Lock);
            }

            if (mustFlushBuffer)
            {
                Dictionary<Pointstamp, Int64> PrivateBufferedUpdates;
                Dictionary<Pointstamp, Int64> FreshBufferedUpdates = new Dictionary<Pointstamp, long>();

                // we don't want to get stuck behind a centralizer -> consumer on the same process.
                NaiadTracing.Trace.LockAcquire(this.scheduler.Controller.GlobalLock);
                lock (this.scheduler.Controller.GlobalLock)
                {
                    NaiadTracing.Trace.LockHeld(this.scheduler.Controller.GlobalLock);

                    // get exclusive access and swap the update buffer.
                    NaiadTracing.Trace.LockAcquire(this.Lock);
                    lock (this.Lock)
                    {
                        NaiadTracing.Trace.LockHeld(this.Lock);
                        PrivateBufferedUpdates = this.BufferedUpdates;
                        this.BufferedUpdates = FreshBufferedUpdates;

                        // update Notifications count to include shipped values.
                        foreach (var pair in PrivateBufferedUpdates)
                        {
                            if (this.Stage.InternalComputation.Reachability.Graph[pair.Key.Location].IsStage)
                            {
                                long prev = 0;
                                this.Notifications.TryGetValue(pair.Key, out prev);

                                if (prev + pair.Value != 0)
                                    this.Notifications[pair.Key] = prev + pair.Value;
                                else
                                    this.Notifications.Remove(pair.Key);
                            }
                        }
                    }
                    NaiadTracing.Trace.LockRelease(this.Lock);

                    var output = this.Output.GetBufferForTime(new Empty());

                    // send positive updates first.
                    foreach (var pair in PrivateBufferedUpdates)
                        if (pair.Value > 0)
                            output.Send(new Update(pair.Key, pair.Value));

                    // send negative updates second.
                    foreach (var pair in PrivateBufferedUpdates)
                        if (pair.Value < 0)
                            output.Send(new Update(pair.Key, pair.Value));

                    // here we might return it to a shared queue of dictionaries
                    PrivateBufferedUpdates.Clear();
                    this.Output.Flush();
                }
                NaiadTracing.Trace.LockRelease(this.scheduler.Controller.GlobalLock);
            }
        }

        public ProgressUpdateAggregator(int index, Stage<Empty> stage)
            : base(index, stage)
        {
            this.Output = new VertexOutputBuffer<Update, Empty>(this);
            NaiadTracing.Trace.LockInfo(this.Lock, "Aggregator Lock");
            // ensure the event stack isn't empty
            this.PushEventTime(new Empty());
        }
    }
}
