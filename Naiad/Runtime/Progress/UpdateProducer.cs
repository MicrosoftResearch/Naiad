/*
 * Naiad ver. 0.3
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
using Microsoft.Research.Naiad.Dataflow.Channels;
using Microsoft.Research.Naiad.CodeGeneration;
using Microsoft.Research.Naiad.Scheduling;
using Microsoft.Research.Naiad.FaultTolerance;
using Microsoft.Research.Naiad.Frameworks;
using Microsoft.Research.Naiad.Dataflow;

namespace Microsoft.Research.Naiad.Runtime.Progress
{
    internal class ProgressUpdateProducer
    {
        internal readonly PointstampCountSet LocalPCS;
        private readonly ProgressUpdateAggregator Aggregator;

        public override string ToString()
        {
            return "ProgressUpdateProducer";
        }

        private readonly Dictionary<Pointstamp, Int64> outstandingRecords = new Dictionary<Pointstamp, Int64>();
        public void UpdateRecordCounts(Pointstamp time, Int64 delta)
        {
            Tracing.Trace("(ProdLock");
            lock (this)
            {
                //if (this.Stage.InternalGraphManager.Controller.Configuration.Impersonation && !this.Stage.InternalGraphManager.Reachability.NoImpersonation.Contains(time.Location) && this.Stage.InternalGraphManager.Reachability.Impersonations[time.Location] != null)
                //{
                //    foreach (var newVersion in this.Stage.InternalGraphManager.Reachability.EnumerateImpersonations(time))
                //        AddToOutstandingRecords(newVersion, delta);
                //
                //    this.LocalPCS.UpdatePointstampCount(time, delta);
                //}
                //else

                AddToOutstandingRecords(time, delta);
            }
            Tracing.Trace(")ProdLock");
        }

        private void AddToOutstandingRecords(Pointstamp time, Int64 delta)
        {
            var count = 0L;
            if (!outstandingRecords.TryGetValue(time, out count))
                outstandingRecords.Add(new Pointstamp(time), delta); // we want a new time, to avoid capturing the int[]
            else
            {
                outstandingRecords[time] = count + delta;
                if (outstandingRecords[time] == 0)
                    outstandingRecords.Remove(time);
            }
        }

        /// <summary>
        /// Lock the producer and transmit pointstamp counts to the appropriate consumer(s)
        /// </summary>
        public void Start()
        {
            Tracing.Trace("(ProdLock");
            lock (this)
            {
                // note: FOC may return without sending stuff due to re-entrancy.
                if (outstandingRecords.Count > 0)
                {
                    Aggregator.OnRecv(outstandingRecords);
                    outstandingRecords.Clear();
                }
            }
            Tracing.Trace(")ProdLock");
        }

        public void Checkpoint(NaiadWriter writer)
        {
            this.outstandingRecords.Checkpoint(writer);
        }

        public void Restore(NaiadReader reader)
        {
            this.outstandingRecords.Restore(reader);
        }

        internal ProgressUpdateProducer(InternalGraphManager manager, ProgressUpdateAggregator aggregator)
        {
            this.LocalPCS = new PointstampCountSet(manager.Reachability);
            this.Aggregator = aggregator;
        }
    }
}