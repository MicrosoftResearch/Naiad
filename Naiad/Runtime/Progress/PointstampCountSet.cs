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
using Naiad.Dataflow.Channels;
using Naiad.CodeGeneration;
using Naiad.DataStructures;
using Naiad.FaultTolerance;
using Naiad.Scheduling;

namespace Naiad.Runtime.Progress
{
    internal class PointstampCountSet : ICheckpointable
    {
        internal readonly Dictionary<Pointstamp, Int64> Counts = new Dictionary<Pointstamp, Int64>();
        private readonly PointstampFrontier actualFrontier;

        internal PointstampCountSet(Reachability reachability)
        {
            actualFrontier = new PointstampFrontier(reachability);
        }

        public Scheduling.Pointstamp[] Frontier = new Pointstamp[0];

        internal bool UpdatePointstampCount(Pointstamp version, Int64 delta)
        {
            var oldFrontier = Frontier;
            var count = 0L;
            if (!Counts.TryGetValue(version, out count))
            {
                version = new Pointstamp(version);

                Counts.Add(version, delta);

                // Potentially add this version to the frontier
                if (actualFrontier.Add(version))
                    Frontier = actualFrontier.Antichain.ToArray();
            }
            else
            {
                if (count + delta == 0)
                {
                    Counts.Remove(version);
                    if (actualFrontier.Remove(version))
                        Frontier = actualFrontier.Antichain.ToArray();
                }
                else
                {
                    Counts[version] = count + delta;
                }
            }

            return Frontier != oldFrontier;
        }

        public void Checkpoint(NaiadWriter writer)
        {

            this.Counts.Checkpoint(writer, Pointstamp.Serializer, PrimitiveSerializers.Int64);
            this.actualFrontier.Checkpoint(writer, Pointstamp.Serializer);
            this.Frontier.Checkpoint(this.Frontier.Length, writer, Pointstamp.Serializer);
        }

        public void Restore(NaiadReader reader)
        {
            this.Counts.Restore(reader, Pointstamp.Serializer, PrimitiveSerializers.Int64);
            this.actualFrontier.Restore(reader, Pointstamp.Serializer);
            this.Frontier = FaultToleranceExtensionMethods.RestoreArray<Pointstamp>(reader, n => new Pointstamp[n], Pointstamp.Serializer);
        }

        public bool Stateful { get { return true; } }
    }
}
