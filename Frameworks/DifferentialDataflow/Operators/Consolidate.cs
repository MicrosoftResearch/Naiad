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

using System.Collections.Concurrent;
using System.IO;

using Microsoft.Research.Naiad.DataStructures;
using Microsoft.Research.Naiad.Dataflow.Channels;
using Microsoft.Research.Naiad.Serialization;
using Microsoft.Research.Naiad;
using Microsoft.Research.Naiad.Dataflow;
using Microsoft.Research.Naiad.Dataflow.StandardVertices;

namespace Microsoft.Research.Naiad.Frameworks.DifferentialDataflow.Operators
{
    internal class Consolidate<S, T> : UnaryVertex<Weighted<S>, Weighted<S>, T>
        where S : IEquatable<S>
        where T : Time<T>
    {
        Dictionary<T, Dictionary<S, Int64>> accumulations = new Dictionary<T, Dictionary<S, Int64>>();

        public override void OnReceive(Message<Weighted<S>, T> message)
        {
            //this.OnRecv(message.payload[i], message.time);
            if (!accumulations.ContainsKey(message.time))
            {
                accumulations.Add(message.time, new Dictionary<S, Int64>());
                this.NotifyAt(message.time);
            }

            var accum = accumulations[message.time];

            for (int i = 0; i < message.length; i++)
            {
                var record = message.payload[i];

                // update dictionary, adding/removing if needed
                Int64 count;
                accum.TryGetValue(record.record, out count);
                if (count + record.weight == 0)
                    accum.Remove(record.record);
                else
                    accum[record.record] = count + record.weight;
            }
        }

        //public override void DoWork(Naiad.Scheduling.VertexVersion version)
        public override void OnNotify(T time)
        {
            if (accumulations.Count > 0)
            {
                if (accumulations.ContainsKey(time))
                {
                    var output = this.Output.GetBufferForTime(time);

                    // send the accumulations at workTime
                    foreach (var pair in accumulations[time])
                        if (pair.Value != 0)
                            output.Send(new Weighted<S>(pair.Key, pair.Value));

                    accumulations.Remove(time);

                    Flush();
                }
            }
        }

        /* Checkpoint format:
         * (base)
         * if !terminated
         *     int                                       accumulationsCount
         *     (T, Dictionary<S,int>)*accumulationsCount accumulations
         */
        
        protected override void Checkpoint(NaiadWriter writer)
        {
            base.Checkpoint(writer);
            //if (!this.terminated)
            {
                writer.Write(this.accumulations.Count);
                foreach (var kvp in this.accumulations)
                {
                    writer.Write(kvp.Key);
                    kvp.Value.Checkpoint(writer);
                }
            }
        }

        protected override void Restore(NaiadReader reader)
        {
            base.Restore(reader);
            //if (!this.terminated)
            {
                
                this.accumulations.Clear();
                int accumulationsCount = reader.Read<int>();
                for (int i = 0; i < accumulationsCount; ++i)
                {
                    T key = reader.Read<T>();
                    Dictionary<S, Int64> value = new Dictionary<S, Int64>();
                    value.Restore(reader);
                    this.accumulations[key] = value;
                }
            }
        }

        public Consolidate(int index, Stage<T> collection)
            : base(index, collection)
        {
        }
    }
}
