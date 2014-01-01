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
using Naiad;
using Naiad.Dataflow.Channels;
using Naiad.Runtime.Controlling;
using Naiad.Dataflow;
using Naiad.Frameworks;
using Naiad.Scheduling;
using Naiad.Frameworks.DifferentialDataflow;

namespace Naiad.Frameworks.DifferentialDataflow.Operators
{
    public class IntegratorShard<R> : Vertex<Epoch>
        where R : IEquatable<R>
    {
        private readonly VertexInputBuffer<Weighted<R>, Epoch> input;
        private readonly VertexOutputBuffer<Weighted<R>, Epoch> output;

        private readonly Dictionary<R, Int64> currentIntegration;

        public static Stream<Weighted<R>, Epoch> NewStage(Stream<Weighted<R>, Epoch> source)
        {
            var stage = Foundry.NewStage(source.Context, (i, v) => new IntegratorShard<R>(i, v), "Integrator");

            stage.NewInput(source, shard => shard.input, source.PartitionedBy);

            return stage.NewOutput(shard => shard.output, source.PartitionedBy);
        }

        public override void OnDone(Epoch time)
        {
            var newRecords = this.input.GetRecordsAt(time);

            foreach (var record in newRecords)
            {
                var currentWeight = 0L;
                this.currentIntegration.TryGetValue(record.record, out currentWeight);
                if (currentWeight + record.weight == 0)
                    this.currentIntegration.Remove(record.record);
                else
                    this.currentIntegration[record.record] = currentWeight + record.weight;
            }

            foreach (var pair in this.currentIntegration)
            {
                this.output.Send(pair.Key.ToWeighted(pair.Value), time);
            }

            this.output.Flush();
        }

        public IntegratorShard(int index, Stage<Epoch> stage)
            : base(index, stage)
        {
            this.input = new VertexInputBuffer<Weighted<R>, Epoch>(this);
            this.output = new VertexOutputBuffer<Weighted<R>, Epoch>(this);

            this.currentIntegration = new Dictionary<R, Int64>();
        }
    }
}
