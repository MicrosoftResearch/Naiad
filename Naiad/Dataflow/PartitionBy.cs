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
using System.Linq.Expressions;
using System.Text;
using Naiad.Dataflow.Channels;
using Naiad.Dataflow;
using Naiad.Frameworks;

namespace Naiad.Dataflow.PartitionBy
{
    public static class ExtensionMethods
    {
        public static Stream<R, T> PartitionBy<R, T>(this Stream<R, T> stream, Expression<Func<R,int>> partitionBy)
            where T : Time<T>
        {
            // if the data are already partitioned (or claim to be) just return the stream.
            if (partitionBy == null || Naiad.CodeGeneration.ExpressionComparer.Instance.Equals(stream.PartitionedBy, partitionBy))
                return stream;

            return Foundry.NewStage(stream, (i, v) => new PartitionByShard<R, T>(i, v, null), partitionBy, partitionBy, "PartitionBy");
        }

        public static Stream<R, T> AssumePartitionedBy<R, T>(Stream<R, T> stream, Expression<Func<R, int>> partitionBy)
            where T : Time<T>
        {
            return Foundry.NewStage(stream, (i, v) => new PartitionByShard<R, T>(i, v, null), null, partitionBy, "PartitionBy");
        }

        public static Stream<R, T> AssertPartitionedBy<R, T>(Stream<R, T> stream, Expression<Func<R, int>> partitionBy)
            where T : Time<T>
        {
            return Foundry.NewStage(stream, (i, v) => new PartitionByShard<R, T>(i, v, partitionBy), null, partitionBy, "PartitionBy");
        }
    }

    internal class PartitionByShard<R, T> : Naiad.Frameworks.UnaryVertex<R, R, T>
        where T : Time<T>
    {
        private readonly Func<R,int> key;
        private readonly int Shards;

        public override void MessageReceived(Message<Pair<R, T>> message)
        {
            for (int i = 0; i < message.length; ++i)
                if (key != null && (key(message.payload[i].v1) % this.Shards != this.VertexId))
                    Console.Error.WriteLine("Partitioning error {0} in shard {1}", message.payload[i], this.VertexId);

            this.Output.Send(message);
        }

        public override bool Stateful { get { return false; } }

        public PartitionByShard(int index, Stage<T> stage, Expression<Func<R,int>> keyFunc)
            : base(index, stage)
        {
            this.key = keyFunc == null ? null : keyFunc.Compile();
            this.Shards = this.Stage.Placement.Count;
        }
    }
}
