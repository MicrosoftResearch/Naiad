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
using System.Linq.Expressions;
using System.Text;
using Microsoft.Research.Naiad.Dataflow.Channels;
using Microsoft.Research.Naiad.Dataflow;
using Microsoft.Research.Naiad.Dataflow.StandardVertices;

namespace Microsoft.Research.Naiad.Dataflow.PartitionBy
{
    /// <summary>
    /// Extension methods
    /// </summary>
    public static class ExtensionMethods
    {
        /// <summary>
        /// Partitions a stream by a function.
        /// </summary>
        /// <typeparam name="TRecord">record type</typeparam>
        /// <typeparam name="TTime">time type</typeparam>
        /// <param name="stream">stream</param>
        /// <param name="partitionBy">partitioning function</param>
        /// <returns>a repartitioned stream</returns>
        public static Stream<TRecord, TTime> PartitionBy<TRecord, TTime>(this Stream<TRecord, TTime> stream, Expression<Func<TRecord,int>> partitionBy)
            where TTime : Time<TTime>
        {
            // if the data are already partitioned correctly (or claim to be) just return the stream.
            if (partitionBy == null || Microsoft.Research.Naiad.Utilities.ExpressionComparer.Instance.Equals(stream.PartitionedBy, partitionBy))
                return stream;

            return stream.NewUnaryStage((i, v) => new PartitionByVertex<TRecord, TTime>(i, v, null), partitionBy, partitionBy, "PartitionBy");
        }

        /// <summary>
        /// Claims to partition a stream by a function, but does not actually.
        /// </summary>
        /// <typeparam name="TRecord">record type</typeparam>
        /// <typeparam name="TTime">time type</typeparam>
        /// <param name="stream">stream</param>
        /// <param name="partitionBy">partitioning function</param>
        /// <returns>a repartitioned stream</returns>
        public static Stream<TRecord, TTime> AssumePartitionedBy<TRecord, TTime>(this Stream<TRecord, TTime> stream, Expression<Func<TRecord, int>> partitionBy)
            where TTime : Time<TTime>
        {
            return stream.NewUnaryStage((i, v) => new PartitionByVertex<TRecord, TTime>(i, v, null), null, partitionBy, "PartitionBy");
        }

        /// <summary>
        /// Asserts a stream is partitioned by a function, and complains otherwise.
        /// </summary>
        /// <typeparam name="TRecord">record type</typeparam>
        /// <typeparam name="TTime">time type</typeparam>
        /// <param name="stream">stream</param>
        /// <param name="partitionBy">partitioning function</param>
        /// <returns>a repartitioned stream</returns>
        public static Stream<TRecord, TTime> AssertPartitionedBy<TRecord, TTime>(this Stream<TRecord, TTime> stream, Expression<Func<TRecord, int>> partitionBy)
            where TTime : Time<TTime>
        {
            return stream.NewUnaryStage((i, v) => new PartitionByVertex<TRecord, TTime>(i, v, partitionBy), null, partitionBy, "PartitionBy");
        }
    }

    internal class PartitionByVertex<R, T> : UnaryVertex<R, R, T>
        where T : Time<T>
    {
        private readonly Func<R,int> key;
        private readonly int Vertices;

        public override void OnReceive(Message<R, T> message)
        {
            for (int i = 0; i < message.length; ++i)
                if (key != null && (key(message.payload[i]) % this.Vertices != this.VertexId))
                    Console.Error.WriteLine("Partitioning error {0} in vertex {1}", message.payload[i], this.VertexId);

            this.Output.Send(message);
        }

        internal override bool Stateful { get { return false; } }

        public PartitionByVertex(int index, Stage<T> stage, Expression<Func<R,int>> keyFunc)
            : base(index, stage)
        {
            this.key = keyFunc == null ? null : keyFunc.Compile();
            this.Vertices = this.Stage.Placement.Count;
        }
    }
}
