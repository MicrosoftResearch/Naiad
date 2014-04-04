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
using System.Linq.Expressions;
using System.Text;
using Microsoft.Research.Naiad.Dataflow.Channels;
using Microsoft.Research.Naiad.Dataflow;
using Microsoft.Research.Naiad.Frameworks;
using Microsoft.Research.Naiad.Dataflow.Iteration;
using Microsoft.Research.Naiad;
using Microsoft.Research.Naiad.Frameworks.Reduction;

namespace Microsoft.Research.Naiad.Frameworks.Lindi
{
    /// <summary>
    /// Standard LINQ-like methods.
    /// </summary>
    public static class ExtensionMethods
    {
        /// <summary>
        /// Record by record stream transformation
        /// </summary>
        /// <typeparam name="TInput">Input type</typeparam>
        /// <typeparam name="TOutput">Output type</typeparam>
        /// <typeparam name="TTime">Time type</typeparam>
        /// <param name="stream">input stream</param>
        /// <param name="function">transformation</param>
        /// <returns>stream of transformed records</returns>
        public static Stream<TOutput, TTime> Select<TInput, TOutput, TTime>(this Stream<TInput, TTime> stream, Func<TInput, TOutput> function) where TTime : Time<TTime>
        {
            return Foundry.NewUnaryStage(stream, (i, v) => new SelectVertex<TInput, TOutput, TTime>(i, v, function), null, null, "Select");
        }

        /// <summary>
        /// Record by record filtering
        /// </summary>
        /// <typeparam name="TRecord">Input type</typeparam>
        /// <typeparam name="TTime">Time type</typeparam>
        /// <param name="stream">input stream</param>
        /// <param name="predicate">predicate</param>
        /// <returns>filtered stream</returns>
        public static Stream<TRecord, TTime> Where<TRecord, TTime>(this Stream<TRecord, TTime> stream, Func<TRecord, bool> predicate) where TTime : Time<TTime>
        {
            return Foundry.NewUnaryStage(stream, (i, v) => new WhereVertex<TRecord, TTime>(i, v, predicate), stream.PartitionedBy, stream.PartitionedBy, "Where");
        }

        /// <summary>
        /// One to many record by record transformation
        /// </summary>
        /// <typeparam name="TInput">Input type</typeparam>
        /// <typeparam name="TOutput">Output type</typeparam>
        /// <typeparam name="TTime">Time type</typeparam>
        /// <param name="stream">input stream</param>
        /// <param name="function">transformation</param>
        /// <returns>the concatenation of all results produced from each input record</returns>
        public static Stream<TOutput, TTime> SelectMany<TInput, TOutput, TTime>(this Stream<TInput, TTime> stream, Func<TInput, IEnumerable<TOutput>> function) where TTime : Time<TTime>
        {
            return Foundry.NewUnaryStage(stream, (i, v) => new SelectManyVertex<TInput, TOutput, TTime>(i, v, function), null, null, "SelectMany");
        }

        /// <summary>
        /// Concatenates two input streams
        /// </summary>
        /// <typeparam name="TRecord">Record type</typeparam>
        /// <typeparam name="TTime">Time type</typeparam>
        /// <param name="stream1">first input stream</param>
        /// <param name="stream2">second input stream</param>
        /// <returns>the [unordered] concatenation of the two streams</returns>
        public static Stream<TRecord, TTime> Concat<TRecord, TTime>(this Stream<TRecord, TTime> stream1, Stream<TRecord, TTime> stream2) where TTime : Time<TTime>
        {
            // test to see if they are partitioned properly, and if so maintain the information in output partitionedby information.
            var partitionedBy = Microsoft.Research.Naiad.CodeGeneration.ExpressionComparer.Instance.Equals(stream1.PartitionedBy, stream2.PartitionedBy) ? stream1.PartitionedBy : null;
            return Foundry.NewBinaryStage(stream1, stream2, (i, v) => new ConcatVertex<TRecord, TTime>(i, v), partitionedBy, partitionedBy, partitionedBy, "Concat");
        }

        /// <summary>
        /// Groups records by a key and applies a reducer to each group.
        /// </summary>
        /// <typeparam name="TInput">Input type</typeparam>
        /// <typeparam name="TKey">Key type</typeparam>
        /// <typeparam name="TOutput">Output type</typeparam>
        /// <typeparam name="TTime">Time type</typeparam>
        /// <param name="stream">input stream</param>
        /// <param name="key">key selector function</param>
        /// <param name="selector">result selector function</param>
        /// <returns>all results from applying selector to each group</returns>
        public static Stream<TOutput, TTime> GroupBy<TInput, TKey, TOutput, TTime>(this Stream<TInput, TTime> stream, Func<TInput, TKey> key, Func<TKey, IEnumerable<TInput>, IEnumerable<TOutput>> selector) where TTime : Time<TTime>
        {
            return stream.UnaryExpression(x => key(x).GetHashCode(), x => x.GroupBy(key, selector).SelectMany(y => y), "GroupBy");
        }

        /// <summary>
        /// Groups records from each input by a key and applies a reducer to each pair of groups.
        /// </summary>
        /// <typeparam name="TInput1">First input type</typeparam>
        /// <typeparam name="TInput2">Second input type</typeparam>
        /// <typeparam name="TKey">Key type</typeparam>
        /// <typeparam name="TOutput">Output type</typeparam>
        /// <typeparam name="TTime">Time type</typeparam>
        /// <param name="stream1">first input stream</param>
        /// <param name="stream2">second input stream</param>
        /// <param name="key1">first key selector</param>
        /// <param name="key2">second key selector</param>
        /// <param name="reducer">result selector</param>
        /// <returns>all results from applying reducer to each pair of groups </returns>
        public static Stream<TOutput, TTime> CoGroupBy<TInput1, TInput2, TKey, TOutput, TTime>(this Stream<TInput1, TTime> stream1, Stream<TInput2, TTime> stream2, Func<TInput1, TKey> key1, Func<TInput2, TKey> key2, Expression<Func<IEnumerable<TInput1>, IEnumerable<TInput2>, IEnumerable<TOutput>>> reducer) where TTime : Time<TTime>
        {
            return stream1.BinaryExpression<TInput1, TInput2, TOutput, TTime>(stream2, x => key1(x).GetHashCode(), x => key2(x).GetHashCode(), reducer, "CoGroupBy");
        }

        /// <summary>
        /// Joins two input streams.
        /// </summary>
        /// <typeparam name="TInput1">First input type</typeparam>
        /// <typeparam name="TInput2">Second input type</typeparam>
        /// <typeparam name="TKey">Key type</typeparam>
        /// <typeparam name="TResult">Result type</typeparam>
        /// <typeparam name="TTime">Time type</typeparam>
        /// <param name="stream1">first input stream</param>
        /// <param name="stream2">second input stream</param>
        /// <param name="key1">first key selector</param>
        /// <param name="key2">second key selector</param>
        /// <param name="reducer">result selector</param>
        /// <returns>each pair of matching records, subjected to the reducer function</returns>
        public static Stream<TResult, TTime> Join<TInput1, TInput2, TKey, TResult, TTime>(this Stream<TInput1, TTime> stream1, Stream<TInput2, TTime> stream2, Func<TInput1, TKey> key1, Func<TInput2, TKey> key2, Func<TInput1, TInput2, TResult> reducer)  where TTime : Time<TTime>
        {
            //return stream1.BinaryExpression(stream2, x => key1(x).GetHashCode(), x => key2(x).GetHashCode(), (x1, x2) => x1.Join(x2, key1, key2, reducer), "Join");
            return Foundry.NewBinaryStage(stream1, stream2, (i, s) => new JoinVertex<TInput1, TInput2, TKey, TResult, TTime>(i, s, key1, key2, reducer), x => key1(x).GetHashCode(), x => key2(x).GetHashCode(), null, "Join");
        }

        /// <summary>
        /// Returns distinct elements in the input stream.
        /// </summary>
        /// <typeparam name="TRecord">Record type</typeparam>
        /// <typeparam name="TTime">Time type</typeparam>
        /// <param name="stream">input stream</param>
        /// <returns>distinct elements of the input stream</returns>
        public static Stream<TRecord, TTime> Distinct<TRecord, TTime>(this Stream<TRecord, TTime> stream) where TTime : Time<TTime>
        {
            return stream.NewUnaryStage((i, v) => new DistinctVertex<TRecord, TTime>(i, v), x => x.GetHashCode(), x => x.GetHashCode(), "Distinct");
        }

        /// <summary>
        /// Returns elements in either input stream.
        /// </summary>
        /// <typeparam name="TRecord">Record type</typeparam>
        /// <typeparam name="TTime">Time type</typeparam>
        /// <param name="stream1">first input stream</param>
        /// <param name="stream2">second input stream</param>
        /// <returns></returns>
        public static Stream<TRecord, TTime> Union<TRecord, TTime>(this Stream<TRecord, TTime> stream1, Stream<TRecord, TTime> stream2) where TTime : Time<TTime>
        {
            return stream1.Concat(stream2).Distinct();
        }

        /// <summary>
        /// Returns elements in both input streams
        /// </summary>
        /// <typeparam name="TRecord">Record type</typeparam>
        /// <typeparam name="TTime">Time type</typeparam>
        /// <param name="stream1">first input stream</param>
        /// <param name="stream2">second input stream</param>
        /// <returns>stream of records in both input streams</returns>
        public static Stream<TRecord, TTime> Intersect<TRecord, TTime>(this Stream<TRecord, TTime> stream1, Stream<TRecord, TTime> stream2) where TTime : Time<TTime>
        {
            return Foundry.NewBinaryStage(stream1, stream2, (i, s) => new IntersectVertex<TRecord, TTime>(i, s), x => x.GetHashCode(), x => x.GetHashCode(), x => x.GetHashCode(), "Intersect");
        }

        /// <summary>
        /// Returns elements in the first stream but not the second stream.
        /// </summary>
        /// <typeparam name="TRecord">Record type</typeparam>
        /// <typeparam name="TTime">Time type</typeparam>
        /// <param name="stream1">first input stream</param>
        /// <param name="stream2">second input stream</param>
        /// <returns></returns>
        public static Stream<TRecord, TTime> Except<TRecord, TTime>(this Stream<TRecord, TTime> stream1, Stream<TRecord, TTime> stream2) where TTime : Time<TTime>
        {
            return Foundry.NewBinaryStage(stream1, stream2, (i, s) => new ExceptVertex<TRecord, TTime>(i, s), x => x.GetHashCode(), x => x.GetHashCode(), x => x.GetHashCode(), "Except");
        }

        /// <summary>
        /// Counts each of the input records.
        /// </summary>
        /// <typeparam name="TRecord">Record type</typeparam>
        /// <typeparam name="TTime">Time type</typeparam>
        /// <param name="stream">input stream</param>
        /// <returns>stream of pairs (record, count) for each input record</returns>
        public static Stream<Pair<TRecord, Int64>, TTime> Count<TRecord, TTime>(this Stream<TRecord, TTime> stream) where TTime : Time<TTime>
        {
            return stream.Reduce<CountReducer<TRecord>, Int64, TRecord, Int64, TRecord, TRecord, TTime>(x => x, x => x, () => new CountReducer<TRecord>(), "Count");
        }

        /// <summary>
        /// Computes the minimum value by key
        /// </summary>
        /// <typeparam name="TRecord">Record type</typeparam>
        /// <typeparam name="TKey">Key type</typeparam>
        /// <typeparam name="TValue">Value type</typeparam>
        /// <typeparam name="TTime">Time type</typeparam>
        /// <param name="stream">input stream</param>
        /// <param name="keySelector">key selector</param>
        /// <param name="valueSelector">value selector</param>
        /// <returns>unique record for each key, with the least value</returns>
        public static Stream<Pair<TKey, TValue>, TTime> Min<TRecord, TKey, TValue, TTime>(this Stream<TRecord, TTime> stream, Func<TRecord, TKey> keySelector, Func<TRecord, TValue> valueSelector)
            where TTime : Time<TTime>
            where TValue : IComparable<TValue>
        {
            return stream.Reduce<MinReducer<TValue>, TValue, TValue, TValue, TKey, TRecord, TTime>(keySelector, valueSelector, () => new MinReducer<TValue>(), "Min");
        }

        /// <summary>
        /// Computes the maximum value by key
        /// </summary>
        /// <typeparam name="TRecord">Record type</typeparam>
        /// <typeparam name="TKey">Key type</typeparam>
        /// <typeparam name="TValue">Value type</typeparam>
        /// <typeparam name="TTime">Time type</typeparam>
        /// <param name="stream">input stream</param>
        /// <param name="keySelector">key selector</param>
        /// <param name="valueSelector">value selector</param>
        /// <returns>unique record for each key, with the largest value</returns>
        public static Stream<Pair<TKey, TValue>, TTime> Max<TRecord, TKey, TValue, TTime>(this Stream<TRecord, TTime> stream, Func<TRecord, TKey> keySelector, Func<TRecord, TValue> valueSelector)
            where TTime : Time<TTime>
            where TValue : IComparable<TValue>
        {
            return stream.Reduce<MaxReducer<TValue>, TValue, TValue, TValue, TKey, TRecord, TTime>(keySelector, valueSelector, () => new MaxReducer<TValue>(), "Max");
        }

    }

    #region Custom implementations of standard vertices

    internal class DistinctVertex<S, T> : UnaryVertex<S, S, T>
        where T : Time<T>
    {
        private readonly Dictionary<T, HashSet<S>> values = new Dictionary<T, HashSet<S>>();

        internal DistinctVertex(int i, Stage<T> stage) : base(i, stage) { }

        public override void OnReceive(Message<S, T> message)
        {
            if (!this.values.ContainsKey(message.time))
            {
                this.values.Add(message.time, new HashSet<S>());
                this.NotifyAt(message.time);
            }

            var currentSet = this.values[message.time];
            var output = this.Output.GetBufferForTime(message.time);
            for (int i = 0; i < message.length; i++)
                if (currentSet.Add(message.payload[i]))
                    output.Send(message.payload[i]);
        }

        public override void OnNotify(T time)
        {
            this.values.Remove(time);
        }
    }

    internal class SelectVertex<S, R, T> : UnaryVertex<S, R, T>
        where T : Time<T>
    {
        Func<S, R> Function;

        public override void OnReceive(Dataflow.Message<S, T> message)
        {
            var output = this.Output.GetBufferForTime(message.time);
            for (int i = 0; i < message.length; i++)
                output.Send(this.Function(message.payload[i]));
        }

        public SelectVertex(int index, Stage<T> stage, Func<S, R> function)
            : base(index, stage)
        {
            this.Function = function;
        }
    }

    internal class WhereVertex<S, T> : UnaryVertex<S, S, T>
        where T : Time<T>
    {
        Func<S, bool> Function;

        public override void OnReceive(Message<S, T> message)
        {
            var output = this.Output.GetBufferForTime(message.time);
            for (int i = 0; i < message.length; i++)
                if (Function(message.payload[i]))
                    output.Send(message.payload[i]);
        }

        public WhereVertex(int index, Stage<T> stage, Func<S, bool> function)
            : base(index, stage)
        {
            this.Function = function;
        }
    }

    internal class ConcatVertex<S, T> : Microsoft.Research.Naiad.Frameworks.BinaryVertex<S, S, S, T>
        where T : Time<T>
    {
        public override void OnReceive1(Message<S, T> message) { this.Output.Send(message); }
        public override void OnReceive2(Message<S, T> message) { this.Output.Send(message); }

        public ConcatVertex(int index, Microsoft.Research.Naiad.Dataflow.Stage<T> stage) : base(index, stage) { }
    }

    internal class SelectManyVertex<S, R, T> : UnaryVertex<S, R, T>
        where T : Time<T>
    {
        Func<S, IEnumerable<R>> Function;

        public override void OnReceive(Message<S, T> message)
        {
            var output = this.Output.GetBufferForTime(message.time);
            for (int i = 0; i < message.length; i++)
                foreach (var result in this.Function(message.payload[i]))
                    output.Send(result);;            
        }

        public SelectManyVertex(int index, Stage<T> stage, Func<S, IEnumerable<R>> function)
            : base(index, stage)
        {
            this.Function = function;
        }
    }

    internal class JoinVertex<S1, S2, K, R, T> : BinaryVertex<S1, S2, R, T> where T : Time<T>
    {
        private readonly Dictionary<T, Dictionary<K, Pair<List<S1>, List<S2>>>> values = new Dictionary<T, Dictionary<K, Pair<List<S1>, List<S2>>>>();
        
        private readonly Func<S1, K> keySelector1;
        private readonly Func<S2, K> keySelector2;
        private readonly Func<S1, S2, R> resultSelector;

        public override void OnReceive1(Message<S1, T> message)
        {
            if (!this.values.ContainsKey(message.time))
            {
                this.values.Add(message.time, new Dictionary<K, Pair<List<S1>, List<S2>>>());
                this.NotifyAt(message.time);
            }

            var currentValues = this.values[message.time];

            var output = this.Output.GetBufferForTime(message.time);

            for (int i = 0; i < message.length; i++)
            {
                var key = keySelector1(message.payload[i]);

                Pair<List<S1>, List<S2>> currentEntry;
                if (!currentValues.TryGetValue(key, out currentEntry))
                {
                    currentEntry = new Pair<List<S1>, List<S2>>(new List<S1>(), new List<S2>());
                    currentValues[key] = currentEntry;
                }

                currentEntry.v1.Add(message.payload[i]);
                foreach (var match in currentEntry.v2)
                    output.Send(resultSelector(message.payload[i], match));
            }
        }

        public override void OnReceive2(Message<S2, T> message)
        {
            if (!this.values.ContainsKey(message.time))
            {
                this.values.Add(message.time, new Dictionary<K, Pair<List<S1>, List<S2>>>());
                this.NotifyAt(message.time);
            }

            var currentValues = this.values[message.time];
            var output = this.Output.GetBufferForTime(message.time);

            for (int i = 0; i < message.length; i++)
            {
                var key = keySelector2(message.payload[i]);

                Pair<List<S1>, List<S2>> currentEntry;
                if (!currentValues.TryGetValue(key, out currentEntry))
                {
                    currentEntry = new Pair<List<S1>, List<S2>>(new List<S1>(), new List<S2>());
                    currentValues[key] = currentEntry;
                }

                currentEntry.v2.Add(message.payload[i]);
                foreach (var match in currentEntry.v1)
                    output.Send(resultSelector(match, message.payload[i]));
            }
        }

        public override void OnNotify(T time)
        {
            this.values.Remove(time);
        }

        public JoinVertex(int index, Stage<T> stage, Func<S1, K> key1, Func<S2, K> key2, Func<S1, S2, R> result)
            : base(index, stage)
        {
            this.values = new Dictionary<T, Dictionary<K, Pair<List<S1>, List<S2>>>>();
            this.keySelector1 = key1;
            this.keySelector2 = key2;
            this.resultSelector = result;
        }
    }

    internal class IntersectVertex<S, T> : BinaryVertex<S, S, S, T> where T : Time<T>
    {
        private readonly Dictionary<T, HashSet<S>> values1 = new Dictionary<T, HashSet<S>>();
        private readonly Dictionary<T, HashSet<S>> values2 = new Dictionary<T, HashSet<S>>();

        internal IntersectVertex(int i, Stage<T> stage) : base(i, stage) { }

        public override void OnReceive1(Message<S, T> message)
        {
            if (!this.values1.ContainsKey(message.time))
            {
                this.values1.Add(message.time, new HashSet<S>());
                this.NotifyAt(message.time);
            }

            var currentSet = this.values1[message.time];
            var output = this.Output.GetBufferForTime(message.time);

            for (int i = 0; i < message.length; i++)
                if (currentSet.Add(message.payload[i]))
                    if (this.values2.ContainsKey(message.time) && this.values2[message.time].Contains(message.payload[i]))
                        output.Send(message.payload[i]);
        }

        public override void OnReceive2(Message<S, T> message)
        {
            if (!this.values2.ContainsKey(message.time))
            {
                this.values2.Add(message.time, new HashSet<S>());
                this.NotifyAt(message.time);
            }

            var currentSet = this.values2[message.time];
            var output = this.Output.GetBufferForTime(message.time);

            for (int i = 0; i < message.length; i++)
                if (currentSet.Add(message.payload[i]))
                    if (this.values1.ContainsKey(message.time) && this.values1[message.time].Contains(message.payload[i]))
                        output.Send(message.payload[i]);
        }

        public override void OnNotify(T time)
        {
            this.values1.Remove(time);
            this.values2.Remove(time);
        }
    }

    internal class ExceptVertex<S, T> : BinaryVertex<S, S, S, T> where T : Time<T>
    {
        private readonly Dictionary<T, Dictionary<S, int>> values = new Dictionary<T, Dictionary<S, int>>();

        internal ExceptVertex(int index, Stage<T> stage) : base(index, stage) { }

        public override void OnReceive1(Message<S, T> message)
        {
            if (!this.values.ContainsKey(message.time))
            {
                this.values.Add(message.time, new Dictionary<S, int>());
                this.NotifyAt(message.time);
            }

            var currentValue = this.values[message.time];

            for (int i = 0; i < message.length; i++)
            {
                var currentCount = 0;
                if (!currentValue.TryGetValue(message.payload[i], out currentCount))
                    currentCount = 0;

                if (currentCount >= 0)
                    currentValue[message.payload[i]] = currentCount + 1;
                // else
                //    ;   // set negative by the other input; shame!
            }
        }

        public override void OnReceive2(Message<S, T> message)
        {
            if (!this.values.ContainsKey(message.time))
            {
                this.values.Add(message.time, new Dictionary<S, int>());
                this.NotifyAt(message.time);
            }

            var currentValue = this.values[message.time];

                    // set the value negative to exclude it.
            for (int i = 0; i < message.length; i++)
                currentValue[message.payload[i]] = -1;  
        }

        public override void OnNotify(T time)
        {
            var output = this.Output.GetBufferForTime(time);

            foreach (var pair in this.values[time])
                for (int i = 0; i < pair.Value; i++)
                    output.Send(pair.Key);

            this.values.Remove(time);
        }
    }

    #endregion

    /// <summary>
    /// Less standard LINQ-ish methods exposing loops and parallelism.
    /// </summary>
    public static class NonStandardExtensionMethods
    {
        /// <summary>
        /// Select with access to vertex index information
        /// </summary>
        /// <typeparam name="TInput">Input type</typeparam>
        /// <typeparam name="TOutput">Output type</typeparam>
        /// <typeparam name="TTime">Time type</typeparam>
        /// <param name="stream">input stream</param>
        /// <param name="function">transformation</param>
        /// <returns>record by record transformation of the input stream</returns>
        public static Stream<TOutput, TTime> SelectByVertex<TInput, TOutput, TTime>(this Stream<TInput, TTime> stream, Func<int, TInput, TOutput> function)
            where TTime : Time<TTime>
        {
            return Foundry.NewUnaryStage(stream, (i, v) => new VertexSelect<TInput, TOutput, TTime>(i, v, function), null, null, "SelectByVertex");
        }

        /// <summary>
        /// SelectMany instance producing an ArraySegment instead of an IEnumerable, avoiding allocations.
        /// </summary>
        /// <typeparam name="TInput">Input type</typeparam>
        /// <typeparam name="TOutput">Output type</typeparam>
        /// <typeparam name="TTime">Time type</typeparam>
        /// <param name="stream">input stream</param>
        /// <param name="function">result selector</param>
        /// <returns>accumulation of all produced results</returns>
        public static Stream<TOutput, TTime> SelectManyArraySegment<TInput, TOutput, TTime>(this Stream<TInput, TTime> stream, Func<TInput, IEnumerable<ArraySegment<TOutput>>> function)
            where TTime : Time<TTime>
        {
            return Foundry.NewUnaryStage(stream, (i, v) => new SelectManyArraySegment<TInput, TOutput, TTime>(i, v, function), null, null, "SelectManyArraySegment");
        }

        /// <summary>
        /// Buffers input until all workers have all input, and then releases the buffer.
        /// </summary>
        /// <typeparam name="TRecord">Record type</typeparam>
        /// <typeparam name="TTime">Time type</typeparam>
        /// <param name="values">input stream</param>
        /// <returns>input stream</returns>
        public static Stream<TRecord, TTime> Synchronize<TRecord, TTime>(this Stream<TRecord, TTime> values)
            where TTime : Time<TTime>
        {
            return values.UnaryExpression(values.PartitionedBy, x => x, "Delay");
        }

        /// <summary>
        /// Iteratively applies a function to an input stream.
        /// </summary>
        /// <typeparam name="TRecord">Record type</typeparam>
        /// <typeparam name="TTime">Time type</typeparam>
        /// <param name="input">input stream</param>
        /// <param name="function">function to apply</param>
        /// <param name="partitionedBy">inductive partitioning requirement</param>
        /// <param name="iterations">number of iterations to perform</param>
        /// <param name="name">descriptive name</param>
        /// <returns>stream reflecting multiple applications of function to the input</returns>
        public static Stream<TRecord, TTime> Iterate<TRecord, TTime>(this Stream<TRecord, TTime> input, Func<Microsoft.Research.Naiad.Dataflow.Iteration.LoopContext<TTime>, Stream<TRecord, IterationIn<TTime>>, Stream<TRecord, IterationIn<TTime>>> function, Expression<Func<TRecord, int>> partitionedBy, int iterations, string name)
            where TTime : Time<TTime>
        {
            return input.Iterate(function, x => 0, partitionedBy, iterations, name);
        }

        /// <summary>
        /// Iteratively applies a function to an input stream.
        /// </summary>
        /// <typeparam name="TRecord">Record type</typeparam>
        /// <typeparam name="TTime">Time type</typeparam>
        /// <param name="input">input stream</param>
        /// <param name="function">function to apply</param>
        /// <param name="initialIteration">initial iteration selector</param>
        /// <param name="partitionedBy">inductive partitioning requirement</param>
        /// <param name="iterations">number of iterations to perform</param>
        /// <param name="name">descriptive name</param>
        /// <returns>stream reflecting multiple applications of function to the input</returns>
        public static Stream<TRecord, TTime> Iterate<TRecord, TTime>(this Stream<TRecord, TTime> input, Func<Microsoft.Research.Naiad.Dataflow.Iteration.LoopContext<TTime>, Stream<TRecord, IterationIn<TTime>>, Stream<TRecord, IterationIn<TTime>>> function, Expression<Func<TRecord, int>> initialIteration, Expression<Func<TRecord, int>> partitionedBy, int iterations, string name)
            where TTime : Time<TTime>
        {
            var helper = new Microsoft.Research.Naiad.Dataflow.Iteration.LoopContext<TTime>(input.Context, name);

            var delayed = helper.Delay<TRecord>(partitionedBy, iterations);

            var ingress = Microsoft.Research.Naiad.Dataflow.PartitionBy.ExtensionMethods.PartitionBy(helper.EnterLoop(input, initialIteration.Compile()), partitionedBy);

            var loopHead = Microsoft.Research.Naiad.Frameworks.Lindi.ExtensionMethods.Concat(ingress, delayed.Output);

            var loopTail = function(helper, loopHead);

            delayed.Input = loopTail;

            return helper.ExitLoop(loopTail, iterations);
        }

        /// <summary>
        /// Iteratively applies a function to an input stream and accumulates all iterates.
        /// </summary>
        /// <typeparam name="TRecord">Record type</typeparam>
        /// <typeparam name="TTime">Time type</typeparam>
        /// <param name="input">input stream</param>
        /// <param name="function">function to apply</param>
        /// <param name="partitionedBy">inductive partitioning requirement</param>
        /// <param name="iterations">number of iterations to perform</param>
        /// <param name="name">descriptive name</param>
        /// <returns>stream reflecting multiple applications of function to the input</returns>
        public static Stream<TRecord, TTime> IterateAndAccumulate<TRecord, TTime>(this Stream<TRecord, TTime> input, Func<Microsoft.Research.Naiad.Dataflow.Iteration.LoopContext<TTime>, Stream<TRecord, IterationIn<TTime>>, Stream<TRecord, IterationIn<TTime>>> function, Expression<Func<TRecord, int>> partitionedBy, int iterations, string name)
            where TTime : Time<TTime>
        {
            return input.IterateAndAccumulate(function, x => 0, partitionedBy, iterations, name);
        }

        /// <summary>
        /// Iteratively applies a function to an input stream and accumulates all iterates
        /// </summary>
        /// <typeparam name="TRecord">Record type</typeparam>
        /// <typeparam name="TTime">Time type</typeparam>
        /// <param name="input">input stream</param>
        /// <param name="function">function to apply</param>
        /// <param name="initialIteration">initial iteration selector</param>
        /// <param name="partitionedBy">inductive partitioning requirement</param>
        /// <param name="iterations">number of iterations to perform</param>
        /// <param name="name">descriptive name</param>
        /// <returns>stream reflecting multiple applications of function to the input</returns>
        public static Stream<TRecord, TTime> IterateAndAccumulate<TRecord, TTime>(this Stream<TRecord, TTime> input, Func<Microsoft.Research.Naiad.Dataflow.Iteration.LoopContext<TTime>, Stream<TRecord, IterationIn<TTime>>, Stream<TRecord, IterationIn<TTime>>> function, Expression<Func<TRecord, int>> initialIteration, Expression<Func<TRecord, int>> partitionedBy, int iterations, string name)
            where TTime : Time<TTime>
        {
            var helper = new Microsoft.Research.Naiad.Dataflow.Iteration.LoopContext<TTime>(input.Context, name);

            var delayed = helper.Delay<TRecord>(partitionedBy, iterations);

            var ingress = Microsoft.Research.Naiad.Dataflow.PartitionBy.ExtensionMethods.PartitionBy(helper.EnterLoop(input, initialIteration.Compile()), partitionedBy);

            var loopHead = Microsoft.Research.Naiad.Frameworks.Lindi.ExtensionMethods.Concat(ingress, delayed.Output);

            var loopTail = function(helper, loopHead);

            delayed.Input = loopTail;

            return helper.ExitLoop(loopTail);
        }

        /// <summary>
        /// Used to write records to files.
        /// </summary>
        /// <typeparam name="S">Record type</typeparam>
        /// <param name="input">Source of records</param>
        /// <param name="format">Format string for filename; {0} replaced with vertex id</param>
        /// <param name="action">Operation to apply to each record and the output stream. Often (r,s) => s.Write(r);</param>
        public static void WriteToFiles<S>(this Stream<S, Epoch> input, string format, Action<S, System.IO.BinaryWriter> action)
        {
            Foundry.NewSinkStage(input, (i, v) => new Writer<S>(i, v, action, format), null, "Writer");
        }
    }
   
    #region Custom implementations of non-standard vertex implementations

    internal class VertexSelect<S, R, T> : UnaryVertex<S, R, T>
        where T : Time<T>
    {
        Func<int, S, R> Function;

        public override void OnReceive(Message<S, T> message)
        {
            var output = this.Output.GetBufferForTime(message.time);
            for (int i = 0; i < message.length; i++)
                output.Send(this.Function(this.VertexId, message.payload[i]));
        }

        public VertexSelect(int index, Stage<T> stage, Func<int, S, R> function)
            : base(index, stage)
        {
            this.Function = function;
        }
    }

    internal class SelectManyArraySegment<S, R, T> : UnaryVertex<S, R, T>
        where T : Time<T>
    {
        Func<S, IEnumerable<ArraySegment<R>>> Function;

        public override void OnReceive(Message<S, T> message)
        {
            var output = this.Output.GetBufferForTime(message.time);
            for (int ii = 0; ii < message.length; ii++)
            {
                var record = message.payload[ii];
                foreach (var result in Function(record))
                    for (int i = result.Offset; i < result.Offset + result.Count; i++)
                        output.Send(result.Array[i]);
            }
        }

        public SelectManyArraySegment(int index, Stage<T> stage, Func<S, IEnumerable<ArraySegment<R>>> function)
            : base(index, stage)
        {
            this.Function = function;
        }
    }

    internal class Writer<S> : Microsoft.Research.Naiad.Frameworks.SinkVertex<S, Epoch>
    {
        private readonly Dictionary<Epoch, System.IO.BinaryWriter> writers = new Dictionary<Epoch,System.IO.BinaryWriter>();
        private readonly Action<S, System.IO.BinaryWriter> Action;
        private readonly string format;

        public override void OnReceive(Message<S, Epoch> message)
        {
            if (!this.writers.ContainsKey(message.time))
            {
                var filename = String.Format(this.format, this.VertexId, message.time.t);
                if (System.IO.File.Exists(filename))
                    System.IO.File.Delete(filename);

                this.writers[message.time] = new System.IO.BinaryWriter(System.IO.File.OpenWrite(filename));
                this.NotifyAt(message.time);
            }

            var writer = this.writers[message.time];

            for (int i = 0; i < message.length; i++)
                Action(message.payload[i], writer);
        }

        public override void OnNotify(Epoch time)
        {
            this.writers[time].Dispose();
            this.writers.Remove(time);
        }

        public Writer(int index, Microsoft.Research.Naiad.Dataflow.Stage<Epoch> stage, Action<S, System.IO.BinaryWriter> action, string format)
            : base(index, stage)
        {
            this.format = format;

            this.Action = action;
        }
    }

    #endregion 
}
