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
using Microsoft.Research.Naiad.Dataflow.Iteration;
using Microsoft.Research.Naiad;
using Microsoft.Research.Naiad.Frameworks.Reduction;
using Microsoft.Research.Naiad.Dataflow.StandardVertices;


namespace Microsoft.Research.Naiad.Frameworks.Lindi
{
    /// <summary>
    /// The Lindi framework contains extension methods that add LINQ-style operators for streams.
    /// </summary>
    /// <remarks>
    /// The Lindi operators process the data in each logical <see cref="Time{TTime}"/> independently. To perform
    /// computations that are incremental across epochs and iterations, use the <see cref="N:Microsoft.Research.Naiad.Frameworks.DifferentialDataflow"/>
    /// framework instead.
    /// </remarks>
    class NamespaceDoc
    {

    }

    /// <summary>
    /// The Lindi operators are implemented as extension methods on Naiad <see cref="Stream{TRecord,TTime}"/> objects.
    /// </summary>
    public static class ExtensionMethods
    {
        /// <summary>
        /// Transforms each record in the input <paramref name="stream"/> using the given <paramref name="selector"/> function.
        /// </summary>
        /// <typeparam name="TInput">The type of the input records.</typeparam>
        /// <typeparam name="TTime">The type of timestamp on each record.</typeparam>
        /// <typeparam name="TOutput">The type of the transformed records.</typeparam>
        /// <param name="stream">The input stream.</param>
        /// <param name="selector">A transform function to apply to each record.</param>
        /// <returns>The stream of transformed records.</returns>
        public static Stream<TOutput, TTime> Select<TInput, TOutput, TTime>(this Stream<TInput, TTime> stream, Func<TInput, TOutput> selector) where TTime : Time<TTime>
        {
            return Foundry.NewUnaryStage(stream, (i, v) => new SelectVertex<TInput, TOutput, TTime>(i, v, selector), null, null, "Select");
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

        /// <summary>
        /// Filters the input <paramref name="stream"/> to contain only record that match the given <paramref name="predicate"/>.
        /// </summary>
        /// <typeparam name="TRecord">The type of the input records.</typeparam>
        /// <typeparam name="TTime">The type of timestamp on each record.</typeparam>
        /// <param name="stream">The input stream.</param>
        /// <param name="predicate">A function that returns <c>true</c> if and only if the record will be kept in the output.</param>
        /// <returns>The stream of records that satisfy the predicate.</returns>
        public static Stream<TRecord, TTime> Where<TRecord, TTime>(this Stream<TRecord, TTime> stream, Func<TRecord, bool> predicate) where TTime : Time<TTime>
        {
            return Foundry.NewUnaryStage(stream, (i, v) => new WhereVertex<TRecord, TTime>(i, v, predicate), stream.PartitionedBy, stream.PartitionedBy, "Where");
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

        /// <summary>
        /// Transforms each record in the collection using the given <paramref name="selector"/> function and flattens the result. 
        /// </summary>
        /// <typeparam name="TInput">The type of the input records.</typeparam>
        /// <typeparam name="TOutput">The type of elements of the sequence returned by <paramref name="selector"/>.</typeparam>
        /// <typeparam name="TTime">The type of timestamp on each record.</typeparam>
        /// <param name="stream">The input stream.</param>
        /// <param name="selector">A transform function to apply to each record.</param>
        /// <returns>The flattened stream of transformed records.</returns>
        public static Stream<TOutput, TTime> SelectMany<TInput, TOutput, TTime>(this Stream<TInput, TTime> stream, Func<TInput, IEnumerable<TOutput>> selector) where TTime : Time<TTime>
        {
            return Foundry.NewUnaryStage(stream, (i, v) => new SelectManyVertex<TInput, TOutput, TTime>(i, v, selector), null, null, "SelectMany");
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
                        output.Send(result); ;
            }

            public SelectManyVertex(int index, Stage<T> stage, Func<S, IEnumerable<R>> function)
                : base(index, stage)
            {
                this.Function = function;
            }
        }

        /// <summary>
        /// Computes the concatention of <paramref name="stream1"/> and <paramref name="stream2"/>.
        /// </summary>
        /// <typeparam name="TRecord">The type of the input records.</typeparam>
        /// <typeparam name="TTime">The type of timestamp on each record.</typeparam>
        /// <param name="stream1">The first input stream.</param>
        /// <param name="stream2">The second input stream.</param>
        /// <returns>The unordered concatenation of the two input streams.</returns>
        public static Stream<TRecord, TTime> Concat<TRecord, TTime>(this Stream<TRecord, TTime> stream1, Stream<TRecord, TTime> stream2) where TTime : Time<TTime>
        {
            // test to see if they are partitioned properly, and if so maintain the information in output partitionedby information.
            var partitionedBy = Microsoft.Research.Naiad.Utilities.ExpressionComparer.Instance.Equals(stream1.PartitionedBy, stream2.PartitionedBy) ? stream1.PartitionedBy : null;
            return Foundry.NewBinaryStage(stream1, stream2, (i, v) => new ConcatVertex<TRecord, TTime>(i, v), partitionedBy, partitionedBy, partitionedBy, "Concat");
        }

        internal class ConcatVertex<S, T> : BinaryVertex<S, S, S, T>
            where T : Time<T>
        {
            public override void OnReceive1(Message<S, T> message) { this.Output.Send(message); }
            public override void OnReceive2(Message<S, T> message) { this.Output.Send(message); }

            public ConcatVertex(int index, Microsoft.Research.Naiad.Dataflow.Stage<T> stage) : base(index, stage) { }
        }


        /// <summary>
        /// Groups records using the supplied key selector, and applies the given reduction function.
        /// </summary>
        /// <typeparam name="TKey">The key type.</typeparam>
        /// <typeparam name="TInput">The type of the input records.</typeparam>
        /// <typeparam name="TOutput">The result type.</typeparam>
        /// <typeparam name="TTime">The type of timestamp on each record.</typeparam>
        /// <param name="stream">The input stream.</param>
        /// <param name="keySelector">Function that extracts a key from each record.</param>
        /// <param name="reducer">Function that transforms a sequence of input records to a sequence of output records.</param>
        /// <returns>The stream of output records for each group in <paramref name="stream"/>.</returns>
        public static Stream<TOutput, TTime> GroupBy<TInput, TKey, TOutput, TTime>(this Stream<TInput, TTime> stream, Func<TInput, TKey> keySelector, Func<TKey, IEnumerable<TInput>, IEnumerable<TOutput>> reducer) where TTime : Time<TTime>
        {
            return stream.UnaryExpression(x => keySelector(x).GetHashCode(), x => x.GroupBy(keySelector, reducer).SelectMany(y => y), "GroupBy");
        }

        /// <summary>
        /// Groups records from both input streams using the respective key selector, and applies the given reduction function.
        /// </summary>
        /// <typeparam name="TInput1">The type of records in <paramref name="stream1"/>.</typeparam>
        /// <typeparam name="TInput2">The type of records in <paramref name="stream2"/>.</typeparam>
        /// <typeparam name="TKey">The key type.</typeparam>
        /// <typeparam name="TOutput">The result type.</typeparam>
        /// <typeparam name="TTime">The type of timestamp on each record.</typeparam>
        /// <param name="stream1">The first input stream.</param>
        /// <param name="stream2">The second input stream.</param>
        /// <param name="keySelector1">The key selector applied to records in <paramref name="stream1"/>.</param>
        /// <param name="keySelector2">The key selector applied to records in <paramref name="stream2"/>.</param>
        /// <param name="reducer">Function that transforms two sequences of records from each input stream to a sequence of output records.</param>
        /// <returns>The stream of output records for each group in either input stream.</returns>
        public static Stream<TOutput, TTime> CoGroupBy<TInput1, TInput2, TKey, TOutput, TTime>(this Stream<TInput1, TTime> stream1, Stream<TInput2, TTime> stream2, Func<TInput1, TKey> keySelector1, Func<TInput2, TKey> keySelector2, Expression<Func<IEnumerable<TInput1>, IEnumerable<TInput2>, IEnumerable<TOutput>>> reducer) where TTime : Time<TTime>
        {
            var compiledReducer = reducer.Compile();
            return stream1.NewBinaryStage(stream2, (i, s) => new CoGroupByVertex<TKey, TInput1, TInput2, List<TInput1>, List<TInput2>, TOutput, TTime>(i, s, keySelector1, keySelector2, () => new List<TInput1>(), () => new List<TInput2>(), (l, elem) => { l.Add(elem); return l; }, (l, elem) => { l.Add(elem); return l; }, (k, l1, l2) => compiledReducer(l1, l2)), x => keySelector1(x).GetHashCode(), x => keySelector2(x).GetHashCode(), null, "CoGroupBy");
        }

        internal class CoGroupByVertex<K, R1, R2, A1, A2, S, T> : BinaryVertex<R1, R2, S, T>
            where T : Time<T>
        {
            private readonly Dictionary<T, Dictionary<K, A1>> leftGroupsByTime;
            private readonly Dictionary<T, Dictionary<K, A2>> rightGroupsByTime;

            private readonly Func<R1, K> leftKeySelector;
            private readonly Func<R2, K> rightKeySelector;
            private readonly Func<A1> leftAggregateInitializer;
            private readonly Func<A2> rightAggregateInitializer;
            private readonly Func<A1, R1, A1> leftAggregator;
            private readonly Func<A2, R2, A2> rightAggregator;
            private readonly Func<K, A1, A2, IEnumerable<S>> resultSelector;

            public CoGroupByVertex(int index,
                                   Stage<T> stage,
                                   Func<R1, K> leftKeySelector,
                                   Func<R2, K> rightKeySelector,
                                   Func<A1> leftAggregateInitializer,
                                   Func<A2> rightAggregateInitializer,
                                   Func<A1, R1, A1> leftAggregator,
                                   Func<A2, R2, A2> rightAggregator,
                                   Func<K, A1, A2, IEnumerable<S>> resultSelector)
                : base(index, stage)
            {
                this.leftGroupsByTime = new Dictionary<T, Dictionary<K, A1>>();
                this.rightGroupsByTime = new Dictionary<T, Dictionary<K, A2>>();

                this.leftKeySelector = leftKeySelector;
                this.rightKeySelector = rightKeySelector;

                this.leftAggregateInitializer = leftAggregateInitializer;
                this.rightAggregateInitializer = rightAggregateInitializer;

                this.leftAggregator = leftAggregator;
                this.rightAggregator = rightAggregator;

                this.resultSelector = resultSelector;
            }

            public override void OnReceive1(Message<R1, T> message)
            {
                T time = message.time;
                Dictionary<K, A1> byKey;
                if (!this.leftGroupsByTime.TryGetValue(time, out byKey))
                {
                    byKey = new Dictionary<K, A1>();
                    this.leftGroupsByTime.Add(time, byKey);
                }

                for (int i = 0; i < message.length; ++i)
                {
                    K key = this.leftKeySelector(message.payload[i]);

                    A1 currentAggregate;
                    if (!byKey.TryGetValue(key, out currentAggregate))
                    {
                        currentAggregate = this.leftAggregateInitializer();
                    }
                    currentAggregate = this.leftAggregator(currentAggregate, message.payload[i]);
                    byKey[key] = currentAggregate;
                }
                this.NotifyAt(time);
            }

            public override void OnReceive2(Message<R2, T> message)
            {
                T time = message.time;
                Dictionary<K, A2> byKey;
                if (!this.rightGroupsByTime.TryGetValue(time, out byKey))
                {
                    byKey = new Dictionary<K, A2>();
                    this.rightGroupsByTime.Add(time, byKey);
                }

                for (int i = 0; i < message.length; ++i)
                {
                    K key = this.rightKeySelector(message.payload[i]);

                    A2 currentAggregate;
                    if (!byKey.TryGetValue(key, out currentAggregate))
                    {
                        currentAggregate = this.rightAggregateInitializer();
                    }
                    currentAggregate = this.rightAggregator(currentAggregate, message.payload[i]);
                    byKey[key] = currentAggregate;
                }
                this.NotifyAt(time);
            }

            /// <summary>
            /// Sends, at the given time, the results of the result selector for each key
            /// seen on the left and/or right inputs, and the corresponding (possibly empty)
            /// aggregates.
            /// </summary>
            /// <param name="time">The notification time.</param>
            public override void OnNotify(T time)
            {
                VertexOutputBufferPerTime<S, T> timeOutput = this.Output.GetBufferForTime(time);

                Dictionary<K, A1> leftGroups;
                if (!this.leftGroupsByTime.TryGetValue(time, out leftGroups))
                    leftGroups = new Dictionary<K, A1>();
                else
                    this.leftGroupsByTime.Remove(time);

                Dictionary<K, A2> rightGroups;
                if (!this.rightGroupsByTime.TryGetValue(time, out rightGroups))
                    rightGroups = new Dictionary<K, A2>();
                else
                    this.rightGroupsByTime.Remove(time);

                foreach (KeyValuePair<K, A1> pair in leftGroups)
                {
                    K key = pair.Key;
                    A1 leftAggregate = pair.Value;
                    A2 rightAggregate;
                    if (!rightGroups.TryGetValue(key, out rightAggregate))
                    {
                        rightAggregate = this.rightAggregateInitializer();
                    }
                    else
                    {
                        // Delete it so that the only keys remaining in rightGroups
                        // are those unmatched by a key in leftGroups.
                        rightGroups.Remove(key);
                    }

                    foreach (S result in this.resultSelector(key, leftAggregate, rightAggregate))
                    {
                        timeOutput.Send(result);
                    }
                }

                // The remaining keys have no matching values in leftGroups at this time.
                foreach (KeyValuePair<K, A2> pair in rightGroups)
                {
                    K key = pair.Key;
                    A1 leftAggregate = this.leftAggregateInitializer();
                    A2 rightAggregate = pair.Value;

                    foreach (S result in this.resultSelector(key, leftAggregate, rightAggregate))
                    {
                        timeOutput.Send(result);
                    }
                }
            }
        }

        /// <summary>
        /// Joins the records in <paramref name="stream1"/> with the records in <paramref name="stream2"/>, using the respective key selectors.
        /// </summary>
        /// <typeparam name="TInput1">The type of records in <paramref name="stream1"/>.</typeparam>
        /// <typeparam name="TInput2">The type of records in <paramref name="stream2"/>.</typeparam>
        /// <typeparam name="TKey">The key type.</typeparam>
        /// <typeparam name="TOutput">The result type.</typeparam>
        /// <typeparam name="TTime">The type of timestamp on each record.</typeparam>
        /// <param name="stream1">The first input stream.</param>
        /// <param name="stream2">The second input stream.</param>
        /// <param name="keySelector1">The key selector applied to records in <paramref name="stream1"/>.</param>
        /// <param name="keySelector2">The key selector applied to records in <paramref name="stream2"/>.</param>
        /// <param name="resultSelector">Function that transforms records with matching keys to an output record.</param>
        /// <returns>The stream of output records.</returns>
        public static Stream<TOutput, TTime> Join<TInput1, TInput2, TKey, TOutput, TTime>(this Stream<TInput1, TTime> stream1, Stream<TInput2, TTime> stream2, Func<TInput1, TKey> keySelector1, Func<TInput2, TKey> keySelector2, Func<TInput1, TInput2, TOutput> resultSelector) where TTime : Time<TTime>
        {
            //return stream1.BinaryExpression(stream2, x => key1(x).GetHashCode(), x => key2(x).GetHashCode(), (x1, x2) => x1.Join(x2, key1, key2, reducer), "Join");
            return Foundry.NewBinaryStage(stream1, stream2, (i, s) => new JoinVertex<TInput1, TInput2, TKey, TOutput, TTime>(i, s, keySelector1, keySelector2, resultSelector), x => keySelector1(x).GetHashCode(), x => keySelector2(x).GetHashCode(), null, "Join");
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

                    currentEntry.First.Add(message.payload[i]);
                    foreach (var match in currentEntry.Second)
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

                    currentEntry.Second.Add(message.payload[i]);
                    foreach (var match in currentEntry.First)
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

        /// <summary>
        /// Computes the set of distinct records in <paramref name="stream"/>.
        /// </summary>
        /// <typeparam name="TInput">The type of the input records.</typeparam>
        /// <typeparam name="TTime">The type of timestamp on each record.</typeparam>
        /// <param name="stream">The input stream.</param>
        /// <returns>The stream containing at most one instance of each distinct record in the input stream.</returns>
        public static Stream<TInput, TTime> Distinct<TInput, TTime>(this Stream<TInput, TTime> stream) where TTime : Time<TTime>
        {
            return stream.NewUnaryStage((i, v) => new DistinctVertex<TInput, TTime>(i, v), x => x.GetHashCode(), x => x.GetHashCode(), "Distinct");
        }

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


        /// <summary>
        /// Computes the set union of records in <paramref name="stream1"/> and <paramref name="stream2"/>.
        /// </summary>
        /// <typeparam name="TInput">The type of the input records.</typeparam>
        /// <typeparam name="TTime">The type of timestamp on each record.</typeparam>
        /// <param name="stream1">The first input stream.</param>
        /// <param name="stream2">The second input stream.</param>
        /// <returns>The stream containing at most one instance of each record in either input stream.</returns>
        public static Stream<TInput, TTime> Union<TInput, TTime>(this Stream<TInput, TTime> stream1, Stream<TInput, TTime> stream2) where TTime : Time<TTime>
        {
            return stream1.Concat(stream2).Distinct();
        }

        /// <summary>
        /// Computes the set intersection of records in <paramref name="stream1"/> and <paramref name="stream2"/>.
        /// </summary>
        /// <typeparam name="TInput">The type of the input records.</typeparam>
        /// <typeparam name="TTime">The type of timestamp on each record.</typeparam>
        /// <param name="stream1">The first input stream.</param>
        /// <param name="stream2">The second input stream.</param>
        /// <returns>The stream containing at most one instance of each record in both input stream.</returns>
        public static Stream<TInput, TTime> Intersect<TInput, TTime>(this Stream<TInput, TTime> stream1, Stream<TInput, TTime> stream2) where TTime : Time<TTime>
        {
            return Foundry.NewBinaryStage(stream1, stream2, (i, s) => new IntersectVertex<TInput, TTime>(i, s), x => x.GetHashCode(), x => x.GetHashCode(), x => x.GetHashCode(), "Intersect");
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

        /// <summary>
        /// Computes the difference of records in <paramref name="stream1"/> but not in <paramref name="stream2"/>.
        /// </summary>
        /// <typeparam name="TInput">The type of the input records.</typeparam>
        /// <typeparam name="TTime">The type of timestamp on each record.</typeparam>
        /// <param name="stream1">The first input stream.</param>
        /// <param name="stream2">The second input stream.</param>
        /// <returns>The stream containing the records in <paramref name="stream1"/> that do not match any record in <paramref name="stream2"/>.</returns>
        public static Stream<TInput, TTime> Except<TInput, TTime>(this Stream<TInput, TTime> stream1, Stream<TInput, TTime> stream2) where TTime : Time<TTime>
        {
            return Foundry.NewBinaryStage(stream1, stream2, (i, s) => new ExceptVertex<TInput, TTime>(i, s), x => x.GetHashCode(), x => x.GetHashCode(), x => x.GetHashCode(), "Except");
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

        /// <summary>
        /// Counts the number of occurrences of each record in <paramref name="stream"/>.
        /// </summary>
        /// <typeparam name="TInput">The type of the input records.</typeparam>
        /// <typeparam name="TTime">The type of timestamp on each record.</typeparam>
        /// <param name="stream">The input stream.</param> 
        /// <returns>The stream containing pairs of a record and its respective count for each input record.</returns>
        public static Stream<Pair<TInput, Int64>, TTime> Count<TInput, TTime>(this Stream<TInput, TTime> stream) where TTime : Time<TTime>
        {
            return stream.Select(x => x.PairWith(1L))
                         .Aggregate((a, b) => a + b, true);
        }

        /// <summary>
        /// Groups records using the supplied key selector, and computes the minimum value in each group.
        /// </summary>
        /// <typeparam name="TInput">The type of the input records.</typeparam>
        /// <typeparam name="TTime">The type of timestamp on each record.</typeparam>
        /// <typeparam name="TKey">The key type.</typeparam>
        /// <typeparam name="TValue">The type of values to be used for comparison.</typeparam>
        /// <param name="stream">The input stream.</param>
        /// <param name="keySelector">Function that extracts a key from each record.</param>
        /// <param name="valueSelector">Function that extracts from each record the value to be used in the comparison.</param>
        /// <returns>The stream of one key-value pair for each key and the respective minimum value in the input stream.</returns>
        public static Stream<Pair<TKey, TValue>, TTime> Min<TInput, TKey, TValue, TTime>(this Stream<TInput, TTime> stream, Func<TInput, TKey> keySelector, Func<TInput, TValue> valueSelector)
            where TTime : Time<TTime>
            where TValue : IComparable<TValue>
        {
            return stream.Select(x => keySelector(x).PairWith(valueSelector(x)))
                         .Aggregate((a, b) => a.CompareTo(b) < 0 ? a : b, true);
        }

        /// <summary>
        /// Groups records using the supplied key selector, and computes the maximum value in each group.
        /// </summary>
        /// <typeparam name="TInput">The type of the input records.</typeparam>
        /// <typeparam name="TTime">The type of timestamp on each record.</typeparam>
        /// <typeparam name="TKey">The key type.</typeparam>
        /// <typeparam name="TValue">The type of values to be used for comparison.</typeparam>
        /// <param name="stream">The input stream.</param>
        /// <param name="keySelector">Function that extracts a key from each record.</param>
        /// <param name="valueSelector">Function that extracts from each record the value to be used in the comparison.</param>
        /// <returns>The stream of one key-value pair for each key and the respective maximum value in the input stream.</returns>
        public static Stream<Pair<TKey, TValue>, TTime> Max<TInput, TKey, TValue, TTime>(this Stream<TInput, TTime> stream, Func<TInput, TKey> keySelector, Func<TInput, TValue> valueSelector)
            where TTime : Time<TTime>
            where TValue : IComparable<TValue>
        {
            return stream.Select(x => keySelector(x).PairWith(valueSelector(x)))
                         .Aggregate((a, b) => a.CompareTo(b) > 0 ? a : b, true);
        }

        /// <summary>
        /// Groups records using the supplied key selector, and applies the given aggregation function.
        /// </summary>
        /// <typeparam name="TInput">The type of the input records.</typeparam>
        /// <typeparam name="TKey">The key type.</typeparam>
        /// <typeparam name="TState">The intermediate state type.</typeparam>
        /// <typeparam name="TTime">The type of timestamp on each record.</typeparam>
        /// <typeparam name="TOutput">The type of the output records.</typeparam>
        /// <param name="stream">The input stream.</param>
        /// <param name="keySelector">Function that extracts a key from each record.</param>
        /// <param name="stateSelector">Function that extracts from each record the state to be aggregated.</param>
        /// <param name="combiner">A function from current state and incoming update, to the new state.</param>
        /// <param name="resultSelector">Function that transforms a key and final combined state to an output record.</param>
        /// <returns>The stream of output records.</returns>
        public static Stream<TOutput, TTime> Aggregate<TInput, TKey, TState, TOutput, TTime>(this Stream<TInput, TTime> stream, Func<TInput, TKey> keySelector, Func<TInput, TState> stateSelector, Func<TState, TState, TState> combiner, Func<TKey, TState, TOutput> resultSelector)
            where TTime : Time<TTime>
        {
            return stream.Aggregate(keySelector, stateSelector, combiner, resultSelector, false);
        }

        /// <summary>
        /// Groups records using the supplied key selector, and applies the given aggregation function.
        /// </summary>
        /// <typeparam name="TInput">The type of the input records.</typeparam>
        /// <typeparam name="TKey">The key type.</typeparam>
        /// <typeparam name="TState">The intermediate state type.</typeparam>
        /// <typeparam name="TTime">The type of timestamp on each record.</typeparam>
        /// <typeparam name="TOutput">The type of the output records.</typeparam>
        /// <param name="stream">The input stream.</param>
        /// <param name="keySelector">Function that extracts a key from each record.</param>
        /// <param name="stateSelector">Function that extracts from each record the state to be aggregated.</param>
        /// <param name="combiner">A function from current state and incoming update, to the new state.</param>
        /// <param name="resultSelector">Function that transforms a key and final combined state to an output record.</param>
        /// <param name="locallyCombine">If <c>true</c>, perform local aggregation on each worker before global aggregation.</param>
        /// <returns>The stream of output records.</returns>
        public static Stream<TOutput, TTime> Aggregate<TInput, TKey, TState, TOutput, TTime>(this Stream<TInput, TTime> stream, Func<TInput, TKey> keySelector, Func<TInput, TState> stateSelector, Func<TState, TState, TState> combiner, Func<TKey, TState, TOutput> resultSelector, bool locallyCombine)
            where TTime : Time<TTime>
        {
            return stream.Select(x => new Pair<TKey, TState>(keySelector(x), stateSelector(x))).Aggregate(combiner).Select(x => resultSelector(x.First, x.Second));
        }

        /// <summary>
        /// Groups records using the supplied key selector, and applies the given aggregation function.
        /// </summary>
        /// <typeparam name="TKey">The key type.</typeparam>
        /// <typeparam name="TState">The intermediate state type.</typeparam>
        /// <typeparam name="TTime">The type of timestamp on each record.</typeparam>
        /// <param name="stream">The input stream.</param>
        /// <param name="combiner">A function from current state and incoming update, to the new state.</param>
        /// <returns>The stream of one key-value pair for each key and the respective final combined state in the input stream.</returns>
        public static Stream<Pair<TKey, TState>, TTime> Aggregate<TKey, TState, TTime>(this Stream<Pair<TKey, TState>, TTime> stream, Func<TState, TState, TState> combiner)
            where TTime : Time<TTime>
        {
            return stream.NewUnaryStage((i, s) => new AggregateVertex<TKey, TState, TTime>(i, s, combiner), x => x.First.GetHashCode(), x => x.First.GetHashCode(), "Combiner");
        }

        /// <summary>
        /// Groups records using the supplied key selector, and applies the given aggregation function.
        /// </summary>
        /// <typeparam name="TKey">The key type.</typeparam>
        /// <typeparam name="TState">The intermediate state type.</typeparam>
        /// <typeparam name="TTime">The type of timestamp on each record.</typeparam>
        /// <param name="stream">The input stream.</param>
        /// <param name="combiner">A function from current state and incoming update, to the new state.</param>
        /// <param name="locallyCombine">If <c>true</c>, perform local aggregation on each worker before global aggregation.</param>
        /// <returns>The stream of one key-value pair for each key and the respective final combined state in the input stream.</returns>
        public static Stream<Pair<TKey, TState>, TTime> Aggregate<TKey, TState, TTime>(this Stream<Pair<TKey, TState>, TTime> stream, Func<TState, TState, TState> combiner, bool locallyCombine)
            where TTime : Time<TTime>
        {
            if (locallyCombine)
                return stream.NewUnaryStage((i, s) => new AggregateVertex<TKey, TState, TTime>(i, s, combiner), null, null, "Combiner")
                             .NewUnaryStage((i, s) => new AggregateVertex<TKey, TState, TTime>(i, s, combiner), x => x.First.GetHashCode(), x => x.First.GetHashCode(), "Combiner");
            else
                return stream.Aggregate(combiner);
        }

        internal class AggregateVertex<TKey, TState, TTime> : UnaryVertex<Pair<TKey, TState>, Pair<TKey, TState>, TTime>
            where TTime : Time<TTime>
        {
            private readonly Func<TState, TState, TState> combiner;
            private readonly Dictionary<TTime, Dictionary<TKey, TState>> statesByTime;

            public override void OnReceive(Message<Pair<TKey, TState>, TTime> message)
            {
                if (!this.statesByTime.ContainsKey(message.time))
                {
                    this.statesByTime.Add(message.time, new Dictionary<TKey, TState>());
                    this.NotifyAt(message.time);
                }

                var states = this.statesByTime[message.time];

                for (int i = 0; i < message.length; i++)
                {
                    var key = message.payload[i].First;
                    if (!states.ContainsKey(key))
                        states.Add(key, message.payload[i].Second);
                    else
                        states[key] = this.combiner(states[key], message.payload[i].Second);
                }
            }

            public override void OnNotify(TTime time)
            {
                if (this.statesByTime.ContainsKey(time))
                {
                    var output = this.Output.GetBufferForTime(time);
                    foreach (var pair in this.statesByTime[time])
                        output.Send(new Pair<TKey, TState>(pair.Key, pair.Value));

                    this.statesByTime.Remove(time);
                }
            }

            public AggregateVertex(int index, Stage<TTime> stage, Func<TState, TState, TState> combiner)
                : base(index, stage)
            {
                this.combiner = combiner;
                this.statesByTime = new Dictionary<TTime, Dictionary<TKey, TState>>();
            }
        }
    }

    /// <summary>
    /// Less standard LINQ-ish methods exposing loops and parallelism.
    /// </summary>
    public static class NonStandardExtensionMethods
    {
        /// <summary>
        /// Transforms each record in the input <paramref name="stream"/> using the given <paramref name="selector"/> function.
        /// </summary>
        /// <typeparam name="TInput">The type of the input records.</typeparam>
        /// <typeparam name="TTime">The type of timestamp on each record.</typeparam>
        /// <typeparam name="TOutput">The type of the transformed records.</typeparam>
        /// <param name="stream">The input stream.</param>
        /// <param name="selector">Function that transforms a vertex ID and record to an output record.</param>
        /// <returns>The stream of transformed records.</returns>
        /// <remarks>
        /// This overload supports the inclusion of location information in the result, which can be used to optimized
        /// data transfer.
        /// </remarks>
        public static Stream<TOutput, TTime> SelectByVertex<TInput, TOutput, TTime>(this Stream<TInput, TTime> stream, Func<int, TInput, TOutput> selector)
            where TTime : Time<TTime>
        {
            return Foundry.NewUnaryStage(stream, (i, v) => new VertexSelect<TInput, TOutput, TTime>(i, v, selector), null, null, "SelectByVertex");
        }

        /// <summary>
        /// Transforms each record in the collection using the given <paramref name="selector"/> function and flattens the result.
        /// </summary>
        /// <typeparam name="TInput">The type of the input records.</typeparam>
        /// <typeparam name="TTime">The type of timestamp on each record.</typeparam>
        /// <typeparam name="TOutput">The type of elements of the array segments in the sequence returned by <paramref name="selector"/>.</typeparam>
        /// <param name="stream">The input stream.</param>
        /// <param name="selector">A transform function to apply to each record.</param>
        /// <returns>The flattened stream of transformed records.</returns>
        /// <remarks>
        /// This overload supports optimizing the performance of <see cref="Microsoft.Research.Naiad.Frameworks.Lindi.ExtensionMethods.SelectMany{TInput,TOutput,TTime}(Stream{TInput,TTime},Func{TInput,IEnumerable{TOutput}})"/> by using
        /// <see cref="ArraySegment{TOutput}"/> objects to batch the elements returned by <paramref name="selector"/>.
        /// </remarks>
        public static Stream<TOutput, TTime> SelectManyArraySegment<TInput, TOutput, TTime>(this Stream<TInput, TTime> stream, Func<TInput, IEnumerable<ArraySegment<TOutput>>> selector)
            where TTime : Time<TTime>
        {
            return Foundry.NewUnaryStage(stream, (i, v) => new SelectManyArraySegment<TInput, TOutput, TTime>(i, v, selector), null, null, "SelectManyArraySegment");
        }

        /// <summary>
        /// For each timestamp, buffers the given <paramref name="stream"/> until all workers have all input for that time, and then releases the buffer.
        /// </summary>
        /// <typeparam name="TInput">The type of the input records.</typeparam>
        /// <typeparam name="TTime">The type of timestamp on each record.</typeparam>
        /// <param name="stream">The input stream.</param>
        /// <returns>The input stream.</returns>
        public static Stream<TInput, TTime> Synchronize<TInput, TTime>(this Stream<TInput, TTime> stream)
            where TTime : Time<TTime>
        {
            return stream.UnaryExpression(stream.PartitionedBy, x => x, "Delay");
        }

        /// <summary>
        /// Iteratively applies <paramref name="function"/> to the given <paramref name="stream"/>.
        /// </summary>
        /// <typeparam name="TInput">The type of the input records.</typeparam>
        /// <typeparam name="TTime">The type of timestamp on each record.</typeparam>
        /// <param name="stream">The input stream.</param>
        /// <param name="function">The Naiad computation to apply iteratively.</param>
        /// <param name="partitionedBy">Partitioning function for the input stream.</param>
        /// <param name="iterations">The number of iterations to perform.</param>
        /// <param name="name">Descriptive name for the loop.</param>
        /// <returns>The stream corresponding to applying <paramref name="function"/> to the input <paramref name="iterations"/> times.</returns>
        public static Stream<TInput, TTime> Iterate<TInput, TTime>(this Stream<TInput, TTime> stream, Func<Microsoft.Research.Naiad.Dataflow.Iteration.LoopContext<TTime>, Stream<TInput, IterationIn<TTime>>, Stream<TInput, IterationIn<TTime>>> function, Expression<Func<TInput, int>> partitionedBy, int iterations, string name)
            where TTime : Time<TTime>
        {
            return stream.Iterate(function, x => 0, partitionedBy, iterations, name);
        }

        /// <summary>
        /// Iteratively applies <paramref name="function"/> to the given <paramref name="stream"/>.
        /// </summary>
        /// <typeparam name="TInput">The type of the input records.</typeparam>
        /// <typeparam name="TTime">The type of timestamp on each record.</typeparam>
        /// <param name="stream">The input stream.</param>
        /// <param name="function">The Naiad computation to apply iteratively.</param>
        /// <param name="partitionedBy">Partitioning function for the input stream.</param>
        /// <param name="iterationSelector">Function that maps an input record to the iteration at which that record should be introduced.</param>
        /// <param name="iterations">The number of iterations to perform.</param>
        /// <param name="name">Descriptive name for the loop.</param>
        /// <returns>The stream corresponding to applying <paramref name="function"/> to the input <paramref name="iterations"/> times.</returns>
        public static Stream<TInput, TTime> Iterate<TInput, TTime>(this Stream<TInput, TTime> stream, Func<Microsoft.Research.Naiad.Dataflow.Iteration.LoopContext<TTime>, Stream<TInput, IterationIn<TTime>>, Stream<TInput, IterationIn<TTime>>> function, Func<TInput, int> iterationSelector, Expression<Func<TInput, int>> partitionedBy, int iterations, string name)
            where TTime : Time<TTime>
        {
            var helper = new Microsoft.Research.Naiad.Dataflow.Iteration.LoopContext<TTime>(stream.Context, name);

            var delayed = helper.Delay<TInput>(partitionedBy, iterations);

            var ingress = Microsoft.Research.Naiad.Dataflow.PartitionBy.ExtensionMethods.PartitionBy(helper.EnterLoop(stream, iterationSelector), partitionedBy);

            var loopHead = Microsoft.Research.Naiad.Frameworks.Lindi.ExtensionMethods.Concat(ingress, delayed.Output);

            var loopTail = function(helper, loopHead);

            delayed.Input = loopTail;

            return helper.ExitLoop(loopTail, iterations);
        }

        /// <summary>
        /// Iteratively applies <paramref name="function"/> to the given <paramref name="stream"/> and accumulates all of the iterates.
        /// </summary>
        /// <typeparam name="TInput">The type of the input records.</typeparam>
        /// <typeparam name="TTime">The type of timestamp on each record.</typeparam>
        /// <param name="stream">The input stream.</param>
        /// <param name="function">The Naiad computation to apply iteratively.</param>
        /// <param name="partitionedBy">Partitioning function for the input stream.</param>
        /// <param name="iterations">The number of iterations to perform.</param>
        /// <param name="name">Descriptive name for the loop.</param>
        /// <returns>The stream corresponding to applying <paramref name="function"/> to the input <paramref name="iterations"/> times and accumulating the iterates.</returns>
        public static Stream<TInput, TTime> IterateAndAccumulate<TInput, TTime>(this Stream<TInput, TTime> stream, Func<Microsoft.Research.Naiad.Dataflow.Iteration.LoopContext<TTime>, Stream<TInput, IterationIn<TTime>>, Stream<TInput, IterationIn<TTime>>> function, Expression<Func<TInput, int>> partitionedBy, int iterations, string name)
            where TTime : Time<TTime>
        {
            return stream.IterateAndAccumulate(function, x => 0, partitionedBy, iterations, name);
        }

        /// <summary>
        /// Iteratively applies <paramref name="function"/> to the given <paramref name="stream"/> and accumulates all of the iterates.
        /// </summary>
        /// <typeparam name="TInput">The type of the input records.</typeparam>
        /// <typeparam name="TTime">The type of timestamp on each record.</typeparam>
        /// <param name="stream">The input stream.</param>
        /// <param name="function">The Naiad computation to apply iteratively.</param>
        /// <param name="partitionedBy">Partitioning function for the input stream.</param>
        /// <param name="iterationSelector">Function that maps an input record to the iteration at which that record should be introduced.</param>
        /// <param name="iterations">The number of iterations to perform.</param>
        /// <param name="name">Descriptive name for the loop.</param>
        /// <returns>The stream corresponding to applying <paramref name="function"/> to the input <paramref name="iterations"/> times and accumulating the iterates.</returns>
        public static Stream<TInput, TTime> IterateAndAccumulate<TInput, TTime>(this Stream<TInput, TTime> stream, Func<Microsoft.Research.Naiad.Dataflow.Iteration.LoopContext<TTime>, Stream<TInput, IterationIn<TTime>>, Stream<TInput, IterationIn<TTime>>> function, Func<TInput, int> iterationSelector, Expression<Func<TInput, int>> partitionedBy, int iterations, string name)
            where TTime : Time<TTime>
        {
            var helper = new Microsoft.Research.Naiad.Dataflow.Iteration.LoopContext<TTime>(stream.Context, name);

            var delayed = helper.Delay<TInput>(partitionedBy, iterations);

            var ingress = Microsoft.Research.Naiad.Dataflow.PartitionBy.ExtensionMethods.PartitionBy(helper.EnterLoop(stream, iterationSelector), partitionedBy);

            var loopHead = Microsoft.Research.Naiad.Frameworks.Lindi.ExtensionMethods.Concat(ingress, delayed.Output);

            var loopTail = function(helper, loopHead);

            delayed.Input = loopTail;

            return helper.ExitLoop(loopTail);
        }

        /// <summary>
        /// Writes the records in the given <paramref name="stream"/> to files.
        /// </summary>
        /// <typeparam name="TInput">The type of the input records.</typeparam>
        /// <param name="stream">The input stream.</param>
        /// <param name="format">Format string for filename, where {0} is replaced with the vertex ID.</param>
        /// <param name="action">Action to apply on each record and the corresponding <see cref="System.IO.BinaryWriter"/>.</param>
        /// <remarks>
        /// The <paramref name="action"/> parameter is used to write each record to its respective file. Often the action is 
        /// similar to <c>(record, writer) => writer.Write(record)</c>.
        /// </remarks>
        public static void WriteToFiles<TInput>(this Stream<TInput, Epoch> stream, string format, Action<TInput, System.IO.BinaryWriter> action)
        {
            Foundry.NewSinkStage(stream, (i, v) => new Writer<TInput>(i, v, action, format), null, "Writer");
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

    internal class Writer<S> : SinkVertex<S, Epoch>
    {
        private readonly Dictionary<Epoch, System.IO.BinaryWriter> writers = new Dictionary<Epoch,System.IO.BinaryWriter>();
        private readonly Action<S, System.IO.BinaryWriter> Action;
        private readonly string format;

        public override void OnReceive(Message<S, Epoch> message)
        {
            if (!this.writers.ContainsKey(message.time))
            {
                var filename = String.Format(this.format, this.VertexId, message.time.epoch);
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
