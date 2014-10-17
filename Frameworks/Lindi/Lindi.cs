/*
 * Naiad ver. 0.5
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
using Microsoft.Research.Naiad.Dataflow;
using Microsoft.Research.Naiad.Dataflow.PartitionBy;
using Microsoft.Research.Naiad.Dataflow.StandardVertices;
using Microsoft.Research.Naiad.Utilities;
using Microsoft.Research.Naiad.Dataflow.Iteration;
using System.Linq.Expressions;


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
            if (stream == null) throw new ArgumentNullException("stream");
            if (selector == null) throw new ArgumentNullException("selector");
            return stream.NewUnaryStage((i, v) => new SelectVertex<TInput, TOutput, TTime>(i, v, selector), null, null, "Select");
        }

        internal class SelectVertex<TInput, TOutput, TTime> : UnaryVertex<TInput, TOutput, TTime>
            where TTime : Time<TTime>
        {
            private readonly Func<TInput, TOutput> function;

            public override void OnReceive(Message<TInput, TTime> message)
            {
                var output = this.Output.GetBufferForTime(message.time);
                for (int i = 0; i < message.length; i++)
                    output.Send(this.function(message.payload[i]));
            }

            internal SelectVertex(int index, Stage<TTime> stage, Func<TInput, TOutput> function)
                : base(index, stage)
            {
                this.function = function;
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
            if (stream == null) throw new ArgumentNullException("stream");
            if (predicate == null) throw new ArgumentNullException("predicate");
            return stream.NewUnaryStage((i, v) => new WhereVertex<TRecord, TTime>(i, v, predicate), stream.PartitionedBy, stream.PartitionedBy, "Where");
        }

        internal class WhereVertex<TRecord, TTime> : UnaryVertex<TRecord, TRecord, TTime>
            where TTime : Time<TTime>
        {
            private readonly Func<TRecord, bool> function;

            public override void OnReceive(Message<TRecord, TTime> message)
            {
                var output = this.Output.GetBufferForTime(message.time);
                for (int i = 0; i < message.length; i++)
                    if (function(message.payload[i]))
                        output.Send(message.payload[i]);
            }

            public WhereVertex(int index, Stage<TTime> stage, Func<TRecord, bool> function)
                : base(index, stage)
            {
                this.function = function;
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
            if (stream == null) throw new ArgumentNullException("stream");
            if (selector == null) throw new ArgumentNullException("selector");
            return stream.NewUnaryStage((i, v) => new SelectManyVertex<TInput, TOutput, TTime>(i, v, selector), null, null, "SelectMany");
        }

        internal class SelectManyVertex<TInput, TOutput, TTime> : UnaryVertex<TInput, TOutput, TTime>
            where TTime : Time<TTime>
        {
            private readonly Func<TInput, IEnumerable<TOutput>> function;

            public override void OnReceive(Message<TInput, TTime> message)
            {
                var output = this.Output.GetBufferForTime(message.time);
                for (int i = 0; i < message.length; i++)
                    foreach (var result in this.function(message.payload[i]))
                        output.Send(result);
            }

            public SelectManyVertex(int index, Stage<TTime> stage, Func<TInput, IEnumerable<TOutput>> function)
                : base(index, stage)
            {
                this.function = function;
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
            if (stream1 == null) throw new ArgumentNullException("stream1");
            if (stream2 == null) throw new ArgumentNullException("stream2");
            // test to see if they are partitioned properly, and if so maintain the information in output partitionedby information.
            var partitionedBy = ExpressionComparer.Instance.Equals(stream1.PartitionedBy, stream2.PartitionedBy) ? stream1.PartitionedBy : null;
            return stream1.NewBinaryStage(stream2, (i, v) => new ConcatVertex<TRecord, TTime>(i, v), partitionedBy, partitionedBy, partitionedBy, "Concat");
        }

        internal class ConcatVertex<TRecord, TTime> : BinaryVertex<TRecord, TRecord, TRecord, TTime>
            where TTime : Time<TTime>
        {
            public override void OnReceive1(Message<TRecord, TTime> message) { this.Output.Send(message); }
            public override void OnReceive2(Message<TRecord, TTime> message) { this.Output.Send(message); }

            public ConcatVertex(int index, Stage<TTime> stage) : base(index, stage) { }
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
        /// <param name="reducer">Function that transforms a key and sequence of input records to a sequence of output records.</param>
        /// <returns>The stream of output records for each group in <paramref name="stream"/>.</returns>
        public static Stream<TOutput, TTime> GroupBy<TInput, TKey, TOutput, TTime>(this Stream<TInput, TTime> stream, Func<TInput, TKey> keySelector, Func<TKey, IEnumerable<TInput>, IEnumerable<TOutput>> reducer) where TTime : Time<TTime>
        {
            if (stream == null) throw new ArgumentNullException("stream");
            if (keySelector == null) throw new ArgumentNullException("keySelector");
            if (reducer == null) throw new ArgumentNullException("reducer");
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
        /// <param name="reducer">Function that transforms a key and two sequences of records from each input stream to a sequence of output records.</param>
        /// <returns>The stream of output records for each group in either input stream.</returns>
        public static Stream<TOutput, TTime> CoGroupBy<TInput1, TInput2, TKey, TOutput, TTime>(this Stream<TInput1, TTime> stream1, Stream<TInput2, TTime> stream2, Func<TInput1, TKey> keySelector1, Func<TInput2, TKey> keySelector2, Func<TKey, IEnumerable<TInput1>, IEnumerable<TInput2>, IEnumerable<TOutput>> reducer) where TTime : Time<TTime>
        {
            if (stream1 == null) throw new ArgumentNullException("stream1");
            if (stream2 == null) throw new ArgumentNullException("stream2");
            if (keySelector1 == null) throw new ArgumentNullException("keySelector1");
            if (keySelector2 == null) throw new ArgumentNullException("keySelector2");
            if (reducer == null) throw new ArgumentNullException("reducer");
            return stream1.NewBinaryStage(stream2, (i, s) => new CoGroupByVertex<TKey, TInput1, TInput2, List<TInput1>, List<TInput2>, TOutput, TTime>(i, s, keySelector1, keySelector2, () => new List<TInput1>(), () => new List<TInput2>(), (l, elem) => { l.Add(elem); return l; }, (l, elem) => { l.Add(elem); return l; }, reducer), x => keySelector1(x).GetHashCode(), x => keySelector2(x).GetHashCode(), null, "CoGroupBy");
        }

        internal class CoGroupByVertex<TKey, TInput1, TInput2, TState1, TState2, TOutput, TTime> : BinaryVertex<TInput1, TInput2, TOutput, TTime>
            where TTime : Time<TTime>
        {
            private readonly Dictionary<TTime, Dictionary<TKey, TState1>> leftGroupsByTime;
            private readonly Dictionary<TTime, Dictionary<TKey, TState2>> rightGroupsByTime;

            private readonly Func<TInput1, TKey> leftKeySelector;
            private readonly Func<TInput2, TKey> rightKeySelector;
            private readonly Func<TState1> leftAggregateInitializer;
            private readonly Func<TState2> rightAggregateInitializer;
            private readonly Func<TState1, TInput1, TState1> leftAggregator;
            private readonly Func<TState2, TInput2, TState2> rightAggregator;
            private readonly Func<TKey, TState1, TState2, IEnumerable<TOutput>> resultSelector;

            public CoGroupByVertex(int index,
                                   Stage<TTime> stage,
                                   Func<TInput1, TKey> leftKeySelector,
                                   Func<TInput2, TKey> rightKeySelector,
                                   Func<TState1> leftAggregateInitializer,
                                   Func<TState2> rightAggregateInitializer,
                                   Func<TState1, TInput1, TState1> leftAggregator,
                                   Func<TState2, TInput2, TState2> rightAggregator,
                                   Func<TKey, TState1, TState2, IEnumerable<TOutput>> resultSelector)
                : base(index, stage)
            {
                this.leftGroupsByTime = new Dictionary<TTime, Dictionary<TKey, TState1>>();
                this.rightGroupsByTime = new Dictionary<TTime, Dictionary<TKey, TState2>>();

                this.leftKeySelector = leftKeySelector;
                this.rightKeySelector = rightKeySelector;

                this.leftAggregateInitializer = leftAggregateInitializer;
                this.rightAggregateInitializer = rightAggregateInitializer;

                this.leftAggregator = leftAggregator;
                this.rightAggregator = rightAggregator;

                this.resultSelector = resultSelector;
            }

            public override void OnReceive1(Message<TInput1, TTime> message)
            {
                TTime time = message.time;
                Dictionary<TKey, TState1> byKey;
                if (!this.leftGroupsByTime.TryGetValue(time, out byKey))
                {
                    byKey = new Dictionary<TKey, TState1>();
                    this.leftGroupsByTime.Add(time, byKey);
                }

                for (int i = 0; i < message.length; ++i)
                {
                    TKey key = this.leftKeySelector(message.payload[i]);

                    TState1 currentAggregate;
                    if (!byKey.TryGetValue(key, out currentAggregate))
                    {
                        currentAggregate = this.leftAggregateInitializer();
                    }
                    currentAggregate = this.leftAggregator(currentAggregate, message.payload[i]);
                    byKey[key] = currentAggregate;
                }
                this.NotifyAt(time);
            }

            public override void OnReceive2(Message<TInput2, TTime> message)
            {
                TTime time = message.time;
                Dictionary<TKey, TState2> byKey;
                if (!this.rightGroupsByTime.TryGetValue(time, out byKey))
                {
                    byKey = new Dictionary<TKey, TState2>();
                    this.rightGroupsByTime.Add(time, byKey);
                }

                for (int i = 0; i < message.length; ++i)
                {
                    TKey key = this.rightKeySelector(message.payload[i]);

                    TState2 currentAggregate;
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
            public override void OnNotify(TTime time)
            {
                VertexOutputBufferPerTime<TOutput, TTime> timeOutput = this.Output.GetBufferForTime(time);

                Dictionary<TKey, TState1> leftGroups;
                if (!this.leftGroupsByTime.TryGetValue(time, out leftGroups))
                    leftGroups = new Dictionary<TKey, TState1>();
                else
                    this.leftGroupsByTime.Remove(time);

                Dictionary<TKey, TState2> rightGroups;
                if (!this.rightGroupsByTime.TryGetValue(time, out rightGroups))
                    rightGroups = new Dictionary<TKey, TState2>();
                else
                    this.rightGroupsByTime.Remove(time);

                foreach (KeyValuePair<TKey, TState1> pair in leftGroups)
                {
                    TKey key = pair.Key;
                    TState1 leftAggregate = pair.Value;
                    TState2 rightAggregate;
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

                    foreach (TOutput result in this.resultSelector(key, leftAggregate, rightAggregate))
                    {
                        timeOutput.Send(result);
                    }
                }

                // The remaining keys have no matching values in leftGroups at this time.
                foreach (KeyValuePair<TKey, TState2> pair in rightGroups)
                {
                    TKey key = pair.Key;
                    TState1 leftAggregate = this.leftAggregateInitializer();
                    TState2 rightAggregate = pair.Value;

                    foreach (TOutput result in this.resultSelector(key, leftAggregate, rightAggregate))
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
            if (stream1 == null) throw new ArgumentNullException("stream1");
            if (stream2 == null) throw new ArgumentNullException("stream2");
            if (keySelector1 == null) throw new ArgumentNullException("keySelector1");
            if (keySelector2 == null) throw new ArgumentNullException("keySelector2");
            if (resultSelector == null) throw new ArgumentNullException("resultSelector");
            return stream1.NewBinaryStage(stream2, (i, s) => new JoinVertex<TInput1, TInput2, TKey, TOutput, TTime>(i, s, keySelector1, keySelector2, resultSelector), x => keySelector1(x).GetHashCode(), x => keySelector2(x).GetHashCode(), null, "Join");
        }

        internal class JoinVertex<TInput1, TInput2, TKey, TOutput, TTime> : BinaryVertex<TInput1, TInput2, TOutput, TTime> where TTime : Time<TTime>
        {
            private readonly Dictionary<TTime, Dictionary<TKey, Pair<List<TInput1>, List<TInput2>>>> values = new Dictionary<TTime, Dictionary<TKey, Pair<List<TInput1>, List<TInput2>>>>();

            private readonly Func<TInput1, TKey> keySelector1;
            private readonly Func<TInput2, TKey> keySelector2;
            private readonly Func<TInput1, TInput2, TOutput> resultSelector;

            public override void OnReceive1(Message<TInput1, TTime> message)
            {
                if (!this.values.ContainsKey(message.time))
                {
                    this.values.Add(message.time, new Dictionary<TKey, Pair<List<TInput1>, List<TInput2>>>());
                    this.NotifyAt(message.time);
                }

                var currentValues = this.values[message.time];

                var output = this.Output.GetBufferForTime(message.time);

                for (int i = 0; i < message.length; i++)
                {
                    var key = keySelector1(message.payload[i]);

                    Pair<List<TInput1>, List<TInput2>> currentEntry;
                    if (!currentValues.TryGetValue(key, out currentEntry))
                    {
                        currentEntry = new Pair<List<TInput1>, List<TInput2>>(new List<TInput1>(), new List<TInput2>());
                        currentValues[key] = currentEntry;
                    }

                    currentEntry.First.Add(message.payload[i]);
                    foreach (var match in currentEntry.Second)
                        output.Send(resultSelector(message.payload[i], match));
                }
            }

            public override void OnReceive2(Message<TInput2, TTime> message)
            {
                if (!this.values.ContainsKey(message.time))
                {
                    this.values.Add(message.time, new Dictionary<TKey, Pair<List<TInput1>, List<TInput2>>>());
                    this.NotifyAt(message.time);
                }

                var currentValues = this.values[message.time];
                var output = this.Output.GetBufferForTime(message.time);

                for (int i = 0; i < message.length; i++)
                {
                    var key = keySelector2(message.payload[i]);

                    Pair<List<TInput1>, List<TInput2>> currentEntry;
                    if (!currentValues.TryGetValue(key, out currentEntry))
                    {
                        currentEntry = new Pair<List<TInput1>, List<TInput2>>(new List<TInput1>(), new List<TInput2>());
                        currentValues[key] = currentEntry;
                    }

                    currentEntry.Second.Add(message.payload[i]);
                    foreach (var match in currentEntry.First)
                        output.Send(resultSelector(match, message.payload[i]));
                }
            }

            public override void OnNotify(TTime time)
            {
                this.values.Remove(time);
            }

            public JoinVertex(int index, Stage<TTime> stage, Func<TInput1, TKey> key1, Func<TInput2, TKey> key2, Func<TInput1, TInput2, TOutput> result)
                : base(index, stage)
            {
                this.values = new Dictionary<TTime, Dictionary<TKey, Pair<List<TInput1>, List<TInput2>>>>();
                this.keySelector1 = key1;
                this.keySelector2 = key2;
                this.resultSelector = result;
            }
        }

        /// <summary>
        /// Computes the set of distinct records in <paramref name="stream"/>.
        /// </summary>
        /// <typeparam name="TRecord">The type of the input records.</typeparam>
        /// <typeparam name="TTime">The type of timestamp on each record.</typeparam>
        /// <param name="stream">The input stream.</param>
        /// <returns>The stream containing at most one instance of each distinct record in the input stream.</returns>
        public static Stream<TRecord, TTime> Distinct<TRecord, TTime>(this Stream<TRecord, TTime> stream) where TTime : Time<TTime>
        {
            if (stream == null) throw new ArgumentNullException("stream");
            return stream.NewUnaryStage((i, v) => new DistinctVertex<TRecord, TTime>(i, v), x => x.GetHashCode(), x => x.GetHashCode(), "Distinct");
        }

        internal class DistinctVertex<TRecord, TTime> : UnaryVertex<TRecord, TRecord, TTime>
            where TTime : Time<TTime>
        {
            private readonly Dictionary<TTime, HashSet<TRecord>> values = new Dictionary<TTime, HashSet<TRecord>>();

            internal DistinctVertex(int i, Stage<TTime> stage) : base(i, stage) { }

            public override void OnReceive(Message<TRecord, TTime> message)
            {
                if (!this.values.ContainsKey(message.time))
                {
                    this.values.Add(message.time, new HashSet<TRecord>());
                    this.NotifyAt(message.time);
                }

                var currentSet = this.values[message.time];
                var output = this.Output.GetBufferForTime(message.time);
                for (int i = 0; i < message.length; i++)
                    if (currentSet.Add(message.payload[i]))
                        output.Send(message.payload[i]);
            }

            public override void OnNotify(TTime time)
            {
                this.values.Remove(time);
            }
        }


        /// <summary>
        /// Computes the set union of records in <paramref name="stream1"/> and <paramref name="stream2"/>.
        /// </summary>
        /// <typeparam name="TRecord">The type of the input records.</typeparam>
        /// <typeparam name="TTime">The type of timestamp on each record.</typeparam>
        /// <param name="stream1">The first input stream.</param>
        /// <param name="stream2">The second input stream.</param>
        /// <returns>The stream containing at most one instance of each record in either input stream.</returns>
        public static Stream<TRecord, TTime> Union<TRecord, TTime>(this Stream<TRecord, TTime> stream1, Stream<TRecord, TTime> stream2) where TTime : Time<TTime>
        {
            if (stream1 == null) throw new ArgumentNullException("stream1");
            if (stream2 == null) throw new ArgumentNullException("stream2");
            return stream1.Concat(stream2).Distinct();
        }

        /// <summary>
        /// Computes the set intersection of records in <paramref name="stream1"/> and <paramref name="stream2"/>.
        /// </summary>
        /// <typeparam name="TRecord">The type of the input records.</typeparam>
        /// <typeparam name="TTime">The type of timestamp on each record.</typeparam>
        /// <param name="stream1">The first input stream.</param>
        /// <param name="stream2">The second input stream.</param>
        /// <returns>The stream containing at most one instance of each record in both input stream.</returns>
        public static Stream<TRecord, TTime> Intersect<TRecord, TTime>(this Stream<TRecord, TTime> stream1, Stream<TRecord, TTime> stream2) where TTime : Time<TTime>
        {
            if (stream1 == null) throw new ArgumentNullException("stream1");
            if (stream2 == null) throw new ArgumentNullException("stream2");
            return stream1.NewBinaryStage(stream2, (i, s) => new IntersectVertex<TRecord, TTime>(i, s), x => x.GetHashCode(), x => x.GetHashCode(), x => x.GetHashCode(), "Intersect");
        }

        internal class IntersectVertex<TRecord, TTime> : BinaryVertex<TRecord, TRecord, TRecord, TTime> where TTime : Time<TTime>
        {
            private readonly Dictionary<TTime, HashSet<TRecord>> values1 = new Dictionary<TTime, HashSet<TRecord>>();
            private readonly Dictionary<TTime, HashSet<TRecord>> values2 = new Dictionary<TTime, HashSet<TRecord>>();

            internal IntersectVertex(int i, Stage<TTime> stage) : base(i, stage) { }

            public override void OnReceive1(Message<TRecord, TTime> message)
            {
                if (!this.values1.ContainsKey(message.time))
                {
                    this.values1.Add(message.time, new HashSet<TRecord>());
                    this.NotifyAt(message.time);
                }

                var currentSet = this.values1[message.time];
                var output = this.Output.GetBufferForTime(message.time);

                for (int i = 0; i < message.length; i++)
                    if (currentSet.Add(message.payload[i]))
                        if (this.values2.ContainsKey(message.time) && this.values2[message.time].Contains(message.payload[i]))
                            output.Send(message.payload[i]);
            }

            public override void OnReceive2(Message<TRecord, TTime> message)
            {
                if (!this.values2.ContainsKey(message.time))
                {
                    this.values2.Add(message.time, new HashSet<TRecord>());
                    this.NotifyAt(message.time);
                }

                var currentSet = this.values2[message.time];
                var output = this.Output.GetBufferForTime(message.time);

                for (int i = 0; i < message.length; i++)
                    if (currentSet.Add(message.payload[i]))
                        if (this.values1.ContainsKey(message.time) && this.values1[message.time].Contains(message.payload[i]))
                            output.Send(message.payload[i]);
            }

            public override void OnNotify(TTime time)
            {
                this.values1.Remove(time);
                this.values2.Remove(time);
            }
        }

        /// <summary>
        /// Computes the difference of records in <paramref name="stream1"/> but not in <paramref name="stream2"/>.
        /// </summary>
        /// <typeparam name="TRecord">The type of the input records.</typeparam>
        /// <typeparam name="TTime">The type of timestamp on each record.</typeparam>
        /// <param name="stream1">The first input stream.</param>
        /// <param name="stream2">The second input stream.</param>
        /// <returns>The stream containing the records in <paramref name="stream1"/> that do not match any record in <paramref name="stream2"/>.</returns>
        public static Stream<TRecord, TTime> Except<TRecord, TTime>(this Stream<TRecord, TTime> stream1, Stream<TRecord, TTime> stream2) where TTime : Time<TTime>
        {
            if (stream1 == null) throw new ArgumentNullException("stream1");
            if (stream2 == null) throw new ArgumentNullException("stream2");
            return stream1.NewBinaryStage(stream2, (i, s) => new ExceptVertex<TRecord, TTime>(i, s), x => x.GetHashCode(), x => x.GetHashCode(), x => x.GetHashCode(), "Except");
        }

        internal class ExceptVertex<TRecord, TTime> : BinaryVertex<TRecord, TRecord, TRecord, TTime> where TTime : Time<TTime>
        {
            private readonly Dictionary<TTime, Dictionary<TRecord, int>> values = new Dictionary<TTime, Dictionary<TRecord, int>>();

            internal ExceptVertex(int index, Stage<TTime> stage) : base(index, stage) { }

            public override void OnReceive1(Message<TRecord, TTime> message)
            {
                if (!this.values.ContainsKey(message.time))
                {
                    this.values.Add(message.time, new Dictionary<TRecord, int>());
                    this.NotifyAt(message.time);
                }

                var currentValue = this.values[message.time];

                for (int i = 0; i < message.length; i++)
                {
                    int currentCount;
                    if (!currentValue.TryGetValue(message.payload[i], out currentCount))
                        currentCount = 0;

                    if (currentCount >= 0)
                        currentValue[message.payload[i]] = currentCount + 1;
                    // else
                    //    ;   // set negative by the other input; shame!
                }
            }

            public override void OnReceive2(Message<TRecord, TTime> message)
            {
                if (!this.values.ContainsKey(message.time))
                {
                    this.values.Add(message.time, new Dictionary<TRecord, int>());
                    this.NotifyAt(message.time);
                }

                var currentValue = this.values[message.time];

                // set the value negative to exclude it.
                for (int i = 0; i < message.length; i++)
                    currentValue[message.payload[i]] = -1;
            }

            public override void OnNotify(TTime time)
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
        /// <typeparam name="TRecord">The type of the input records.</typeparam>
        /// <typeparam name="TTime">The type of timestamp on each record.</typeparam>
        /// <param name="stream">The input stream.</param> 
        /// <returns>The stream containing pairs of a record and its respective count for each input record.</returns>
        public static Stream<Pair<TRecord, Int64>, TTime> Count<TRecord, TTime>(this Stream<TRecord, TTime> stream) where TTime : Time<TTime>
        {
            if (stream == null) throw new ArgumentNullException("stream");
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
            if (stream == null) throw new ArgumentNullException("stream");
            if (keySelector == null) throw new ArgumentNullException("keySelector");
            if (valueSelector == null) throw new ArgumentNullException("valueSelector");
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
            if (stream == null) throw new ArgumentNullException("stream");
            if (keySelector == null) throw new ArgumentNullException("keySelector");
            if (valueSelector == null) throw new ArgumentNullException("valueSelector");
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
            if (stream == null) throw new ArgumentNullException("stream");
            if (keySelector == null) throw new ArgumentNullException("keySelector");
            if (stateSelector == null) throw new ArgumentNullException("stateSelector");
            if (combiner == null) throw new ArgumentNullException("combiner");
            if (resultSelector == null) throw new ArgumentNullException("resultSelector");
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
            if (stream == null) throw new ArgumentNullException("stream");
            if (keySelector == null) throw new ArgumentNullException("keySelector");
            if (stateSelector == null) throw new ArgumentNullException("stateSelector");
            if (combiner == null) throw new ArgumentNullException("combiner");
            if (resultSelector == null) throw new ArgumentNullException("resultSelector");
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
            if (stream == null) throw new ArgumentNullException("stream");
            if (combiner == null) throw new ArgumentNullException("combiner");
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
            if (stream == null) throw new ArgumentNullException("stream");
            if (combiner == null) throw new ArgumentNullException("combiner");
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
            if (stream == null) throw new ArgumentNullException("stream");
            if (selector == null) throw new ArgumentNullException("selector");
            return stream.NewUnaryStage((i, v) => new VertexSelect<TInput, TOutput, TTime>(i, v, selector), null, null, "SelectByVertex");
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
            if (stream == null) throw new ArgumentNullException("stream");
            if (selector == null) throw new ArgumentNullException("selector");
            return stream.NewUnaryStage((i, v) => new SelectManyArraySegment<TInput, TOutput, TTime>(i, v, selector), null, null, "SelectManyArraySegment");
        }

        /// <summary>
        /// For each timestamp, buffers the given <paramref name="stream"/> until all workers have all input for that time, and then releases the buffer.
        /// </summary>
        /// <typeparam name="TRecord">The type of the input records.</typeparam>
        /// <typeparam name="TTime">The type of timestamp on each record.</typeparam>
        /// <param name="stream">The input stream.</param>
        /// <param name="predicate">A predicate indicating which times to synchronize in</param>
        /// <returns>The input stream.</returns>
        public static Stream<TRecord, TTime> Synchronize<TRecord, TTime>(this Stream<TRecord, TTime> stream, Func<TTime, bool> predicate)
            where TTime : Time<TTime>
        {
            if (stream == null) throw new ArgumentNullException("stream");
            if (predicate == null) throw new ArgumentNullException("predicate");

            return stream.NewUnaryStage((i, s) => new SynchronizeVertex<TRecord, TTime>(i, s, predicate), null, stream.PartitionedBy, "Synchronize");
        }

        /// <summary>
        /// Iteratively applies <paramref name="function"/> to the given <paramref name="stream"/>.
        /// </summary>
        /// <typeparam name="TRecord">The type of the input records.</typeparam>
        /// <typeparam name="TTime">The type of timestamp on each record.</typeparam>
        /// <param name="stream">The input stream.</param>
        /// <param name="function">The Naiad computation to apply iteratively.</param>
        /// <param name="iterations">The number of iterations to perform.</param>
        /// <param name="name">Descriptive name for the loop.</param>
        /// <returns>The stream corresponding to applying <paramref name="function"/> to the input <paramref name="iterations"/> times.</returns>
        public static Stream<TRecord, TTime> Iterate<TRecord, TTime>(this Stream<TRecord, TTime> stream, Func<LoopContext<TTime>, Stream<TRecord, IterationIn<TTime>>, Stream<TRecord, IterationIn<TTime>>> function, int iterations, string name)
            where TTime : Time<TTime>
        {
            if (stream == null) throw new ArgumentNullException("stream");
            if (function == null) throw new ArgumentNullException("function");
            return stream.Iterate(function, null, iterations, name);
        }

        /// <summary>
        /// Iteratively applies <paramref name="function"/> to the given <paramref name="stream"/>.
        /// </summary>
        /// <typeparam name="TRecord">The type of the input records.</typeparam>
        /// <typeparam name="TTime">The type of timestamp on each record.</typeparam>
        /// <param name="stream">The input stream.</param>
        /// <param name="function">The Naiad computation to apply iteratively.</param>
        /// <param name="partitionedBy">Partitioning function for the input stream.</param>
        /// <param name="iterations">The number of iterations to perform.</param>
        /// <param name="name">Descriptive name for the loop.</param>
        /// <returns>The stream corresponding to applying <paramref name="function"/> to the input <paramref name="iterations"/> times.</returns>
        public static Stream<TRecord, TTime> Iterate<TRecord, TTime>(this Stream<TRecord, TTime> stream, Func<LoopContext<TTime>, Stream<TRecord, IterationIn<TTime>>, Stream<TRecord, IterationIn<TTime>>> function, Expression<Func<TRecord, int>> partitionedBy, int iterations, string name)
            where TTime : Time<TTime>
        {
            if (stream == null) throw new ArgumentNullException("stream");
            if (function == null) throw new ArgumentNullException("function");
            return stream.Iterate(function, x => 0, partitionedBy, iterations, name);
        }

        /// <summary>
        /// Iteratively applies <paramref name="function"/> to the given <paramref name="stream"/>.
        /// </summary>
        /// <typeparam name="TRecord">The type of the input records.</typeparam>
        /// <typeparam name="TTime">The type of timestamp on each record.</typeparam>
        /// <param name="stream">The input stream.</param>
        /// <param name="function">The Naiad computation to apply iteratively.</param>
        /// <param name="partitionedBy">Partitioning function for the input stream.</param>
        /// <param name="iterationSelector">Function that maps an input record to the iteration at which that record should be introduced.</param>
        /// <param name="iterations">The number of iterations to perform.</param>
        /// <param name="name">Descriptive name for the loop.</param>
        /// <returns>The stream corresponding to applying <paramref name="function"/> to the input <paramref name="iterations"/> times.</returns>
        public static Stream<TRecord, TTime> Iterate<TRecord, TTime>(this Stream<TRecord, TTime> stream, Func<LoopContext<TTime>, Stream<TRecord, IterationIn<TTime>>, Stream<TRecord, IterationIn<TTime>>> function, Func<TRecord, int> iterationSelector, Expression<Func<TRecord, int>> partitionedBy, int iterations, string name)
            where TTime : Time<TTime>
        {
            if (stream == null) throw new ArgumentNullException("stream");
            if (function == null) throw new ArgumentNullException("function");
            if (iterationSelector == null) throw new ArgumentNullException("iterationSelector");
            var helper = new LoopContext<TTime>(stream.Context, name);

            var delayed = helper.Delay(partitionedBy, iterations);

            var ingress = helper.EnterLoop(stream, iterationSelector).PartitionBy(partitionedBy);

            var loopHead = ingress.Concat(delayed.Output);

            var loopTail = function(helper, loopHead);

            delayed.Input = loopTail;

            return helper.ExitLoop(loopTail, iterations);
        }

        /// <summary>
        /// Iteratively applies <paramref name="function"/> to the given <paramref name="stream"/> and accumulates all of the iterates.
        /// </summary>
        /// <typeparam name="TRecord">The type of the input records.</typeparam>
        /// <typeparam name="TTime">The type of timestamp on each record.</typeparam>
        /// <param name="stream">The input stream.</param>
        /// <param name="function">The Naiad computation to apply iteratively.</param>
        /// <param name="iterations">The number of iterations to perform.</param>
        /// <param name="name">Descriptive name for the loop.</param>
        /// <returns>The stream corresponding to applying <paramref name="function"/> to the input <paramref name="iterations"/> times and accumulating the iterates.</returns>
        public static Stream<TRecord, TTime> IterateAndAccumulate<TRecord, TTime>(this Stream<TRecord, TTime> stream, Func<LoopContext<TTime>, Stream<TRecord, IterationIn<TTime>>, Stream<TRecord, IterationIn<TTime>>> function, int iterations, string name)
            where TTime : Time<TTime>
        {
            if (stream == null) throw new ArgumentNullException("stream");
            if (function == null) throw new ArgumentNullException("function");
            return stream.IterateAndAccumulate(function, null, iterations, name);
        }

        /// <summary>
        /// Iteratively applies <paramref name="function"/> to the given <paramref name="stream"/> and accumulates all of the iterates.
        /// </summary>
        /// <typeparam name="TRecord">The type of the input records.</typeparam>
        /// <typeparam name="TTime">The type of timestamp on each record.</typeparam>
        /// <param name="stream">The input stream.</param>
        /// <param name="function">The Naiad computation to apply iteratively.</param>
        /// <param name="partitionedBy">Partitioning function for the input stream.</param>
        /// <param name="iterations">The number of iterations to perform.</param>
        /// <param name="name">Descriptive name for the loop.</param>
        /// <returns>The stream corresponding to applying <paramref name="function"/> to the input <paramref name="iterations"/> times and accumulating the iterates.</returns>
        public static Stream<TRecord, TTime> IterateAndAccumulate<TRecord, TTime>(this Stream<TRecord, TTime> stream, Func<LoopContext<TTime>, Stream<TRecord, IterationIn<TTime>>, Stream<TRecord, IterationIn<TTime>>> function, Expression<Func<TRecord, int>> partitionedBy, int iterations, string name)
            where TTime : Time<TTime>
        {
            if (stream == null) throw new ArgumentNullException("stream");
            if (function == null) throw new ArgumentNullException("function");
            return stream.IterateAndAccumulate(function, x => 0, partitionedBy, iterations, name);
        }

        /// <summary>
        /// Iteratively applies <paramref name="function"/> to the given <paramref name="stream"/> and accumulates all of the iterates.
        /// </summary>
        /// <typeparam name="TRecord">The type of the input records.</typeparam>
        /// <typeparam name="TTime">The type of timestamp on each record.</typeparam>
        /// <param name="stream">The input stream.</param>
        /// <param name="function">The Naiad computation to apply iteratively.</param>
        /// <param name="partitionedBy">Partitioning function for the input stream.</param>
        /// <param name="iterationSelector">Function that maps an input record to the iteration at which that record should be introduced.</param>
        /// <param name="iterations">The number of iterations to perform.</param>
        /// <param name="name">Descriptive name for the loop.</param>
        /// <returns>The stream corresponding to applying <paramref name="function"/> to the input <paramref name="iterations"/> times and accumulating the iterates.</returns>
        public static Stream<TRecord, TTime> IterateAndAccumulate<TRecord, TTime>(this Stream<TRecord, TTime> stream, Func<LoopContext<TTime>, Stream<TRecord, IterationIn<TTime>>, Stream<TRecord, IterationIn<TTime>>> function, Func<TRecord, int> iterationSelector, Expression<Func<TRecord, int>> partitionedBy, int iterations, string name)
            where TTime : Time<TTime>
        {
            if (stream == null) throw new ArgumentNullException("stream");
            if (function == null) throw new ArgumentNullException("function");
            if (iterationSelector == null) throw new ArgumentNullException("iterationSelector");
            var helper = new LoopContext<TTime>(stream.Context, name);

            var delayed = helper.Delay(partitionedBy, iterations);

            var ingress = helper.EnterLoop(stream, iterationSelector).PartitionBy(partitionedBy);

            var loopHead = ingress.Concat(delayed.Output);

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
            if (stream == null) throw new ArgumentNullException("stream");
            if (format == null) throw new ArgumentNullException("format");
            if (action == null) throw new ArgumentNullException("action");
            stream.NewSinkStage((i, v) => new Writer<TInput>(i, v, action, format), null, "Writer");
        }
    }
   
    #region Custom implementations of non-standard vertex implementations

    internal class VertexSelect<TInput, TOutput, TTime> : UnaryVertex<TInput, TOutput, TTime>
        where TTime : Time<TTime>
    {
        private readonly Func<int, TInput, TOutput> function;

        public override void OnReceive(Message<TInput, TTime> message)
        {
            var output = this.Output.GetBufferForTime(message.time);
            for (int i = 0; i < message.length; i++)
                output.Send(this.function(this.VertexId, message.payload[i]));
        }

        public VertexSelect(int index, Stage<TTime> stage, Func<int, TInput, TOutput> function)
            : base(index, stage)
        {
            this.function = function;
        }
    }

    internal class SelectManyArraySegment<TInput, TOutput, TTime> : UnaryVertex<TInput, TOutput, TTime>
        where TTime : Time<TTime>
    {
        private readonly Func<TInput, IEnumerable<ArraySegment<TOutput>>> function;

        public override void OnReceive(Message<TInput, TTime> message)
        {
            var output = this.Output.GetBufferForTime(message.time);
            for (int ii = 0; ii < message.length; ii++)
            {
                var record = message.payload[ii];
                foreach (var result in function(record))
                    for (int i = result.Offset; i < result.Offset + result.Count; i++)
                        output.Send(result.Array[i]);
            }
        }

        public SelectManyArraySegment(int index, Stage<TTime> stage, Func<TInput, IEnumerable<ArraySegment<TOutput>>> function)
            : base(index, stage)
        {
            this.function = function;
        }
    }

    internal class SynchronizeVertex<TRecord, TTime> : UnaryVertex<TRecord, TRecord, TTime>
        where TTime : Time<TTime>
    {
        private readonly Func<TTime, bool> Predicate;
        private readonly Dictionary<TTime, List<TRecord>> Records;

        public override void OnReceive(Message<TRecord, TTime> message)
        {
            if (this.Predicate(message.time))
            {
                if (!this.Records.ContainsKey(message.time))
                {
                    this.Records.Add(message.time, new List<TRecord>());
                    this.NotifyAt(message.time);
                }

                var list = this.Records[message.time];
                for (int i = 0; i < message.length; i++)
                    list.Add(message.payload[i]);
            }
            else
                this.Output.Send(message);
        }

        public override void OnNotify(TTime time)
        {
            if (this.Records.ContainsKey(time))
            {
                var list = this.Records[time];
                this.Records.Remove(time);

                var output = this.Output.GetBufferForTime(time);
                for (int i = 0; i < list.Count; i++)
                    output.Send(list[i]);
            }
        }

        public SynchronizeVertex(int index, Stage<TTime> stage, Func<TTime, bool> predicate)
            : base(index, stage)
        {
            this.Predicate = predicate;
            this.Records = new Dictionary<TTime, List<TRecord>>();
        }
    }

    internal class Writer<TRecord> : SinkVertex<TRecord, Epoch>
    {
        private readonly Dictionary<Epoch, System.IO.BinaryWriter> writers = new Dictionary<Epoch,System.IO.BinaryWriter>();
        private readonly Action<TRecord, System.IO.BinaryWriter> action;
        private readonly string format;

        public override void OnReceive(Message<TRecord, Epoch> message)
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
                action(message.payload[i], writer);
        }

        public override void OnNotify(Epoch time)
        {
            this.writers[time].Dispose();
            this.writers.Remove(time);
        }

        public Writer(int index, Stage<Epoch> stage, Action<TRecord, System.IO.BinaryWriter> action, string format)
            : base(index, stage)
        {
            this.format = format;

            this.action = action;
        }
    }

    #endregion 
}
