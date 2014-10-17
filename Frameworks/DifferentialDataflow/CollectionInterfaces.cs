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
using System.Linq.Expressions;
using System.Text;
using System.IO;

using Microsoft.Research.Naiad.DataStructures;
using Microsoft.Research.Naiad.Dataflow.Channels;
using Microsoft.Research.Naiad.Scheduling;
using Microsoft.Research.Naiad;
using Microsoft.Research.Naiad.Dataflow;

namespace Microsoft.Research.Naiad.Frameworks.DifferentialDataflow
{
    /// <summary>
    /// The Differential Dataflow framework contains extension methods that support LINQ-style incremental and iterative operators.
    /// </summary>
    /// <remarks>
    /// The Differential Dataflow operators are defined in terms of <see cref="Collection{TRecord,TTime}"/> objects, each of which wraps
    /// a Naiad stream and allows it to be interpreted with multiset semantics.
    /// 
    /// The <see cref="InputCollection{TRecord}"/> class is the Differential Dataflow&#x2013;specific wrapper for the Naiad
    /// <see cref="Microsoft.Research.Naiad.Input.BatchedDataSource{TRecord}"/> class.
    /// </remarks>
    /// <example>
    /// A simple Differential Dataflow program can be written as follows:
    /// 
    /// <code>
    /// using Microsoft.Research.Naiad;
    /// using Microsoft.Research.Naiad.Frameworks.DifferentialDataflow;
    /// 
    /// class Program
    /// {
    ///     public static void Main(string[] args)
    ///     {
    ///         using (Controller controller = NewController.FromArgs(ref args)
    ///         {
    ///             using (Computation computation = controller.NewComputation())
    ///             {
    ///                 // Define a computation in terms of collections.
    ///                 InputCollection&lt;int&gt; input = computation.NewInputCollection();
    ///                 
    ///                 var histogram = input.Count(x => x);
    ///                 
    ///                 // A subscription collects the result of the computation, and applies
    ///                 // an action to the array of weighted updates.
    ///                 Subscription subscription = histogram.Subscribe(changes => { /* ... */ });
    /// 
    ///                 computation.Activate();
    /// 
    ///                 // The OnNext() method takes an IEnumerable&lt;int&gt; that specifies records to add.
    ///                 input.OnNext(new int[] { 1, 1, 2, 3, 5, 8 });
    ///                 input.Sync(0); 
    ///                 
    ///                 input.OnNext(new int[] { 13 });
    ///                 input.Sync(1);
    ///                 
    ///                 // This OnNext() overload allows elements to be added with integer weights.
    ///                 // The weights may be positive (adding records) or negative (removing records).
    ///                 input.OnNext(new Weighted&lt;int&gt;[] { new Weighted&lt;int&gt;(1, -2) });
    ///                 input.Sync(2);
    /// 
    ///                 computation.Join();
    ///             }
    /// 
    ///             controller.Join();
    ///         }
    ///         
    ///     }
    /// }
    /// </code>
    /// </example>
    /// <seealso cref="Microsoft.Research.Naiad.Controller"/>
    /// <seealso cref="Microsoft.Research.Naiad.NewController"/>
    /// <seealso cref="Microsoft.Research.Naiad.Computation"/>
    /// <seealso cref="InputCollection{TRecord}"/>
    /// <seealso cref="ExtensionMethods.OnNext{TRecord}(InputCollection{TRecord},IEnumerable{TRecord})"/>
    /// <seealso cref="InputCollection{TRecord}.OnNext(IEnumerable{Weighted{TRecord}})"/>
    /// <seealso cref="ExtensionMethods.Subscribe{TRecord}"/>
    /// <seealso cref="Weighted{TRecord}"/>
    class NamespaceDoc
    {

    }

    /// <summary>
    /// A collection is a multiset of records that varies according to a logical timestamp.
    /// </summary>
    /// <typeparam name="TRecord">The type of records in the collection.</typeparam>
    /// <typeparam name="TTime">The type of timestamp on each record.</typeparam>
    /// <remarks>
    /// The <typeparamref name="TTime"/> type parameter is used to ensure that collections are
    /// combined in compatible ways, and need not be manipulated directly by the programmer. Initially,
    /// <see cref="InputCollection{TRecord}"/> collections have an integer-valued <see cref="Epoch"/> timestamp,
    /// which indicates that they vary according to a stream of input epochs. The <see cref="EnterLoop(Microsoft.Research.Naiad.Dataflow.Iteration.LoopContext{TTime})"/> method
    /// is used to refer to a collection within a loop, by augmenting its timestamp to have an <see cref="IterationIn{TTime}"/> value
    /// with an additional loop counter.
    /// </remarks>
    public interface Collection<TRecord, TTime>
        where TRecord : IEquatable<TRecord>
        where TTime : Time<TTime>
    {
        #region Naiad operators

        /// <summary>
        /// The underlying Naiad <see cref="Microsoft.Research.Naiad.Stream{TWeightedRecord,TTime}"/> of
        /// <see cref="Weighted{TRecord}"/> elements.
        /// </summary>
        Stream<Weighted<TRecord>, TTime> Output { get; }

        /// <summary>
        /// Partitions a collection by the given key selector.
        /// </summary>
        /// <typeparam name="TKey">The key type.</typeparam>
        /// <param name="keySelector">Function that extracts a key from each record.</param>
        /// <returns>A collection with the same elements, but in which all records with the same key will be
        /// processed by the same worker.</returns>
        Collection<TRecord, TTime> PartitionBy<TKey>(Expression<Func<TRecord, TKey>> keySelector);

        #region Consolidation

        /// <summary>
        /// Consolidates a collection so that each record occurs with at most one weight.
        /// </summary>
        /// <returns>A collection with the same elements, but in which all identical records are stored once with a canonical weight.</returns>
        Collection<TRecord, TTime> Consolidate();

        /// <summary>
        /// Consolidates representation so that each record occurs with at most one weight.
        /// </summary>
        /// <typeparam name="TKey">The key type.</typeparam>
        /// <param name="keySelector">Function that extracts a key from each record.</param>
        /// <returns>A collection with the same elements, but in which all identical records are stored once with a canonical weight,
        /// and all records with the same key will be processed by the same worker.</returns>
        Collection<TRecord, TTime> Consolidate<TKey>(Expression<Func<TRecord, TKey>> keySelector);

        #endregion

        #region Select/Where/SelectMany

        /// <summary>
        /// Transforms each record in the collection using the given <paramref name="selector"/> function.
        /// </summary>
        /// <typeparam name="TOutput">The type of the transformed records.</typeparam>
        /// <param name="selector">A transform function to apply to each record.</param>
        /// <returns>The collection of transformed records.</returns>
        Collection<TOutput, TTime> Select<TOutput>(Expression<Func<TRecord, TOutput>> selector)
            where TOutput : IEquatable<TOutput>;

        /// <summary>
        /// Filters the collection to contain only records that match the given <paramref name="predicate"/>.
        /// </summary>
        /// <param name="predicate">A function that returns <c>true</c> if and only if the record will be kept in the output.</param>
        /// <returns>The collection of records that satisfy the predicate.</returns>
        Collection<TRecord, TTime> Where(Expression<Func<TRecord, bool>> predicate);

        /// <summary>
        /// Transforms each record in the collection using the given <paramref name="selector"/> function and flattens the result. 
        /// </summary>
        /// <typeparam name="TOutput">The type of elements of the sequence returned by <paramref name="selector"/>.</typeparam>
        /// <param name="selector">A transform function to apply to each record.</param>
        /// <returns>The flattened collection of transformed records.</returns>
        Collection<TOutput, TTime> SelectMany<TOutput>(Expression<Func<TRecord, IEnumerable<TOutput>>> selector)
            where TOutput : IEquatable<TOutput>;

        /// <summary>
        /// Transforms each record in the collection using the given <paramref name="selector"/> function and flattens the result.
        /// </summary>
        /// <typeparam name="TOutput">The type of elements of the array segments in the sequence returned by <paramref name="selector"/>.</typeparam>
        /// <param name="selector">A transform function to apply to each record.</param>
        /// <returns>The flattened collection of transformed records.</returns>
        /// <remarks>
        /// This overload supports optimizing the performance of <see cref="SelectMany{TOutput}(Expression{Func{TRecord,IEnumerable{TOutput}}})"/> by using
        /// <see cref="ArraySegment{TOutput}"/> objects to batch the elements returned by <paramref name="selector"/>.
        /// </remarks>
        Collection<TOutput, TTime> SelectMany<TOutput>(Expression<Func<TRecord, IEnumerable<ArraySegment<TOutput>>>> selector)
            where TOutput : IEquatable<TOutput>;

        #endregion Select/Where/SelectMany

        #region GroupBy/CoGroupBy
        /// <summary>
        /// Groups records using the supplied key selector, and applies the given reduction function.
        /// </summary>
        /// <typeparam name="TKey">The key type.</typeparam>
        /// <typeparam name="TValue">The intermediate value type.</typeparam>
        /// <typeparam name="TOutput">The result type.</typeparam>
        /// <param name="keySelector">Function that extracts a key from each record.</param>
        /// <param name="valueSelector">Function that transforms a record to the intermediate value that is stored for each record.</param>
        /// <param name="reducer">Function that transforms a sequence of intermediate values to a sequence of output records.</param>
        /// <returns>The collection of output records for each group in the input collection.</returns>
        /// <remarks>This overload can reduce the amount of storage required compared to <see cref="GroupBy{TKey,TOutput}"/> in cases
        /// where the intermediate value is small relative to each original record.
        /// 
        /// If the reducer is a commutative and associative aggregation function, consider using the <see cref="Aggregate{TKey,TValue,TOutput}"/> (or a related aggregation)
        /// operator, which stores a single value for each key, rather than a sequence of values.</remarks>
        /// <seealso cref="Count{TKey}"/>
        /// <seealso cref="Count{TKey,TOutput}"/>
        /// <seealso cref="Max{TKey,TComparable}"/>
        /// <seealso cref="Max{TKey,TComparable,TValue}"/>
        /// <seealso cref="Min{TKey,TComparable}(Expression{Func{TRecord,TKey}},Expression{Func{TRecord,TComparable}})"/>
        /// <seealso cref="Min{TKey,TComparable,TValue}"/>
        /// <seealso cref="Sum{TKey,TOutput}(Expression{Func{TRecord,TKey}},Expression{Func{TRecord,int}},Expression{Func{TKey,int,TOutput}})"/>
        Collection<TOutput, TTime> GroupBy<TKey, TValue, TOutput>(Expression<Func<TRecord, TKey>> keySelector, Expression<Func<TRecord, TValue>> valueSelector, Func<TKey, IEnumerable<TValue>, IEnumerable<TOutput>> reducer)
            where TKey : IEquatable<TKey>
            where TValue : IEquatable<TValue>
            where TOutput : IEquatable<TOutput>;

        /// <summary>
        /// Groups records using the supplied key selector, and applies the given reduction function.
        /// </summary>
        /// <typeparam name="TKey">The key type.</typeparam>
        /// <typeparam name="TOutput">The result type.</typeparam>
        /// <param name="keySelector">Function that extracts a key from each record.</param>
        /// <param name="reducer">Function that transforms a sequence of input records to a sequence of output records.</param>
        /// <returns>The collection of output records for each group in the input collection.</returns>
        /// <remarks>The <see cref="GroupBy{TKey,TValue,TOutput}"/> overload can reduce the amount of storage required compared to this overload in cases
        /// where the intermediate value is small relative to each original record.
        /// 
        /// If the <paramref name="reducer"/> is a commutative and associative aggregation function, consider using the <see cref="Aggregate{TKey,TValue,TOutput}"/> (or a related aggregation)
        /// operator, which stores a single value for each key, rather than a sequence of values.</remarks>
        /// <seealso cref="Count{TKey}"/>
        /// <seealso cref="Count{TKey,TOutput}"/>
        /// <seealso cref="Max{TKey,TComparable}"/>
        /// <seealso cref="Max{TKey,TComparable,TValue}"/>
        /// <seealso cref="Min{TKey,TComparable}(Expression{Func{TRecord,TKey}},Expression{Func{TRecord,TComparable}})"/>
        /// <seealso cref="Min{TKey,TComparable,TValue}"/>
        /// <seealso cref="Sum{TKey,TOutput}(Expression{Func{TRecord,TKey}},Expression{Func{TRecord,int}},Expression{Func{TKey,int,TOutput}})"/>

        Collection<TOutput, TTime> GroupBy<TKey, TOutput>(Expression<Func<TRecord, TKey>> keySelector, Func<TKey, IEnumerable<TRecord>, IEnumerable<TOutput>> reducer)
            where TKey : IEquatable<TKey>
            where TOutput : IEquatable<TOutput>;

        /// <summary>
        /// Groups records from both input collections using the respective key selector, and applies the given reduction function.
        /// </summary>
        /// <typeparam name="TKey">The key type.</typeparam>
        /// <typeparam name="TValue1">The type of intermediate values stored for this collection.</typeparam>
        /// <typeparam name="TValue2">The type of intermediate values stored from the <paramref name="other"/> collection.</typeparam>
        /// <typeparam name="TRecord2">The type of records in the <paramref name="other"/> collection.</typeparam>
        /// <typeparam name="TOutput">The result type.</typeparam>
        /// <param name="other">The other collection.</param>
        /// <param name="keySelector1">The key selector applied to records in this collection.</param>
        /// <param name="keySelector2">The key selector applied to records in the <paramref name="other"/> collection.</param>
        /// <param name="valueSelector1">Function that transforms a record in this collection to the intermediate value that is stored for each record.</param>
        /// <param name="valueSelector2">Function that transforms a record in the <paramref name="other"/> collection to the intermediate value that is stored for each record.</param>
        /// <param name="reducer">Function that transforms two sequences of intermediate values from each input collection to a sequence of output records.</param>
        /// <returns>The collection of output records for each group in either input collection.</returns>
        /// <remarks>This overload can reduce the amount of storage required compared to <see cref="CoGroupBy{TKey,TRecord2,TOutput}(Collection{TRecord2,TTime},Expression{Func{TRecord,TKey}},Expression{Func{TRecord2,TKey}},Expression{Func{TKey,IEnumerable{TRecord},IEnumerable{TRecord2},IEnumerable{TOutput}}})"/> in cases
        /// where the intermediate values are small relative to each original record.
        /// 
        /// The <see cref="CoGroupBy{TKey,TValue1,TValue2,TRecord2,TOutput}(Collection{TRecord2,TTime},Expression{Func{TRecord,TKey}},Expression{Func{TRecord2,TKey}},Expression{Func{TRecord,TValue1}},Expression{Func{TRecord2,TValue2}},Expression{Func{TKey,IEnumerable{Weighted{TValue1}},IEnumerable{Weighted{TValue2}},IEnumerable{Weighted{TOutput}}}})"/>
        /// overload can reduce the amount of computation required compared to this overload, by compressing multiple instances of the
        /// same value into a single <see cref="Weighted{TValue1}"/> value.
        /// </remarks>
        Collection<TOutput, TTime> CoGroupBy<TKey, TValue1, TValue2, TRecord2, TOutput>(Collection<TRecord2, TTime> other, Expression<Func<TRecord, TKey>> keySelector1, Expression<Func<TRecord2, TKey>> keySelector2, Expression<Func<TRecord, TValue1>> valueSelector1, Expression<Func<TRecord2, TValue2>> valueSelector2, Expression<Func<TKey, IEnumerable<TValue1>, IEnumerable<TValue2>, IEnumerable<TOutput>>> reducer)
            where TKey : IEquatable<TKey>
            where TValue1 : IEquatable<TValue1>
            where TValue2 : IEquatable<TValue2>
            where TRecord2 : IEquatable<TRecord2>
            where TOutput : IEquatable<TOutput>;

        /// <summary>
        /// Groups records from both input collections using the respective key selector, and applies the given reduction function.
        /// </summary>
        /// <typeparam name="TKey">The key type.</typeparam>
        /// <typeparam name="TRecord2">The type of records in the <paramref name="other"/> collection.</typeparam>
        /// <typeparam name="TOutput">The result type.</typeparam>
        /// <param name="other">The other collection.</param>
        /// <param name="keySelector1">The key selector applied to records in this collection.</param>
        /// <param name="keySelector2">The key selector applied to records in the <paramref name="other"/> collection.</param>
        /// <param name="reducer">Function that transforms two sequences of records from each input collection to a sequence of output records.</param>
        /// <returns>The collection of output records for each group in either input collection.</returns>
        /// <remarks>The <see cref="CoGroupBy{TKey,TValue1,TValue2,TRecord2,TOutput}(Collection{TRecord2,TTime},Expression{Func{TRecord,TKey}},Expression{Func{TRecord2,TKey}},Expression{Func{TRecord,TValue1}},Expression{Func{TRecord2,TValue2}},Expression{Func{TKey,IEnumerable{TValue1},IEnumerable{TValue2},IEnumerable{TOutput}}})"/>
        /// overload can reduce the amount of storage required compared to this overload in cases
        /// where the intermediate values are small relative to each original record.
        /// 
        /// The <see cref="CoGroupBy{TKey,TRecord2,TOutput}(Collection{TRecord2,TTime},Expression{Func{TRecord,TKey}},Expression{Func{TRecord2,TKey}},Expression{Func{TKey,IEnumerable{Weighted{TRecord}},IEnumerable{Weighted{TRecord2}},IEnumerable{Weighted{TOutput}}}})"/>
        /// overload can reduce the amount of computation required compared to this overload, by compressing multiple instances of the
        /// same value into a single <see cref="Weighted{TRecord}"/> value.</remarks>
        Collection<TOutput, TTime> CoGroupBy<TKey, TRecord2, TOutput>(Collection<TRecord2, TTime> other, Expression<Func<TRecord, TKey>> keySelector1, Expression<Func<TRecord2, TKey>> keySelector2, Expression<Func<TKey, IEnumerable<TRecord>, IEnumerable<TRecord2>, IEnumerable<TOutput>>> reducer)
            where TKey : IEquatable<TKey>
            where TRecord2 : IEquatable<TRecord2>
            where TOutput : IEquatable<TOutput>;

        /// <summary>
        /// Groups records from both input collections using the respective key selector, and applies the given reduction function.
        /// </summary>
        /// <typeparam name="TKey">The key type.</typeparam>
        /// <typeparam name="TValue1">The type of intermediate values stored for this collection.</typeparam>
        /// <typeparam name="TValue2">The type of intermediate values stored from the <paramref name="other"/> collection.</typeparam>
        /// <typeparam name="TRecord2">The type of records in the <paramref name="other"/> collection.</typeparam>
        /// <typeparam name="TOutput">The result type.</typeparam>
        /// <param name="other">The other collection.</param>
        /// <param name="keySelector1">The key selector applied to records in this collection.</param>
        /// <param name="keySelector2">The key selector applied to records in the <paramref name="other"/> collection.</param>
        /// <param name="valueSelector1">Function that transforms a record in this collection to the intermediate value that is stored for each record.</param>
        /// <param name="valueSelector2">Function that transforms a record in the <paramref name="other"/> collection to the intermediate value that is stored for each record.</param>
        /// <param name="reducer">Function that transforms two sequences of weighted intermediate values from each input collection to a sequence of weighted output records.</param>
        /// <returns>The collection of output records for each group in either input collection.</returns>
        /// <remarks>This overload can reduce the amount of storage required compared to <see cref="CoGroupBy{TKey,TRecord2,TOutput}(Collection{TRecord2,TTime},Expression{Func{TRecord,TKey}},Expression{Func{TRecord2,TKey}},Expression{Func{TKey,IEnumerable{Weighted{TRecord}},IEnumerable{Weighted{TRecord2}},IEnumerable{Weighted{TOutput}}}})"/> in cases
        /// where the intermediate values are small relative to each original record.
        /// 
        /// This overload can reduce the amount of computation required compared to the <see cref="CoGroupBy{TKey,TValue1,TValue2,TRecord2,TOutput}(Collection{TRecord2,TTime},Expression{Func{TRecord,TKey}},Expression{Func{TRecord2,TKey}},Expression{Func{TRecord,TValue1}},Expression{Func{TRecord2,TValue2}},Expression{Func{TKey,IEnumerable{TValue1},IEnumerable{TValue2},IEnumerable{TOutput}}})"/> overload, by compressing multiple instances of the
        /// same value into a single <see cref="Weighted{TValue1}"/> value.
        /// </remarks>
        Collection<TOutput, TTime> CoGroupBy<TKey, TValue1, TValue2, TRecord2, TOutput>(Collection<TRecord2, TTime> other, Expression<Func<TRecord, TKey>> keySelector1, Expression<Func<TRecord2, TKey>> keySelector2, Expression<Func<TRecord, TValue1>> valueSelector1, Expression<Func<TRecord2, TValue2>> valueSelector2, Expression<Func<TKey, IEnumerable<Weighted<TValue1>>, IEnumerable<Weighted<TValue2>>, IEnumerable<Weighted<TOutput>>>> reducer)
            where TKey : IEquatable<TKey>
            where TValue1 : IEquatable<TValue1>
            where TValue2 : IEquatable<TValue2>
            where TRecord2 : IEquatable<TRecord2>
            where TOutput : IEquatable<TOutput>;

        /// <summary>
        /// Groups records from both input collections using the respective key selector, and applies the given reduction function.
        /// </summary>
        /// <typeparam name="TKey">The key type.</typeparam>
        /// <typeparam name="TRecord2">The type of records in the <paramref name="other"/> collection.</typeparam>
        /// <typeparam name="TOutput">The result type.</typeparam>
        /// <param name="other">The other collection.</param>
        /// <param name="keySelector1">The key selector applied to records in this collection.</param>
        /// <param name="keySelector2">The key selector applied to records in the <paramref name="other"/> collection.</param>
        /// <param name="reducer">Function that transforms two sequences of weighted records from each input collection to a sequence of weighted output records.</param>
        /// <returns>The collection of output records for each group in either input collection.</returns>
        /// <remarks>The <see cref="CoGroupBy{TKey,TValue1,TValue2,TRecord2,TOutput}(Collection{TRecord2,TTime},Expression{Func{TRecord,TKey}},Expression{Func{TRecord2,TKey}},Expression{Func{TRecord,TValue1}},Expression{Func{TRecord2,TValue2}},Expression{Func{TKey,IEnumerable{Weighted{TValue1}},IEnumerable{Weighted{TValue2}},IEnumerable{Weighted{TOutput}}}})"/>
        /// overload can reduce the amount of storage required compared to this overload in cases
        /// where the intermediate values are small relative to each original record.
        /// 
        /// This overload can reduce the amount of computation required compared to the <see cref="CoGroupBy{TKey,TRecord2,TOutput}(Collection{TRecord2,TTime},Expression{Func{TRecord,TKey}},Expression{Func{TRecord2,TKey}},Expression{Func{TKey,IEnumerable{TRecord},IEnumerable{TRecord2},IEnumerable{TOutput}}})"/> overload, by compressing multiple instances of the
        /// same value into a single <see cref="Weighted{TRecord}"/> value.
        /// </remarks>
        Collection<TOutput, TTime> CoGroupBy<TKey, TRecord2, TOutput>(Collection<TRecord2, TTime> other, Expression<Func<TRecord, TKey>> keySelector1, Expression<Func<TRecord2, TKey>> keySelector2, Expression<Func<TKey, IEnumerable<Weighted<TRecord>>, IEnumerable<Weighted<TRecord2>>, IEnumerable<Weighted<TOutput>>>> reducer)
            where TKey : IEquatable<TKey>
            where TRecord2 : IEquatable<TRecord2>
            where TOutput : IEquatable<TOutput>;
        #endregion GroupBy/CoGroupBy

        #region Join
        /// <summary>
        /// Joins this collection with the <paramref name="other"/> collection, using the respective key selectors.
        /// </summary>
        /// <typeparam name="TKey">The key type.</typeparam>
        /// <typeparam name="TRecord2">The type of records in the <paramref name="other"/> collection.</typeparam>
        /// <typeparam name="TOutput">The result type.</typeparam>
        /// <param name="other">The other collection.</param>
        /// <param name="keySelector1">The key selector applied to records in this collection.</param>
        /// <param name="keySelector2">The key selector applied to records in the <paramref name="other"/> collection.</param>
        /// <param name="resultSelector">Function that transforms records with matching keys to an output record.</param>
        /// <returns>The collection of output records.</returns>
        Collection<TOutput, TTime> Join<TKey, TRecord2, TOutput>(Collection<TRecord2, TTime> other, Expression<Func<TRecord, TKey>> keySelector1, Expression<Func<TRecord2, TKey>> keySelector2, Expression<Func<TRecord, TRecord2, TOutput>> resultSelector)
            where TKey : IEquatable<TKey>
            where TRecord2 : IEquatable<TRecord2>
            where TOutput : IEquatable<TOutput>;

        /// <summary>
        /// Joins this collection with the <paramref name="other"/> collection, using the respective key selectors.
        /// </summary>
        /// <typeparam name="TKey">The key type.</typeparam>
        /// <typeparam name="TValue1">The type of intermediate values stored for this collection.</typeparam>
        /// <typeparam name="TValue2">The type of intermediate values stored from the <paramref name="other"/> collection.</typeparam>
        /// <typeparam name="TRecord2">The type of records in the <paramref name="other"/> collection.</typeparam>
        /// <typeparam name="TOutput">The result type.</typeparam>
        /// <param name="other">The other collection.</param>
        /// <param name="keySelector1">The key selector applied to records in this collection.</param>
        /// <param name="keySelector2">The key selector applied to records in the <paramref name="other"/> collection.</param>
        /// <param name="valueSelector1">Function that transforms a record in this collection to the intermediate value that is stored for each record.</param>
        /// <param name="valueSelector2">Function that transforms a record in the <paramref name="other"/> collection to the intermediate value that is stored for each record.</param>
        /// <param name="resultSelector">Function that transforms intermediate values from records with matching keys to an output record.</param>
        /// <returns>The collection of output records.</returns>
        Collection<TOutput, TTime> Join<TKey, TValue1, TValue2, TRecord2, TOutput>(Collection<TRecord2, TTime> other, Expression<Func<TRecord, TKey>> keySelector1, Expression<Func<TRecord2, TKey>> keySelector2, Expression<Func<TRecord, TValue1>> valueSelector1, Expression<Func<TRecord2, TValue2>> valueSelector2, Expression<Func<TKey, TValue1, TValue2, TOutput>> resultSelector)
            where TKey : IEquatable<TKey>
            where TValue1 : IEquatable<TValue1>
            where TValue2 : IEquatable<TValue2>
            where TRecord2 : IEquatable<TRecord2>
            where TOutput : IEquatable<TOutput>;

        /// <summary>
        /// Joins this collection with the <paramref name="other"/> collection, using the respective integer-valued key selectors.
        /// </summary>
        /// <typeparam name="TValue1">The type of intermediate values stored for this collection.</typeparam>
        /// <typeparam name="TValue2">The type of intermediate values stored from the <paramref name="other"/> collection.</typeparam>
        /// <typeparam name="TRecord2">The type of records in the <paramref name="other"/> collection.</typeparam>
        /// <typeparam name="TOutput">The result type.</typeparam>
        /// <param name="other">The other collection.</param>
        /// <param name="keySelector1">The key selector applied to records in this collection.</param>
        /// <param name="keySelector2">The key selector applied to records in the <paramref name="other"/> collection.</param>
        /// <param name="valueSelector1">Function that transforms a record in this collection to the intermediate value that is stored for each record.</param>
        /// <param name="valueSelector2">Function that transforms a record in the <paramref name="other"/> collection to the intermediate value that is stored for each record.</param>
        /// <param name="resultSelector">Function that transforms intermediate values from records with matching keys to an output record.</param>
        /// <param name="useDenseIntKeys">If <c>true</c>, use optimizations for dense-valued keys, otherwise treat keys as sparse.</param>
        /// <returns>The collection of output records.</returns>
        /// <remarks>
        /// This overload is specialized for collections with integer keys. If <paramref name="useDenseIntKeys"/> is <c>true</c>,
        /// the implementation uses a further specialization that exploits the dense nature of the keys.
        /// </remarks>
        Collection<TOutput, TTime> Join<TValue1, TValue2, TRecord2, TOutput>(Collection<TRecord2, TTime> other, Expression<Func<TRecord, Int32>> keySelector1, Expression<Func<TRecord2, Int32>> keySelector2, Expression<Func<TRecord, TValue1>> valueSelector1, Expression<Func<TRecord2, TValue2>> valueSelector2, Expression<Func<Int32, TValue1, TValue2, TOutput>> resultSelector, bool useDenseIntKeys)
            where TValue1 : IEquatable<TValue1>
            where TValue2 : IEquatable<TValue2>
            where TRecord2 : IEquatable<TRecord2>
            where TOutput : IEquatable<TOutput>;

        #endregion Join

        #region Data-parallel aggregations

        /// <summary>
        /// Groups records using the supplied key selector, and applies the given aggregation function.
        /// </summary>
        /// <typeparam name="TKey">The key type.</typeparam>
        /// <typeparam name="TValue">The intermediate value type.</typeparam>
        /// <typeparam name="TOutput">The result type.</typeparam>
        /// <param name="keySelector">Function that extracts a key from each record.</param>
        /// <param name="valueSelector">Function that transforms a record to the intermediate value that is stored for each record.</param>
        /// <param name="resultSelector">Function that transforms a key and aggregate value to an output record.</param>
        /// <param name="axpy">A function that multiplies the first argument by the second and adds the third (cf. SAXPY).</param>
        /// <param name="isZeroPredicate">A predicate that returns <c>true</c> if and only if the given value is zero.</param>
        /// <returns>The collection of output records.</returns>
        Collection<TOutput, TTime> Aggregate<TKey, TValue, TOutput>(Expression<Func<TRecord, TKey>> keySelector, Expression<Func<TRecord, TValue>> valueSelector, Expression<Func<Int64, TValue, TValue, TValue>> axpy, Expression<Func<TValue, bool>> isZeroPredicate, Expression<Func<TKey, TValue, TOutput>> resultSelector)
            where TKey : IEquatable<TKey>
            where TOutput : IEquatable<TOutput>
            where TValue : IEquatable<TValue>;

        /// <summary>
        /// Groups records using the supplied key selector, and counts the number of records in each group.
        /// </summary>
        /// <typeparam name="TKey">The key type.</typeparam>
        /// <param name="keySelector">Function that extracts a key from each record.</param>
        /// <returns>The collection of pairs of keys and the respective counts for each group.</returns>
        Collection<Pair<TKey, Int64>, TTime> Count<TKey>(Expression<Func<TRecord, TKey>> keySelector)
            where TKey : IEquatable<TKey>;

        /// <summary>
        /// Groups records using the supplied key selector, and counts the number of records in each group.
        /// </summary>
        /// <typeparam name="TKey">The key type.</typeparam>
        /// <typeparam name="TOutput">The output type.</typeparam>
        /// <param name="keySelector">Function that extracts a key from each record.</param>
        /// <param name="resultSelector">Function that transforms a key and count to an output record.</param>
        /// <returns>The collection of output records.</returns>
        Collection<TOutput, TTime> Count<TKey, TOutput>(Expression<Func<TRecord, TKey>> keySelector, Expression<Func<TKey, Int64, TOutput>> resultSelector)
            where TKey : IEquatable<TKey>
            where TOutput : IEquatable<TOutput>;

        /// <summary>
        /// Groups records using the supplied key selector, and computes the sum of the records in each group.
        /// </summary>
        /// <typeparam name="TKey">The key type.</typeparam>
        /// <typeparam name="TOutput">The output type.</typeparam>
        /// <param name="keySelector">Function that extracts a key from each record.</param>
        /// <param name="valueSelector">Function that extracts the integer value to be summed each record.</param>
        /// <param name="resultSelector">Function that transforms a key and count to an output record.</param>
        /// <returns>The collection of output records.</returns>
        Collection<TOutput, TTime> Sum<TKey, TOutput>(Expression<Func<TRecord, TKey>> keySelector, Expression<Func<TRecord, int>> valueSelector, Expression<Func<TKey, int, TOutput>> resultSelector)
            where TKey : IEquatable<TKey>
            where TOutput : IEquatable<TOutput>;

        /// <summary>
        /// Groups records using the supplied key selector, and computes the sum of the records in each group.
        /// </summary>
        /// <typeparam name="TKey">The key type.</typeparam>
        /// <typeparam name="TOutput">The output type.</typeparam>
        /// <param name="keySelector">Function that extracts a key from each record.</param>
        /// <param name="valueSelector">Function that extracts the long value to be summed each record.</param>
        /// <param name="resultSelector">Function that transforms a key and count to an output record.</param>
        /// <returns>The collection of output records.</returns>
        Collection<TOutput, TTime> Sum<TKey, TOutput>(Expression<Func<TRecord, TKey>> keySelector, Expression<Func<TRecord, Int64>> valueSelector, Expression<Func<TKey, Int64, TOutput>> resultSelector)
            where TKey : IEquatable<TKey>
            where TOutput : IEquatable<TOutput>;

        /// <summary>
        /// Groups records using the supplied key selector, and computes the sum of the records in each group.
        /// </summary>
        /// <typeparam name="TKey">The key type.</typeparam>
        /// <typeparam name="TOutput">The output type.</typeparam>
        /// <param name="keySelector">Function that extracts a key from each record.</param>
        /// <param name="valueSelector">Function that extracts the floating-point value to be summed each record.</param>
        /// <param name="resultSelector">Function that transforms a key and count to an output record.</param>
        /// <returns>The collection of output records.</returns>
        Collection<TOutput, TTime> Sum<TKey, TOutput>(Expression<Func<TRecord, TKey>> keySelector, Expression<Func<TRecord, float>> valueSelector, Expression<Func<TKey, float, TOutput>> resultSelector)
            where TKey : IEquatable<TKey>
            where TOutput : IEquatable<TOutput>;

        /// <summary>
        /// Groups records using the supplied key selector, and computes the sum of the records in each group.
        /// </summary>
        /// <typeparam name="TKey">The key type.</typeparam>
        /// <typeparam name="TOutput">The output type.</typeparam>
        /// <param name="keySelector">Function that extracts a key from each record.</param>
        /// <param name="valueSelector">Function that extracts the 64-bit floating-point value to be summed each record.</param>
        /// <param name="resultSelector">Function that transforms a key and count to an output record.</param>
        /// <returns>The collection of output records.</returns>
        Collection<TOutput, TTime> Sum<TKey, TOutput>(Expression<Func<TRecord, TKey>> keySelector, Expression<Func<TRecord, double>> valueSelector, Expression<Func<TKey, double, TOutput>> resultSelector)
            where TKey : IEquatable<TKey>
            where TOutput : IEquatable<TOutput>;

        /// <summary>
        /// Groups records using the supplied key selector, and computes the minimum value in each group.
        /// </summary>
        /// <typeparam name="TKey">The key type.</typeparam>
        /// <typeparam name="TComparable">The type of values to be used for comparison.</typeparam>
        /// <param name="keySelector">Function that extracts a key from each record.</param>
        /// <param name="comparableSelector">Function that extracts the portion of a record to be used in the comparison.</param>
        /// <returns>The collection of minimum-valued records in each group.</returns>
        Collection<TRecord, TTime> Min<TKey,TComparable>(Expression<Func<TRecord, TKey>> keySelector, Expression<Func<TRecord, TComparable>> comparableSelector)
            where TKey : IEquatable<TKey>
            where TComparable : IEquatable<TComparable>, IComparable<TComparable>;

        /// <summary>
        /// Groups records using the supplied key selector, and computes the minimum value in each group.
        /// </summary>
        /// <typeparam name="TKey">The key type.</typeparam>
        /// <typeparam name="TValue">The intermediate value type.</typeparam>
        /// <typeparam name="TComparable">The type of values to be used for comparison.</typeparam>
        /// <param name="keySelector">Function that extracts a key from each record.</param>
        /// <param name="valueSelector">Function that transforms a record to the intermediate value that is stored for each record.</param>
        /// <param name="comparableSelector">Function that extracts the portion of a key-value pair to be used in the comparison.</param>
        /// <param name="resultSelector">Function that transforms a key and the minimum value to an output record.</param>
        /// <returns>The collection of output records.</returns>
        Collection<TRecord, TTime> Min<TKey, TComparable, TValue>(Expression<Func<TRecord, TKey>> keySelector, Expression<Func<TRecord, TValue>> valueSelector, Expression<Func<TKey,TValue,TComparable>> comparableSelector, Expression<Func<TKey, TValue, TRecord>> resultSelector)
            where TKey : IEquatable<TKey>
            where TValue : IEquatable<TValue>
            where TComparable : IEquatable<TComparable>, IComparable<TComparable>;


        /// <summary>
        /// Groups records using the supplied integer-valued key selector, and computes the minimum value in each group.
        /// </summary>
        /// <typeparam name="TValue">The intermediate value type.</typeparam>
        /// <typeparam name="TComparable">The type of values to be used for comparison.</typeparam>
        /// <param name="keySelector">Function that extracts an integer-valued key from each record.</param>
        /// <param name="valueSelector">Function that transforms a record to the intermediate value that is stored for each record.</param>
        /// <param name="comparableSelector">Function that extracts the portion of a key-value pair to be used in the comparison.</param>
        /// <param name="resultSelector">Function that transforms a key and the minimum value to an output record.</param>
        /// <param name="useDenseIntKeys">If <c>true</c>, use optimizations for dense-valued keys, otherwise treat keys as sparse.</param>
        /// <returns>The collection of output records.</returns>
        Collection<TRecord, TTime> Min<TValue, TComparable>(Expression<Func<TRecord, int>> keySelector, Expression<Func<TRecord, TValue>> valueSelector, Expression<Func<int, TValue, TComparable>> comparableSelector, Expression<Func<int, TValue, TRecord>> resultSelector, bool useDenseIntKeys)
            where TValue : IEquatable<TValue>
            where TComparable : IEquatable<TComparable>, IComparable<TComparable>;

        /// <summary>
        /// Groups records using the supplied key selector, and computes the maximum value in each group.
        /// </summary>
        /// <typeparam name="TKey">The key type.</typeparam>
        /// <typeparam name="TComparable">The type of values to be used for comparison.</typeparam>
        /// <param name="keySelector">Function that extracts a key from each record.</param>
        /// <param name="comparableSelector">Function that extracts the portion of a record to be used in the comparison.</param>
        /// <returns>The collection of maximum-valued records in each group.</returns>
        Collection<TRecord, TTime> Max<TKey, TComparable>(Expression<Func<TRecord, TKey>> keySelector, Expression<Func<TRecord, TComparable>> comparableSelector)
            where TKey : IEquatable<TKey>
            where TComparable : IComparable<TComparable>;

        /// <summary>
        /// Groups records using the supplied key selector, and computes the maximum value in each group.
        /// </summary>
        /// <typeparam name="TKey">The key type.</typeparam>
        /// <typeparam name="TValue">The intermediate value type.</typeparam>
        /// <typeparam name="TComparable">The type of values to be used for comparison.</typeparam>
        /// <param name="keySelector">Function that extracts a key from each record.</param>
        /// <param name="valueSelector">Function that transforms a record to the intermediate value that is stored for each record.</param>
        /// <param name="comparableSelector">Function that extracts the portion of a key-value pair to be used in the comparison.</param>
        /// <param name="resultSelector">Function that transforms a key and the maximum value to an output record.</param>
        /// <returns>The collection of output records.</returns>
        Collection<TRecord, TTime> Max<TKey, TComparable, TValue>(Expression<Func<TRecord, TKey>> keySelector, Expression<Func<TRecord, TValue>> valueSelector, Expression<Func<TKey, TValue, TComparable>> comparableSelector, Expression<Func<TKey, TValue, TRecord>> resultSelector)
            where TKey : IEquatable<TKey>
            where TComparable : IComparable<TComparable>
            where TValue : IEquatable<TValue>;

        #endregion Data-parallel aggregations

        #region MultiSet operations
        
        /// <summary>
        /// Computes the set of distinct records in this collection.
        /// </summary>
        /// <returns>The collection of distinct records.</returns>
        Collection<TRecord, TTime> Distinct();

        /// <summary>
        /// Computes the multiset union of this collection and the <paramref name="other"/> collection.
        /// </summary>
        /// <param name="other">The other collection.</param>
        /// <returns>The collection containing the multiset union of the two input collections.</returns>
        /// <remarks>The multiset union contains each record in either this collection or the <paramref name="other"/> collection.
        /// For each record in either collection, the multiplicity of that record in the multiset union will be the greater of its
        /// multiplicities in either collection.
        /// 
        /// The union operator is stateful. If precise multiplicities are not important (e.g. because the output feeds into a <see cref="Distinct"/>,
        /// <see cref="Aggregate"/>, or similarly idempotent operator), the <see cref="Concat"/> operator is a more efficient substitute.
        /// </remarks>
        Collection<TRecord, TTime> Union(Collection<TRecord, TTime> other);

        /// <summary>
        /// Computes the multiset intersection of this collection and the <paramref name="other"/> collection.
        /// </summary>
        /// <param name="other">The other collection.</param>
        /// <returns>The collection containing the multiset intersection of the two input collections.</returns>
        /// <remarks>The multiset union contains each record in both this collection and the <paramref name="other"/> collection.
        /// For each record in both collections, the multiplicity of that record in the multiset intersection will be the lesser of its
        /// multiplicities in either collection.
        /// </remarks>
        Collection<TRecord, TTime> Intersect(Collection<TRecord, TTime> other);

        /// <summary>
        /// Computes the multiset symmetric difference of this collection and the <paramref name="other"/> collection.
        /// </summary>
        /// <param name="other">The other collection.</param>
        /// <returns>The collection containing the multiset symmetric difference of the two input collections.</returns>
        /// <remarks>The multiset symmetric difference contains each record that is in the <see cref="Union"/>, but not in the <see cref="Intersect"/>
        /// of this collection and the <paramref name="other"/> collection. For each record in either collection, the multiplicity of that record in the
        /// multiset symmetric difference is the absolute value of the difference between its multiplicities in either collection.</remarks>
        Collection<TRecord, TTime> SymmetricDifference(Collection<TRecord, TTime> other);

        /// <summary>
        /// Computes the concatenation of this collection and the <paramref name="other"/> collection.
        /// </summary>
        /// <param name="other">The other collection.</param>
        /// <returns>The collection containing all records in the two input collections.</returns>
        /// <remarks>This operator is stateless. As such, it can be a more efficient alternative to the <see cref="Union"/> operator, when
        /// precise multiplicities are not important (e.g. because the output feeds into a <see cref="Distinct"/>,
        /// <see cref="Aggregate"/>, or similarly idempotent operator).
        /// </remarks>
        Collection<TRecord, TTime> Concat(Collection<TRecord, TTime> other);

        /// <summary>
        /// Computes the multiset difference of this collection minus the <paramref name="other"/> collection.
        /// </summary>
        /// <param name="other">The other collection.</param>
        /// <returns>The collection containing all records in this collection that are not in the <paramref name="other"/> collection.</returns>
        /// <remarks>The behavior of this operator is undefined when the <paramref name="other"/> collection contains records that are not 
        /// in this collection.</remarks>
        Collection<TRecord, TTime> Except(Collection<TRecord, TTime> other);

        #endregion MultiSet operations

        #region Fixed Point

        /// <summary>
        /// Enables this collection to be used as a constant in the given loop context.
        /// </summary>
        /// <param name="context">The loop context.</param>
        /// <returns>This collection, where each timestamp has an additional loop counter.</returns>
        Collection<TRecord, IterationIn<TTime>> EnterLoop(Microsoft.Research.Naiad.Dataflow.Iteration.LoopContext<TTime> context);

        /// <summary>
        /// Enables this collection to be used in the given loop context, where each record
        /// may be introduced at a different iteration.
        /// </summary>
        /// <param name="context">The loop context.</param>
        /// <param name="iterationSelector">Function that maps an input record to the iteration at which that record should be introduced.</param>
        /// <returns>This collection, where each timestamp has an addition loop counter.</returns>
        Collection<TRecord, IterationIn<TTime>> EnterLoop(Microsoft.Research.Naiad.Dataflow.Iteration.LoopContext<TTime> context, Func<TRecord, int> iterationSelector);

        /// <summary>
        /// Computes the fixed point of the subquery <paramref name="f"/> applied to this collection.
        /// </summary>
        /// <typeparam name="TKey">The key type.</typeparam>
        /// <param name="f">The subquery to apply iteratively.</param>
        /// <param name="iterationSelector">Function that maps an input record to the iteration at which that record should be introduced.</param>
        /// <param name="keySelector">Function that extracts a key from each record, to be used for partitioning the input collection.</param>
        /// <param name="maxIterations">The maximum number of iterations to compute.</param>
        /// <returns>The result of applying a subquery g to this collection <paramref name="maxIterations"/> times,
        /// where g^{i+1} = f(g^i + input.EnterLoop(iterationSelector)^i).</returns>
        Collection<TRecord, TTime> GeneralFixedPoint<TKey>(
            Func<Microsoft.Research.Naiad.Dataflow.Iteration.LoopContext<TTime>, Collection<TRecord, IterationIn<TTime>>, Collection<TRecord, IterationIn<TTime>>> f, // (lc, initial, x) => f(x)
            Func<TRecord, int> iterationSelector,
            Expression<Func<TRecord, TKey>> keySelector,
            int maxIterations);

        /// <summary>
        /// Computes the fixed point of the subquery <paramref name="f"/> applied to this collection.
        /// </summary>
        /// <param name="f">The subquery to apply iteratively.</param>
        /// <returns>The result of applying <paramref name="f"/> to this collection until reaching fixed point.</returns>
        Collection<TRecord, TTime> FixedPoint(
            Func<Microsoft.Research.Naiad.Dataflow.Iteration.LoopContext<TTime>,
            Collection<TRecord, IterationIn<TTime>>, Collection<TRecord, IterationIn<TTime>>> f);

        /// <summary>
        /// Computes the fixed point of the subquery <paramref name="f"/> applied to this collection.
        /// </summary>
        /// <param name="f">The subquery to apply iteratively.</param>
        /// <param name="maxIterations">The maximum number of iterations to compute.</param>
        /// <returns>The result of applying <paramref name="f"/> to this collection <paramref name="maxIterations"/> times, or until fixed point is reached, whichever is earlier.</returns>
        Collection<TRecord, TTime> FixedPoint(
            Func<Microsoft.Research.Naiad.Dataflow.Iteration.LoopContext<TTime>,
            Collection<TRecord, IterationIn<TTime>>, Collection<TRecord, IterationIn<TTime>>> f, int maxIterations);

        /// <summary>
        /// Computes the fixed point of the subquery <paramref name="f"/> applied to this collection.
        /// </summary>
        /// <typeparam name="TKey">The key type.</typeparam>
        /// <param name="f">The subquery to apply iteratively.</param>
        /// <param name="keySelector">Function that extracts a key from each record, to be used for partitioning the input collection.</param>
        /// <returns>The result of applying <paramref name="f"/> to this collection until fixed point is reached.</returns>
        Collection<TRecord, TTime> FixedPoint<TKey>(
            Func<Microsoft.Research.Naiad.Dataflow.Iteration.LoopContext<TTime>,
            Collection<TRecord, IterationIn<TTime>>, Collection<TRecord, IterationIn<TTime>>> f, Expression<Func<TRecord, TKey>> keySelector);

        /// <summary>
        /// Computes the fixed point of the subquery <paramref name="f"/> applied to this collection.
        /// </summary>
        /// <typeparam name="TKey">The key type.</typeparam>
        /// <param name="f">The subquery to apply iteratively.</param>
        /// <param name="keySelector">Function that extracts a key from each record, to be used for partitioning the input collection.</param>
        /// <param name="maxIterations">The maximum number of iterations to compute.</param>
        /// <returns>The result of applying <paramref name="f"/> to this collection <paramref name="maxIterations"/> times, or until fixed point is reached, whichever is earlier.</returns>
        Collection<TRecord, TTime> FixedPoint<TKey>(
            Func<Microsoft.Research.Naiad.Dataflow.Iteration.LoopContext<TTime>,
            Collection<TRecord, IterationIn<TTime>>, Collection<TRecord, IterationIn<TTime>>> f, Expression<Func<TRecord, TKey>> keySelector, int maxIterations);
        #endregion Fixed Point

        #region Monitoring
        /// <summary>
        /// Applies the given action to the records in this collection once for each timestamp on each worker, for monitoring.
        /// </summary>
        /// <param name="action">The action to apply to each group of records, and the respective worker index.</param>
        /// <returns>The input collection.</returns>
        Collection<TRecord, TTime> Monitor(Action<int, List<Pair<Weighted<TRecord>, TTime>>> action);
        #endregion Monitoring

        #endregion Naiad operators

        #region Collection decorations

#if false

        Collection<R, T> WithSpilling();
        Collection<R, T> WithDownstreamPlacement(Placement downstreamPlacement);

#endif
        #endregion


    }

    /// <summary>
    /// A <see cref="Microsoft.Research.Naiad.Frameworks.DifferentialDataflow.Collection{TRecord,Epoch}"/> that can be modified by adding or removing records.
    /// </summary>
    /// <typeparam name="TRecord">The type of records in this collection.</typeparam>
    public interface InputCollection<TRecord> : Collection<TRecord, Epoch>, IObserver<IEnumerable<Weighted<TRecord>>>
        where TRecord : IEquatable<TRecord>
    {
        /// <summary>
        /// Introduces a batch of <paramref name="values"/> to this collection in a new <see cref="Epoch"/>, and signals that no more records will be added to or removed from this collection.
        /// </summary>
        /// <param name="values">The records to be added or removed.</param>
        void OnCompleted(IEnumerable<Weighted<TRecord>> values);

        /// <summary>
        /// Introduces a batch of <paramref name="values"/> to this collection in a new <see cref="Epoch"/>.
        /// </summary>
        /// <param name="values">The records to be added or removed.</param>
        new void OnNext(IEnumerable<Weighted<TRecord>> values);

        /// <summary>
        /// Signals that no more records will be added to or removed from this collection.
        /// </summary>
        new void OnCompleted();

        //void OnNext(IEnumerable<Weighted<R>> value);        
        //void OnCompleted();
        //void OnError(Exception error);
    }
}
