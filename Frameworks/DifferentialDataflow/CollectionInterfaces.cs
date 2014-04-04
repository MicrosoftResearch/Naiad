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
using System.IO;

using Microsoft.Research.Naiad.DataStructures;
using Microsoft.Research.Naiad.Dataflow.Channels;
using Microsoft.Research.Naiad.Scheduling;
using Microsoft.Research.Naiad;
using Microsoft.Research.Naiad.Dataflow;

namespace Microsoft.Research.Naiad.Frameworks.DifferentialDataflow
{
    /// <summary>
    /// The methods on a Collection that are exposed to the user program
    /// </summary>
    public interface Collection<R, T>
        where R : IEquatable<R>
        where T : Time<T>
    {
        #region Naiad operators

        Stream<Weighted<R>, T> Output { get; }

        Collection<R, T> PartitionBy<K>(Expression<Func<R, K>> partitionFunction);

        #region Consolidation

        /// <summary>
        /// Consolidates representation so that each record occurs with at most one weight.
        /// </summary>
        /// <returns>Identical collection as input, but consolidated.</returns>
        Collection<R, T> Consolidate();

        /// <summary>
        /// Consolidates representation so that each record occurs with at most one weight.
        /// </summary>
        /// <param name="partitionFunction">Function by which the data should be partitioned</param>
        /// <returns>Identical collection as input, but consolidated.</returns>
        Collection<R, T> Consolidate<K>(Expression<Func<R, K>> partitionFunction);

        #endregion

        #region Lattice adjustment
        /// <summary>
        /// EXPERIMENTAL: Applies an arbitrary function to the lattice coordinate, under the requirement that the element only advance.
        /// </summary>
        /// <param name="transformation">lattice transformation</param>
        /// <returns>It is a mystery.</returns>
        Collection<R, T> AdjustLattice(Func<R, T, T> transformation);
        #endregion Lattice adjustment

        #region Select/Where/SelectMany

        /// <summary>
        /// Applies input function selector to each record.
        /// </summary>
        /// <typeparam name="R2">Result</typeparam>
        /// <param name="selector">Function from input records to output records.</param>
        /// <returns>Collection of transformed records.</returns>
        Collection<R2, T> Select<R2>(Expression<Func<R, R2>> selector)
            where R2 : IEquatable<R2>;

        /// <summary>
        /// Filters collection by the supplied predicate.
        /// </summary>
        /// <param name="predicate">Indicates whether a record will be kept.</param>
        /// <returns>Collection of the subset of records satisfying predicate.</returns>
        Collection<R, T> Where(Expression<Func<R, bool>> predicate);

        /// <summary>
        /// Applies input function selector to each record and flattens the result.
        /// </summary>
        /// <typeparam name="R2">Result</typeparam>
        /// <param name="selector">Function from input records to lists of output records.</param>
        /// <returns>Concatenation of application of selector to each input record.</returns>
        Collection<R2, T> SelectMany<R2>(Expression<Func<R, IEnumerable<R2>>> selector)
            where R2 : IEquatable<R2>;

        Collection<R2, T> SelectMany<R2>(Expression<Func<R, IEnumerable<ArraySegment<R2>>>> selector)
            where R2 : IEquatable<R2>;

        #endregion Select/Where/SelectMany

        #region GroupBy/CoGroupBy
        /// <summary>
        /// Groups records by supplied key, and applies reduction function.
        /// </summary>
        /// <typeparam name="K">Key</typeparam>
        /// <typeparam name="V">Intermediate</typeparam>
        /// <typeparam name="R2">Result</typeparam>
        /// <param name="key">Indicates group of input record</param>
        /// <param name="selector">Transforms record to intermediate value</param>
        /// <param name="reducer">Transforms collection of intermediate values to collection of results</param>
        /// <returns>Collection containing concatenation of reducer applied to the collation of each group in input collection.</returns>
        Collection<R2, T> GroupBy<K, V, R2>(Expression<Func<R, K>> key, Expression<Func<R, V>> selector, Func<K, IEnumerable<V>, IEnumerable<R2>> reducer)
            where K : IEquatable<K>
            where V : IEquatable<V>
            where R2 : IEquatable<R2>;

        /// <summary>
        /// Groups records by supplied key, and applies reduction function.
        /// </summary>
        /// <typeparam name="K">Key</typeparam>
        /// <typeparam name="R2">Result</typeparam>
        /// <param name="key">Indicates group of input record</param>
        /// <param name="reducer">Transforms collection of intermediate values to collection of results</param>
        /// <returns>Collection containing concatenation of reducer applied to the collation of each group in input collection.</returns>
        Collection<R2, T> GroupBy<K, R2>(Expression<Func<R, K>> key, Func<K, IEnumerable<R>, IEnumerable<R2>> reducer)
            where K : IEquatable<K>
            where R2 : IEquatable<R2>;

        /// <summary>
        /// Groups records by supplied key, and applies reduction function to corresponding pairs of groups if either is non-empty.
        /// </summary>
        /// <typeparam name="K">Key</typeparam>
        /// <typeparam name="V1">Value1</typeparam>
        /// <typeparam name="V2">Value2</typeparam>
        /// <typeparam name="R2">Input</typeparam>
        /// <typeparam name="R3">Result</typeparam>
        /// <param name="other">Other collection</param>
        /// <param name="key1">First key function</param>
        /// <param name="key2">Second key function</param>
        /// <param name="selector1">First value selector</param>
        /// <param name="selector2">Second value selector</param>
        /// <param name="reducer">Result selector</param>
        /// <returns>Collection of the concatenation of the reducer applied to each pair of non-empty groups.</returns>
        Collection<R3, T> CoGroupBy<K, V1, V2, R2, R3>(Collection<R2, T> other, Expression<Func<R, K>> key1, Expression<Func<R2, K>> key2, Expression<Func<R, V1>> selector1, Expression<Func<R2, V2>> selector2, Expression<Func<K, IEnumerable<V1>, IEnumerable<V2>, IEnumerable<R3>>> reducer)
            where K : IEquatable<K>
            where V1 : IEquatable<V1>
            where V2 : IEquatable<V2>
            where R2 : IEquatable<R2>
            where R3 : IEquatable<R3>;

        /// <summary>
        /// Groups records by supplied key, and applies reduction function to corresponding pairs of groups if either is non-empty.
        /// </summary>
        /// <typeparam name="K">Key</typeparam>
        /// <typeparam name="R2">Input</typeparam>
        /// <typeparam name="R3">Result</typeparam>
        /// <param name="other">Other collection</param>
        /// <param name="key1">First key function</param>
        /// <param name="key2">Second key function</param>
        /// <param name="reducer">Result selector</param>
        /// <returns>Collection of the concatenation of the reducer applied to each pair of non-empty groups.</returns>
        Collection<R3, T> CoGroupBy<K, R2, R3>(Collection<R2, T> other, Expression<Func<R, K>> key1, Expression<Func<R2, K>> key2, Expression<Func<K, IEnumerable<R>, IEnumerable<R2>, IEnumerable<R3>>> reducer)
            where K : IEquatable<K>
            where R2 : IEquatable<R2>
            where R3 : IEquatable<R3>;

        /// <summary>
        /// Groups records by supplied key, and applies reduction function to corresponding pairs of groups if either is non-empty.
        /// </summary>
        /// <typeparam name="K">Key</typeparam>
        /// <typeparam name="V1">Value1</typeparam>
        /// <typeparam name="V2">Value2</typeparam>
        /// <typeparam name="R2">Input</typeparam>
        /// <typeparam name="R3">Result</typeparam>
        /// <param name="other">Other collection</param>
        /// <param name="key1">First key function</param>
        /// <param name="key2">Second key function</param>
        /// <param name="selector1">First value selector</param>
        /// <param name="selector2">Second value selector</param>
        /// <param name="reducer">Result selector</param>
        /// <returns>Collection of the concatenation of the reducer applied to each pair of non-empty groups.</returns>
        Collection<R3, T> CoGroupBy<K, V1, V2, R2, R3>(Collection<R2, T> other, Expression<Func<R, K>> key1, Expression<Func<R2, K>> key2, Expression<Func<R, V1>> selector1, Expression<Func<R2, V2>> selector2, Expression<Func<K, IEnumerable<Weighted<V1>>, IEnumerable<Weighted<V2>>, IEnumerable<Weighted<R3>>>> reducer)
            where K : IEquatable<K>
            where V1 : IEquatable<V1>
            where V2 : IEquatable<V2>
            where R2 : IEquatable<R2>
            where R3 : IEquatable<R3>;

        /// <summary>
        /// Groups records by supplied key, and applies reduction function to corresponding pairs of groups if either is non-empty.
        /// </summary>
        /// <typeparam name="K">Key</typeparam>
        /// <typeparam name="R2">Input</typeparam>
        /// <typeparam name="R3">Result</typeparam>
        /// <param name="other">Other collection</param>
        /// <param name="key1">First key function</param>
        /// <param name="key2">Second key function</param>
        /// <param name="reducer">Result selector</param>
        /// <returns>Collection of the concatenation of the reducer applied to each pair of non-empty groups.</returns>
        Collection<R3, T> CoGroupBy<K, R2, R3>(Collection<R2, T> other, Expression<Func<R, K>> key1, Expression<Func<R2, K>> key2, Expression<Func<K, IEnumerable<Weighted<R>>, IEnumerable<Weighted<R2>>, IEnumerable<Weighted<R3>>>> reducer)
            where K : IEquatable<K>
            where R2 : IEquatable<R2>
            where R3 : IEquatable<R3>;
        #endregion GroupBy/CoGroupBy

        #region Join
        /// <summary>
        /// Joins two input collections.
        /// </summary>
        /// <typeparam name="K">Key</typeparam>
        /// <typeparam name="R2">Other</typeparam>
        /// <typeparam name="R3">Result</typeparam>
        /// <param name="other">Other collection</param>
        /// <param name="key1">first key function</param>
        /// <param name="key2">second key function</param>
        /// <param name="reducer">reduces each matching pair of records</param>
        /// <returns>Collection of reducer applied to all matching pairs from inputs.</returns>
        Collection<R3, T> Join<K, R2, R3>(Collection<R2, T> other, Expression<Func<R, K>> key1, Expression<Func<R2, K>> key2, Expression<Func<R, R2, R3>> reducer)
            where K : IEquatable<K>
            where R2 : IEquatable<R2>
            where R3 : IEquatable<R3>;

        /// <summary>
        /// Joins two input collections. Optional intermediate value selectors.
        /// </summary>
        /// <typeparam name="K">Key</typeparam>
        /// <typeparam name="V1">First intermediate</typeparam>
        /// <typeparam name="V2">Second intermediate</typeparam>
        /// <typeparam name="R2">Other</typeparam>
        /// <typeparam name="R3">Result</typeparam>
        /// <param name="other">Other collection</param>
        /// <param name="key1">first key function</param>
        /// <param name="key2">second key function</param>
        /// <param name="val1">first value selector</param>
        /// <param name="val2">second value selector</param>
        /// <param name="reducer">reduces each matching pair of intermediate values</param>
        /// <returns>Collection of reducer applied to all matching pairs from inputs.</returns>
        Collection<R3, T> Join<K, V1, V2, R2, R3>(Collection<R2, T> other, Expression<Func<R, K>> key1, Expression<Func<R2, K>> key2, Expression<Func<R, V1>> val1, Expression<Func<R2, V2>> val2, Expression<Func<K, V1, V2, R3>> reducer)
            where K : IEquatable<K>
            where V1 : IEquatable<V1>
            where V2 : IEquatable<V2>
            where R2 : IEquatable<R2>
            where R3 : IEquatable<R3>;

        /// <summary>
        /// Joins two input collections. Optional intermediate value selectors.
        /// </summary>
        /// <typeparam name="K">Key</typeparam>
        /// <typeparam name="V1">First intermediate</typeparam>
        /// <typeparam name="V2">Second intermediate</typeparam>
        /// <typeparam name="R2">Other</typeparam>
        /// <typeparam name="R3">Result</typeparam>
        /// <param name="other">Other collection</param>
        /// <param name="key1">first key function</param>
        /// <param name="key2">second key function</param>
        /// <param name="val1">first value selector</param>
        /// <param name="val2">second value selector</param>
        /// <param name="reducer">reduces each matching pair of intermediate values</param>
        /// <param name="useDenseIntKeys">set to enable arrays in place of dictionaries</param>
        /// <returns>Collection of reducer applied to all matching pairs from inputs.</returns>
        Collection<R3, T> Join<V1, V2, R2, R3>(Collection<R2, T> other, Expression<Func<R, Int32>> key1, Expression<Func<R2, Int32>> key2, Expression<Func<R, V1>> val1, Expression<Func<R2, V2>> val2, Expression<Func<Int32, V1, V2, R3>> reducer, bool useDenseIntKeys)
            where V1 : IEquatable<V1>
            where V2 : IEquatable<V2>
            where R2 : IEquatable<R2>
            where R3 : IEquatable<R3>;

        #endregion Join

        #region Data-parallel aggregations

        /// <summary>
        /// Key-wise aggregation using supplied combiner.
        /// </summary>
        /// <typeparam name="K">Key</typeparam>
        /// <typeparam name="V">Value</typeparam>
        /// <typeparam name="R2">Result</typeparam>
        /// <param name="key">key selector</param>
        /// <param name="value">value selector</param>
        /// <param name="axpy">"a * x + y" function</param>
        /// <param name="isZero">predicate indicating zero values of V</param>
        /// <param name="reducer">reduction function</param>
        /// <returns></returns>
        Collection<R2, T> Aggregate<K, V, R2>(Expression<Func<R, K>> key, Expression<Func<R, V>> value, Expression<Func<Int64, V, V, V>> axpy, Expression<Func<V, bool>> isZero, Expression<Func<K, V, R2>> reducer)
            where K : IEquatable<K>
            where R2 : IEquatable<R2>
            where V : IEquatable<V>;

        /// <summary>
        /// Counts the number of records, by group.
        /// </summary>
        /// <typeparam name="K">Key</typeparam>
        /// <typeparam name="R2">Result</typeparam>
        /// <param name="key">key function</param>
        /// <param name="reducer">reduction function</param>
        /// <returns>Collection of results of applying reduction funtion to each group of non-zero elements.</returns>
        Collection<Pair<K, Int64>, T> Count<K>(Expression<Func<R, K>> key)
            where K : IEquatable<K>;

        /// <summary>
        /// Counts the number of records, by group.
        /// </summary>
        /// <typeparam name="K">Key</typeparam>
        /// <typeparam name="R2">Result</typeparam>
        /// <param name="key">key function</param>
        /// <param name="reducer">reduction function</param>
        /// <returns>Collection of results of applying reduction funtion to each group of non-zero elements.</returns>
        Collection<R2, T> Count<K, R2>(Expression<Func<R, K>> key, Expression<Func<K, Int64, R2>> reducer)
            where K : IEquatable<K>
            where R2 : IEquatable<R2>;

        /// <summary>
        /// Key-wise summation, using Int32 values.
        /// </summary>
        /// <typeparam name="K">Key</typeparam>
        /// <typeparam name="R2">Result</typeparam>
        /// <param name="key">key selector</param>
        /// <param name="valueSelector">value selector</param>
        /// <param name="reducer">reducer</param>
        /// <returns>Collection of reducer applied to the sum for each key</returns>
        Collection<R2, T> Sum<K, R2>(Expression<Func<R, K>> key, Expression<Func<R, int>> valueSelector, Expression<Func<K, int, R2>> reducer)
            where K : IEquatable<K>
            where R2 : IEquatable<R2>;

        /// <summary>
        /// Key-wise summation, using Int64 values.
        /// </summary>
        /// <typeparam name="K">Key</typeparam>
        /// <typeparam name="R2">Result</typeparam>
        /// <param name="key">key selector</param>
        /// <param name="valueSelector">value selector</param>
        /// <param name="reducer">reducer</param>
        /// <returns>Collection of reducer applied to the sum for each key</returns>
        Collection<R2, T> Sum<K, R2>(Expression<Func<R, K>> key, Expression<Func<R, Int64>> valueSelector, Expression<Func<K, Int64, R2>> reducer)
            where K : IEquatable<K>
            where R2 : IEquatable<R2>;

        /// <summary>
        /// Key-wise summation, using float values.
        /// </summary>
        /// <typeparam name="K">Key</typeparam>
        /// <typeparam name="R2">Result</typeparam>
        /// <param name="key">key selector</param>
        /// <param name="valueSelector">value selector</param>
        /// <param name="reducer">reducer</param>
        /// <returns>Collection of reducer applied to the sum for each key</returns>
        Collection<R2, T> Sum<K, R2>(Expression<Func<R, K>> key, Expression<Func<R, float>> valueSelector, Expression<Func<K, float, R2>> reducer)
            where K : IEquatable<K>
            where R2 : IEquatable<R2>;

        /// <summary>
        /// Key-wise summation, using double values.
        /// </summary>
        /// <typeparam name="K">Key</typeparam>
        /// <typeparam name="R2">Result</typeparam>
        /// <param name="key">key selector</param>
        /// <param name="valueSelector">value selector</param>
        /// <param name="reducer">reducer</param>
        /// <returns>Collection of reducer applied to the sum for each key</returns>
        Collection<R2, T> Sum<K, R2>(Expression<Func<R, K>> key, Expression<Func<R, double>> valueSelector, Expression<Func<K, double, R2>> reducer)
            where K : IEquatable<K>
            where R2 : IEquatable<R2>;

        Collection<R, T> Min<K,M>(Expression<Func<R, K>> key, Expression<Func<R, M>> minBy)
            where K : IEquatable<K>
            where M : IEquatable<M>, IComparable<M>;

        /// <summary>
        /// Key-wise minimum using intermediate value selectors.
        /// </summary>
        /// <typeparam name="K">Key</typeparam>
        /// <typeparam name="S">State</typeparam>
        /// <param name="key">key selector</param>
        /// <param name="selector">state selector</param>
        /// <param name="value">value selector</param>
        /// <param name="reducer">reducer</param>
        /// <returns>Collection containing for each key the minimum record under value</returns>
        Collection<R, T> Min<K, V, M>(Expression<Func<R, K>> key, Expression<Func<R, V>> value, Expression<Func<K,V,M>> minBy, Expression<Func<K, V, R>> reducer)
            where K : IEquatable<K>
            where V : IEquatable<V>
            where M : IEquatable<M>, IComparable<M>;

        /// <summary>
        /// Key-wise minimum using intermediate value selectors.
        /// </summary>
        /// <typeparam name="S">State</typeparam>
        /// <param name="key">key selector</param>
        /// <param name="selector">state selector</param>
        /// <param name="value">value selector</param>
        /// <param name="reducer">reducer</param>
        /// <param name="useDenseIntKeys">set to use arrays in place of dictionaries</param>
        /// <returns>Collection containing for each key the minimum record under value</returns>
        Collection<R, T> Min<V,M>(Expression<Func<R, int>> key, Expression<Func<R, V>> value, Expression<Func<int, V, M>> minBy, Expression<Func<int, V, R>> reducer, bool useDenseIntKeys)
            where V : IEquatable<V>
            where M : IEquatable<M>, IComparable<M>;

        /// <summary>
        /// Key-wise maximum.
        /// </summary>
        /// <typeparam name="K">Key</typeparam>
        /// <param name="key">key selector</param>
        /// <param name="value">value selector</param>
        /// <returns>Collection containing for each key the maximum record under value</returns>
        Collection<R, T> Max<K, M>(Expression<Func<R, K>> key, Expression<Func<R, M>> value)
            where K : IEquatable<K>
            where M : IComparable<M>;

        /// <summary>
        /// Key-wise minimum using intermediate value selectors.
        /// </summary>
        /// <typeparam name="K">Key</typeparam>
        /// <typeparam name="S">State</typeparam>
        /// <param name="key">key selector</param>
        /// <param name="selector">state selector</param>
        /// <param name="value">value selector</param>
        /// <param name="reducer">reducer</param>
        /// <returns>Collection containing for each key the minimum record under value</returns>
        Collection<R, T> Max<K, M, S>(Expression<Func<R, K>> key, Expression<Func<R, S>> selector, Expression<Func<K, S, M>> value, Expression<Func<K, S, R>> reducer)
            where K : IEquatable<K>
            where M : IComparable<M>
            where S : IEquatable<S>;

        #endregion Data-parallel aggregations

        #region MultiSet operations
        
        /// <summary>
        /// Converts a multiset to a set, by removing duplicates.
        /// </summary>
        /// <param name="threshold">optinal parameter indicating count above which a record is produced</param>
        /// <returns>Collection with exactly one occurence of each input record</returns>
        Collection<R, T> Distinct();

        /// <summary>
        /// Unions two multisets.
        /// </summary>
        /// <param name="other">Other collection</param>
        /// <returns>Collection where counts of each record are the maximum of the two input collections.</returns>
        Collection<R, T> Union(Collection<R, T> other);

        /// <summary>
        /// Intersects two multisets.
        /// </summary>
        /// <param name="other">Other collection</param>
        /// <returns>Collection where the counts of each record are the minimum of the two input collections.</returns>
        Collection<R, T> Intersect(Collection<R, T> other);

        /// <summary>
        /// The symmetric difference of two multisets.
        /// </summary>
        /// <param name="other">Other collection</param>
        /// <returns>Collection where the counts of each record are the absolute value of the difference of those in each of the input collections.</returns>
        Collection<R, T> SymmetricDifference(Collection<R, T> other);

        /// <summary>
        /// Concatenates two multisets.
        /// </summary>
        /// <param name="other">Other collection</param>
        /// <returns>Collection containing all input records.</returns>
        Collection<R, T> Concat(Collection<R, T> other);

        /// <summary>
        /// Subtracts one multiset from another. Records may go negative.
        /// </summary>
        /// <param name="other">Other collection</param>
        /// <returns>Collection where the weight of each record is the difference between the first and second.</returns>
        Collection<R, T> Except(Collection<R, T> other);

        #endregion MultiSet operations

        #region Fixed Point

        /// <summary>
        /// Extends the lattice associated with a collection to nest in a loop.
        /// </summary>
        /// <returns>The same collection, varying with an additional index</returns>
        Collection<R, IterationIn<T>> EnterLoop(Microsoft.Research.Naiad.Dataflow.Iteration.LoopContext<T> context);

        /// <summary>
        /// Extends the lattice associated with a collection to nest in a loop.
        /// </summary>
        /// <returns>The same collection, varying with an additional index</returns>
        Collection<R, IterationIn<T>> EnterLoop(Microsoft.Research.Naiad.Dataflow.Iteration.LoopContext<T> context, Func<R, int> initialIteration);

        /// <summary>
        /// Fixed point computation where each iterate can be changed arbitrarily by the input.
        /// </summary>
        /// <typeparam name="K">key type</typeparam>
        /// <param name="f">loop body</param>
        /// <param name="introductionIteration">iteration in which to introduce a record</param>
        /// <param name="partitionedBy">partitioning function</param>
        /// <param name="maxIterations">maximum number of iterations</param>
        /// <returns>g^maxIterations, where g^{i+1} = f(g^i + initialIteration(input)^i)</returns>
        Collection<R, T> GeneralFixedPoint<K>(
            Func<Microsoft.Research.Naiad.Dataflow.Iteration.LoopContext<T>, Collection<R, IterationIn<T>>, Collection<R, IterationIn<T>>> f, // (lc, initial, x) => f(x)
            Func<R, int> introductionIteration,
            Expression<Func<R, K>> partitionedBy,
            int maxIterations);

        /// <summary>
        /// Fixed-point computation.
        /// </summary>
        /// <param name="f">fixed-point body</param>
        /// <returns>f^maxIterations(input)</returns>
        Collection<R, T> FixedPoint(
            Func<Microsoft.Research.Naiad.Dataflow.Iteration.LoopContext<T>,
            Collection<R, IterationIn<T>>, Collection<R, IterationIn<T>>> f);

        /// <summary>
        /// Fixed-point computation.
        /// </summary>
        /// <param name="f">fixed-point body</param>
        /// <param name="maxIterations">maximum number of iterations</param>
        /// <returns>f^maxIterations(input)</returns>
        Collection<R, T> FixedPoint(
            Func<Microsoft.Research.Naiad.Dataflow.Iteration.LoopContext<T>,
            Collection<R, IterationIn<T>>, Collection<R, IterationIn<T>>> f, int iterations);

        /// <summary>
        /// Fixed-point computation with partitioning operator.
        /// </summary>
        /// <typeparam name="K">Key</typeparam>
        /// <param name="f">fixed-point body</param>
        /// <param name="consolidateFunction">partitioning function</param>
        /// <returns>f^infinity(input)</returns>
        Collection<R, T> FixedPoint<K>(
            Func<Microsoft.Research.Naiad.Dataflow.Iteration.LoopContext<T>,
            Collection<R, IterationIn<T>>, Collection<R, IterationIn<T>>> f, Expression<Func<R, K>> consolidateFunction);

        /// <summary>
        /// Fixed-point computation with partitioning operator.
        /// </summary>
        /// <typeparam name="K">Key</typeparam>
        /// <param name="f">fixed-point body</param>
        /// <param name="consolidateFunction">partitioning function</param>
        /// <param name="maxIterations">maximum number of iterations</param>
        /// <returns>f^maxIterations(input)</returns>
        Collection<R, T> FixedPoint<K>(
            Func<Microsoft.Research.Naiad.Dataflow.Iteration.LoopContext<T>,
            Collection<R, IterationIn<T>>, Collection<R, IterationIn<T>>> f, Expression<Func<R, K>> consolidateFunction, int iterations);
        #endregion Fixed Point

        #region Monitoring
        /// <summary>
        /// Monitory records passing through, applies action to each group.
        /// </summary>
        /// <param name="action">Action applied to groups of records and worker index.</param>
        /// <returns>Same collection as input.</returns>
        Collection<R, T> Monitor(Action<int, List<NaiadRecord<R, T>>> action);
        #endregion Monitoring

        #endregion Naiad operators

        #region Collection decorations

#if false

        Collection<R, T> WithSpilling();
        Collection<R, T> WithDownstreamPlacement(Placement downstreamPlacement);

#endif
        #endregion


    }

    public interface InputCollection<R> : Collection<R, Epoch>, IObserver<IEnumerable<Weighted<R>>>
        where R : IEquatable<R>
    { }
}
