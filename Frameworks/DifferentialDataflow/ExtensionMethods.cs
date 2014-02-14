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
using System.Collections.Concurrent;
using System.Linq;
using System.Linq.Expressions;
using System.Text;
using Naiad.Dataflow.Channels;
using Naiad;

namespace Naiad.Frameworks.DifferentialDataflow
{
    public static class ExtensionMethods
    {
        /// <summary>
        /// Converts a record to a weighted record.
        /// </summary>
        /// <typeparam name="T">Record</typeparam>
        /// <param name="x">record</param>
        /// <param name="w">weighted</param>
        /// <returns>Weighted record</returns>
        public static Weighted<T> ToWeighted<T>(this T x, Int64 w) where T : IEquatable<T> { return new Weighted<T>(x, w); }

        internal static NaiadRecord<S, T> ToNaiadRecord<S, T>(this S x, Int64 w, T t)
            where S : IEquatable<S>
            where T : Time<T> { return new NaiadRecord<S, T>(x, w, t); }
        internal static NaiadRecord<S, T> ToNaiadRecord<S, T>(this Weighted<S> x, T t)
            where S : IEquatable<S>
            where T : Time<T> { return new NaiadRecord<S, T>(x.record, x.weight, t); }
    }


    public static class PartitioningExtensionMethods
    {
        /// <summary>
        /// Sets metadata associated with a Collection to indicate that data are partitioned by a specified function.
        /// </summary>
        /// <typeparam name="R">Record</typeparam>
        /// <typeparam name="T">Lattice</typeparam>
        /// <typeparam name="K">Key</typeparam>
        /// <param name="input">Source collection</param>
        /// <param name="func">Partitioning function</param>
        /// <returns>Source collection, annotated with a partitioning function.</returns>
        public static Collection<R, T> AssumePartitionedBy<R, T, K>(this Collection<R, T> input, Expression<Func<R, K>> func)
            where R : IEquatable<R>
            where T : Time<T>
            where K : IEquatable<K>
        {
            return Naiad.Dataflow.PartitionBy.ExtensionMethods.AssumePartitionedBy(input.Output, func.ConvertToWeightedFuncAndHashCode())
                                                               .ToCollection((input as TypedCollection<R,T>).Immutable);
        }
    }

    public static class ImmutabilityExtensionMethods
    {
        /// <summary>
        /// Sets metadata associated with a Collection to indicate that the collection does not vary with a lattice.
        /// </summary>
        /// <typeparam name="R">Record</typeparam>
        /// <typeparam name="T">Lattice</typeparam>
        /// <param name="input">Source collection</param>
        /// <returns>Source collection, annotated with an immutable bit.</returns>
        public static Collection<R, T> AssumeImmutable<R, T>(this Collection<R, T> input)
            where R : IEquatable<R>
            where T : Time<T>
        {
            var local = input as TypedCollection<R, T>;

            local.immutable = true;
            return local;
        }
    }

    public static class SlidingWindowExtensionMethods
    {
        /// <summary>
        /// Adjusts a Int-varying collection to only retain records for a fixed window.
        /// </summary>
        /// <typeparam name="R">Record</typeparam>
        /// <param name="input">Source collection</param>
        /// <param name="windowSize">Number of epochs for each record</param>
        /// <returns>Collection reflecting a sliding window over the source.</returns>
        public static Collection<R, Epoch> SlidingWindow<R>(this Collection<R, Epoch> input, int windowSize)
            where R : IEquatable<R>
        {
            var adjustedLattice = input.AdjustLattice((r, i) => new Epoch(i.t + windowSize));

            return input.Except(adjustedLattice);
        }
    }

    public static class EnumerationExtensionMethods
    {
        /// <summary>
        /// Introduces several records to an observer.
        /// </summary>
        /// <typeparam name="R">Record</typeparam>
        /// <param name="observer">Recipient</param>
        /// <param name="records">List of records</param>
        public static void OnNext<R>(this IObserver<IEnumerable<Weighted<R>>> observer, IEnumerable<R> records)
            where R : IEquatable<R>
        {
            if (records == null)
                throw new ArgumentNullException("records");

            observer.OnNext(records.Select(x => new Weighted<R>(x, 1)));
        }

        /// <summary>
        /// Introduces several records to an observer.
        /// </summary>
        /// <typeparam name="R">Record</typeparam>
        /// <param name="observer">Recipient</param>
        /// <param name="records">List of records</param>
        /// <param name="weight">Integer weight for each record</param>
        public static void OnNext<R>(this IObserver<IEnumerable<Weighted<R>>> observer, IEnumerable<R> records, int weight)
            where R : IEquatable<R>
        {
            if (records == null)
                throw new ArgumentNullException("records");

            observer.OnNext(records.Select(x => new Weighted<R>(x, weight)));
        }

        /// <summary>
        /// Introduces a record to an observer.
        /// </summary>
        /// <typeparam name="R">Record</typeparam>
        /// <param name="observer">Recipient</param>
        /// <param name="record">The record</param>
        public static void OnNext<R>(this IObserver<IEnumerable<Weighted<R>>> observer, R record)
            where R : IEquatable<R>
        {
            observer.OnNext(new Weighted<R>[] { new Weighted<R>(record, 1) });
        }

        /// <summary>
        /// Introduces a record to an observer.
        /// </summary>
        /// <typeparam name="R">Record</typeparam>
        /// <param name="observer">Recipient</param>
        /// <param name="record">The record</param>
        /// <param name="weight">Integer weight for the record</param>
        public static void OnNext<R>(this IObserver<IEnumerable<Weighted<R>>> observer, R record, int weight)
            where R : IEquatable<R>
        {
            observer.OnNext(new Weighted<R>[] { new Weighted<R>(record, weight) });
        }

        /// <summary>
        /// Introduces no records to an observer.
        /// </summary>
        /// <typeparam name="R">Record</typeparam>
        /// <param name="observer">Recipient</param>
        public static void OnNext<R>(this IObserver<IEnumerable<Weighted<R>>> observer)
            where R : IEquatable<R>
        {
            observer.OnNext(null);
        }

        /// <summary>
        /// Registers a callback that will be invoked each time the collection changes with a list of record,weight pairs.
        /// </summary>
        /// <typeparam name="R">Record</typeparam>
        /// <param name="output">Source collection</param>
        /// <param name="action">Callback action</param>
        /// <returns>A subscription whose disposal should disconnect the action from the source, but doesn't.</returns>
        public static Naiad.Subscription Subscribe<R>(this Collection<R, Epoch> output, Action<Weighted<R>[]> action)
            where R : IEquatable<R>
        {
            return output.Output.Subscribe(x => action(x.ToArray()));
        }
    }

    public static class DataflowExtensionMethods
    {
        public static Collection<R, T> AsCollection<R, T>(this Naiad.Dataflow.Stream<Weighted<R>, T> port, bool immutable)
            where R : IEquatable<R>
            where T : Time<T>
        {
            var result = new DataflowCollection<R, T>(port);

            result.immutable = immutable;

            return result;
        }
    }
}
