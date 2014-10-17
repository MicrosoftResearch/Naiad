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
using System.Collections.Concurrent;
using System.Linq;
using System.Linq.Expressions;
using System.Text;

using Microsoft.Research.Naiad.Frameworks.Lindi;
using Microsoft.Research.Naiad.Dataflow;
using Microsoft.Research.Naiad;


namespace Microsoft.Research.Naiad.Frameworks.DifferentialDataflow
{
    /// <summary>
    /// Extension methods for Differential Dataflow <see cref="Collection{TRecord,TTime}"/> objects
    /// and related types.
    /// </summary>
    public static class ExtensionMethods
    {
        /// <summary>
        /// Converts a record to a weighted record with the given weight.
        /// </summary>
        /// <typeparam name="TRecord">The record type.</typeparam>
        /// <param name="record">The record.</param>
        /// <param name="weight">The weight.</param>
        /// <returns>The weighted record.</returns>
        public static Weighted<TRecord> ToWeighted<TRecord>(this TRecord record, Int64 weight) where TRecord : IEquatable<TRecord> { return new Weighted<TRecord>(record, weight); }

        /// <summary>
        /// Instructs downstream operators to assume that the <paramref name="input"/> collection is
        /// partitioned according to the given key selector.
        /// </summary>
        /// <typeparam name="TKey">The key type.</typeparam>
        /// <typeparam name="TTime">The type of timestamp on each record.</typeparam>
        /// <typeparam name="TRecord">The type of the input records.</typeparam>
        /// <param name="input">The input collection.</param>
        /// <param name="keySelector">Function that extracts a key from each record.</param>
        /// <returns>The input collection.</returns>
        /// <remarks>This operator supplies metadata for the given collection that allows downstream operators
        /// to optimize partitioning.
        /// 
        /// If the input collection is not partitioned according to the given key selector, the behavior of 
        /// downstream operators is undefined.
        /// </remarks>
        public static Collection<TRecord, TTime> AssumePartitionedBy<TRecord, TTime, TKey>(this Collection<TRecord, TTime> input, Expression<Func<TRecord, TKey>> keySelector)
            where TRecord : IEquatable<TRecord>
            where TTime : Time<TTime>
            where TKey : IEquatable<TKey>
        {
            return Microsoft.Research.Naiad.Dataflow.PartitionBy.ExtensionMethods.AssumePartitionedBy(input.Output, keySelector.ConvertToWeightedFuncAndHashCode())
                                                               .ToCollection((input as TypedCollection<TRecord,TTime>).Immutable);
        }

        /// <summary>
        /// Indicates that the <paramref name="input"/> collection is immutable.
        /// </summary>
        /// <typeparam name="TTime">The type of timestamp on each record.</typeparam>
        /// <typeparam name="TRecord">The type of the input records.</typeparam>
        /// <param name="input">The input collection.</param>
        /// <returns>The input collection.</returns>
        /// <remarks>This operator supplies metadata for the given collection that allows downstream operators
        /// to optimize the data representation.
        /// 
        /// If the input collection is not immutable, the behavior of downstream operators is undefined.
        /// </remarks>
        public static Collection<TRecord, TTime> AssumeImmutable<TRecord, TTime>(this Collection<TRecord, TTime> input)
            where TRecord : IEquatable<TRecord>
            where TTime : Time<TTime>
        {
            var local = input as TypedCollection<TRecord, TTime>;

            local.immutable = true;
            return local;
        }

        #region Time adjustment
        /// <summary>
        /// EXPERIMENTAL: Adjusts the timestamp on each record, under the requirement that the timestamp may only advance.
        /// </summary>
        /// <typeparam name="TTime">The type of timestamp on each record.</typeparam>
        /// <typeparam name="TRecord">The type of the input records.</typeparam>
        /// <param name="input">The input collection.</param>
        /// <param name="timeSelector">Function that maps a record and time to a new time for that record.</param>
        /// <returns>See remarks.</returns>
        /// <remarks>
        /// This operator can be used in the inner loops of differential dataflow programs to "delay" the processing of
        /// individual records.
        /// </remarks>
        /// <seealso cref="Collection{TRecord,TTime}.EnterLoop(Microsoft.Research.Naiad.Dataflow.Iteration.LoopContext{TTime},Func{TRecord,int})"/>
        /// <seealso cref="Collection{TRecord,TTime}.GeneralFixedPoint{TKey}"/>
        public static Collection<TRecord, TTime> AdjustTime<TRecord, TTime>(this Collection<TRecord, TTime> input, Func<TRecord, TTime, TTime> timeSelector)
            where TRecord : IEquatable<TRecord>
            where TTime : Time<TTime>
        {
            var local = input as TypedCollection<TRecord, TTime>;

            return local.AdjustTime(timeSelector);
        }
        #endregion

        /// <summary>
        /// Computes a sliding window over the given <paramref name="windowSize"/> number of <see cref="Epoch"/>s for the <paramref name="input"/> collection.
        /// </summary>
        /// <typeparam name="TRecord">The type of the input records.</typeparam>
        /// <param name="input">The input collection</param>
        /// <param name="windowSize">Number of epochs in which each record should appear.</param>
        /// <returns>The collection that represents a sliding window over the input.</returns>
        public static Collection<TRecord, Epoch> SlidingWindow<TRecord>(this Collection<TRecord, Epoch> input, int windowSize)
            where TRecord : IEquatable<TRecord>
        {
            var adjustedLattice = input.AdjustTime((r, i) => new Epoch(i.epoch + windowSize));

            return input.Except(adjustedLattice);
        }

        /// <summary>
        /// Computes a sliding window over the given <paramref name="windowSize"/> number of <see cref="Epoch"/>s for the <paramref name="input"/> collection.
        /// </summary>
        /// <typeparam name="TRecord">The type of the input records.</typeparam>
        /// <param name="input">The input collection</param>
        /// <param name="windowSize">Number of epochs in which each record should appear.</param>
        /// <returns>The collection that represents a sliding window over the input.</returns>
        public static Collection<TRecord, Epoch> SlidingWindow<TRecord>(this Stream<TRecord, Epoch> input, int windowSize)
            where TRecord : IEquatable<TRecord>
        {
            return input.Select(x => new Weighted<TRecord>(x, 1))
                        .AsCollection(false)
                        .SlidingWindow(windowSize);
        }
    

        /// <summary>
        /// Adds records to an <see cref="InputCollection{TRecord}"/>.
        /// </summary>
        /// <typeparam name="TRecord">The type of the records.</typeparam>
        /// <param name="input">The input.</param>
        /// <param name="records">The records.</param>
        public static void OnNext<TRecord>(this InputCollection<TRecord> input, IEnumerable<TRecord> records)
            where TRecord : IEquatable<TRecord>
        {
            if (records == null)
                throw new ArgumentNullException("records");

            input.OnNext(records.Select(x => new Weighted<TRecord>(x, 1)));
        }

        /// <summary>
        /// Introduces several records to an <see cref="InputCollection{TRecord}"/> with the same integer weight.
        /// </summary>
        /// <typeparam name="TRecord">The type of the records.</typeparam>
        /// <param name="input">The input.</param>
        /// <param name="records">The records.</param>
        /// <param name="weight">Positive or negative weight for each record</param>
        public static void OnNext<TRecord>(this InputCollection<TRecord> input, IEnumerable<TRecord> records, int weight)
            where TRecord : IEquatable<TRecord>
        {
            if (records == null)
                throw new ArgumentNullException("records");

            input.OnNext(records.Select(x => new Weighted<TRecord>(x, weight)));
        }

        /// <summary>
        /// Adds a record to an <see cref="InputCollection{TRecord}"/>.
        /// </summary>
        /// <typeparam name="TRecord">The type of the record.</typeparam>
        /// <param name="input">The input.</param>
        /// <param name="record">The record.</param>
        public static void OnNext<TRecord>(this InputCollection<TRecord> input, TRecord record)
            where TRecord : IEquatable<TRecord>
        {
            input.OnNext(new Weighted<TRecord>[] { new Weighted<TRecord>(record, 1) });
        }

        /// <summary>
        /// Introduces a record to an <see cref="InputCollection{TRecord}"/> with an integer weight.
        /// </summary>
        /// <typeparam name="TRecord">The type of the record.</typeparam>
        /// <param name="input">The input.</param>
        /// <param name="record">The record.</param>
        /// <param name="weight">Positive or negative weight for the record.</param>
        public static void OnNext<TRecord>(this InputCollection<TRecord> input, TRecord record, int weight)
            where TRecord : IEquatable<TRecord>
        {
            input.OnNext(new Weighted<TRecord>[] { new Weighted<TRecord>(record, weight) });
        }

        /// <summary>
        /// Introduces no records to a <see cref="InputCollection{TRecord}"/>.
        /// </summary>
        /// <remarks>
        /// This extension method is typically used when a computation has multiple inputs, to "tick" the
        /// inputs that have not changed in an epoch.
        /// </remarks>
        /// <typeparam name="TRecord">The type of records in the input.</typeparam>
        /// <param name="input">The input.</param>
        public static void OnNext<TRecord>(this InputCollection<TRecord> input)
            where TRecord : IEquatable<TRecord>
        {
            input.OnNext((IEnumerable<Weighted<TRecord>>)null);
        }

        /// <summary>
        /// Adds a single record with an integer weight and signals that the <see cref="InputCollection{TRecord}"/> is complete.
        /// </summary>
        /// <param name="input">The input.</param>
        /// <param name="value">The record.</param>
        /// <typeparam name="TRecord">The type of the record.</typeparam>
        public static void OnCompleted<TRecord>(this InputCollection<TRecord> input, Weighted<TRecord> value)
            where TRecord : IEquatable<TRecord>
        {
            input.OnCompleted(new Weighted<TRecord>[] { value });
        }

        /// <summary>
        /// Adds several records and signals that the <see cref="InputCollection{TRecord}"/> is complete.
        /// </summary>
        /// <param name="input">The input.</param>
        /// <param name="values">The records.</param>
        /// <typeparam name="TRecord">The type of the records.</typeparam>
        public static void OnCompleted<TRecord>(this InputCollection<TRecord> input, IEnumerable<TRecord> values)
            where TRecord : IEquatable<TRecord>
        {
            input.OnCompleted(values.Select(x => new Weighted<TRecord>(x, 1)));
        }

        /// <summary>
        /// Adds a single record and signals that the <see cref="InputCollection{TRecord}"/> is complete.
        /// </summary>
        /// <param name="input">The input.</param>
        /// <param name="value">The record.</param>
        /// <typeparam name="TRecord">The type of the records.</typeparam>
        public static void OnCompleted<TRecord>(this InputCollection<TRecord> input, TRecord value)
            where TRecord : IEquatable<TRecord>
        {
            input.OnCompleted(new Weighted<TRecord>[] { new Weighted<TRecord>(value, 1) });
        }


        /// <summary>
        /// Registers a callback that will be invoked each time the collection changes with a list of weighted records.
        /// </summary>
        /// <typeparam name="TRecord">The type of the input records.</typeparam>
        /// <param name="input">The input collection.</param>
        /// <param name="action">An action that is called with the list of weighted records from each epoch.</param>
        /// <returns>A <see cref="Subscription"/> object for synchronization.</returns>
        public static Microsoft.Research.Naiad.Subscription Subscribe<TRecord>(this Collection<TRecord, Epoch> input, Action<Weighted<TRecord>[]> action)
            where TRecord : IEquatable<TRecord>
        {
            return input.Output.Subscribe(x => action(x.ToArray()));
        }

        /// <summary>
        /// Converts a stream of weighted records to a differential dataflow <see cref="Collection{TRecord,TTime}"/>.
        /// </summary>
        /// <typeparam name="TRecord">The type of the input records.</typeparam>
        /// <typeparam name="TTime">The type of timestamp on each record.</typeparam>
        /// <param name="stream">The input stream.</param>
        /// <param name="immutable"><code>true</code> if and only if the stream is immutable.</param>
        /// <returns>A <see cref="Collection{TRecord,TTime}"/> based on the given <paramref name="stream"/> of weighted records.</returns>
        public static Collection<TRecord, TTime> AsCollection<TRecord, TTime>(this Stream<Weighted<TRecord>, TTime> stream, bool immutable)
            where TRecord : IEquatable<TRecord>
            where TTime : Time<TTime>
        {
            var result = new DataflowCollection<TRecord, TTime>(stream);

            result.immutable = immutable;

            return result;
        }
    }
}
