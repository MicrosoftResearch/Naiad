/*
 * Naiad ver. 0.6
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
using System.Diagnostics;
using Microsoft.Research.Naiad.Serialization;
using Microsoft.Research.Naiad.Runtime.Progress;

namespace Microsoft.Research.Naiad
{
    /// <summary>
    /// Represents a logical timestamp in a timely dataflow computation. All messages in a
    /// timely dataflow computation are labeled with a logical timestamp.
    /// </summary>
    /// <remarks>
    /// This interface and its concrete implementations <see cref="Dataflow.Epoch"/> and <see cref="Dataflow.IterationIn{TTime}"/> are the typed equivalent of the <see cref="Pointstamp.Timestamp"/> field,
    /// corresponding to a sequence of integers.
    /// </remarks>
    /// <seealso cref="Microsoft.Research.Naiad.Dataflow.Epoch"/>
    /// <seealso cref="Microsoft.Research.Naiad.Dataflow.IterationIn{TTime}"/>
    /// <typeparam name="TTime">The concrete type of the timestamp.</typeparam>
    public interface Time<TTime> : IEquatable<TTime>, IComparable<TTime>
    {
        /// <summary>
        /// Returns a hashcode for this time.
        /// </summary>
        /// <returns>Returns a hashcode for this time.</returns>
        int GetHashCode();

        /// <summary>
        /// Returns <c>true</c> if and only if this time is less than or equal to the <paramref name="other"/>
        /// time, according to the partial order defined on those times.
        /// </summary>
        /// <param name="other">The other time.</param>
        /// <returns><c>true</c> if and only if <c>this</c> is less than or equal to <c>other</c>.</returns>
        bool LessThan(TTime other);

        /// <summary>
        /// Computes the least upper bound of this and <paramref name="other"/>, according to the
        /// partial order defined on those times.
        /// </summary>
        /// <param name="other">The other time.</param>
        /// <returns>The least upper bound of the two times.</returns>
        TTime Join(TTime other);

        /// <summary>
        /// Computes the greatest lower bound of this and <paramref name="other"/>, according to the
        /// partial order defined on those times.
        /// </summary>
        /// <param name="other">The other time.</param>
        /// <returns>The greatest lower bound of the two times.</returns>
        TTime Meet(TTime other);

        /// <summary>
        /// The number of integer coordinates in timestamps of this type.
        /// </summary>
        int Coordinates { get; }

        /// <summary>
        /// Populates a <see cref="Pointstamp"/> from a typed timestamp.
        /// </summary>
        /// <param name="pointstamp">The <see cref="Pointstamp"/> to be populated.</param>
        /// <returns>The number of coordinates populated.</returns>
        int Populate(ref Pointstamp pointstamp);

        /// <summary>
        /// Returns a timestamp initialized from the given <paramref name="pointstamp"/>.
        /// </summary>
        /// <param name="pointstamp">The pointstamp.</param>
        /// <param name="length">The number of coordinates to use.</param>
        /// <returns>The initialized timestamp.</returns>
        TTime InitializeFrom(Pointstamp pointstamp, int length);
    }
}

namespace Microsoft.Research.Naiad.Dataflow
{
 
    /// <summary>
    /// Represents a non-varying logical time.
    /// </summary>
    public struct Empty : Time<Empty>
    {
        /// <summary>
        /// A dummy value, for compatibility with the current serializer.
        /// </summary>
        public int zero;

        /// <summary>
        /// Returns an empty time.
        /// </summary>
        /// <param name="other">The other time.</param>
        /// <returns>An empty time.</returns>
        public Empty Join(Empty other) { return this; }

        /// <summary>
        /// Returns an empty time.
        /// </summary>
        /// <param name="other">The other time.</param>
        /// <returns>An empty time.</returns>
        public Empty Meet(Empty other) { return this; }

        /// <summary>
        /// The number of integer coordinates in timestamps of this type (i.e. zero).
        /// </summary>
        public int Coordinates { get { return 0; } }

        /// <summary>
        /// Populates a <see cref="Pointstamp"/> with an empty timestamp.
        /// </summary>
        /// <param name="pointstamp">The <see cref="Pointstamp"/> to be populated.</param>
        /// <returns>The number of coordinates populated (i.e. zero).</returns>
        public int Populate(ref Pointstamp pointstamp) { return 0; }

        /// <summary>
        /// Returns an empty time.
        /// </summary>
        /// <param name="pointstamp">Ignored.</param>
        /// <param name="length">Ignored.</param>
        /// <returns>An empty time.</returns>
        public Empty InitializeFrom(Pointstamp pointstamp, int length) { return new Empty(); }

        /// <summary>
        /// GetHashCode override
        /// </summary>
        /// <returns>zero</returns>
        public override int GetHashCode()
        {
            return 0;
        }

        /// <summary>
        /// Returns <c>true</c>.
        /// </summary>
        /// <param name="other">The other time.</param>
        /// <returns><c>true</c>.</returns>
        public bool Equals(Empty other) { return true; }

        /// <summary>
        /// Returns <c>0</c>.
        /// </summary>
        /// <param name="other">The other time.</param>
        /// <returns>0</returns>
        public int CompareTo(Empty other) { return 0; }

        /// <summary>
        /// Returns <c>true</c>.
        /// </summary>
        /// <param name="other">The other time.</param>
        /// <returns><c>true</c></returns>
        public bool LessThan(Empty other) { return true; }
    }

    /// <summary>
    /// Represents the logical timestamp in a streaming context.
    /// </summary>
    public struct Epoch : Time<Epoch>
    {
        /// <summary>
        /// The integer epoch ID.
        /// </summary>
        public int epoch;

        /// <summary>
        /// Returns <c>true</c> if and only if this epoch is less than or equal to the <paramref name="other"/>
        /// epoch.
        /// </summary>
        /// <param name="other">The other epoch.</param>
        /// <returns><c>true</c> if and only if <c>this</c> is less than or equal to <c>other</c>.</returns>
        public bool LessThan(Epoch other) { return this.epoch <= other.epoch; }
        
        /// <summary>
        /// Returns <c>true</c> if and only if this epoch is equal to the <paramref name="other"/>
        /// epoch.
        /// </summary>
        /// <param name="other">The other epoch.</param>
        /// <returns><c>true</c> if and only if <c>this</c> is equal to <c>other</c>.</returns>
        public bool Equals(Epoch other) { return this.epoch == other.epoch; }

        /// <summary>
        /// Compares this epoch with the <paramref name="other"/> epoch.
        /// </summary>
        /// <param name="other">The other epoch.</param>
        /// <returns>A value that indicates the relative order of the objects being compared.</returns>
        public int CompareTo(Epoch other) { return this.epoch - other.epoch; }

        /// <summary>
        /// Returns the later of this and the <paramref name="other"/> epochs.
        /// </summary>
        /// <param name="other">The other epoch.</param>
        /// <returns>The later of this and the <paramref name="other"/> epochs.</returns>
        public Epoch Join(Epoch other) { return this.epoch < other.epoch ? other : this; }

        /// <summary>
        /// Returns the earlier of this and the <paramref name="other"/> epochs.
        /// </summary>
        /// <param name="other">The other epoch.</param>
        /// <returns>The earlier of this and the <paramref name="other"/> epochs.</returns>
        public Epoch Meet(Epoch other) { return this.epoch < other.epoch ? this : other; }

        /// <summary>
        /// Returns a string representation of this epoch.
        /// </summary>
        /// <returns>A string representation of this epoch.</returns>
        public override string ToString()
        {
            return epoch.ToString();
        }

        /// <summary>
        /// Returns a hashcode for this epoch.
        /// </summary>
        /// <returns>A hashcode for this epoch.</returns>
        public override int GetHashCode()
        {
            return epoch;
        }

        /// <summary>
        /// The number of integer coordinates in timestamps of this type (i.e. one).
        /// </summary>
        public int Coordinates { get { return 1; } }

        /// <summary>
        /// Populates a <see cref="Pointstamp"/> from this epoch.
        /// </summary>
        /// <param name="pointstamp">The <see cref="Pointstamp"/> to be populated.</param>
        /// <returns>The number of coordinates populated (i.e. one).</returns>
        public int Populate(ref Pointstamp pointstamp)
        {
            if (0 < pointstamp.Timestamp.Length)
                pointstamp.Timestamp[0] = epoch;

            return 1;
        }

        /// <summary>
        /// Constructs new Epoch from the given integer ID.
        /// </summary>
        /// <param name="epoch">The integer epoch ID.</param>
        public Epoch(int epoch) { this.epoch = epoch; }

        /// <summary>
        /// Returns an epoch initialized from the given <paramref name="pointstamp"/>.
        /// </summary>
        /// <param name="pointstamp">The pointstamp.</param>
        /// <param name="length">The number of coordinates to use, which should be <c>1</c>.</param>
        /// <returns>The initialized epoch.</returns>
        public Epoch InitializeFrom(Pointstamp pointstamp, int length)
        {
            epoch = pointstamp.Timestamp[0];
            return this;
        }
    }

    /// <summary>
    /// Represents the logical timestamp containing a loop counter nested within another logical <typeparamref name="TTime"/> context.
    /// </summary>
    /// <typeparam name="TTime">The type of the outer timestamp.</typeparam>
    public struct IterationIn<TTime> : Time<IterationIn<TTime>>
        where TTime : Time<TTime>
    {
        /// <summary>
        /// The outer time.
        /// </summary>
        public TTime outerTime;

        /// <summary>
        /// The loop counter.
        /// </summary>
        public int iteration;

        /// <summary>
        /// Compares this timestamp with the <paramref name="other"/> timestamp.
        /// </summary>
        /// <param name="other">The other timestamp.</param>
        /// <returns>A value that indicates the relative order of the objects being compared.</returns>
        public int CompareTo(IterationIn<TTime> other)
        {
            var sCompare = this.outerTime.CompareTo(other.outerTime);
            if (sCompare != 0)
                return sCompare;
            else
                return this.iteration - other.iteration;
        }

        /// <summary>
        /// Returns <c>true</c> if and only if this timestamp is equal to the <paramref name="other"/>
        /// timestamp.
        /// </summary>
        /// <param name="other">The other timestamp.</param>
        /// <returns><c>true</c> if and only if <c>this</c> is equal to <c>other</c>.</returns>
        public bool Equals(IterationIn<TTime> other)
        {
            return this.iteration == other.iteration && this.outerTime.Equals(other.outerTime);
        }

        /// <summary>
        /// Returns <c>true</c> if and only if this timestamp is less than or equal to the <paramref name="other"/>
        /// timestamp.
        /// </summary>
        /// <param name="other">The other timestamp.</param>
        /// <returns><c>true</c> if and only if <c>this</c> is less than or equal to <c>other</c>.</returns>
        public bool LessThan(IterationIn<TTime> other)
        {
            if (this.iteration <= other.iteration)
                return this.outerTime.LessThan(other.outerTime);
            else
                return false;
        }

        /// <summary>
        /// Returns a string representation of this timestamp.
        /// </summary>
        /// <returns>A string representation of this timestamp.</returns>
        public override string ToString()
        {
            return String.Format("[{0}, {1}]", outerTime.ToString(), iteration.ToString());
        }

        /// <summary>
        /// Returns a hashcode for this epoch.
        /// </summary>
        /// <returns>A hashcode for this epoch.</returns>
        public override int GetHashCode()
        {
            return 134123 * outerTime.GetHashCode() + iteration;
        }

        /// <summary>
        /// The number of integer coordinates in timestamps of this type.
        /// </summary>
        public int Coordinates { get { return outerTime.Coordinates + 1; } }

        /// <summary>
        /// Populates a <see cref="Pointstamp"/> from this timestamp.
        /// </summary>
        /// <param name="pointstamp">The <see cref="Pointstamp"/> to be populated.</param>
        /// <returns>The number of coordinates populated.</returns>
        public int Populate(ref Pointstamp pointstamp)
        {
            var position = outerTime.Populate(ref pointstamp);
            if (position < pointstamp.Timestamp.Length)
                pointstamp.Timestamp[position] = iteration;

            return position + 1;
        }

        /// <summary>
        /// Returns the later of this and the <paramref name="other"/> timestamps.
        /// </summary>
        /// <param name="other">The other timestamp.</param>
        /// <returns>The later of this and the <paramref name="other"/> timestamps.</returns>
        public IterationIn<TTime> Join(IterationIn<TTime> other)
        {
            return new IterationIn<TTime>(this.outerTime.Join(other.outerTime), Math.Max(this.iteration, other.iteration));
        }

        /// <summary>
        /// Returns the earlier of this and the <paramref name="other"/> timestamps.
        /// </summary>
        /// <param name="other">The other timestamps.</param>
        /// <returns>The earlier of this and the <paramref name="other"/> timestamps.</returns>
        public IterationIn<TTime> Meet(IterationIn<TTime> other)
        {
            return new IterationIn<TTime>(this.outerTime.Meet(other.outerTime), Math.Min(this.iteration, other.iteration));
        }

        /// <summary>
        /// Constructs a new timestamp from an outer time and the given loop counter.
        /// </summary>
        /// <param name="outerTime">The outer time.</param>
        /// <param name="iteration">The loop counter.</param>
        public IterationIn(TTime outerTime, int iteration) { this.outerTime = outerTime; this.iteration = iteration; }

        /// <summary>
        /// Returns a timestamp initialized from the given <paramref name="pointstamp"/>.
        /// </summary>
        /// <param name="pointstamp">The pointstamp.</param>
        /// <param name="length">The number of coordinates to use.</param>
        /// <returns>The initialized epoch.</returns>
        public IterationIn<TTime> InitializeFrom(Pointstamp pointstamp, int length)
        {
            iteration = pointstamp.Timestamp[length - 1];
            outerTime = outerTime.InitializeFrom(pointstamp, length - 1);
            return this;
        }
    }
}
