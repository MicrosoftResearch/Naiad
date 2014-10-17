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
using System.Text;
using Microsoft.Research.Naiad.Serialization;
using Microsoft.Research.Naiad.DataStructures;
using Microsoft.Research.Naiad.Dataflow;

namespace Microsoft.Research.Naiad.Runtime.Progress
{
    internal static class PointstampConstructor
    {
        public static Pointstamp ToPointstamp<T>(this T time, int graphObjectID) where T : Time<T>
        {
            var pointstamp = new Pointstamp();
            
            pointstamp.Location = graphObjectID;
            pointstamp.Timestamp.Length = time.Coordinates;
            time.Populate(ref pointstamp);

            return pointstamp;
        }
    }

    /// <summary>
    /// Represents a combined dataflow graph location and timestamp,
    /// for use in progress tracking.
    /// </summary>
    /// <seealso cref="Computation.OnFrontierChange"/>
    public struct Pointstamp : IEquatable<Pointstamp>
    {
        /// <summary>
        /// A fake array implementation to avoid heap allocation
        /// </summary>
        public struct FakeArray
        {
            /// <summary>
            /// first coordinate
            /// </summary>
            public int a;

            /// <summary>
            /// second coordinate
            /// </summary>
            public int b;

            /// <summary>
            /// third coordinate
            /// </summary>
            public int c;

            /// <summary>
            /// fourth coordinate
            /// </summary>
            public int d;

            /// <summary>
            /// "length" of array
            /// </summary>
            public int Length;

            /// <summary>
            /// space for anything beyond four coordinates
            /// </summary>
            public int[] spillover;

            /// <summary>
            /// Returns the value at the given <paramref name="index"/>.
            /// </summary>
            /// <param name="index">The index.</param>
            /// <returns>The value at the given <paramref name="index"/>.</returns>
            public int this[int index]
            {
                get 
                {
                    switch (index)
                    {
                        case 0: return a;
                        case 1: return b;
                        case 2: return c;
                        case 3: return d;
                        default: return spillover[index - 4];
                    }
                }
                set 
                {
                    switch (index)
                    {
                        case 0: a = value; break;
                        case 1: b = value; break;
                        case 2: c = value; break;
                        case 3: d = value; break;
                        default: spillover[index - 4] = value; break;
                    }
                }
            }

            /// <summary>
            /// Constructs a FakeArray with the specified size.
            /// </summary>
            /// <param name="size">The size of this FakeArray.</param>
            public FakeArray(int size) 
            { 
                Length = size; 
                a = b = c = d = 0;
                if (Length > 4)
                    spillover = new int[Length - 4];
                else
                    spillover = null;
            }

            /// <summary>
            /// Returns a string representation of this array.
            /// </summary>
            /// <returns>A string representation of this array.</returns>
            public override string ToString()
            {
                var result = new StringBuilder().Append(this[0]);
                for (int i = 1; i < this.Length; i++)
                    result.AppendFormat(", {0}", this[i]);

                return result.ToString();
            }
        }

        /// <summary>
        /// Dataflow graph location
        /// </summary>
        public int Location;

        /// <summary>
        /// Timestamp
        /// </summary>
        public FakeArray Timestamp;

        /// <summary>
        /// Returns a hashcode for this pointstamp.
        /// </summary>
        /// <returns>A hashcode for this pointstamp.</returns>
        public override int GetHashCode()
        {
            var result = Location;
            for (int i = 0; i < Timestamp.Length; i++)
                result += Timestamp[i];

            return result;
        }

        /// <summary>
        /// Returns a string representation of this pointstamp.
        /// </summary>
        /// <returns>A string representation of this pointstamp.</returns>
        public override string ToString()
        {
            return String.Format("[location = {0}, timestamp = <{1}>]", Location, Timestamp);
        }

        /// <summary>
        /// Returns <c>true</c> if and only if this and the other pointstamps are equal.
        /// </summary>
        /// <param name="other">The other pointstamp.</param>
        /// <returns><c>true</c> if and only if this and the other pointstamps are equal.</returns>
        public bool Equals(Pointstamp other)
        {
            if (this.Location != other.Location)
                return false;

            if (this.Timestamp.Length != other.Timestamp.Length)
                return false;

            for (int i = 0; i < this.Timestamp.Length; i++)
                if (this.Timestamp[i] != other.Timestamp[i])
                    return false;

            return true;
        }

        /// <summary>
        /// Constructs a Pointstamp copying from another
        /// </summary>
        /// <param name="that"></param>
        internal Pointstamp(Pointstamp that) 
        {
            this.Location = that.Location;
            this.Timestamp = that.Timestamp;
        }

        /// <summary>
        /// Constructs a new pointstamp from a location and int array
        /// </summary>
        /// <param name="location">dataflow graph location</param>
        /// <param name="indices">timestamp indices</param>
        internal Pointstamp(int location, int[] indices)
        {
            Location = location;
            Timestamp = new FakeArray(indices.Length);
            for (int j = 0; j < indices.Length; j++)
                Timestamp[j] = indices[j];
        }
    }
}
