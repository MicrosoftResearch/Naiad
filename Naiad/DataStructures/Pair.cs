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

namespace Microsoft.Research.Naiad
{
    /// <summary>
    /// Extension methods for the Pair type
    /// </summary>
    public static class PairExtensionMethods
    {
        /// <summary>
        /// Returns a new Pair of two elements
        /// </summary>
        /// <typeparam name="V1">first element type</typeparam>
        /// <typeparam name="V2">second element type</typeparam>
        /// <param name="first">first element</param>
        /// <param name="second">second element</param>
        /// <returns>pair of first and second elements</returns>
        public static Pair<V1, V2> PairWith<V1, V2>(this V1 first, V2 second)
        {
            return new Pair<V1, V2>(first, second);
        }
    }

    /// <summary>
    /// Pair of two elements
    /// </summary>
    /// <typeparam name="TFirst">The first element type.</typeparam>
    /// <typeparam name="TSecond">The second element type.</typeparam>
    public struct Pair<TFirst, TSecond> : IEquatable<Pair<TFirst, TSecond>>
    {
        /// <summary>
        /// First element.
        /// </summary>
        public TFirst First;

        /// <summary>
        /// Second element.
        /// </summary>
        public TSecond Second;

        /// <summary>
        /// Constructs a pair from two elements
        /// </summary>
        /// <param name="first">The first element.</param>
        /// <param name="second">The second element.</param>
        public Pair(TFirst first, TSecond second) { First = first; Second = second; }

        /// <summary>
        /// Returns a string representation of this pair.
        /// </summary>
        /// <returns>A string representation of this pair.</returns>
        public override string ToString()
        {
            return "[" + First.ToString() + " " + Second.ToString() + "]";
        }

        /// <summary>
        /// Returns a 32-bit signed integer hashcode for this pair.
        /// </summary>
        /// <returns>A 32-bit signed integer hashcode for this pair.</returns>
        public override int GetHashCode()
        {
            return (this.First == null ? 0 : this.First.GetHashCode()) + 123412324 * (this.Second == null ? 0 : this.Second.GetHashCode());
        }


        /// <summary>
        /// Compares this pair and the <paramref name="other"/> pair for element-wise equality.
        /// </summary>
        /// <param name="other">The other pair.</param>
        /// <returns><c>true</c>, if and only if both pairs are element-wise equal.</returns>
        public bool Equals(Pair<TFirst, TSecond> other)
        {
            return (EqualityComparer<TFirst>.Default.Equals(this.First, other.First) && EqualityComparer<TSecond>.Default.Equals(this.Second, other.Second));
        }
    }
}
