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

using Microsoft.Research.Naiad;

namespace Microsoft.Research.Naiad.Frameworks.DifferentialDataflow
{
    /// <summary>
    /// An equatable pair of integers
    /// </summary>
    public struct IntPair : IEquatable<IntPair>
    {
        /// <summary>
        /// The first integer.
        /// </summary>
        public int s;

        /// <summary>
        /// The second integer.
        /// </summary>
        public int t;

        /// <summary>
        /// Returns <c>true</c> if and only if this and the other pair are element-wise equal.
        /// </summary>
        /// <param name="other">The other pair.</param>
        /// <returns><c>true</c> if and only if this and the other pair are element-wise equal.</returns>
        public bool Equals(IntPair other)
        {
            return s == other.s && t == other.t;
        }

        /// <summary>
        /// Returns a hashcode for this pair.
        /// </summary>
        /// <returns>A hashcode for this pair.</returns>
        public override int GetHashCode()
        {
            return 47 * s + 36425232 * t;
        }

        /// <summary>
        /// Returns a string representation of this pair.
        /// </summary>
        /// <returns>A string representation of this pair.</returns>
        public override string ToString()
        {
            return String.Format("({0}, {1})", s, t);
        }

        /// <summary>
        /// Constructs a new pair from the given integers.
        /// </summary>
        /// <param name="s">The first integer.</param>
        /// <param name="t">The second integer.</param>
        public IntPair(int s, int t) { this.s = s; this.t = t; }
    }
#if false
    public struct Int64Pair : IEquatable<Int64Pair>
    {
        public Int64 s;
        public Int64 t;

        public bool Equals(Int64Pair that)
        {
            return s == that.s && t == that.t;
        }

        public int CompareTo(Int64Pair that)
        {
            if (this.s < that.s)
                return -1;
            if (this.s > that.s)
                return 1;
            if (this.t < that.t)
                return -1;
            if (this.t > that.t)
                return 1;

            return 0;
        }

        public override int GetHashCode()
        {
            return 47 * s.GetHashCode() + 36425232 * t.GetHashCode();
        }

        public override string ToString()
        {
            return String.Format("({0}, {1})", s, t);
        }

        public Int64Pair(Int64 ss, Int64 tt) { s = ss; t = tt; }
    }

    public struct IntTriple : IEquatable<IntTriple>
    {
        public int first;
        public int second;
        public int third;

        public bool Equals(IntTriple that)
        {
            return first == that.first && second == that.second && third == that.third;
        }

        public int CompareTo(IntTriple that)
        {
            if (this.first != that.first)
                return this.first - that.first;

            if (this.second != that.second)
                return this.second - that.second;

            return this.third - that.third;
        }

        // Embarassing hashcodes
        public override int GetHashCode()
        {
            return first + 1234347 * second + 4311 * third;
        }

        public override string ToString()
        {
            return String.Format("({0},{1},{2})", first, second, third);
        }

        public IntTriple(int x, int y, int z)
        {
            first = x; second = y; third = z;
        }
    }

    public struct IntQuad : IEquatable<IntQuad>
    {
        public int first;
        public int second;
        public int third;
        public int fourth;

        public bool Equals(IntQuad that)
        {
            return first == that.first && second == that.second && third == that.third && fourth == that.fourth;
        }

        public int CompareTo(IntQuad that)
        {
            if (this.first != that.first)
                return this.first - that.first;

            if (this.second != that.second)
                return this.second - that.second;
    
            if (this.third != that.third)
                return this.third - that.third;

            return this.fourth - that.fourth;
        }

        // Embarassing hashcodes
        public override int GetHashCode()
        {
            return 31 * first + 1234347 * second + 4311 * third + 12315 * fourth;
        }

        public override string ToString()
        {
            return String.Format("({0},\t{1},\t{2},\t{3})", first, second, third, fourth);
        }

        public IntQuad(int x, int y, int z, int w)
        {
            first = x; second = y; third = z; fourth = w;
        }
    }
#endif
}
