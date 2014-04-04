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
using System.Linq;
using System.Text;
using System.Diagnostics;
using Microsoft.Research.Naiad.CodeGeneration;

namespace Microsoft.Research.Naiad
{
    /// <summary>
    /// The typed equivalent of a Pointstamp's "Timestamp" field, corresponding to a sequence of integers.
    /// Main use is Join/Meet, and conversion to and from Pointstamp representation.
    /// </summary>
    /// <typeparam name="T"></typeparam>

    public interface Time<T> : IEquatable<T>, IComparable<T>, Scheduling.PartialOrder<T>
    {
        int GetHashCode();

        T Join(T that);
        T Meet(T that);

        int Coordinates();
        int Populate(ref Scheduling.Pointstamp version);

        T InitializeFrom(Scheduling.Pointstamp version, int length);
    }

    public struct Empty : Time<Empty>
    {
        public int zero;

        public Empty Join(Empty that) { return this; }
        public Empty Meet(Empty that) { return this; }
        public int Coordinates() { return 0; }
        public int Populate(ref Scheduling.Pointstamp version) { return 0; }
        public Empty InitializeFrom(Scheduling.Pointstamp version, int length) { return new Empty(); }
        public bool Equals(Empty other) { return true; }
        public int CompareTo(Empty other) { return 0; }
        public bool LessThan(Empty that) { return true; }
    }

    public struct Epoch : Time<Epoch>
    {
        [IntSerialization(IntSerialization.VariableLength)]
        public int t;

        public bool LessThan(Epoch that) { return this.t <= that.t; }
        
        public bool Equals(Epoch that) { return this.t == that.t; }
        public int CompareTo(Epoch that) { return this.t - that.t; }

        public Epoch Join(Epoch that) { return this.t < that.t ? that : this; }
        public Epoch Meet(Epoch that) { return this.t < that.t ? this : that; }

        public override string ToString()
        {
            return t.ToString();
        }
        public override int GetHashCode()
        {
            return t;
        }

        public int Coordinates() { return 1; }

        public int Populate(ref Scheduling.Pointstamp version)
        {
            if (0 < version.Timestamp.Length)
                version.Timestamp[0] = t;

            return 1;
        }

        public Epoch(int tt) { t = tt; }

#if false
        public static implicit operator Epoch(int i)
        {
            return new Epoch(i);
        }
#endif
        public Epoch InitializeFrom(Scheduling.Pointstamp version, int length)
        {
            t = version.Timestamp[0];
            return this;
        }
    }

    public struct IterationIn<S> : Time<IterationIn<S>>
        where S : Time<S>
    {
        public S s;
        [IntSerialization(IntSerialization.VariableLength)]
        public int t;

        public int CompareTo(IterationIn<S> that)
        {
            var sCompare = this.s.CompareTo(that.s);
            if (sCompare != 0)
                return sCompare;
            else
                return this.t - that.t;
        }
        public bool Equals(IterationIn<S> that)
        {
            return this.t == that.t && this.s.Equals(that.s);
        }

        public bool LessThan(IterationIn<S> that)
        {
            if (this.t <= that.t)
                return this.s.LessThan(that.s);
            else
                return false;
        }

        public override string ToString()
        {
            return String.Format("[{0}, {1}]", s.ToString(), t.ToString());
        }

        public override int GetHashCode()
        {
            return 134123 * s.GetHashCode() + t;
        }

        public int Coordinates() { return s.Coordinates() + 1; }

        public int Populate(ref Scheduling.Pointstamp version)
        {
            var position = s.Populate(ref version);
            if (position < version.Timestamp.Length)
                version.Timestamp[position] = t;

            return position + 1;
        }

        public IterationIn<S> Join(IterationIn<S> that)
        {
            return new IterationIn<S>(this.s.Join(that.s), Math.Max(this.t, that.t));
        }
    
        public IterationIn<S> Meet(IterationIn<S> that)
        {
            return new IterationIn<S>(this.s.Meet(that.s), Math.Min(this.t, that.t));
        }

        public IterationIn(S ss, int tt) { s = ss; t = tt; }

        public IterationIn<S> InitializeFrom(Scheduling.Pointstamp version, int length)
        {
            t = version.Timestamp[length - 1];
            s = s.InitializeFrom(version, length - 1);
            return this;
        }
    }
}
