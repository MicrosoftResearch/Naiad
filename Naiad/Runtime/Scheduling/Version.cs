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
using Naiad.CodeGeneration;
using Naiad.DataStructures;

namespace Naiad.Scheduling
{
    public interface PartialOrder<T> : IEquatable<T>
    {
        bool LessThan(T that);
    }

    internal static class PointstampConstructor
    {
        public static Pointstamp ToPointstamp<T>(this T latticeElement, int graphObjectID) where T : Time<T>
        {
            var version = new Pointstamp();
            
            version.Location = graphObjectID;
            version.Timestamp.Length = latticeElement.Coordinates();
            latticeElement.Populate(ref version);

            return version;
        }
    }

    public struct Pointstamp : IEquatable<Pointstamp>, IComparable<Pointstamp>, PartialOrder<Pointstamp>, Time<Pointstamp>
    {
        private static NaiadSerialization<Pointstamp> serializer = null;
        public static NaiadSerialization<Pointstamp> Serializer
        {
            get
            {
                if (serializer == null)
                    serializer = AutoSerialization.GetSerializer<Pointstamp>();
                return serializer;
            }
        }

        public struct FakeArray
        {
            public int a;
            public int b;
            public int c;
            public int d;

            public int Length;

            public int[] spillover;

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

            public FakeArray(int size) 
            { 
                Length = size; 
                a = b = c = d = 0;
                if (Length > 4)
                    spillover = new int[Length - 4];
                else
                    spillover = null;
            }

            public override string ToString()
            {
                var result = new StringBuilder().Append(this[0]);
                for (int i = 1; i < this.Length; i++)
                    result.AppendFormat(", {0}", this[i]);

                return result.ToString();
            }
        }

        // 0: unreachable, +/- x: length of prefix to consider; sign indicates "must increment final"
        //public static NaiadList<NaiadList<int>> ComparisonDepth = new NaiadList<NaiadList<int>>(0);  

        public int Location;
        public FakeArray Timestamp;

        public override int GetHashCode()
        {
            var result = Location;
            for (int i = 0; i < Timestamp.Length; i++)
                result += Timestamp[i];

            return result;
        }

        public override string ToString()
        {
            return String.Format("[graph id = {0}, version = <{1}>]", Location, Timestamp);
        }

        public bool Equals(Pointstamp that)
        {
            if (this.Location != that.Location)
                return false;

            if (this.Timestamp.Length != that.Timestamp.Length)
                return false;

            for (int i = 0; i < this.Timestamp.Length; i++)
                if (this.Timestamp[i] != that.Timestamp[i])
                    return false;

            return true;
        }

        public int CompareTo(Pointstamp that)
        {
            throw new NotImplementedException();
        }

        public bool LessThan(Pointstamp that)
        {
            throw new NotImplementedException();
        }


        public int Coordinates() { return Timestamp.Length; }


        public int Populate(ref Scheduling.Pointstamp version)
        {
            if (version.Timestamp.Length == Timestamp.Length)
                for (int i = 0; i < version.Timestamp.Length; i++)
                    version.Timestamp[i] = Timestamp[i];

            return Timestamp.Length;
        }

    #region Lattice stuff we don't implement yet

        public bool LessThanHelper(Pointstamp that, bool soFar) { throw new Exception("Not implemented"); }
        public Pointstamp Join(Pointstamp that) { throw new Exception("Not implemented"); }
        public Pointstamp Meet(Pointstamp that) { throw new Exception("Not implemented"); }

        public Pointstamp Join(Pointstamp that, out int signal) { throw new Exception("Not implemented"); }
        public Pointstamp Meet(Pointstamp that, out int signal) { throw new Exception("Not implemented"); }

        public Pointstamp InitializeFrom(Scheduling.Pointstamp version, int length) { throw new Exception("Not implemented"); }

    #endregion

        internal Pointstamp(Pointstamp that) 
        {
            this.Location = that.Location;
            this.Timestamp = that.Timestamp;
        }

        internal Pointstamp(int v, int[] i)
        {
            Location = v;
            Timestamp = new FakeArray(i.Length);
            for (int j = 0; j < i.Length; j++)
                Timestamp[j] = i[j];
        }
    }
}
