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
using System.Text;

namespace Microsoft.Research.Naiad
{
    public struct NativeSA 
    {
        public readonly IntPtr ArrayBegin;
        public int CurPos;
        public readonly IntPtr ArrayEnd;

        public bool EnsureAvailable(int need)
        {
            return CurPos + need < ArrayEnd.ToInt64() - ArrayBegin.ToInt64();
        }

        public NativeSA(IntPtr a, int cpos, IntPtr bnd)
        {
            ArrayBegin = a;
            CurPos = cpos;
            ArrayEnd =  bnd;
        }

    }
    public struct SubArray<T> : IEquatable<SubArray<T>>
    {
        public T[] Array;
        public int Count;
        //public const bool Resizable = false;

        public SubArray(T[] a)
        {
            ///Resizable = false;
            Array = a;
            Count = 0;
        }

        public SubArray(T[] a, int c)
        {
            //Resizable = false;
            Array = a;
            Count = c;
        }

#if false
        public SubArray(int size)
        {
            Count = 0;
            Array = size > 0 ? GlobalBufferPool<T>.pool.CheckOut(size) : GlobalBufferPool<T>.pool.Empty;
            Resizable = true;
        }
#endif

        public bool Equals(SubArray<T> that)
        {
            if (this.Count != that.Count)
            {
                return false;
            }

            for (int i = 0; i < this.Count; ++i)
                if (!this.Array[i].Equals(that.Array[i]))
                    return false;

            return true;
        }

        public override bool Equals(object that)
        {
            return that is SubArray<T> && this.Equals((SubArray<T>) that);
        }

        public override int GetHashCode()
        {
            int ret = 0;
            for (int i = 0; i < this.Count; ++i)
                ret ^= this.Array[i].GetHashCode();
            return ret;
        }

        public int Length
        {
            get { return Array == null ? 0 : Array.Length; }
        }

        public T this[int i]
        {
            get { return Array[i]; }
            set { Array[i] = value; }
        }

        public int Available
        {
            get { return Length - Count; }
            set { Count = Length - value; }
        }

        // Methods for resizing a subarray.
        internal BufferPool<T> pool
        {
            get { return GlobalBufferPool<T>.pool; }
        }

        public bool EnsureCapacity(int size)
        {
            return Array.Length >= size;
#if false
            if (Array.Length >= size) return true;
            if (!Resizable) return false;

            var newArraySize = Math.Max(2 * Array.Length, 4);
            while (newArraySize < size)
                newArraySize = 2 * newArraySize;

            var newArray = pool.CheckOut(newArraySize);

            for (int i = 0; i < Count; i++)
                newArray[i] = Array[i];

            pool.CheckIn(Array);

            Array = newArray;
            return true;
#endif
        }

        public bool EnsureAvailable(int size)
        {
            return EnsureCapacity(Count + size);
        }
    }


    public struct RecvBuffer
    {
        public readonly byte[] Buffer;
        public readonly int End;

        public int CurrentPos;

        public RecvBuffer(byte[] buffer, int start, int end)
        {
            this.Buffer = buffer;
            this.End = end;
            this.CurrentPos = start;
        }


        public int Available { get { return End - CurrentPos; } }
    }
}
