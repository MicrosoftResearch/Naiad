/*
 * Naiad ver. 0.4
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

namespace Microsoft.Research.Naiad.Serialization
{
    internal struct NativeSA 
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

    /// <summary>
    /// Represents a growable segment of an array, used for serialization.
    /// </summary>
    /// <typeparam name="TElement">The type of elements.</typeparam>
    public struct SubArray<TElement>
    {
        /// <summary>
        /// The array instance that backs this subarray.
        /// </summary>
        public readonly TElement[] Array;

        /// <summary>
        /// The number of elements that have been written into this subarray.
        /// </summary>
        public int Count;
        
        /// <summary>
        /// Constructs a new empty subarray that can grow to the full size of the given array.
        /// </summary>
        /// <param name="array">The array that backs this subarray.</param>
        public SubArray(TElement[] array) : this(array, 0) { }

        /// <summary>
        /// Constructs a new subarray that contains an initial number of elements and can grow to the full size
        /// of the given array.
        /// </summary>
        /// <param name="array">The array that backs this subarray.</param>
        /// <param name="initialCount">The initial number of elements in this subarray.</param>
        public SubArray(TElement[] array, int initialCount)
        {
            this.Array = array;
            this.Count = initialCount;
        }

        /// <summary>
        /// The total number of (occupied and unoccupied) elements in this subarray.
        /// </summary>
        public int Length
        {
            get { return Array == null ? 0 : Array.Length; }
        }

        /// <summary>
        /// The element of this subarray at the given index.
        /// </summary>
        /// <param name="index">The index.</param>
        /// <returns>The element of this subarray at the given index.</returns>
        public TElement this[int index]
        {
            get { return Array[index]; }
            set { Array[index] = value; }
        }

        /// <summary>
        /// The number of unoccupied elements in this subarray.
        /// </summary>
        public int Available
        {
            get { return Length - Count; }
            set { Count = Length - value; }
        }

        private bool EnsureCapacity(int size)
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

        /// <summary>
        /// Returns true if the given number of elements is available in this subarray.
        /// </summary>
        /// <param name="numElements">The number of elements.</param>
        /// <returns>True if the given number of elements is available in this subarray, otherwise false.</returns>
        public bool EnsureAvailable(int numElements)
        {
            return EnsureCapacity(Count + numElements);
        }
    }

    /// <summary>
    /// Represents a growable segment of an array, used for deserialization.
    /// </summary>
    public struct RecvBuffer
    {
        /// <summary>
        /// The array that backs this buffer.
        /// </summary>
        public readonly byte[] Buffer;

        /// <summary>
        /// The offset (in the backing array) of the first byte after the end of this buffer.
        /// </summary>
        public readonly int End;

        /// <summary>
        /// The current offset in the backing array.
        /// </summary>
        public int CurrentPos;

        internal RecvBuffer(byte[] buffer, int start, int end)
        {
            this.Buffer = buffer;
            this.End = end;
            this.CurrentPos = start;
        }

        /// <summary>
        /// The number of bytes that have not been consumed from this buffer.
        /// </summary>
        public int Available { get { return End - CurrentPos; } }
    }
}
