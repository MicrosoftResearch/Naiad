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
using System.Collections;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Microsoft.Research.Naiad.CodeGeneration;

namespace Microsoft.Research.Naiad.DataStructures
{
    public class NaiadList<S> : IEquatable<NaiadList<S>>, IList<S>
    {
        public S[] Array;
        private int count;

        BufferPool<S> pool
        {
            get { return ThreadLocalBufferPools<S>.pool.Value; }
        }

        public void EnsureCapacity(int size)
        {
            if (Array.Length < size)
            {
                var newArraySize = Math.Max(2 * Array.Length, 4);
                while (newArraySize < size)
                    newArraySize = 2 * newArraySize;

                var newArray = pool.CheckOut(newArraySize);

                for (int i = 0; i < count; i++)
                    newArray[i] = Array[i];

                pool.CheckIn(Array);

                Array = newArray;
            }
        }

        void EnsureAvailable(int size)
        {
            EnsureCapacity(Count + size);
        }

        public void Clear() 
        { 
            Count = 0;
        }

        public void Add(S element)
        {
            EnsureAvailable(1);
            Array[count++] = element;
        }

        public static NaiadList<S> operator+(NaiadList<S> a, NaiadList<S> b)
        {
            var result = new NaiadList<S>(a.count + b.count);
            for (var i = 0; i < a.count; ++i)
            {
                result.Array[i] = a.Array[i];
            }
            for (var i = 0; i < b.count; ++i)
            {
                result.Array[i + a.count] = b.Array[i];
            }
            return result;
        }

        public S RemoveAtAndReturn(int index)
        {
            S element = Array[index];
            Array[index] = Array[count - 1];
            count--;

            return element;
        }

        public void Free() 
        { 
            Count = 0;
            pool.CheckIn(Array);
            Array = pool.Empty; 
        }

        public S[] ToArray()
        {
            var result = new S[this.count];
            for (int i = 0; i < this.count; i++)
                result[i] = this.Array[i];

            return result;
        }

        public NaiadList()
        {
            count = 0;
            Array = pool.Empty; 
        }

        public NaiadList(int size)
        {
            count = 0;
            Array = size > 0 ? pool.CheckOut(size) : pool.Empty;
        }

        public NaiadList(S[] values)
        {
            count = values.Length;
            Array = values;
        }

        public bool Equals(NaiadList<S> other)
        {
            if (ReferenceEquals(null, other)) return false;
            if (ReferenceEquals(this, other)) return true;
            if (other.Count != Count)
                return false;

            for (int i = 0; i < Count; ++i)
            {
                if (!Array[i].Equals(other.Array[i]))
                    return false;
            }
            return true;
        }

        public override bool Equals(object obj)
        {
            if (ReferenceEquals(null, obj)) return false;
            if (ReferenceEquals(this, obj)) return true;
            if (obj.GetType() != typeof(NaiadList<S>)) return false;
            return Equals((NaiadList<S>)obj);
        }

        public override int GetHashCode()
        {
            int ret = 0;
            for (int i = 0; i < this.Count; ++i)
                ret ^= this.Array[i].GetHashCode();
            return ret;
        }

        public IEnumerator<S> GetEnumerator()
        {
            for (int i = 0; i < this.Count; ++i)
                yield return this.Array[i];
        }

        IEnumerator IEnumerable.GetEnumerator()
        {
            return GetEnumerator();
        }

        #region IList<S> members
        public bool IsReadOnly { get { return false; } }
        public int Count { get { return this.count; } set { this.count = value; } }

        public bool Remove(S toRemove)
        {
            for (int i = 0; i < this.count; ++i)
                if (this.Array[i].Equals(toRemove))
                {
                    this.RemoveAtAndReturn(i);
                    return true;
                }
            return false;
        }

        public S this[int index] { get { return this.Array[index]; } set { this.Array[index] = value; } }

        public void Insert(int index, S toInsert)
        {
            this.EnsureCapacity(this.count + 1);
            for (int i = index + 1; i < this.count; ++i)
                this.Array[i] = this.Array[i - 1];
            this.Array[index] = toInsert;
            this.count++;
        }

        public void RemoveAt(int index)
        {
            this.RemoveAtAndReturn(index);
        }

        public int IndexOf(S toFind)
        {
            for (int i = 0; i < this.count; ++i)
                if (this.Array[i].Equals(toFind))
                    return i;
            return -1;
        }

        public void CopyTo(S[] array, int index)
        {
            this.Array.CopyTo(array, index);
        }

        public bool Contains(S toFind)
        {
            return this.IndexOf(toFind) >= 0;
        }
        #endregion
    }
}
