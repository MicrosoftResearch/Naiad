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

namespace Microsoft.Research.Naiad
{
    // Simple circular buffer for logging
    // Not thread safe
    internal class CircularLogBuffer<T>
    {
        public int Head = 0;
        public int Tail = 0;
        public int Length;
        public int NumElements = 0;
        public bool OverwriteIfFull = false;
        public T emptyslot;

        public T[] data;

        // Add an element to the head.
        public void Add(T element)
        {
            data[Head] = element;
            if (NumElements == Length && !OverwriteIfFull)
            {
                throw (new Exception("Tried to add element to full buffer"));
            }
            if (NumElements == Length && Head == Tail)
            {
                Tail++;
            }
            Head++;
            if (Head == Length) Head = 0;
            if (NumElements < Length) NumElements++;
        }

        // Remove the tail element and return it.
        public T Remove()
        {
            if (NumElements == 0)
            {
                throw (new Exception("Tried to remove from empty buffer"));
            }
            T element = data[Tail];
            data[Tail] = emptyslot;
            Tail++;
            if (Tail == Length) Tail = 0;
            NumElements--;
            return element;
        }

        public bool isFull()
        {
            return (NumElements == Length);
        }

        public bool isEmpty()
        {
            return (NumElements == 0);
        }

        public void Clear()
        {
            NumElements = 0;
            Head = 0;
            Tail = 0;
            for (int i = 0; i < Length; i++)
            {
                data[i] = emptyslot;
            }
        }

        public override string ToString()
        {
            string s = String.Format("Head={0} Tail={1} Length={2} NumElements={3} data=[", Head, Tail, Length, NumElements);
            for (int i = 0; i < Length; i++)
            {
                if (i > 0) s = s + ",";
                s = s + data[i];
            }
            s = s + "]";

            return s;
        }

        #region Constructors
        public CircularLogBuffer(int size, T empty)
        {
            data = new T[size];
            Length = size;
            emptyslot = empty;
            for (int i = 0; i < Length; i++)
            {
                data[i] = emptyslot;
            }
        }
        #endregion

    }
}
