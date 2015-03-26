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
using System.Collections;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace Microsoft.Research.Naiad.Frameworks.DifferentialDataflow
{
    /// <summary>
    /// Growable array supporting direct array access.
    /// </summary>
    /// <typeparam name="S"></typeparam>
    internal class NaiadList<S> //: IList<S>
    {
        /// <summary>
        /// Array containing data.
        /// </summary>
        public S[] Array;

        private int count;

        /// <summary>
        /// Ensures that there is enough space for size records.
        /// </summary>
        /// <param name="size">minimum intended capacity</param>
        public void EnsureCapacity(int size)
        {
            if (Array.Length < size)
            {
                var newArraySize = Math.Max(2 * Array.Length, 4);
                while (newArraySize < size)
                    newArraySize = 2 * newArraySize;

                var newArray = new S[newArraySize];

                for (int i = 0; i < count; i++)
                    newArray[i] = Array[i];

                Array = newArray;
            }
        }

        void EnsureAvailable(int size)
        {
            EnsureCapacity(Count + size);
        }

        /// <summary>
        /// Resets the list to contain no elements.
        /// </summary>
        public void Clear() 
        { 
            this.Count = 0;
        }

        /// <summary>
        /// Adds an element to the end of the list
        /// </summary>
        /// <param name="element">element to add</param>
        public void Add(S element)
        {
            this.EnsureAvailable(1);
            this.Array[this.count++] = element;
        }

        /// <summary>
        /// Returns an element at a specific position, replacing it with the last element.
        /// </summary>
        /// <param name="index">index</param>
        /// <returns>element at index</returns>
        public S RemoveAtAndReturn(int index)
        {
            S element = Array[index];
            Array[index] = Array[count - 1];
            count--;

            return element;
        }

        /// <summary>
        /// Gets and sets the count of valid elements.
        /// </summary>
        public int Count { get { return this.count; } set { this.count = value; } }

        /// <summary>
        /// Constructs an empty NaiadList of an initial size.
        /// </summary>
        /// <param name="size">initial size</param>
        public NaiadList(int size)
        {
            count = 0;
            Array = size > 0 ? new S[size] : new S[0];
        }

        /// <summary>
        /// Writes the contents of the list to a NaiadWriter.
        /// </summary>
        /// <param name="writer">naiad writer</param>
        public void Checkpoint(Naiad.Serialization.NaiadWriter writer)
        {
            writer.Write(this.Count);
            for (int i = 0; i < this.Count; ++i)
                writer.Write(this.Array[i]);
        }

        /// <summary>
        /// Restores the contents of the list from a NaiadReader.
        /// </summary>
        /// <param name="reader">naiad reader</param>
        /// <param name="clear">reset the list before restoring if true</param>
        public void Restore(Naiad.Serialization.NaiadReader reader, bool clear = true)
        {
            if (clear) this.Clear();
            int baseCount = this.count;
            int count = reader.Read<int>();
            this.EnsureCapacity(count + baseCount);
            this.Count = count + baseCount;
            for (int i = baseCount; i < this.Count; ++i)
                this.Array[i] = reader.Read<S>();
        }
    }


}
