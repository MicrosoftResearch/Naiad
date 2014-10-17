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
using System.Runtime.InteropServices;
using System.Text;
using Microsoft.Research.Naiad.Dataflow.Channels;
using Microsoft.Research.Naiad.Serialization;

namespace Microsoft.Research.Naiad.DataStructures
{
    /// <summary>
    /// A list with a spine that grows.
    /// </summary>
    /// <typeparam name="T">record type</typeparam>
    internal class SpinedList<T>
    {
        T[][] Spine;

        T[] Small;
        const int SmallInit = 16;
        const int SmallLimit = 65536;

        /// <summary>
        /// Number of valid records.
        /// </summary>
        public int Count;

        private long size;

        /// <summary>
        /// Indexer
        /// </summary>
        /// <param name="index">index</param>
        /// <returns>element at position index</returns>
        public T this[int index]
        {
            get
            {
                if (this.Spine == null)
                {
                    return this.Small[index];
                }
                else
                {
                    if (this.Spine[index / 65536] != null)
                        return this.Spine[index / 65536][index % 65536];
                    else
                        return default(T);
                }
            }
            set
            {
                if (this.Spine == null)
                {
                    this.Small[index] = value;
                }
                else
                {
                    this.Spine[index / 65536][index % 65536] = value;
                }
            }
        }

        int elementSize;

        /// <summary>
        /// Adds an element to the list
        /// </summary>
        /// <param name="element"></param>
        public void Add(T element)
        {
            if (Count < SmallLimit)
            {
                if (Count == Small.Length)
                {
                    T[] old = this.Small;
                    this.Small = new T[old.Length * 2];
                    var extra = old.Length * elementSize;

                    old.CopyTo(this.Small, 0);

                    this.size += extra;
                }
                Small[Count] = element;
            }
            else
            {
                if (Count == SmallLimit)
                {
                    var extra = 0;

                    this.Spine = new T[65536][];
                    extra += 8 * 65536;

                    Spine[0] = Small;

                    this.size += extra;
                }

                if (Count % 65536 == 0)
                {
                    //if (Spine[Count / 65536] == null)
                    Spine[Count / 65536] = new T[65536];
                    this.size += 65536 * elementSize;
                }
                Spine[Count / 65536][Count % 65536] = element;
            }
            Count++;
        }

        /// <summary>
        /// Enumerates the contents of the list AND CONSUMES THE ELEMENTS.
        /// XXX : Implementation should record that data are now invalid.
        /// </summary>
        /// <returns>Element enumeration</returns>
        public IEnumerable<T> AsEnumerable()
        {
            if (Count <= SmallLimit)
            {
                for (int i = 0; i < Count; i++)
                {
                    yield return Small[i];
                }
            }
            else
            {
                // empties the spine as it goes.
                for (int i = 0; i < Count; i++)
                {
                    yield return Spine[i / 65536][i % 65536];

                    if (i > 0 && i % 65536 == 0)
                        Spine[(i / 65536) - 1] = null;
                }
            }
        }

        /// <summary>
        /// Checkpoints to NaiadWriter
        /// </summary>
        /// <param name="writer"></param>
        /// <param name="serializer"></param>
        /// <param name="intSerializer"></param>
        public void Checkpoint(NaiadWriter writer, NaiadSerialization<T> serializer, NaiadSerialization<Int32> intSerializer)
        {
            writer.Write(this.Count, intSerializer);
            for (int i = 0; i < this.Count; ++i)
                writer.Write(this.Spine[i / 65536][i % 65536], serializer);
        }

        /// <summary>
        /// Restores from NaiadReader
        /// </summary>
        /// <param name="reader"></param>
        /// <param name="serializer"></param>
        /// <param name="intSerializer"></param>
        public void Restore(NaiadReader reader, NaiadSerialization<T> serializer, NaiadSerialization<Int32> intSerializer)
        {
            int readCount = reader.Read(intSerializer);
            this.Count = 0;
            Array.Clear(this.Spine, 0, this.Spine.Length);
            for (int i = 0; i < readCount; ++i)
                this.Add(reader.Read(serializer));
        }

        /// <summary>
        /// Constructs a new SpinedList
        /// </summary>
        public SpinedList()
        {
            this.Count = 0;
            this.Small = new T[SmallInit];
            this.elementSize = 4;
            this.size = SmallInit * this.elementSize;
        }
    }
}
