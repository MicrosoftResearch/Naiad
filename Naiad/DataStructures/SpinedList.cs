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
using System.Runtime.InteropServices;
using System.Text;
using Microsoft.Research.Naiad.Dataflow.Channels;
using Microsoft.Research.Naiad.Serialization;

namespace Microsoft.Research.Naiad.DataStructures
{
    /// <summary>
    /// Convenience datastructures for storing data efficiently
    /// </summary>
    class NamespaceDoc
    {
    }

    /// <summary>
    /// A list with a spine that grows.
    /// </summary>
    /// <typeparam name="T">record type</typeparam>
    public class SpinedList<T>
    {
        T[][] Spine;

        T[] Small;

        const int SmallInit = 16;
        const int SmallLimit = 65536;

        /// <summary>
        /// Number of valid records.
        /// </summary>
        public int Count { get { return (int) this.count; } }

        private uint count;

        /// <summary>
        /// Indexer
        /// </summary>
        /// <param name="index">index</param>
        /// <returns>element at position index</returns>
        public T this[uint index]
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

        /// <summary>
        /// Adds an element to the list
        /// </summary>
        /// <param name="element">the element to add</param>
        public void Add(T element)
        {
            if (this.count < SmallLimit)
            {
                if (this.count == Small.Length)
                {
                    T[] old = this.Small;
                    this.Small = new T[old.Length * 2];
                    old.CopyTo(this.Small, 0);
                }
                Small[this.count] = element;
            }
            else
            {
                if (this.count == SmallLimit)
                {
                    this.Spine = new T[65536][];
                    Spine[0] = Small;
                    Small = null;
                }

                if ((this.count % 65536) == 0)
                {
                    Spine[this.count / 65536] = new T[65536];
                }

                Spine[this.count / 65536][this.count % 65536] = element;
            }

            this.count++;
        }

        /// <summary>
        /// adds a sequence of elements to the list
        /// </summary>
        /// <param name="elements">the array of elements</param>
        /// <param name="count">the number of valid elements</param>
        public void AddRange(T[] elements, int count)
        {
            for (int i = 0; i < count; i++)
            {
                var element = elements[i];

                if (this.count < SmallLimit)
                {
                    if (this.count == Small.Length)
                    {
                        T[] old = this.Small;
                        this.Small = new T[old.Length * 2];
                        old.CopyTo(this.Small, 0);
                    }
                    Small[this.count] = element;
                }
                else
                {
                    if (this.count == SmallLimit)
                    {
                        this.Spine = new T[65536][];
                        Spine[0] = Small;
                        Small = null;
                    }

                    if ((this.count % 65536) == 0)
                    {
                        Spine[this.count / 65536] = new T[65536];
                    }

                    Spine[this.count / 65536][this.count % 65536] = element;
                }

                this.count++;
            }
        }


        /// <summary>
        /// Enumerates the contents of the list AND CONSUMES THE ELEMENTS.
        /// XXX : Implementation should record that data are now invalid.
        /// </summary>
        /// <returns>Element enumeration</returns>
        public IEnumerable<T> DequeueAllAndInvalidate()
        {
            if (Count <= SmallLimit)
            {
                for (int i = 0; i < Count; i++)
                {
                    yield return Small[i];
                }

                this.Small = null;
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

                this.Spine = null;
            }
        }

        /// <summary>
        /// perform an action on each element of the list, a batch at a time
        /// </summary>
        /// <param name="action">the action to perform on each batch</param>
        public void IterateOver(Action<T[], int> action)
        {
            if (this.Small != null)
            {
                action(this.Small, this.Count);
            }
            else
            {
                for (int i = 0; i < this.Count / 65536; i++)
                    action(this.Spine[i], 65536);

                if ((this.Count % 65536) > 0)
                    action(this.Spine[this.Count / 65536], this.Count % 65536);
            }
        }

        /// <summary>
        /// Checkpoints to NaiadWriter
        /// </summary>
        /// <param name="writer">data writer</param>
        /// <param name="serializer">record serializer</param>
        /// <param name="intSerializer">integer serializer (for lengths)</param>
        public void Checkpoint(NaiadWriter writer, NaiadSerialization<T> serializer, NaiadSerialization<Int32> intSerializer)
        {
            writer.Write(this.Count, intSerializer);
            for (int i = 0; i < this.Count; ++i)
                writer.Write(this.Spine[i / 65536][i % 65536], serializer);
        }

        /// <summary>
        /// Restores from NaiadReader
        /// </summary>
        /// <param name="reader">data reader</param>
        /// <param name="serializer">record deserializer</param>
        /// <param name="intSerializer">integer deserializer (for lengths)</param>
        public void Restore(NaiadReader reader, NaiadSerialization<T> serializer, NaiadSerialization<Int32> intSerializer)
        {
            int readCount = reader.Read(intSerializer);
            this.count = 0;
            Array.Clear(this.Spine, 0, this.Spine.Length);
            for (int i = 0; i < readCount; ++i)
                this.Add(reader.Read(serializer));
        }

        /// <summary>
        /// Constructs a new SpinedList
        /// </summary>
        public SpinedList()
        {
            this.count = 0;
            this.Small = new T[SmallInit];
        }
    }
}
