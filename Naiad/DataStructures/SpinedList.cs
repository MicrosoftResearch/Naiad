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
using System.Runtime.InteropServices;
using System.Text;
using Microsoft.Research.Naiad.Dataflow.Channels;
using Microsoft.Research.Naiad.CodeGeneration;

namespace Microsoft.Research.Naiad.DataStructures
{
#if false
    public class SpinedList<T>
    {
        T[][] Spine;

        public int Count;

        public T this[int index]
        {
            get
            {
                if (this.Spine[index / 65536] != null)
                    return this.Spine[index / 65536][index % 65536];
                else
                    return default(T);
            }
        }

        public void Add(T element)
        {
            if (Count % 65536 == 0)
                //if (Spine[Count / 65536] == null)
                Spine[Count / 65536] = new T[65536];

            Spine[Count / 65536][Count % 65536] = element;
            Count++;
        }

        public IEnumerable<T> AsEnumerable()
        {
            // empties the spine as it goes.
            for (int i = 0; i < Count; i++)
            {
                yield return Spine[i / 65536][i % 65536];

                if (i > 0 && i % 65536 == 0)
                    Spine[(i / 65536) - 1] = null;
            }
        }

        public void Checkpoint(NaiadWriter writer, NaiadSerialization<T> serializer)
        {
            writer.Write(this.Count, PrimitiveSerializers.Int32);
            for (int i = 0; i < this.Count; ++i)
                writer.Write(this.Spine[i / 65536][i % 65536], serializer);
        }

        public void Restore(NaiadReader reader, NaiadSerialization<T> serializer)
        {
            int readCount = reader.Read(PrimitiveSerializers.Int32);
            this.Count = 0;
            Array.Clear(this.Spine, 0, this.Spine.Length);
            for (int i = 0; i < readCount; ++i)
                this.Add(reader.Read(serializer));
        }

        public SpinedList() { this.Spine = new T[65536][]; this.Count = 0; }
    }

#else
    public class SpinedList<T>
    {
        static long T0;
        T[][] Spine;

        T[] Small;
        const int SmallInit = 16;
        const int SmallLimit = 65536;

        public int Count;
        private long size;

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

                    
                    
#if true
                    Spine[0] = Small;
#else
                    if (SmallLimit < 65536)
                    {
                        Spine[0] = new T[65536];
                        Small.CopyTo(Spine[0], 0);
                        extra += 65536 * elementSize;
                    }
                    else
                    {
                        Spine[0] = Small;
                    }
#endif
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

        public void Checkpoint(NaiadWriter writer, NaiadSerialization<T> serializer, NaiadSerialization<Int32> intSerializer)
        {
            writer.Write(this.Count, intSerializer);
            for (int i = 0; i < this.Count; ++i)
                writer.Write(this.Spine[i / 65536][i % 65536], serializer);
        }

        public void Restore(NaiadReader reader, NaiadSerialization<T> serializer, NaiadSerialization<Int32> intSerializer)
        {
            int readCount = reader.Read(intSerializer);
            this.Count = 0;
            Array.Clear(this.Spine, 0, this.Spine.Length);
            for (int i = 0; i < readCount; ++i)
                this.Add(reader.Read(serializer));
        }

        public SpinedList()
        {
            if (T0 == 0) T0 = DateTime.Now.Ticks;
            this.Count = 0;
            this.Small = new T[SmallInit];
            this.elementSize = 4;
            this.size = SmallInit * this.elementSize;
        }
    }

#endif
}
