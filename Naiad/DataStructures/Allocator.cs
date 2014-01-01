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
using Naiad.Dataflow.Channels;
using Naiad.CodeGeneration;
using Naiad.DataStructures;
using Naiad.FaultTolerance;

namespace Naiad.DataStructures
{
    public struct Handle<T>
    {
        public readonly T[] Array;
        public readonly int Offset;
        public readonly int Length;

        internal readonly Descriptor Descriptor;

        public int Identifier { get { return this.Descriptor.Merged; } }
        public bool IsNull { get { return Descriptor.IsNull; } }

        public Handle(T[] a, int o, int l, Descriptor d)
        {
            Array = a;
            Offset = o;
            Length = l;
            Descriptor = d;
        }
    }

    public struct Descriptor
    {
        internal readonly int HeapLocalOffset;
        internal readonly int HeapIdentifier;   // actually log length
        internal readonly int Merged;

        public bool IsNull { get { return Merged == 0; } }

        public Descriptor(int Identifier)
        {
            this.Merged = Identifier;
            if (Identifier != 0)
            {
                HeapIdentifier = 0;
                while ((Identifier >> HeapIdentifier) % 2 == 0)
                    HeapIdentifier++;

                HeapLocalOffset = Identifier >> (HeapIdentifier + 1);
            }
            else
            {
                HeapLocalOffset = 0;
                HeapIdentifier = 0;
            }
        }
        public Descriptor(int hl, int ho)
        {
            HeapLocalOffset = ho;
            HeapIdentifier = hl;

            Merged = (HeapLocalOffset << (HeapIdentifier + 1)) + (1 << HeapIdentifier);
        }
    }


    // emulates a flat array using a spine of fixed size.
    // each region has a fixed length, and integers index 
    // the region rather than the array offset.
    internal struct FixedLengthHeap<T> : ICheckpointable
    {
        internal NaiadList<T[]> Spine;          //  
        internal NaiadList<int> FreeList;       // consider making this a priority queue...

        internal readonly int SegmentLength;    // the length of each element in the spine
        internal readonly int Length;           // the length of each allocatable region 
                                                // always: (Length <= SegmentLength)

        internal int Allocated;                 // next available offset

        internal Handle<T> Dereference(Descriptor descriptor)
        {
            return new Handle<T>(Spine.Array[(descriptor.HeapLocalOffset * Length) / SegmentLength], (descriptor.HeapLocalOffset * Length) % SegmentLength, Length, descriptor);
        }

        internal Handle<T> Allocate(int LogLength)
        {
            if (FreeList.Count > 0)
            {
                var region = FreeList.RemoveAtAndReturn(FreeList.Count - 1);
                return this.Dereference(new Descriptor(LogLength,region));
            }
            else
            {
                // make sure there are enough elements available
                while (Spine.Count * SegmentLength < (Allocated + 1) * Length)
                    Spine.Add(new T[SegmentLength]);

                return this.Dereference(new Descriptor(LogLength, Allocated++));
            }
        }

        internal void Release(ref Handle<T> handle)
        {
            FreeList.Add(handle.Descriptor.HeapLocalOffset);

            handle = new Handle<T>();
        }

        #region Checkpointing

        /* Checkpoint format:
         * int                         SpineCount
         * T[SegmentLength]*SpineCount Spine
         * NaiadList<int> FreeList
         * int            Allocated
         */

        private static NaiadSerialization<T> tSerializer = null;

        public void Checkpoint(NaiadWriter writer)
        {
            if (tSerializer == null)
                tSerializer = AutoSerialization.GetSerializer<T>();

            writer.Write(this.Spine.Count, PrimitiveSerializers.Int32);
            for (int i = 0; i < this.Spine.Count; ++i)
                for (int j = 0; j < this.SegmentLength; ++j)
                    writer.Write(this.Spine.Array[i][j], tSerializer);

            this.FreeList.Checkpoint(writer, PrimitiveSerializers.Int32);

            writer.Write(this.Allocated, PrimitiveSerializers.Int32);
        }

        public void Restore(NaiadReader reader)
        {
            if (tSerializer == null)
                tSerializer = AutoSerialization.GetSerializer<T>();

            int spineCount = reader.Read<int>(PrimitiveSerializers.Int32);
            this.Spine.Clear();
            this.Spine.EnsureCapacity(spineCount);
            this.Spine.Count = spineCount;
            for (int i = 0; i < spineCount; ++i)
            {
                this.Spine.Array[i] = new T[this.SegmentLength];
                for (int j = 0; j < this.Spine.Array[i].Length; ++j)
                    this.Spine.Array[i][j] = reader.Read<T>(tSerializer);
            }

            this.FreeList.Restore(reader, PrimitiveSerializers.Int32);

            this.Allocated = reader.Read<int>(PrimitiveSerializers.Int32);
        }

        public bool Stateful { get { return true; } }

        #endregion

        internal FixedLengthHeap(int length, int segmentLength)
        {
            Length = length;
            FreeList = new NaiadList<int>(0);
            Spine = new NaiadList<T[]>(0);

            SegmentLength = Math.Max(segmentLength, Length);

            Allocated = 0;
        }
    }

    public struct Allocator<T>
    {
        internal FixedLengthHeap<T>[] heaps;

        public Handle<T> Dereference(int identifier)
        {
            var descriptor = new Descriptor(identifier);
            if (descriptor.IsNull)
            {
                return new Handle<T>(null, 0, 0, descriptor);
            }
            else
            {
                return heaps[descriptor.HeapIdentifier].Dereference(descriptor);
            }
        }

        public void Release(ref Handle<T> handle)
        {
            if (!handle.IsNull)
                heaps[handle.Descriptor.HeapIdentifier].Release(ref handle);
        }

        public Handle<T> Allocate(int capacity)
        {
            var logLength = 0;
            while (capacity > heaps[logLength].Length)
                logLength++;

            return heaps[logLength].Allocate(logLength);
        }

        public void EnsureAllocation(ref Handle<T> handle, int capacity)
        {
            if (handle.IsNull)
                handle = Allocate(capacity);
            else
            {
                if (capacity > heaps[handle.Descriptor.HeapIdentifier].Length)
                {
                    var newHandle = Allocate(capacity);

                    for (int i = 0; i < handle.Length; i++)
                        newHandle.Array[newHandle.Offset + i] = handle.Array[handle.Offset + i];

                    for (int i = handle.Length; i < newHandle.Length; i++)
                        newHandle.Array[newHandle.Offset + i] = default(T);

                    Release(ref handle);

                    handle = newHandle;
                }
            }
        }

        public Allocator(int heapCount, int segmentSize = 1 << 10)
        {
            heaps = Enumerable.Range(0, heapCount)
                              .Select(i => new FixedLengthHeap<T>(1 << i, segmentSize))
                              .ToArray();
        }
    }
}
