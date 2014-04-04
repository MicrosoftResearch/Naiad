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
using Microsoft.Research.Naiad.Dataflow.Channels;
using Microsoft.Research.Naiad.CodeGeneration;
using Microsoft.Research.Naiad.DataStructures;
using Microsoft.Research.Naiad.FaultTolerance;

namespace Microsoft.Research.Naiad.Frameworks.DifferentialDataflow.CollectionTrace
{
    internal struct Handle<T>
    {
        public readonly T[] Array;
        public readonly int Offset;
        public readonly int Length;

        public Handle(T[] array, int offset, int length) { Array = array; Offset = offset; Length = length; }
    }

    internal struct RegionHandle<R>
    {
        public Handle<R> Handle;

        public readonly int Identifier;     // a unique ID for this region
        public readonly int HeapOffset;     // offset into a fixed-length heap
        public readonly int HeapIndex;      // index of the fixed-length heap.

        public bool IsNull { get { return Identifier == 0; } }

        public RegionHandle(int i, int o, int l)
        {
            Handle = default(Handle<R>);

            Identifier = i; HeapOffset = o; HeapIndex = l;
        }

        public RegionHandle(int i)
        {
            Handle = default(Handle<R>);

            Identifier = i;

            if (Identifier != 0)
            {
                HeapIndex = 0;
                while ((Identifier >> HeapIndex) % 2 == 0)
                    HeapIndex++;

                HeapOffset = Identifier >> (HeapIndex + 1);
            }
            else
            {
                HeapIndex = 0;
                HeapOffset = 0;
            }
        }

        public RegionHandle(int o, int l)
        {
            Handle = default(Handle<R>);

            HeapOffset = o;
            HeapIndex = l;

            Identifier = (HeapOffset << (HeapIndex + 1)) + (1 << HeapIndex);
        }
    }

    internal struct FixedLengthHeap<T> : ICheckpointable
        where T : IEquatable<T>
    {
        public NaiadList<T[]> Spine;          //  
        public NaiadList<int> FreeList;       // consider making this a priority queue...

        public readonly int SegmentLength;             // the length of each element in the spine
        public readonly int Length;                    // the length of each allocatable region

        public int Allocated;                 // next available offset

        public Handle<T> Dereference(int Offset)
        {
            return new Handle<T>(Spine.Array[(Offset * Length) / SegmentLength], (Offset * Length) % SegmentLength, Length);
        }

        public int Allocate()
        {
            if (FreeList.Count > 0)
            {
                return FreeList.RemoveAtAndReturn(0);
            }
            else
            {
                // make sure there are enough elements available
                while (Spine.Count * SegmentLength < (Allocated + 1) * Length)
                    Spine.Add(new T[SegmentLength]);

                return Allocated++;
            }
        }

        public void Release(int region)
        {
            // seems unlikely, but may as well try...
            if (region == Allocated - 1)
                Allocated--;
            else
                FreeList.Add(region);
        }

        public double Density()
        {
            if (Allocated > 0)
                return 1.0 - ((double)FreeList.Count) / Allocated;
            else
                return 1.0;
        }

        /* Checkpoint format:
         * int                         SpineCount
         * T[SegmentLength]*SpineCount Spine
         * NaiadList<int> FreeList
         * int            Allocated
         */

        public void Checkpoint(NaiadWriter writer)
        {
            writer.Write(this.Spine.Count);
            for (int i = 0; i < this.Spine.Count; ++i)
                for (int j = 0; j < this.SegmentLength; ++j)
                    writer.Write(this.Spine.Array[i][j]);

            this.FreeList.Checkpoint(writer);

            writer.Write(this.Allocated);
        }

        public void Restore(NaiadReader reader)
        {
            int spineCount = reader.Read<int>();
            this.Spine.Clear();
            this.Spine.EnsureCapacity(spineCount);
            this.Spine.Count = spineCount;
            for (int i = 0; i < spineCount; ++i)
            {
                this.Spine.Array[i] = new T[this.SegmentLength];
                for (int j = 0; j < this.Spine.Array[i].Length; ++j)
                    this.Spine.Array[i][j] = reader.Read<T>();
            }

            this.FreeList.Restore(reader);

            this.Allocated = reader.Read<int>();
        }

        public bool Stateful { get { return true; } }

        public FixedLengthHeap(int length, int segmentLength)
        {
            Length = length;
            FreeList = new NaiadList<int>(0);
            Spine = new NaiadList<T[]>(0);

            SegmentLength = Math.Max(segmentLength, Length);

            Allocated = 0;
        }
    }

    internal struct VariableLengthHeap<T> : ICheckpointable
        where T : IEquatable<T>
    {
        public readonly FixedLengthHeap<T>[] heaps;


        public Handle<T> Dereference(OffsetLength ol)
        {
            if (ol.IsEmpty)
            {
                return new Handle<T>(null, 0, 0);
            }
            else
            {
                var offset = 0;
                var length = 0;

                ol.GetOffsetLength(out offset, out length);

                return heaps[length].Dereference(offset);
            }
        }

        public Handle<T> Dereference(int Offset, int Length)
        {
            return heaps[Length].Dereference(Offset);
        }

        public void Release(ref OffsetLength ol)
        {
            if (!ol.IsEmpty)
            {
                var offset = 0;
                var length = 0;

                ol.GetOffsetLength(out offset, out length);

                heaps[length].Release(offset);
                ol = new OffsetLength();
            }
        }

        public Handle<T> Allocate(out OffsetLength ol, int capacity)
        {
            var offset = 0;
            var length = 0;

            var handle = Allocate(out offset, out length, capacity);

            for (int i = 0; i < handle.Length; i++)
                handle.Array[handle.Offset + i] = default(T);

            ol = new OffsetLength(offset, length);

            return handle;
        }

        public Handle<T> Allocate(out int Offset, out int Length, int capacity)
        {
            Length = 0;
            while (capacity > heaps[Length].Length)
                Length++;

            Offset = heaps[Length].Allocate();

            return heaps[Length].Dereference(Offset);
        }

        public Handle<T> EnsureAllocation(ref OffsetLength ol, int capacity)
        {
            if (ol.IsEmpty)
            {
                return Allocate(out ol, capacity);
            }
            else
            {
                var offset = 0;
                var length = 0;

                ol.GetOffsetLength(out offset, out length);

                var handle = EnsureAllocation(ref offset, ref length, capacity);

                ol = new OffsetLength(offset, length);

                return handle;
            }
        }

        Handle<T> EnsureAllocation(ref int Offset, ref int Length, int capacity)
        {
            if (capacity > heaps[Length].Length)
            {
                var oldLength = Length;
                var oldOffset = Offset;

                var newHandle = Allocate(out Offset, out Length, capacity);
                var oldHandle = heaps[oldLength].Dereference(oldOffset);

                for (int i = 0; i < heaps[oldLength].Length; i++)
                    newHandle.Array[newHandle.Offset + i] = oldHandle.Array[oldHandle.Offset + i];

                for (int i = heaps[oldLength].Length; i < heaps[Length].Length; i++)
                    newHandle.Array[newHandle.Offset + i] = default(T);

                heaps[oldLength].Release(oldOffset);

                return newHandle;
            }
            else
                return heaps[Length].Dereference(Offset);
        }

        /* Checkpoint format:
         * FixedLengthHeap<T>*this.heaps.Length heaps
         * [N.B. this.heaps.Length is provided as a parameter to the constructor
         *       and is always 32.]
         */

        public void Checkpoint(NaiadWriter writer)
        {
            for (int i = 0; i < this.heaps.Length; ++i)
            {
                this.heaps[i].Checkpoint(writer);
            }
        }

        public void Restore(NaiadReader reader)
        {
            for (int i = 0; i < this.heaps.Length; ++i)
            {
                this.heaps[i].Restore(reader);
            }
        }

        public bool Stateful { get { return true; } }

        public VariableLengthHeap(int heapCount)
        {
            heaps = Enumerable.Range(0, heapCount).Select(i => new FixedLengthHeap<T>(1 << i, 1 << 10)).ToArray();
        }
    }

    internal struct VariableLengthHeapAlt<T>
        where T : IEquatable<T>
    {
        public FixedLengthHeap<T>[] heaps;

        public RegionHandle<T> Dereference(int identifier)
        {
            var result = new RegionHandle<T>(identifier);

            if (!result.IsNull)
                result.Handle = heaps[result.HeapIndex].Dereference(result.HeapOffset);

            return result;
        }

        public void Release(ref RegionHandle<T> handle)
        {
            if (!handle.IsNull)
            {
                heaps[handle.HeapIndex].Release(handle.HeapOffset);
                handle = new RegionHandle<T>();
            }
        }

        public RegionHandle<T> Allocate(int capacity)
        {
            var index = 0;
            while (capacity > heaps[index].Length)
                index++;

            var result = new RegionHandle<T>(heaps[index].Allocate(), index);

            result.Handle = heaps[result.HeapIndex].Dereference(result.HeapOffset);

            return result;
        }

        public void EnsureAllocation(ref RegionHandle<T> handle, int capacity)
        {
            if (handle.IsNull)
                handle = Allocate(capacity);
            else
            {
                if (capacity > heaps[handle.HeapIndex].Length)
                {
                    var newHandle = Allocate(capacity);

                    for (int i = 0; i < heaps[handle.HeapIndex].Length; i++)
                        newHandle.Handle.Array[newHandle.Handle.Offset + i] = handle.Handle.Array[handle.Handle.Offset + i];

                    for (int i = heaps[handle.HeapIndex].Length; i < heaps[newHandle.HeapIndex].Length; i++)
                        newHandle.Handle.Array[newHandle.Handle.Offset + i] = default(T);

                    Release(ref handle);

                    handle = newHandle;
                }
            }
        }

        public VariableLengthHeapAlt(int heapCount, int segmentSize = 1 << 10)
        {
            heaps = Enumerable.Range(0, heapCount).Select(i => new FixedLengthHeap<T>(1 << i, segmentSize)).ToArray();
        }
    }
}
