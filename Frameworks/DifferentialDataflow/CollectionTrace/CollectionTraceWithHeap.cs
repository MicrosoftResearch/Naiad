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
using System.Text;
using Microsoft.Research.Naiad.Serialization;
using Microsoft.Research.Naiad.Diagnostics;

namespace Microsoft.Research.Naiad.Frameworks.DifferentialDataflow.CollectionTrace
{
    internal static class RandomOffsets
    {
        
        internal readonly static int[][] Offsets;            // probes to determine if a hash table needs to grow in size
        
        static RandomOffsets()
        {
            var random = new Random(0);

            Offsets = new int[32][];
            for (int i = 0; i < Offsets.Length; i++)
            {
                Offsets[i] = new int[10];
                for (int j = 0; j < Offsets[i].Length; j++)
                    Offsets[i][j] = random.Next();
            }
        }

    }

    internal class CollectionTraceWithHeap<S> : CollectionTraceCheckpointable<S>
        where S : IEquatable<S>
    {

        VariableLengthHeap<Weighted<S>> records;    // stores regions of weighted records, represting differences
        VariableLengthHeap<CollectionTraceWithHeapIncrement> increments;   // stores regions of increments, each corresponding to a time

        readonly Func<int, int, bool> TimeLessThan;          // wraps the "less than" partial order on time indices
        readonly Func<int, int> UpdateTime;                  // wraps the reachability-based time advancement

        OffsetLength cachedIncrementOffset;
        OffsetLength cachedRecordsOffset;

        int cachedTimeIndex;

        public void ReleaseCache()
        {
            // only release if tracking a key
            if (!cachedIncrementOffset.IsEmpty)
            {
                records.Release(ref cachedRecordsOffset);
                cachedIncrementOffset = new OffsetLength();
                cachedTimeIndex = 0;
            }
        }

        // updates the collection at offsetLength to modify the weight of element at timeIndex
        public void Introduce(ref int offsetLength, S element, Int64 weight, int timeIndex)
        {
            var ol = new OffsetLength(offsetLength);

            Introduce(ref ol, element, weight, timeIndex);

            offsetLength = ol.offsetLength;

            ReleaseCache();
        }

        // private version of the above using the decoded OffsetLength type
        void Introduce(ref OffsetLength offsetLength, S element, Int64 weight, int timeIndex)
        {
            if (weight != 0)
            {
                var handle = EnsureTime(ref offsetLength, timeIndex);

                var position = 0;
                while (handle.Array[handle.Offset + position].TimeIndex != timeIndex)
                    position++;

                Introduce(ref handle.Array[handle.Offset + position].OffsetLength, element, weight);

                // if the introduction results in an empty region, we need to clean up
                if (handle.Array[handle.Offset + position].IsEmpty)
                {
                    // drag everything after it down one
                    for (int i = position + 1; i < handle.Length; i++)
                        handle.Array[handle.Offset + i - 1] = handle.Array[handle.Offset + i];

                    handle.Array[handle.Offset + handle.Length - 1] = new CollectionTraceWithHeapIncrement();

                    // if the root element is empty, the list must be empty
                    if (handle.Array[handle.Offset].IsEmpty)
                        increments.Release(ref offsetLength);
                }
            }
        }

        // indicates where in a length array we might find element
        // ideally, switch to the other option, to allow single pass resizing
        protected int GetIntendedIndex(S element, int length)
        {
            //return (int.MaxValue & element.GetHashCode()) / (int.MaxValue / length);
            return ((int.MaxValue & element.GetHashCode()) / this.numVertices) % length;
        }

        private const int HashTableThresholdCount = 32;
        void Introduce(ref OffsetLength offsetLength, S element, Int64 weight)
        {
            if (!offsetLength.IsEmpty && records.heaps[offsetLength.Length].Length >= HashTableThresholdCount)
                IntroduceAsHashTable(ref offsetLength, element, weight);
            else
                IntroduceAsUnsortedList(ref offsetLength, element, weight);
        }

        protected Handle<Weighted<S>> EnsureAllocation(ref OffsetLength ol, int newLength)
        {
            var handle = records.Dereference(ol);
            if (newLength > handle.Length)
            {
                var newOl = new OffsetLength();
                var newHandle = records.Allocate(out newOl, newLength);


#if false
                if (newHandle.Length < HashTableThresholdCount)
                {
                    for (int i = 0; i < handle.Length; i++)
                        newHandle.Array[newHandle.Offset + i] = handle.Array[handle.Offset + i];

                    for (int i = handle.Length; i < newHandle.Length; i++)
                        newHandle.Array[newHandle.Offset + i] = new Weighted<S>();
                }
                else
                {
                    for (int i = 0; i < handle.Length && handle.Array[handle.Offset + i].weight != 0; i++)
                        IntroduceAsHashTable(ref newOl, handle.Array[handle.Offset + i].record, handle.Array[handle.Offset + i].weight);
                }
#else
                for (int i = 0; i < newHandle.Length; i++)
                    newHandle.Array[newHandle.Offset + i] = new Weighted<S>();

                for (int i = 0; i < handle.Length; i++)
                    if (handle.Array[handle.Offset + i].weight != 0)
                        Introduce(ref newOl, handle.Array[handle.Offset + i].record, handle.Array[handle.Offset + i].weight);

#endif
                records.Release(ref ol);
                ol = newOl;

                //return newHandle;
                return records.Dereference(ol);
            }
            else
                return handle;
        }

        /// <summary>
        /// Incorporates one element into an array.
        /// </summary>
        /// <param name="into">Target</param>
        /// <param name="from">Source</param>
        /// <param name="weight">Weight</param>
        protected void IntroduceAsHashTable(ref OffsetLength into, S from, Int64 weight)
        {
            var handle = records.Dereference(into);

            var index = GetIntendedIndex(from, handle.Length);

            var resize = true;
            while (resize)
            {
                for (int i = 0; i < 10; i++)
                {
                    var idx = handle.Offset + ((Int32.MaxValue & (index + RandomOffsets.Offsets[into.Length][i])) % handle.Length);
                    if (idx < 0 || idx >= handle.Array.Length)
                    {
                        Logging.Error("IntroduceAsHashTable: trying to insert at bad index {0}", idx);
                        Logging.Error("handle.Array.Length={0}, handle.Offset={1}, handle.Length={2}, index={3}, randomOffsets[into.Length][i]={4}",
                            handle.Array.Length, handle.Offset, handle.Length, index, RandomOffsets.Offsets[into.Length][i]);
                    }
                    if (handle.Array[idx].weight == 0)
                        resize = false;
                }
                if (resize)
                {
                    handle = EnsureAllocation(ref into, 2 * handle.Length);
                    index = GetIntendedIndex(from, handle.Length);
                }
            }

            var next = index;

            var done = false;
            // until we find somewhere to put the record. The first possibility is that we found an empty location.
            // the other is that we have found someone to swap out for the 
            while (!done)
            {
                // if we found somewhere to put it, put it there!
                if (handle.Array[handle.Offset + next].weight == 0)
                {
                    handle.Array[handle.Offset + next] = new Weighted<S>(from, weight);
                    weight = 0;
                    //if (handle.Array[handle.Offset + next].weight != 0)
                    //    count++;

                    done = true;
                }
                else
                {
                    // what is the hash / intended position of the person in our spot?
                    var other = GetIntendedIndex(handle.Array[handle.Offset + next].record, handle.Length);

                    // we may have found the record! add in weight, and subtract from weight.
                    if (index == other && from.Equals(handle.Array[handle.Offset + next].record))
                    {
                        handle.Array[handle.Offset + next].weight += weight;
                        weight = 0;
                        //if (handle.Array[handle.Offset + next].weight == 0)
                        //    count--;

                        done = true;
                    }
                    // it may be that we have found an inversion, resulting in a big shift.
                    else if ((index > next != other > next) ? index > next : index < other)
                        done = true;
                    // or we might not have found anything, and need to keep going... =/
                    else
                        next = (next + 1) % handle.Length;
                }
            }

            // must shift!
            var shifts = 0;
            if (weight != 0)
            {
                while (weight != 0)
                {
                    var temp = handle.Array[handle.Offset + next];
                    handle.Array[handle.Offset + next] = new Weighted<S>(from, weight);
                    from = temp.record;
                    weight = temp.weight;
                    next = (next + 1) % handle.Length;

                    shifts++;
                }
            }

            // we might have to do some cleaning up. slide 
            else if (weight == 0 && handle.Array[handle.Offset + next].weight == 0)
            {
                var nextnext = (next + 1) % handle.Length;

                // only evaluate hashcode on known valid data
                if (handle.Array[handle.Offset + nextnext].weight != 0)
                    index = GetIntendedIndex(handle.Array[handle.Offset + nextnext].record, handle.Length);

                // as long as a slide would not introduce a new inversion...
                while (index != nextnext && handle.Array[handle.Offset + nextnext].weight != 0)
                {
                    handle.Array[handle.Offset + next] = handle.Array[handle.Offset + nextnext];
                    handle.Array[handle.Offset + nextnext] = new Weighted<S>();

                    next = nextnext;
                    nextnext = (next + 1) % handle.Length;

                    // only evaluate hashcode on known valid data
                    if (handle.Array[handle.Offset + nextnext].weight != 0)
                        index = GetIntendedIndex(handle.Array[handle.Offset + nextnext].record, handle.Length);
                }
            }
        }

        void IntroduceAsUnsortedList(ref OffsetLength offsetLength, S element, Int64 weight)
        {
            if (offsetLength.IsEmpty)
            {
                var handle = records.EnsureAllocation(ref offsetLength, 1);
                handle.Array[handle.Offset] = new Weighted<S>(element, weight);
            }
            else
            {
                var handle = records.Dereference(offsetLength);

                if (handle.Length >= HashTableThresholdCount)
                    throw new Exception("???");

                for (int i = 0; i < handle.Length; i++)
                {
                    if (handle.Array[handle.Offset + i].weight != 0 && handle.Array[handle.Offset + i].record.Equals(element))
                    {
                        handle.Array[handle.Offset + i].weight += weight;
                        if (handle.Array[handle.Offset + i].weight == 0)
                        {
                            for (int j = i + 1; j < handle.Length; j++)
                                handle.Array[handle.Offset + j - 1] = handle.Array[handle.Offset + j];

                            handle.Array[handle.Offset + handle.Length - 1] = new Weighted<S>();

                            if (handle.Array[handle.Offset].weight == 0)
                                records.Release(ref offsetLength);
                        }

                        return;
                    }
                }

                // if the allocation is null, or if it is full, get some more allocation
                if (handle.Length == 0 || handle.Array[handle.Offset + handle.Length - 1].weight != 0)
                    handle = EnsureAllocation(ref offsetLength, handle.Length + 1);

                // we may have crossed into hash territory...
                if (handle.Length >= HashTableThresholdCount)
                    IntroduceAsHashTable(ref offsetLength, element, weight);
                else
                {
                    for (int i = 0; i < handle.Length; i++)
                        if (handle.Array[handle.Offset + i].weight == 0)
                        {
                            handle.Array[handle.Offset + i] = new Weighted<S>(element, weight);
                            return;
                        }
                }
            }
        }

        void IntroduceIncrements(ref OffsetLength thisOffsetLength, OffsetLength thatOffsetLength, int scale)
        {
            var handle = increments.Dereference(thatOffsetLength);
            for (int i = 0; i < handle.Length; i++)
                if (!handle.Array[handle.Offset + i].IsEmpty)
                {
                    var recordHandle = records.Dereference(handle.Array[handle.Offset + i].OffsetLength);

                    for (int j = 0; j < recordHandle.Length; j++)
                        if (recordHandle.Array[recordHandle.Offset + j].weight != 0)
                            Introduce(ref thisOffsetLength, recordHandle.Array[recordHandle.Offset + j].record, scale * recordHandle.Array[recordHandle.Offset + j].weight, handle.Array[handle.Offset + i].TimeIndex);
                }
        }

        void IntroduceRecords(ref OffsetLength thisOffsetLength, OffsetLength thatOffsetLength, int scale)
        {
            if (thisOffsetLength.IsEmpty)
            {
                var thatHandle = records.Dereference(thatOffsetLength);
                var thisHandle = records.EnsureAllocation(ref thisOffsetLength, thatHandle.Length);

                if (thisHandle.Length != thatHandle.Length)
                    throw new Exception("???");

                for (int i = 0; i < thisHandle.Length; i++)
                    thisHandle.Array[thisHandle.Offset + i] = thatHandle.Array[thatHandle.Offset + i];

                if (scale != 1)
                {
                    for (int i = 0; i < thisHandle.Length; i++)
                        thisHandle.Array[thisHandle.Offset + i].weight *= scale;
                }
            }
            else
            {
                var handle = records.Dereference(thatOffsetLength);
                for (int i = 0; i < handle.Length; i++)
                    if (handle.Array[handle.Offset + i].weight != 0)
                        Introduce(ref thisOffsetLength, handle.Array[handle.Offset + i].record, scale * handle.Array[handle.Offset + i].weight);
            }
        }

        Handle<CollectionTraceWithHeapIncrement> EnsureTime(ref OffsetLength offsetLength, int timeIndex)
        {
            var handle = increments.Dereference(offsetLength);

            for (int i = 0; i < handle.Length; i++)
            {
                // if we found the time, it is ensured and we can return
                if (handle.Array[handle.Offset + i].TimeIndex == timeIndex)
                    return handle;

                // if we found an empty slot, new it up and return
                if (handle.Array[handle.Offset + i].IsEmpty)
                {
                    handle.Array[handle.Offset + i] = new CollectionTraceWithHeapIncrement(timeIndex);
                    return handle;
                }
            }

            // if we didn't find it, and no empty space for it
            var oldLength = handle.Length;
            handle = increments.EnsureAllocation(ref offsetLength, handle.Length + 1);
            handle.Array[handle.Offset + oldLength] = new CollectionTraceWithHeapIncrement(timeIndex);

            return handle;
        }

        public void IntroduceFrom(ref int thisKeyIndex, ref int thatKeyIndex, bool delete = true)
        {
            if (delete && thisKeyIndex == 0)
            {
                thisKeyIndex = thatKeyIndex;
                thatKeyIndex = 0;
                ReleaseCache();
            }
            else
            {
                var ol1 = new OffsetLength(thisKeyIndex);
                var ol2 = new OffsetLength(thatKeyIndex);

                if (!ol2.IsEmpty)
                {
                    IntroduceIncrements(ref ol1, ol2, 1);

                    thisKeyIndex = ol1.offsetLength;

                    if (delete)
                        ZeroState(ref thatKeyIndex);

                    ReleaseCache();
                }
            }
        }

        public void SubtractStrictlyPriorDifferences(ref int keyIndex, int timeIndex)
        {
            var ol = new OffsetLength(keyIndex);

            // if there aren't any strictly prior differences we can just return
            if (ol.IsEmpty)
                return;

            var handle = EnsureTime(ref ol, timeIndex);
            var position = 0;
            while (handle.Array[handle.Offset + position].TimeIndex != timeIndex)
                position++;

            // if the destination time is empty, we can swap in the accumulation (negated)
            if (handle.Array[handle.Offset + position].IsEmpty)
            {
                // swap the accumulation in, and zero out the accumulation (the new correct accumulation for this key).
                handle.Array[handle.Offset + position] = new CollectionTraceWithHeapIncrement(UpdateAccumulation(ref ol, timeIndex), timeIndex);

                // we may have ended up with a null accumulation, must clean up
                if (handle.Array[handle.Offset + position].IsEmpty)
                {
                    for (int i = position + 1; i < handle.Length; i++)
                        handle.Array[handle.Offset + i - 1] = handle.Array[handle.Offset + i];

                    handle.Array[handle.Offset + handle.Length - 1] = new CollectionTraceWithHeapIncrement();
                    if (handle.Array[handle.Offset].OffsetLength.IsEmpty)
                        increments.Release(ref ol);
                }
                else
                {
                    var accumHandle = records.Dereference(handle.Array[handle.Offset + position].OffsetLength);

                    for (int i = 0; i < accumHandle.Length; i++)
                        accumHandle.Array[accumHandle.Offset + i].weight *= -1;
                }

                // important to update the cached accumulation to reflect the emptiness
                // only do this if the cached accumulation is what we are working with
                if (cachedIncrementOffset.offsetLength == ol.offsetLength)
                {
                    cachedRecordsOffset = new OffsetLength();
                    cachedIncrementOffset = ol;
                    cachedTimeIndex = timeIndex;
                }
            }
            else
                throw new Exception("Attemping subtraction from non-empty time; something wrong in Operator logic");

            keyIndex = ol.offsetLength;
        }

        Dictionary<S, int> accumulationNotes = new Dictionary<S, int>();
        public void EnumerateCollectionAt(int offsetLength, int timeIndex, NaiadList<Weighted<S>> toFill)
        {
            if (toFill.Count == 0)
            {
                var temp = new OffsetLength(offsetLength);
                var handle = EnumerateCollectionAt(ref temp, timeIndex);

                for (int i = 0; i < handle.Length; i++)
                    if (handle.Array[handle.Offset + i].weight != 0)
                        toFill.Add(handle.Array[handle.Offset + i]);
            }
            else
            {
                accumulationNotes.Clear();
                for (int i = 0; i < toFill.Count; i++)
                    accumulationNotes[toFill.Array[i].record] = i;

                var temp = new OffsetLength(offsetLength);
                var handle = EnumerateCollectionAt(ref temp, timeIndex);

                for (int i = 0; i < handle.Length; i++)
                    if (handle.Array[handle.Offset + i].weight != 0)
                    {
                        var index = 0;
                        if (accumulationNotes.TryGetValue(handle.Array[handle.Offset + i].record, out index))
                            toFill.Array[index].weight += handle.Array[handle.Offset + i].weight;
                        else
                            toFill.Add(handle.Array[handle.Offset + i]);
                    }

                var counter = 0;
                for (int i = 0; i < toFill.Count; i++)
                    if (toFill.Array[i].weight != 0)
                        toFill.Array[counter++] = toFill.Array[i];

                toFill.Count = counter;
            }
        }

        public Handle<Weighted<S>> EnumerateCollectionAt(ref OffsetLength ol, int timeIndex)
        {
            return records.Dereference(UpdateAccumulation(ref ol, timeIndex));
        }

        // no caching at the moment; should do, but need to figure out how...
        OffsetLength UpdateAccumulation(ref OffsetLength ol, int timeIndex)
        {
#if true
            if (ol.IsEmpty)
                return new OffsetLength();

            var handle = increments.Dereference(ol);

            // special-case single element accumulations to avoid unprocessed accumulation dropping processed accumulation
            if (handle.Length == 1)
            {
                if (TimeLessThan(handle.Array[handle.Offset].TimeIndex, timeIndex))
                    return handle.Array[handle.Offset].OffsetLength;
                else
                    return new OffsetLength();
            }

            else
#else
            var handle = increments.Dereference(ol);
#endif
            {
                // if we have a hit on the cache ...
                if (ol.offsetLength == cachedIncrementOffset.offsetLength)
                {
                    for (int i = 0; i < handle.Length && !handle.Array[handle.Offset + i].IsEmpty; i++)
                    {
                        var inNew = TimeLessThan(handle.Array[handle.Offset + i].TimeIndex, timeIndex);
                        var inOld = TimeLessThan(handle.Array[handle.Offset + i].TimeIndex, cachedTimeIndex);

                        if (inOld != inNew)
                            IntroduceRecords(ref cachedRecordsOffset, handle.Array[handle.Offset + i].OffsetLength, inOld ? -1 : +1);
                    }

                    cachedTimeIndex = timeIndex;
                }
                else
                {
                    ReleaseCache(); // blow cache away and start over

                    for (int i = 0; i < handle.Length && !handle.Array[handle.Offset + i].IsEmpty; i++)
                        if (TimeLessThan(handle.Array[handle.Offset + i].TimeIndex, timeIndex))
                            IntroduceRecords(ref cachedRecordsOffset, handle.Array[handle.Offset + i].OffsetLength, 1);

                    cachedIncrementOffset = ol;
                    cachedTimeIndex = timeIndex;
                }

                return cachedRecordsOffset;
            }
        }

        public void EnumerateDifferenceAt(int offsetLength, int timeIndex, NaiadList<Weighted<S>> toFill)
        {
            if (toFill.Count == 0)
            {
                var temp = new OffsetLength(offsetLength);
                var handle = EnumerateDifferenceAt(ref temp, timeIndex);

                for (int i = 0; i < handle.Length; i++)
                    if (handle.Array[handle.Offset + i].weight != 0)
                        toFill.Add(handle.Array[handle.Offset + i]);
            }
            else
            {
                accumulationNotes.Clear();
                for (int i = 0; i < toFill.Count; i++)
                    accumulationNotes[toFill.Array[i].record] = i;

                var temp = new OffsetLength(offsetLength);
                var handle = EnumerateDifferenceAt(ref temp, timeIndex);

                for (int i = 0; i < handle.Length; i++)
                    if (handle.Array[handle.Offset + i].weight != 0)
                    {
                        var index = 0;
                        if (accumulationNotes.TryGetValue(handle.Array[handle.Offset + i].record, out index))
                            toFill.Array[index].weight += handle.Array[handle.Offset + i].weight;
                        else
                            toFill.Add(handle.Array[handle.Offset + i]);
                    }

                var counter = 0;
                for (int i = 0; i < toFill.Count; i++)
                    if (toFill.Array[i].weight != 0)
                        toFill.Array[counter++] = toFill.Array[i];

                toFill.Count = counter;
            }
        }

        public Handle<Weighted<S>> EnumerateDifferenceAt(ref OffsetLength ol, int timeIndex)
        {
            var handle = increments.Dereference(ol);
            for (int i = 0; i < handle.Length && !handle.Array[handle.Offset + i].IsEmpty; i++)
                if (handle.Array[handle.Offset + i].TimeIndex == timeIndex)
                    return records.Dereference(handle.Array[handle.Offset + i].OffsetLength);

            return new Handle<Weighted<S>>(null, 0, 0);
        }

        HashSet<int> hashSet = new HashSet<int>();
        public void EnumerateTimes(int keyIndex, NaiadList<int> timelist)
        {
            var ol = new OffsetLength(keyIndex);

            if (timelist.Count == 0)
            {
                var handle = increments.Dereference(ol);
                for (int i = 0; i < handle.Length && !handle.Array[handle.Offset + i].IsEmpty; i++)
                    timelist.Add(handle.Array[handle.Offset + i].TimeIndex);
            }
            else
            {
                hashSet.Clear();
                for (int i = 0; i < timelist.Count; i++)
                    hashSet.Add(timelist.Array[i]);

                var handle = increments.Dereference(ol);
                for (int i = 0; i < handle.Length && !handle.Array[handle.Offset + i].IsEmpty; i++)
                {
                    var time = handle.Array[handle.Offset + i].TimeIndex;
                    if (!hashSet.Contains(time))
                    {
                        timelist.Add(time);
                        hashSet.Add(time);
                    }
                }
            }
        }

        void Print(OffsetLength ol)
        {
            var handle = increments.Dereference(ol);
            for (int i = 0; i < handle.Length; i++)
                if (!handle.Array[handle.Offset + i].IsEmpty)
                {
                    Console.WriteLine("data at time {0}", handle.Array[handle.Offset + i].TimeIndex);
                    var rHandle = records.Dereference(handle.Array[handle.Offset + i].OffsetLength);
                    for (int j = 0; j < rHandle.Length; j++)
                        if (rHandle.Array[rHandle.Offset + j].weight != 0)
                            Console.WriteLine("  {0}", rHandle.Array[rHandle.Offset + j]);
                }
        }

        public int AllocateState() { throw new NotSupportedException(); }

        public void ZeroState(ref int keyIndex)
        {
            var temp = new OffsetLength(keyIndex);

            if (!temp.IsEmpty)
            {
                var handle = increments.Dereference(temp);
                for (int i = 0; i < handle.Length; i++)
                    if (!handle.Array[handle.Offset + i].IsEmpty)
                        records.Release(ref handle.Array[handle.Offset + i].OffsetLength);

                increments.Release(ref temp);

                keyIndex = temp.offsetLength;
            }

            ReleaseCache();
        }

        public bool IsZero(ref int keyIndex) { return keyIndex == 0; }

        public void EnsureStateIsCurrentWRTAdvancedTimes(ref int offsetLength)
        {
            var ol = new OffsetLength(offsetLength);

            if (!ol.IsEmpty)
            {
                var handle = increments.Dereference(ol);

                for (int i = 0; i < handle.Length; i++)
                {
                    if (!handle.Array[handle.Offset + i].IsEmpty)
                    {
                        var newIndex = UpdateTime(handle.Array[handle.Offset + i].TimeIndex);

                        // if the time has changed, we may need to collapse the increment
                        //if (handle.Array[handle.Offset + i].TimeIndex != newIndex)
                        {
                            handle.Array[handle.Offset + i].TimeIndex = newIndex;

                            // scan the entire array, stopping early if we have emptied the source increment
                            for (int j = 0; j < i && !handle.Array[handle.Offset + i].IsEmpty; j++)
                            {
                                // if we find another location, not at i, add stuff in there. We should only find one (otherwise, error)
                                if (handle.Array[handle.Offset + j].TimeIndex == handle.Array[handle.Offset + i].TimeIndex)
                                {
                                    IntroduceRecords(ref handle.Array[handle.Offset + j].OffsetLength, handle.Array[handle.Offset + i].OffsetLength, 1);
                                    records.Release(ref handle.Array[handle.Offset + i].OffsetLength);
                                    handle.Array[handle.Offset + i] = new CollectionTraceWithHeapIncrement();
                                }
                            }
                        }
                    }
                }

                var position = 0;
                for (int i = 0; i < handle.Length; i++)
                    if (!handle.Array[handle.Offset + i].IsEmpty)
                    {
                        var temp = handle.Array[handle.Offset + i];
                        handle.Array[handle.Offset + i] = new CollectionTraceWithHeapIncrement();
                        handle.Array[handle.Offset + (position++)] = temp;
                    }

                if (handle.Array[handle.Offset].IsEmpty)
                    increments.Release(ref ol);

                offsetLength = ol.offsetLength;
            }
        }

        public void Release() { }

        public void Compact() { }

        /* Checkpoint format
         * VariableLengthHeap records
         * VariableLengthHeap increments
         */

        public void Checkpoint(NaiadWriter writer)
        {
            this.records.Checkpoint(writer);
            this.increments.Checkpoint(writer);
        }

        public void Restore(NaiadReader reader)
        {
            this.ReleaseCache();
            this.records.Restore(reader);
            this.increments.Restore(reader);
        }

        public bool Stateful { get { return true; } }

        private readonly int numVertices;
        public CollectionTraceWithHeap(Func<int, int, bool> tCompare, Func<int, int> update, int numVertices)
        {
            TimeLessThan = tCompare;
            UpdateTime = update;
            this.numVertices = numVertices;

            records = new VariableLengthHeap<Weighted<S>>(32);
            increments = new VariableLengthHeap<CollectionTraceWithHeapIncrement>(32);
        }

    }
}
