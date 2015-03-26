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
using System.Text;
using Microsoft.Research.Naiad.Serialization;
using Microsoft.Research.Naiad.DataStructures;

namespace Microsoft.Research.Naiad.Frameworks.DifferentialDataflow.CollectionTrace
{
    internal class CollectionTraceWithAggregation<S> : CollectionTraceCheckpointable<S>
        where S : IEquatable<S>
    {
        VariableLengthHeap<CollectionTraceWithAggregationIncrement<S>> increments;   // stores regions of increments, each corresponding to a time

        Func<int, int, bool> TimeLessThan;          // wraps the "less than" partial order on time indices
        Func<int, int> UpdateTime;                  // wraps the reachability-based time advancement
        Func<Int64, S, S, S> axpy;                    // (weight, new record, old record) -> newer record
        Func<S, bool> isZero;                       // indicates if x is logically zero.

        OffsetLength cachedIncrementOffset;
        CollectionTraceWithAggregationIncrement<S> cacheContents;

        public void ReleaseCache()
        {
            if (!cachedIncrementOffset.IsEmpty)
            {
                cachedIncrementOffset = new OffsetLength();
                cacheContents = new CollectionTraceWithAggregationIncrement<S>();
            }
        }

        public void Introduce(ref int offsetLength, S element, Int64 weight, int timeIndex)
        {
            var ol = new OffsetLength(offsetLength);

            // internal Introduce uses aggregated element and weight, so should aggregate first

            Introduce(ref ol, this.axpy(weight, element, default(S)), weight, timeIndex);

            offsetLength = ol.offsetLength;

            ReleaseCache();
        }

        void Introduce(ref OffsetLength offsetLength, S element, Int64 weight, int timeIndex)
        {
            var handle = EnsureTime(ref offsetLength, timeIndex);

            var position = 0;
            while (handle.Array[handle.Offset + position].TimeIndex != timeIndex)
                position++;

            if (handle.Array[handle.Offset + position].IsEmpty(isZero))
                handle.Array[handle.Offset + position] = new CollectionTraceWithAggregationIncrement<S>(weight, timeIndex, element);
            else
            {
                var incr = handle.Array[handle.Offset + position];
                var result = new CollectionTraceWithAggregationIncrement<S>(incr.Weight + weight, timeIndex, axpy(1, element, incr.Value));
                handle.Array[handle.Offset + position] = result;
            }

            // if the introduction results in an empty region, we need to clean up
            if (handle.Array[handle.Offset + position].IsEmpty(isZero))
            {
                // drag everything after it down one
                for (int i = position + 1; i < handle.Length; i++)
                    handle.Array[handle.Offset + i - 1] = handle.Array[handle.Offset + i];

                handle.Array[handle.Offset + handle.Length - 1] = new CollectionTraceWithAggregationIncrement<S>();

                // if the root element is empty, the list must be empty
                if (handle.Array[handle.Offset].IsEmpty(isZero))
                    increments.Release(ref offsetLength);
            }
        }

        void Introduce(ref OffsetLength thisOffsetLength, OffsetLength thatOffsetLength, int scale)
        {
            var handle = increments.Dereference(thatOffsetLength);
            for (int i = 0; i < handle.Length && !handle.Array[handle.Offset + i].IsEmpty(isZero); i++)
            {
                Introduce(ref thisOffsetLength, axpy(scale, handle.Array[handle.Offset + i].Value, default(S)), scale * handle.Array[handle.Offset + i].Weight, handle.Array[handle.Offset + i].TimeIndex);
            }
        }

        Handle<CollectionTraceWithAggregationIncrement<S>> EnsureTime(ref OffsetLength offsetLength, int timeIndex)
        {
            var handle = increments.Dereference(offsetLength);

            for (int i = 0; i < handle.Length; i++)
            {
                // if we found the time, it is ensured and we can return
                if (handle.Array[handle.Offset + i].TimeIndex == timeIndex)
                    return handle;

                // if we found an empty slot, new it up and return
                if (handle.Array[handle.Offset + i].IsEmpty(isZero))
                {
                    handle.Array[handle.Offset + i] = new CollectionTraceWithAggregationIncrement<S>(timeIndex);
                    return handle;
                }
            }

            // if we didn't find it, and no empty space for it
            var oldLength = handle.Length;
            handle = increments.EnsureAllocation(ref offsetLength, handle.Length + 1);
            handle.Array[handle.Offset + oldLength] = new CollectionTraceWithAggregationIncrement<S>(timeIndex);

            return handle;
        }

        public void IntroduceFrom(ref int thisKeyIndex, ref int thatKeyIndex, bool delete = true)
        {
            var ol1 = new OffsetLength(thisKeyIndex);
            var ol2 = new OffsetLength(thatKeyIndex);

            if (!ol2.IsEmpty)
            {
                Introduce(ref ol1, ol2, 1);

                thisKeyIndex = ol1.offsetLength;

                if (delete)
                    ZeroState(ref thatKeyIndex);

                ReleaseCache();
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
            if (!handle.Array[handle.Offset + position].IsEmpty(isZero))
            {
                // swap the accumulation in, and zero out the accumulation (the new correct accumulation for this key).
                var accum = UpdateAccumulation(ref ol, timeIndex);
                handle.Array[handle.Offset + position] = new CollectionTraceWithAggregationIncrement<S>(timeIndex).Add(accum, axpy);

                // we may have ended up with a null acculumation, must clean up
                if (handle.Array[handle.Offset + position].Weight == 0)
                {
                    for (int i = position + 1; i < handle.Length; i++)
                        handle.Array[handle.Offset + i - 1] = handle.Array[handle.Offset + i];

                    handle.Array[handle.Offset + handle.Length - 1] = new CollectionTraceWithAggregationIncrement<S>();
                    if (handle.Array[handle.Offset].Weight == 0)
                        increments.Release(ref ol);
                }

                // important to update the cached accumulation to reflect the emptiness
                // only do this if the cached accumulation is what we are working with
                if (cachedIncrementOffset.offsetLength == ol.offsetLength)
                {
                    cachedIncrementOffset = ol;
                    cacheContents = new CollectionTraceWithAggregationIncrement<S>(timeIndex);
                }
            }
            else
                throw new Exception("Attemping subtraction from non-empty time; something wrong in Operator logic");

            keyIndex = ol.offsetLength;
        }

        public void EnumerateCollectionAt(int offsetLength, int timeIndex, NaiadList<Weighted<S>> toFill)
        {
            if (toFill.Count == 0)
            {
                var temp = new OffsetLength(offsetLength);
                var accum = UpdateAccumulation(ref temp, timeIndex);

                if (!accum.IsEmpty(isZero))
                    toFill.Add(new Weighted<S>(accum.Value, 1));
            }
            else
                throw new NotImplementedException();
        }

        // no caching at the moment; should do, but need to figure out how...
        CollectionTraceWithAggregationIncrement<S> UpdateAccumulation(ref OffsetLength ol, int timeIndex)
        {
#if true
            if (ol.IsEmpty)
                return new CollectionTraceWithAggregationIncrement<S>(timeIndex);

            var handle = increments.Dereference(ol);

            // special-case single element accumulations to avoid unprocessed accumulation dropping processed accumulation
            if (handle.Length == 1)
            {
                if (TimeLessThan(handle.Array[handle.Offset].TimeIndex, timeIndex))
                    return handle.Array[handle.Offset];
                else
                    return new CollectionTraceWithAggregationIncrement<S>(timeIndex);
            }

            else
#else
            var handle = increments.Dereference(ol);
#endif
            {
                // if we have a hit on the cache ...
                if (ol.offsetLength == cachedIncrementOffset.offsetLength)
                {
                    for (int i = 0; i < handle.Length && !handle.Array[handle.Offset + i].IsEmpty(isZero); i++)
                    {
                        if (!handle.Array[handle.Offset + i].IsEmpty(isZero))
                        {
                            var inNew = TimeLessThan(handle.Array[handle.Offset + i].TimeIndex, timeIndex);
                            var inOld = TimeLessThan(handle.Array[handle.Offset + i].TimeIndex, cacheContents.TimeIndex);

                            if (inOld != inNew)
                                cacheContents.Add(handle.Array[handle.Offset + i], axpy, inOld ? -1 : +1);
                        }
                    }

                    cacheContents.TimeIndex = timeIndex;
                }
                else
                {
                    ReleaseCache(); // blow cache away and start over

                    for (int i = 0; i < handle.Length && !handle.Array[handle.Offset + i].IsEmpty(isZero); i++)
                        if (TimeLessThan(handle.Array[handle.Offset + i].TimeIndex, timeIndex))
                            cacheContents.Add(handle.Array[handle.Offset + i], axpy);

                    cachedIncrementOffset = ol;
                    cacheContents.TimeIndex = timeIndex;
                }

                return cacheContents;
            }
        }

        public void EnumerateDifferenceAt(int offsetLength, int timeIndex, NaiadList<Weighted<S>> toFill)
        {
            if (toFill.Count == 0)
            {
                var temp = new OffsetLength(offsetLength);


                var accum = new CollectionTraceWithAggregationIncrement<S>();

                var handle = increments.Dereference(temp);
                for (int i = 0; i < handle.Length && !handle.Array[handle.Offset + i].IsEmpty(isZero); i++)
                    if (handle.Array[handle.Offset + i].TimeIndex == timeIndex)
                        accum.Add(handle.Array[handle.Offset + i], axpy);

                if (!accum.IsEmpty(isZero))
                    toFill.Add(new Weighted<S>(accum.Value, 1));
            }
            else
                throw new NotImplementedException();
        }

        public long CountDifferenceAt(int keyIndex, int timeIndex)
        {
            var ol = new OffsetLength(keyIndex);
            var handle = increments.Dereference(ol);

            var accum = new CollectionTraceWithAggregationIncrement<S>();

            for (int i = 0; i < handle.Length && !handle.Array[handle.Offset + i].IsEmpty(isZero); i++)
            {
                if (handle.Array[handle.Offset + i].TimeIndex == timeIndex)
                    accum.Add(handle.Array[handle.Offset + i], axpy);
            }

            return (accum.IsEmpty(isZero)) ? 0L : 1L;
        }

        HashSet<int> hashSet = new HashSet<int>();
        public void EnumerateTimes(int keyIndex, NaiadList<int> timelist)
        {
            var ol = new OffsetLength(keyIndex);

            if (timelist.Count == 0)
            {
                var handle = increments.Dereference(ol);
                for (int i = 0; i < handle.Length && !handle.Array[handle.Offset + i].IsEmpty(isZero); i++)
                    timelist.Add(handle.Array[handle.Offset + i].TimeIndex);
            }
            else
            {
                hashSet.Clear();
                for (int i = 0; i < timelist.Count; i++)
                    hashSet.Add(timelist.Array[i]);

                var handle = increments.Dereference(ol);
                for (int i = 0; i < handle.Length && !handle.Array[handle.Offset + i].IsEmpty(isZero); i++)
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

        public int AllocateState() { throw new NotImplementedException(); }

        public void ReleaseState(ref int keyIndex)
        {
            var temp = new OffsetLength(keyIndex);

            if (!temp.IsEmpty)
            {
                increments.Release(ref temp);
                keyIndex = temp.offsetLength;
            }

            ReleaseCache();
        }

        public void ZeroState(ref int keyIndex)
        {
            ReleaseState(ref keyIndex);
        }

        public bool IsZero(ref int keyIndex) { return keyIndex == 0; }

        public void RemoveStateInTimes(ref int offsetLength, Func<int, bool> keepTime)
        {
            var ol = new OffsetLength(offsetLength);

            if (!ol.IsEmpty)
            {
                var handle = increments.Dereference(ol);

                for (int i = 0; i < handle.Length; i++)
                {
                    if (handle.Array[handle.Offset + i].Weight != 0)
                    {
                        if (!keepTime(handle.Array[handle.Offset + i].TimeIndex))
                        {
                            handle.Array[handle.Offset + i] = new CollectionTraceWithAggregationIncrement<S>();
                        }
                    }
                }

                var position = 0;
                for (int i = 0; i < handle.Length; i++)
                    if (!handle.Array[handle.Offset + i].IsEmpty(isZero))
                    {
                        var temp = handle.Array[handle.Offset + i];
                        handle.Array[handle.Offset + i] = new CollectionTraceWithAggregationIncrement<S>();
                        handle.Array[handle.Offset + (position++)] = temp;
                    }

                if (handle.Array[handle.Offset].IsEmpty(isZero))
                    increments.Release(ref ol);


                offsetLength = ol.offsetLength;
            }

            ReleaseCache();
        }

        public void EnsureStateIsCurrentWRTAdvancedTimes(ref int offsetLength)
        {
            var ol = new OffsetLength(offsetLength);

            if (!ol.IsEmpty)
            {
                var handle = increments.Dereference(ol);

                for (int i = 0; i < handle.Length; i++)
                {
                    if (handle.Array[handle.Offset + i].Weight != 0)
                    {
                        handle.Array[handle.Offset + i].TimeIndex = UpdateTime(handle.Array[handle.Offset + i].TimeIndex);
                        for (int j = 0; j < i && !handle.Array[handle.Offset + i].IsEmpty(isZero); j++)
                        {
                            if (handle.Array[handle.Offset + j].TimeIndex == handle.Array[handle.Offset + i].TimeIndex)
                            {
                                handle.Array[handle.Offset + j].Add(handle.Array[handle.Offset + i], axpy);
                                handle.Array[handle.Offset + i] = new CollectionTraceWithAggregationIncrement<S>();
                            }
                        }
                    }
                }

                var position = 0;
                for (int i = 0; i < handle.Length; i++)
                    if (!handle.Array[handle.Offset + i].IsEmpty(isZero))
                    {
                        var temp = handle.Array[handle.Offset + i];
                        handle.Array[handle.Offset + i] = new CollectionTraceWithAggregationIncrement<S>();
                        handle.Array[handle.Offset + (position++)] = temp;
                    }

                if (handle.Array[handle.Offset].IsEmpty(isZero))
                    increments.Release(ref ol);


                offsetLength = ol.offsetLength;
            }
        }

        public void TransferTimesToNewInternTable(int offsetLength, Func<int, int> transferTime)
        {
            var ol = new OffsetLength(offsetLength);

            if (!ol.IsEmpty)
            {
                var handle = increments.Dereference(ol);

                for (int i = 0; i < handle.Length; i++)
                {
                    if (handle.Array[handle.Offset + i].Weight != 0)
                    {
                        handle.Array[handle.Offset + i].TimeIndex = transferTime(handle.Array[handle.Offset + i].TimeIndex);
                    }
                }
            }
        }

        public void InstallNewUpdateFunction(Func<int, int, bool> newLessThan, Func<int, int> newUpdateTime)
        {
            this.ReleaseCache();

            this.TimeLessThan = newLessThan;
            this.UpdateTime = newUpdateTime;
        }

        public void Release() { }
        public void Compact() { }

        public void Checkpoint(NaiadWriter writer)
        {
            this.increments.Checkpoint(writer);
        }

        public void Restore(NaiadReader reader)
        {
            this.ReleaseCache();
            this.increments.Restore(reader);
        }

        public bool Stateful { get { return true; } }

        public CollectionTraceWithAggregation(Func<int, int, bool> tCompare, Func<int, int> update, Func<Int64, S, S, S> a, Func<S, bool> isZ)
        {
            TimeLessThan = tCompare;
            UpdateTime = update;

            axpy = a;
            isZero = isZ;

            increments = new VariableLengthHeap<CollectionTraceWithAggregationIncrement<S>>(32);
        }
    }
}
