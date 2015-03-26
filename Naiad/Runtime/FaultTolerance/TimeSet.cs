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
using System.Threading.Tasks;

using Microsoft.Research.Naiad.Serialization;
using Microsoft.Research.Naiad.Runtime.Progress;
using Microsoft.Research.Naiad.Dataflow;

namespace Microsoft.Research.Naiad.Runtime.FaultTolerance
{
    internal interface TimeSet
    {
        IEnumerable<Pointstamp> ToPointstamps();
        TimeSet Intersect(FTFrontier frontier);
        TimeSet Intersect(int setNodeId, int projectedNodeId, FTFrontier frontier);
        TimeSet Except(FTFrontier frontier);
        TimeSet Except(int setNodeId, int projectedNodeId, FTFrontier frontier);
        TimeSet Projected(int projectedNodeId);
        FTFrontier SetBelowDifference(TimeSet other);
        bool ContainedIn(FTFrontier frontier);
        FTFrontier AddTo(FTFrontier frontier);
        FTFrontier ToFrontier();
        bool TrySerialize(SendBufferPage buffer, SerializationFormat serializationFormat);
    }

    internal sealed class TimeSet<T> : TimeSet where T : Time<T>
    {
        private readonly int nodeId;
        private readonly HashSet<T> timeSet;

        public TimeSet(int nodeId)
        {
            this.nodeId = nodeId;
            this.timeSet = new HashSet<T>();
        }

        private TimeSet(int nodeId, HashSet<T> times)
        {
            this.nodeId = nodeId;
            this.timeSet = times;
        }

        public IEnumerable<Pointstamp> ToPointstamps()
        {
            return this.timeSet.Select(t => t.ToPointstamp(this.nodeId));
        }

        public bool TrySerialize(SendBufferPage buffer, SerializationFormat serializationFormat)
        {
            bool fits = buffer.Write(serializationFormat.GetSerializer<int>(), this.nodeId);
            fits = fits && buffer.Write(serializationFormat.GetSerializer<int>(), this.timeSet.Count);
            return fits &&
                (buffer.WriteElements(
                    serializationFormat.GetSerializer<T>(),
                    new ArraySegment<T>(this.timeSet.ToArray())) == this.timeSet.Count);
        }

        public bool TryDeserialize(ref RecvBuffer buffer, SerializationFormat serializationFormat)
        {
            int nodeId = -1;
            if (!serializationFormat.GetSerializer<int>().TryDeserialize(ref buffer, out nodeId))
            {
                return false;
            }
            if (this.nodeId != nodeId)
            {
                throw new ApplicationException("Read mismatched timeset: node " + this.nodeId + " != " + nodeId);
            }

            int nElements = -1;
            if (!serializationFormat.GetSerializer<int>().TryDeserialize(ref buffer, out nElements))
            {
                return false;
            }

            T[] elements = new T[nElements];
            if (serializationFormat
                    .GetSerializer<T>()
                    .TryDeserializeMany(ref buffer, new ArraySegment<T>(elements)) != elements.Length)
            {
                return false;
            }

            foreach (T time in elements)
            {
                this.timeSet.Add(time);
            }

            return true;
        }

        public bool Empty { get { return timeSet.Count == 0; } }

        public bool Add(T time)
        {
            if (this.timeSet.Contains(time))
            {
                return false;
            }

            this.timeSet.Add(time);

            return true;
        }

        public FTFrontier AddTo(FTFrontier frontier)
        {
            FTFrontier augmentedFrontier = new FTFrontier(frontier);

            foreach (T time in this.timeSet)
            {
                augmentedFrontier.Add(time.ToPointstamp(this.nodeId));
            }

            return augmentedFrontier;
        }

        public FTFrontier ToFrontier()
        {
            return this.AddTo(new FTFrontier(false));
        }

        public TimeSet Intersect(FTFrontier frontier)
        {
            return this.IntersectTyped(this.nodeId, this.nodeId, frontier);
        }

        public TimeSet Intersect(int setNodeId, int projectedNodeId, FTFrontier frontier)
        {
            return this.IntersectTyped(setNodeId, projectedNodeId, frontier);
        }

        public TimeSet<T> IntersectTyped(FTFrontier frontier)
        {
            return this.IntersectTyped(this.nodeId, this.nodeId, frontier);
        }

        public TimeSet<T> IntersectTyped(int setNodeId, int projectedNodeId, FTFrontier frontier)
        {
            TimeSet<T> intersected = new TimeSet<T>(setNodeId);
            foreach (T time in this.timeSet)
            {
                Pointstamp pointstamp = time.ToPointstamp(projectedNodeId);
                if (frontier.Contains(pointstamp))
                {
                    intersected.Add(time);
                }
            }

            return intersected;
        }

        public TimeSet Except(FTFrontier frontier)
        {
            return this.Except(this.nodeId, this.nodeId, frontier);
        }

        public TimeSet<T> ExceptTyped(FTFrontier frontier)
        {
            return this.ExceptTyped(this.nodeId, this.nodeId, frontier);
        }

        public TimeSet Except(int setNodeId, int projectedNodeId, FTFrontier frontier)
        {
            return this.ExceptTyped(setNodeId, projectedNodeId, frontier);
        }

        public TimeSet<T> ExceptTyped(int setNodeId, int projectedNodeId, FTFrontier frontier)
        {
            TimeSet<T> excepted = new TimeSet<T>(setNodeId);
            foreach (T time in this.timeSet)
            {
                Pointstamp pointstamp = time.ToPointstamp(projectedNodeId);
                if (!frontier.Contains(pointstamp))
                {
                    excepted.Add(time);
                }
            }

            return excepted;
        }

        public TimeSet Projected(int projectedNodeId)
        {
            return new TimeSet<T>(projectedNodeId, this.timeSet);
        }

        public FTFrontier SetBelowDifference(TimeSet otherUntyped)
        {
            bool setLowest = false;
            T lowest = default(T);

            TimeSet<T> other = otherUntyped as TimeSet<T>;
            foreach (T time in this.timeSet)
            {
                if (!other.timeSet.Contains(time))
                {
                    if (setLowest)
                    {
                        if (FTFrontier.IsLessThanOrEqualTo(time.ToPointstamp(0), lowest.ToPointstamp(0)))
                        {
                            lowest = time;
                        }
                    }
                    else
                    {
                        lowest = time;
                        setLowest = true;
                    }
                }
            }

            if (setLowest)
            {
                return FTFrontier.SetBelow(lowest.ToPointstamp(this.nodeId));
            }
            else
            {
                return new FTFrontier(true);
            }
        }

        public bool ContainedIn(FTFrontier frontier)
        {
            foreach (T time in this.timeSet)
            {
                Pointstamp pointstamp = time.ToPointstamp(this.nodeId);
                if (!frontier.Contains(pointstamp))
                {
                    return false;
                }
            }

            return true;
        }

        internal IEnumerable<Pointstamp> ToPointstamps(int nodeId)
        {
            return this.timeSet.Select(t => t.ToPointstamp(nodeId));
        }

        public int NodeId { get { return this.nodeId; } }
    }

    internal interface TimeSetBundle
    {
        TimeSet TimeSet(int vertexId);
        IEnumerable<Pair<int, IEnumerable<Pointstamp>>> TimeBundle(int nodeId, int vertexId);
        bool TryDeserialize(ref RecvBuffer buffer, SerializationFormat format);
    }

    internal class TimeSetBundle<T> : TimeSetBundle where T : Time<T>
    {
        private readonly bool exchanges;
        private readonly TimeSet<T>[] set;

        public TimeSetBundle(Edge edge, bool forSent)
        {
            this.exchanges = edge.Exchanges;
            if (this.exchanges)
            {
                int count = (forSent) ? edge.TargetStage.Placement.Count : edge.SourceStage.Placement.Count;
                this.set = new TimeSet<T>[count];
            }
            else
            {
                this.set = new TimeSet<T>[1];
            }

            for (int i=0; i<this.set.Length; ++i)
            {
                this.set[i] = new TimeSet<T>(edge.ChannelId);
            }
        }

        private int SetIndex(int vertexId)
        {
            if (this.exchanges)
            {
                return vertexId;
            }
            else
            {
                return 0;
            }
        }

        public bool Add(int vertexId, T time)
        {
            return this.set[this.SetIndex(vertexId)].Add(time);
        }

        public TimeSet TimeSet(int vertexId)
        {
            return this.set[this.SetIndex(vertexId)];
        }

        public IEnumerable<Pair<int,IEnumerable<Pointstamp>>> TimeBundle(int nodeId, int vertexId)
        {
            if (this.exchanges)
            {
                return this.set.Select((s, i) => i.PairWith(s.ToPointstamps(nodeId)));
            }
            else
            {
                return new Pair<int, IEnumerable<Pointstamp>>[]
                {
                    vertexId.PairWith(this.set[0].ToPointstamps(nodeId))
                };
            }
        }

        public void IntersectAll(int frontierNodeId, FTFrontier frontier)
        {
            for (int i=0; i<this.set.Length; ++i)
            {
                this.set[i] = this.set[i].IntersectTyped(this.set[i].NodeId, frontierNodeId, frontier);
            }
        }

        public void ExceptAll(int frontierNodeId, FTFrontier frontier)
        {
            for (int i = 0; i < this.set.Length; ++i)
            {
                this.set[i] = this.set[i].ExceptTyped(this.set[i].NodeId, frontierNodeId, frontier);
            }
        }

        public bool TrySerialize(int frontierNodeId, FTFrontier upTo, FTFrontier lastCheckpoint, SendBufferPage buffer, SerializationFormat serializationFormat)
        {
            bool fits = true;
            for (int vertexId = 0; vertexId < this.set.Length; ++vertexId)
            {
                fits = fits && this.set[vertexId]
                    .Intersect(this.set[vertexId].NodeId, frontierNodeId, upTo)
                    .Except(this.set[vertexId].NodeId, frontierNodeId, lastCheckpoint)
                    .TrySerialize(buffer, serializationFormat);
            }
            return fits;
        }

        public bool TryDeserialize(ref RecvBuffer buffer, SerializationFormat format)
        {
            bool success = true;
            for (int vertexId = 0; vertexId < this.set.Length; ++vertexId)
            {
                success = success && this.set[vertexId].TryDeserialize(ref buffer, format);
            }
            return success;
        }
    }

    /// <summary>
    /// count of records sent for different times on a channel. This is public so it can be serialized
    /// </summary>
    /// <typeparam name="T">time type</typeparam>
    public struct TimeCountData<T>
    {
        /// <summary>
        /// the counts of times of sent records
        /// </summary>
        public Pair<T, long>[] timeCounts;
    }

    internal sealed class TimeCount<T> where T : Time<T>
    {
        private readonly Dictionary<T, long> timeCounts;

        public TimeCount()
        {
            this.timeCounts = new Dictionary<T,long>();
        }

        public TimeCount<T> Intersect(int nodeId, FTFrontier frontier)
        {
            TimeCount<T> intersected = new TimeCount<T>();
            foreach (KeyValuePair<T, long> timeCount in this.timeCounts.Where(tc => frontier.Contains(tc.Key.ToPointstamp(nodeId))))
            {
                intersected.Add(timeCount.Key, timeCount.Value);
            }
            return intersected;
        }

        public TimeCount<T> Except(int nodeId, FTFrontier frontier)
        {
            TimeCount<T> excepted = new TimeCount<T>();
            foreach (KeyValuePair<T, long> timeCount in this.timeCounts.Where(tc => !frontier.Contains(tc.Key.ToPointstamp(nodeId))))
            {
                excepted.Add(timeCount.Key, timeCount.Value);
            }
            return excepted;
        }

        public TimeSet<T> ToSet(int nodeId)
        {
            TimeSet<T> set = new TimeSet<T>(nodeId);
            foreach (T time in this.timeCounts.Keys)
            {
                set.Add(time);
            }
            return set;
        }

        public IEnumerable<Pointstamp> ToPointstamps(int nodeId)
        {
            return this.timeCounts.Keys.Select(t => t.ToPointstamp(nodeId));
        }

        public FTFrontier ToFrontier(int nodeId)
        {
            return new FTFrontier(false).Add(this.timeCounts.Keys.Select(t => t.ToPointstamp(nodeId)));
        }

        public bool TrySerialize(SendBufferPage buffer, SerializationFormat serializationFormat)
        {
            return buffer.Write(
                serializationFormat.GetSerializer<TimeCountData<T>>(),
                new TimeCountData<T> { timeCounts = this.timeCounts.Select(kv => kv.Key.PairWith(kv.Value)).ToArray() });
        }

        public bool TryDeserialize(ref RecvBuffer buffer, SerializationFormat format)
        {
            TimeCountData<T> data = new TimeCountData<T>();
            bool success = format.GetSerializer<TimeCountData<T>>()
                .TryDeserialize(ref buffer, out data);

            if (success)
            {
                foreach (Pair<T, long> time in data.timeCounts)
                {
                    this.timeCounts.Add(time.First, time.Second);
                }
            }

            return success;
        }

        public Dictionary<T, long> Counts { get { return this.timeCounts; } }

        public void Add(T time, long count)
        {
            long existingCount;
            if (!this.timeCounts.TryGetValue(time, out existingCount))
            {
                existingCount = 0;
                this.timeCounts.Add(time, existingCount);
            }

            this.timeCounts[time] = existingCount + count;
        }

        public long GetCountIfAny(T time)
        {
            long count = 0;
            this.timeCounts.TryGetValue(time, out count);
            return count;
        }
    }

    internal class TimeCountDictionary<T> where T : Time<T>
    {
        private readonly Dictionary<T, Dictionary<Pair<T,T>, long>> counts = new Dictionary<T,Dictionary<Pair<T,T>,long>>();

        public void Add(T baseTime, T requirement, T capability, long count)
        {
            Dictionary<Pair<T, T>, long> baseCounts;
            if (!counts.TryGetValue(baseTime, out baseCounts))
            {
                baseCounts = new Dictionary<Pair<T, T>, long>();
                counts.Add(baseTime, baseCounts);
            }
            long existingCount = 0;
            if (baseCounts.TryGetValue(requirement.PairWith(capability), out existingCount))
            {
                baseCounts[requirement.PairWith(capability)] = existingCount + count;
            }
            else
            {
                baseCounts.Add(requirement.PairWith(capability), count);
            }
        }

        public IEnumerable<Pair<T, Pair<T, Pair<T, long>>>> Requests()
        {
            return this.counts.SelectMany(b => b.Value
                .Select(c => b.Key.PairWith(c.Key.First.PairWith(c.Key.Second.PairWith(c.Value)))));
        }

        public bool TrySerialize(int nodeId, FTFrontier upTo, FTFrontier lastCheckpoint, SendBufferPage buffer, SerializationFormat serializationFormat)
        {
            TimeCountDictionary<T> filtered = this.Intersect(nodeId, upTo, lastCheckpoint);

            bool fits = buffer.Write(serializationFormat.GetSerializer<int>(), filtered.counts.Count);
            foreach (KeyValuePair<T, Dictionary<Pair<T, T>, long>> c in filtered.counts)
            {
                fits = fits && buffer.Write(serializationFormat.GetSerializer<T>(), c.Key);
                fits = fits && buffer.Write(serializationFormat.GetSerializer<int>(), c.Value.Count);
                foreach (KeyValuePair<Pair<T, T>, long> cc in c.Value)
                {
                    fits = fits && buffer.Write(serializationFormat.GetSerializer<T>(), cc.Key.First);
                    fits = fits && buffer.Write(serializationFormat.GetSerializer<T>(), cc.Key.Second);
                    fits = fits && buffer.Write(serializationFormat.GetSerializer<long>(), cc.Value);
                }
            }

            return fits;
        }

        public bool TryDeserialize(ref RecvBuffer buffer, SerializationFormat format)
        {
            bool success = true;

            int baseCounts = 0;
            success = success && format.GetSerializer<int>().TryDeserialize(ref buffer, out baseCounts);
            for (int i=0; success && i<baseCounts; ++i)
            {
                T baseTime = default(T);
                success = success && format.GetSerializer<T>().TryDeserialize(ref buffer, out baseTime);
                Dictionary<Pair<T, T>, long> thisCounts = new Dictionary<Pair<T, T>, long>();
                int entries = 0;
                success = success && format.GetSerializer<int>().TryDeserialize(ref buffer, out entries);
                for (int e = 0; success && e < entries; ++e)
                {
                    T requirement = default(T);
                    success = success && format.GetSerializer<T>().TryDeserialize(ref buffer, out requirement);
                    T capability = default(T);
                    success = success && format.GetSerializer<T>().TryDeserialize(ref buffer, out capability);
                    long count = 0;
                    success = success && format.GetSerializer<long>().TryDeserialize(ref buffer, out count);

                    if (success)
                    {
                        thisCounts.Add(requirement.PairWith(capability), count);
                    }
                }

                if (success)
                {
                    this.counts.Add(baseTime, thisCounts);
                }
            }

            return success;
        }

        public TimeCountDictionary<T> Intersect(int nodeId, FTFrontier upTo, FTFrontier lastFrontier)
        {
            TimeCountDictionary<T> intersected = new TimeCountDictionary<T>();
            foreach (KeyValuePair<T, Dictionary<Pair<T,T>,long>> baseCount in this.counts.Where(tc =>
                { Pointstamp p = tc.Key.ToPointstamp(nodeId); return upTo.Contains(p) && !lastFrontier.Contains(p); }))
            {
                intersected.counts.Add(baseCount.Key, baseCount.Value);
            }
            // this is a hack for inputs which have a request inserted before time starts
            if (lastFrontier.Empty && upTo.Empty)
            {
                foreach (KeyValuePair<T, Dictionary<Pair<T,T>,long>> baseCount in this.counts.Where(tc =>
                { Pointstamp p = tc.Key.ToPointstamp(nodeId); return p.Timestamp[0] < 0; }))
            {
                intersected.counts.Add(baseCount.Key, baseCount.Value);
            }
            }
            return intersected;
        }

        public void RemoveStale(int nodeId, FTFrontier frontier)
        {
            foreach (var time in this.counts.Values)
            {
                var staleCounts = time.Keys.Where(c => frontier.Contains(c.Second.ToPointstamp(nodeId))).ToArray();
                foreach (var count in staleCounts)
                {
                    time.Remove(count);
                }
            }

            var staleTimes = this.counts.Where(t => t.Value.Count == 0).Select(t => t.Key).ToArray();
            foreach (var time in staleTimes)
            {
                this.counts.Remove(time);
            }
        }
    }

    internal class TimeCountBundle<T> where T : Time<T>
    {
        private readonly bool exchanges;
        private readonly TimeCount<T>[] counts;

        private TimeCountBundle(bool exchanges, int length)
        {
            this.exchanges = exchanges;
            this.counts = new TimeCount<T>[length];

            for (int i = 0; i < this.counts.Length; ++i)
            {
                this.counts[i] = new TimeCount<T>();
            }
        }

        public TimeCountBundle(Edge edge, bool forSent)
        {
            this.exchanges = edge.Exchanges;
            if (this.exchanges)
            {
                int count = (forSent) ? edge.TargetStage.Placement.Count : edge.SourceStage.Placement.Count;
                this.counts = new TimeCount<T>[count];
            }
            else
            {
                this.counts = new TimeCount<T>[1];
            }

            for (int i=0; i<this.counts.Length; ++i)
            {
                this.counts[i] = new TimeCount<T>();
            }
        }

        private int CountIndex(int vertexId)
        {
            if (this.exchanges)
            {
                return vertexId;
            }
            else
            {
                return 0;
            }
        }

        public IEnumerable<int> Vertices(int senderVertexId)
        {
            if (this.exchanges)
            {
                return Enumerable.Range(0, this.counts.Length);
            }
            else
            {
                return Enumerable.Range(senderVertexId, 1);
            }
        }

        public void Add(int vertexId, T time, long count)
        {
            this.counts[this.CountIndex(vertexId)].Add(time, count);
        }

        public TimeCount<T> Counts(int vertexId)
        {
            return this.counts[this.CountIndex(vertexId)];
        }

        public TimeCountBundle<T> Filter(int nodeId, FTFrontier frontier)
        {
            bool addedAny = false;
            TimeCountBundle<T> newBundle = new TimeCountBundle<T>(this.exchanges, this.counts.Length);

            for (int i=0; i<this.counts.Length; ++i)
            {
                foreach (KeyValuePair<T, long> count in this.counts[i].Counts
                    .Where(c => !frontier.Contains(c.Key.ToPointstamp(nodeId))))
                {
                    newBundle.counts[i].Counts.Add(count.Key, count.Value);
                }
                addedAny = addedAny || newBundle.counts[i].Counts.Count > 0;
            }

            if (addedAny)
            {
                return newBundle;
            }
            else
            {
                return null;
            }
        }

        public bool TrySerialize(SendBufferPage buffer, SerializationFormat serializationFormat)
        {
            bool fits = true;
            for (int vertexId = 0; vertexId < this.counts.Length; ++vertexId)
            {
                fits = fits && this.Counts(vertexId).TrySerialize(buffer, serializationFormat);
            }
            return fits;
        }

        public bool TryDeserialize(ref RecvBuffer buffer, SerializationFormat format)
        {
            bool success = true;
            for (int vertexId = 0; vertexId < this.counts.Length; ++vertexId)
            {
                success = success && this.Counts(vertexId).TryDeserialize(ref buffer, format);
            }
            return success;
        }
    }
}
