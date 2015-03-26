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
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

using Microsoft.Research.Naiad.Dataflow;
using Microsoft.Research.Naiad.Runtime.Progress;
using Microsoft.Research.Naiad.Serialization;

namespace Microsoft.Research.Naiad.Runtime.FaultTolerance
{
    internal interface ICheckpointWriter<T> where T : Time<T>
    {
        Task Checkpoint(FTFrontier frontier);
        IEnumerable<GarbageCollectionWrapper> GarbageCollectStreams();
        void GarbageCollect(FTFrontier lowWatermark);
        void SetRetentionFrontier(FTFrontier frontier);
        void RestoreFromCheckpoint(FTFrontier frontier);
        bool TrySerialize(FTFrontier upTo, FTFrontier lastCheckpoint, SendBufferPage buffer, SerializationFormat format);
        bool TryDeserialize(ref RecvBuffer source, SerializationFormat format);
        void Initialize();
        Pair<ICheckpoint<T>, ICheckpoint<T>> LastCheckpointsInRestoration(FTFrontier frontier);
    }

    internal class NullCheckpointWriter<T> : ICheckpointWriter<T> where T : Time<T>
    {
        private readonly Checkpointer<T> parent;

        public Task Checkpoint(FTFrontier frontier)
        {
            return Task.FromResult(true);
        }

        public IEnumerable<GarbageCollectionWrapper> GarbageCollectStreams()
        {
            return new GarbageCollectionWrapper[0];
        }

        public void GarbageCollect(FTFrontier lowWatermark)
        {
        }

        public void SetRetentionFrontier(FTFrontier frontier)
        {
        }

        public void RestoreFromCheckpoint(FTFrontier frontier)
        {
            this.parent.vertex.StartRestoration(frontier.ToPointstamps<T>(this.parent.stage.StageId));
        }

        public bool TrySerialize(FTFrontier upTo, FTFrontier lastCheckpoint, SendBufferPage buffer, SerializationFormat format)
        {
            return true;
        }

        public bool TryDeserialize(ref RecvBuffer source, SerializationFormat format)
        {
            return true;
        }

        public void Initialize()
        {
        }

        public Pair<ICheckpoint<T>, ICheckpoint<T>> LastCheckpointsInRestoration(FTFrontier frontier)
        {
            // always return an empty checkpoint
            return
                FrontierCheckpointTester<T>.CreateDownwardClosed(new Pointstamp[0]).PairWith(
                FrontierCheckpointTester<T>.CreateDownwardClosed(new Pointstamp[0]));
        }

        public NullCheckpointWriter(Checkpointer<T> parent)
        {
            this.parent = parent;
        }
    }

    internal class CheckpointWriter<T> : ICheckpointWriter<T> where T : Time<T>
    {
        private struct Region
        {
            public FTFrontier frontier;
            public StreamSequenceIndex start;
            public StreamSequenceIndex end;
            public StreamSequenceIndex previous;
        };

        private readonly int pageSize = 16 * 1024;

        private StreamSequenceIndex previousIndex;
        private Dictionary<StreamSequenceIndex, Region> regionExtents = new Dictionary<StreamSequenceIndex, Region>();

        private readonly Checkpointer<T> parent;
        private readonly NaiadSerialization<int> intSerializer;
        private readonly NaiadSerialization<long> longSerializer;

        public IEnumerable<GarbageCollectionWrapper> GarbageCollectStreams()
        {
            StreamSequence streams = this.parent.StreamManager.Checkpoint;

            HashSet<int> usedIndices = new HashSet<int>();
            foreach (Region region in this.regionExtents.Values)
            {
                foreach (int index in streams.ExtentRange(region.start.PairWith(region.end)))
                {
                    usedIndices.Add(index);
                }
            }

            if (!usedIndices.Contains(streams.CurrentIndex))
            {
                streams.CloseCurrentStream();
            }

            return streams.FindStreams()
                .Where(i => !usedIndices.Contains(i))
                .Select(i => new GarbageCollectionWrapper(streams, i));
        }

        private void FilterExtents(FTFrontier frontier, bool keep)
        {
            StreamSequence streams = this.parent.StreamManager.Checkpoint;

            Dictionary<StreamSequenceIndex, Region> newExtents = new Dictionary<StreamSequenceIndex, Region>();
            foreach (KeyValuePair<StreamSequenceIndex, Region> region in this.regionExtents.OrderBy(r => r.Value.frontier))
            {
                bool keepThis;
                if (keep)
                {
                    keepThis = frontier.Contains(region.Value.frontier);
                }
                else
                {
                    keepThis = region.Value.frontier.Contains(frontier);
                }

                if (keepThis)
                {
                    newExtents.Add(region.Key, region.Value);

                    StreamSequenceIndex previous = region.Value.previous;
                    while (!previous.Equals(streams.UnboundedIndex))
                    {
                        if (newExtents.ContainsKey(previous))
                        {
                            previous = streams.UnboundedIndex;
                        }
                        else
                        {
                            newExtents[previous] = this.regionExtents[previous];
                            previous = newExtents[previous].previous;
                        }
                    }
                }
            }

            this.regionExtents = newExtents;

            if (!this.regionExtents.ContainsKey(this.previousIndex))
            {
                this.previousIndex = this.parent.StreamManager.Checkpoint.UnboundedIndex;
            }
        }

        public void SetRetentionFrontier(FTFrontier frontier)
        {
            this.FilterExtents(frontier, true);
        }

        public void Initialize()
        {
            this.SetRetentionFrontier(new FTFrontier(false));
            foreach (var wrapper in this.GarbageCollectStreams())
            {
                wrapper.Collect();
            }
        }

        public void GarbageCollect(FTFrontier lowWatermark)
        {
            this.FilterExtents(lowWatermark, false);
        }

        public Pair<ICheckpoint<T>, ICheckpoint<T>> LastCheckpointsInRestoration(FTFrontier frontier)
        {
            if (frontier.Empty)
            {
                return
                    FrontierCheckpointTester<T>.CreateEmpty().PairWith(
                    FrontierCheckpointTester<T>.CreateEmpty());
            }

            StreamSequence streams = this.parent.StreamManager.Checkpoint;

            foreach (Region region in this.regionExtents.Values)
            {
                if (region.frontier.Equals(frontier))
                {
                    List<Region> regions = new List<Region>();
                    StreamSequenceIndex previous = region.start;
                    while (!previous.Equals(streams.UnboundedIndex))
                    {
                        regions.Add(this.regionExtents[previous]);
                        previous = this.regionExtents[previous].previous;
                    }
                    regions.Reverse();

                    ICheckpoint<T> lastFull =
                        new FrontierCheckpointTester<T>(new FTFrontier(false), regions[0].frontier, this.parent.vertex.Stage.StageId);
                    ICheckpoint<T> lastIncremental = (regions.Count == 1) ?
                        FrontierCheckpointTester<T>.CreateEmpty() :
                        new FrontierCheckpointTester<T>(regions[regions.Count - 2].frontier, regions.Last().frontier, this.parent.vertex.Stage.StageId);

                    return lastFull.PairWith(lastIncremental);
                }
            }

            throw new ApplicationException("Tried to restore from nonexistent checkpoint");
        }

        public void RestoreFromCheckpoint(FTFrontier frontier)
        {
            if (frontier.Empty)
            {
                this.parent.vertex.StartRestoration(frontier.ToPointstamps<T>(this.parent.stage.StageId));
                return;
            }

            StreamSequence streams = this.parent.StreamManager.Checkpoint;

            foreach (Region region in this.regionExtents.Values)
            {
                if (region.frontier.Equals(frontier))
                {
                    List<Region> regions = new List<Region>();
                    StreamSequenceIndex previous = region.start;
                    while (!previous.Equals(streams.UnboundedIndex))
                    {
                        regions.Add(this.regionExtents[previous]);
                        previous = this.regionExtents[previous].previous;
                    }
                    regions.Reverse();

                    this.parent.vertex.StartRestoration(frontier.ToPointstamps<T>(this.parent.stage.StageId));

                    for (int i=0; i<regions.Count; ++i)
                    {
                        Region r = regions[i];
                        Stream partialStream = streams.Read(r.start, r.end);
                        using (NaiadReader reader = new NaiadReader(partialStream, this.parent.vertex.SerializationFormat))
                        {
                            ICheckpoint<T> checkpoint = new FrontierCheckpointTester<T>(
                                i == 0 ? new FTFrontier(false) : regions[i-1].frontier,
                                regions[i].frontier, this.parent.stage.StageId);
                            this.parent.vertex.ReadPartialCheckpoint(reader, checkpoint);
                        }
                    }

                    return;
                }
            }

            throw new ApplicationException("Tried to restore from nonexistent checkpoint");
        }

        public IEnumerable<FTFrontier> AvailableCheckpoints()
        {
            return this.regionExtents.Values.Select(r => r.frontier);
        }

        public bool TrySerialize(FTFrontier upTo, FTFrontier lastCheckpoint, SendBufferPage buffer, SerializationFormat format)
        {
            IEnumerable<Region> regions =
                this.regionExtents.Values.Where(r => upTo.Contains(r.frontier) && !lastCheckpoint.Contains(r.frontier));
            bool fit = buffer.Write(this.intSerializer, regions.Count());
            foreach (Region region in regions.OrderBy(r => r.start))
            {
                fit = fit && buffer.Write(this.intSerializer, region.start.index);
                fit = fit && buffer.Write(this.longSerializer, region.start.position);
                fit = fit && buffer.Write(this.intSerializer, region.end.index);
                fit = fit && buffer.Write(this.longSerializer, region.end.position);
                fit = fit && buffer.Write(this.intSerializer, region.previous.index);
                fit = fit && buffer.Write(this.longSerializer, region.previous.position);
                fit = fit && region.frontier.TrySerialize(buffer, format);
            }

            return fit;
        }

        public bool TryDeserialize(ref RecvBuffer source, SerializationFormat format)
        {
            int numberOfRegions = 0;
            bool success = this.intSerializer.TryDeserialize(ref source, out numberOfRegions);
            for (int i = 0; success && i < numberOfRegions; ++i)
            {
                StreamSequenceIndex start = new StreamSequenceIndex();
                StreamSequenceIndex end = new StreamSequenceIndex();
                StreamSequenceIndex previous = new StreamSequenceIndex();
                success = success && this.intSerializer.TryDeserialize(ref source, out start.index);
                success = success && this.longSerializer.TryDeserialize(ref source, out start.position);
                success = success && this.intSerializer.TryDeserialize(ref source, out end.index);
                success = success && this.longSerializer.TryDeserialize(ref source, out end.position);
                success = success && this.intSerializer.TryDeserialize(ref source, out previous.index);
                success = success && this.longSerializer.TryDeserialize(ref source, out previous.position);
                FTFrontier frontier = new FTFrontier(false);
                success = success && frontier.TryDeserialize(ref source, format);
                if (success)
                {
                    this.regionExtents.Add(start, new Region { frontier = frontier, start = start, end = end, previous = previous });
                }
            }

            return success;
        }

        public Task Checkpoint(FTFrontier frontier)
        {
            StreamSequence streams = this.parent.StreamManager.Checkpoint;

            if (!this.previousIndex.Equals(streams.UnboundedIndex) &&
                frontier.Equals(this.regionExtents[this.previousIndex].frontier))
            {
                // nothing has changed since the last checkpoint
                return Task.FromResult(true);
            }

            long startTicks = this.parent.vertex.scheduler.Controller.Stopwatch.ElapsedTicks;

            if (streams.Index.position >= this.parent.vertex.TypedStage.CheckpointStreamThreshold ||
                this.previousIndex.Equals(streams.UnboundedIndex) ||
                this.parent.vertex.MustStartFullCheckpoint(
                    new FrontierCheckpointTester<T>(
                        this.regionExtents[this.previousIndex].frontier, frontier, this.parent.stage.StageId)))
            {
                streams.CloseCurrentStream();
                this.previousIndex = streams.UnboundedIndex;
            }

            FTFrontier previousIncremental = new FTFrontier(false);

            if (!this.previousIndex.Equals(streams.UnboundedIndex))
            {
                previousIncremental = this.regionExtents[this.previousIndex].frontier;
            }

            StreamSequenceIndex start = streams.Index;

            var sender = new StreamSequenceSerializedMessageSender(streams, this.pageSize);
            using (NaiadWriter writer = new NaiadWriter(
                new StreamSequenceSerializedMessageSender[] { sender },
                this.parent.vertex.SerializationFormat))
            {
                FrontierCheckpointTester<T> checkpoint = new FrontierCheckpointTester<T>(previousIncremental, frontier, this.parent.stage.StageId);
                this.parent.vertex.WriteCheckpoint(writer, checkpoint);
            }
            Task toWait = sender.CompletedTask();

            this.parent.vertex.NotifyPotentialRollbackRange(
                new FrontierCheckpointTester<T>(new FTFrontier(false), this.parent.lastGarbageCollection, this.parent.stage.StageId),
                new FrontierCheckpointTester<T>(this.parent.lastGarbageCollection, new FTFrontier(true), this.parent.stage.StageId));

            Region region = new Region { frontier = frontier, start = start, end = streams.Index, previous = this.previousIndex };
            this.regionExtents.Add(start, region);

            long endTicks = this.parent.vertex.scheduler.Controller.Stopwatch.ElapsedTicks;
            long totalMicroSeconds = (endTicks * 1000000L) / System.Diagnostics.Stopwatch.Frequency;
            long microSeconds = ((endTicks - startTicks) * 1000000L) / System.Diagnostics.Stopwatch.Frequency;
            string entry;
            if (previousIncremental.Empty)
            {
                entry = String.Format("{0:D3}.{1:D3} F {2:D7} {3:D11} {4:D11} {5}",
                    this.parent.vertex.Stage.StageId, this.parent.vertex.VertexId, microSeconds, totalMicroSeconds,
                        streams.TotalWritten, frontier);
            }
            else
            {
                entry = String.Format("{0:D3}.{1:D3} P {2:D7} {3:D11} {4:D11} {5} {6}",
                    this.parent.vertex.Stage.StageId, this.parent.vertex.VertexId, microSeconds, totalMicroSeconds,
                        streams.TotalWritten, previousIncremental, frontier);
            }
            this.parent.vertex.scheduler.WriteLogEntry(entry);

            this.previousIndex = start;

            return toWait;
        }

        public CheckpointWriter(Checkpointer<T> parent)
        {
            this.parent = parent;
            this.previousIndex = this.parent.StreamManager.Checkpoint.UnboundedIndex;
            this.intSerializer = this.parent.vertex.SerializationFormat.GetSerializer<int>();
            this.longSerializer = this.parent.vertex.SerializationFormat.GetSerializer<long>();
        }
    }

    internal interface InputCacheCheckpointWriter<T> where T : Time<T>
    {
        void ReplayInputs();
    }

    internal class InputCacheCheckpointWriter<S, T>
        : ICheckpointWriter<T>, InputCacheCheckpointWriter<T>, IInputLogger<S, T>  where T : Time<T>
    {
        private class TimeBatches
        {
            public int sentCount = 0;
            public List<S[]> batches = new List<S[]>();
        }

        private readonly Checkpointer<T> parent;
        private readonly Dictionary<T, TimeBatches> cachedInputsForRollback;
        private Action<T, S[]> replayInput;
        private Pointstamp? maxCollected = null;

        public Task Checkpoint(FTFrontier frontier)
        {
            return Task.FromResult(true);
        }

        public IEnumerable<GarbageCollectionWrapper> GarbageCollectStreams()
        {
            return new GarbageCollectionWrapper[0];
        }

        public void GarbageCollect(FTFrontier lowWatermark)
        {
            lock (this)
            {
                // we won't need to re-send any inputs at this epoch or earlier
                T[] staleTimes = this.cachedInputsForRollback.Keys
                    .Where(t => lowWatermark.Contains(t.ToPointstamp(this.parent.stage.StageId)))
                    .ToArray();
                foreach (T time in staleTimes)
                {
                    Pointstamp stamp = time.ToPointstamp(this.parent.stage.StageId);
                    if (!maxCollected.HasValue ||
                        FTFrontier.IsLessThanOrEqualTo(maxCollected.Value, stamp))
                    {
                        this.maxCollected = stamp;
                    }
                    this.cachedInputsForRollback.Remove(time);
                }
            }
        }

        public void SetRetentionFrontier(FTFrontier frontier)
        {
            // we have to resend any input in times that have rolled back
            foreach (var time in this.cachedInputsForRollback
                .Where(time => !frontier.Contains(time.Key.ToPointstamp(this.parent.stage.StageId))))
            {
                time.Value.sentCount = 0;
            }
        }

        public void ReplayInputs()
        {
        }

        public void RestoreFromCheckpoint(FTFrontier frontier)
        {
            // this puts the necessary inputs back in the scheduler queue, to be re-sent when computation
            // restarts. Send them in the correct order!
            foreach (var input in this.cachedInputsForRollback.OrderBy(time => FTFrontier.FromPointstamps(new Pointstamp[] { time.Key.ToPointstamp(this.parent.stage.StageId) })))
            {
                for (int i = input.Value.sentCount; i < input.Value.batches.Count; ++i)
                {
                    this.replayInput(input.Key, input.Value.batches[i]);
                }
            }
        }

        public bool TrySerialize(FTFrontier upTo, FTFrontier lastCheckpoint, SendBufferPage buffer, SerializationFormat format)
        {
            return true;
        }

        public bool TryDeserialize(ref RecvBuffer source, SerializationFormat format)
        {
            return true;
        }

        public void Initialize()
        {
        }

        public Pair<ICheckpoint<T>, ICheckpoint<T>> LastCheckpointsInRestoration(FTFrontier frontier)
        {
            // always return an empty checkpoint
            return
                FrontierCheckpointTester<T>.CreateDownwardClosed(new Pointstamp[0]).PairWith(
                FrontierCheckpointTester<T>.CreateDownwardClosed(new Pointstamp[0]));
        }

        public void LogInput(T time, S[] payload)
        {
            lock (this)
            {
                TimeBatches timeBatch = null;
                if (!this.cachedInputsForRollback.TryGetValue(time, out timeBatch))
                {
                    timeBatch = new TimeBatches();
                    this.cachedInputsForRollback.Add(time, timeBatch);
                }

                if (timeBatch.batches.Count > 0 && timeBatch.batches.Last() == null)
                {
                    throw new ApplicationException("Out of order inputs");
                }
                timeBatch.batches.Add(payload);
            }
        }

        public void LogInputProcessing(T time, S[] payload)
        {
            lock (this)
            {
                TimeBatches timeBatch;
                // the batch may not be there if we just heard we will never have to roll back this far again
                if (this.cachedInputsForRollback.TryGetValue(time, out timeBatch))
                {
                    ++timeBatch.sentCount;
                    if (timeBatch.sentCount > timeBatch.batches.Count)
                    {
                        throw new ApplicationException("Sent counts mismatch");
                    }
                }
            }
        }

        public void UpdateReplayAction(Action<T, S[]> replayInput)
        {
            this.replayInput = replayInput;
        }

        public Pointstamp? MaxCompleted()
        {
            Pointstamp? maxCompleted = this.maxCollected;

            // find any batches where we have sent all the inputs we were given, including an OnNext indicator
            foreach (var time in this.cachedInputsForRollback
                .Where(t =>
                    t.Value.sentCount == t.Value.batches.Count &&
                    t.Value.batches.Last() == null))
            {
                Pointstamp stamp = time.Key.ToPointstamp(this.parent.stage.StageId);
                if (!maxCompleted.HasValue || FTFrontier.IsLessThanOrEqualTo(maxCompleted.Value, stamp))
                {
                    maxCompleted = stamp;
                }
            }

            return maxCompleted;
        }

        public InputCacheCheckpointWriter(Checkpointer<T> parent)
        {
            this.parent = parent;
            this.cachedInputsForRollback = new Dictionary<T, TimeBatches>();
        }
    }
}
