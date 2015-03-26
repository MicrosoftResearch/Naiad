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

using Microsoft.Research.Naiad;
using Microsoft.Research.Naiad.Runtime.Progress;
using Microsoft.Research.Naiad.Dataflow;
using Microsoft.Research.Naiad.Serialization;

namespace Microsoft.Research.Naiad.Runtime.FaultTolerance
{
    internal interface ILogger
    {
        void Initialize();
        IEnumerable<GarbageCollectionWrapper> GarbageCollectStreams();
        Task FlushAsync();
        void MarkFrontier(FTFrontier upTo);
        void SetRetentionFrontier(FTFrontier frontier, bool sealEagerly);
        void Shutdown();
        bool TrySerialize(FTFrontier upTo, FTFrontier lastCheckpoint, SendBufferPage buffer, SerializationFormat serializationFormat);
        bool TryDeserialize(ref RecvBuffer buffer, SerializationFormat serializationFormat);
    }

    internal class Logger<T, TVertex, TMessage> : ILogger
        where T : IEquatable<T>
        where TVertex : Time<TVertex>
        where TMessage : Time<TMessage>
    {
        private struct TimeExtent
        {
            public StreamSequenceIndex start;
            public StreamSequenceIndex end;
            public FTFrontier sentMessageFrontier;

            public TimeExtent(StreamSequence sequence)
            {
                this.start = sequence.Index;
                this.end = sequence.UnboundedIndex;
                this.sentMessageFrontier = new FTFrontier(false);
            }

            public TimeExtent(StreamSequenceIndex start, StreamSequenceIndex end, FTFrontier sentMessageFrontier)
            {
                this.start = start;
                this.end = end;
                this.sentMessageFrontier = sentMessageFrontier;
            }
        }

        private static int initialBufferSize = 4 * 1024 * 1024;

        private int currentChannelId;
        private int currentVertexId;
        private T currentTime;

        private byte[] buffer;
        private SubArray<byte> bufferAsSubarray;
        private int countOffset;
        private int currentCount;

        private readonly SerializationFormat serializationFormat;
        private readonly NaiadSerialization<int> intSerializer;
        private readonly NaiadSerialization<T> timeSerializer;
        private readonly NaiadSerialization<long> longSerializer;
        private readonly NaiadSerialization<TVertex> vertexTimeSerializer;
        private readonly Func<T, TVertex> vertexTimeFunction;
        protected readonly int stageId;
        private HashSet<TVertex> timesToSeal = new HashSet<TVertex>();
        private Dictionary<TVertex, TimeExtent> timeExtents = new Dictionary<TVertex,TimeExtent>();
        protected Dictionary<int, Pair<Edge, Dictionary<TVertex, TimeCountBundle<TMessage>>>> sentRecordCounts =
            new Dictionary<int, Pair<Edge, Dictionary<TVertex, TimeCountBundle<TMessage>>>>();

        private FTFrontier retentionFrontier = new FTFrontier(false);
        private Action<TVertex> notificationAction;
        private readonly Dictionary<int, IMessageReplayer<T>> messageReplayers = new Dictionary<int,IMessageReplayer<T>>();
        private readonly HashSet<int> channelsToFlush = new HashSet<int>();

        private StreamSequence streams;

        private void StartBuffer()
        {
            this.bufferAsSubarray = new SubArray<byte>(this.buffer);
            this.intSerializer.Serialize(ref this.bufferAsSubarray, -1);
        }

        private void EnsureSegment(T time, int channelId, int vertexId)
        {
            if (this.currentVertexId != vertexId ||
                this.currentChannelId != channelId ||
                !this.currentTime.Equals(time))
            {
                this.StartSegment(time, channelId, vertexId);
            }
        }

        private void StartSegment(T time, int channelId, int vertexId)
        {
            this.FinishSegment();

            int newCountOffset = -1;

            bool success = this.timeSerializer.Serialize(ref this.bufferAsSubarray, time);
            if (channelId == -1)
            {
                // this is a notification
                success = success && this.intSerializer.Serialize(ref this.bufferAsSubarray, -1);
            }
            else
            {
                success = success && this.intSerializer.Serialize(ref this.bufferAsSubarray, channelId);
                success = success && this.intSerializer.Serialize(ref this.bufferAsSubarray, vertexId);
                newCountOffset = this.bufferAsSubarray.Count;
                // put in a placeholder for the count
                success = success && this.intSerializer.Serialize(ref this.bufferAsSubarray, -1);
            }

            if (success)
            {
                this.MarkTime(this.vertexTimeFunction(time));
                this.countOffset = newCountOffset;
                this.currentTime = time;
                this.currentChannelId = channelId;
                this.currentVertexId = vertexId;
            }
            else
            {
                this.SendBuffer();
                // we assume the buffer size is large enough that this is bound to succeed with a fresh buffer
                this.StartSegment(time, channelId, vertexId);
            }
        }

        private void FinishSegment()
        {
            if (this.countOffset >= 0)
            {
                SubArray<byte> countRegion = new SubArray<byte>(this.buffer, this.countOffset);
                bool success = this.intSerializer.Serialize(ref countRegion, this.currentCount);
                if (!success)
                {
                    throw new ApplicationException("Logic bug filling in count");
                }
                this.countOffset = -1;
                this.currentCount = 0;
            }

            this.currentChannelId = -1;
            this.currentVertexId = -1;
        }

        private long nextLogTicks;
        private System.Diagnostics.Stopwatch logStopwatch;
        private Action<string, object[]> writeLogEntry;

        private void SendBuffer()
        {
            this.FinishSegment();

            if (this.bufferAsSubarray.Count > sizeof(int))
            {
                SubArray<byte> lengthRegion = new SubArray<byte>(this.buffer);
                this.intSerializer.Serialize(ref lengthRegion, this.bufferAsSubarray.Count);

                foreach (TVertex time in this.timesToSeal)
                {
                    TimeExtent extent;
                    if (this.timeExtents.TryGetValue(time, out extent))
                    {
                        extent.end = this.streams.Index;
                        extent.end.position += this.bufferAsSubarray.Count;
                        this.timeExtents[time] = extent;
                    }
                }

                this.streams.Write(this.buffer, 0, this.bufferAsSubarray.Count);

                if (this.logStopwatch == null)
                {
                    this.logStopwatch = new System.Diagnostics.Stopwatch();
                    this.logStopwatch.Start();
                    this.nextLogTicks = 1000 * TimeSpan.TicksPerMillisecond;
                }

                if (this.logStopwatch.ElapsedTicks > this.nextLogTicks)
                {
                    this.writeLogEntry("{0} {1:D11} {2:D11}", new object[] {
                        this.streams.BaseName,
                        this.logStopwatch.ElapsedTicks / TimeSpan.TicksPerMillisecond,
                        this.streams.TotalWritten
                    });
                }

                this.StartBuffer();
            }
            else
            {
                foreach (TVertex time in this.timesToSeal)
                {
                    TimeExtent extent;
                    if (this.timeExtents.TryGetValue(time, out extent))
                    {
                        extent.end = this.streams.Index;
                        this.timeExtents[time] = extent;
                    }
                }
            }

            this.timesToSeal.Clear();
        }

        private void SendFinalBuffer()
        {
            if (this.bufferAsSubarray.Count > sizeof(int))
            {
                SubArray<byte> lengthRegion = new SubArray<byte>(this.buffer);
                this.intSerializer.Serialize(ref lengthRegion, -1);

                this.streams.Write(this.buffer, 0, lengthRegion.Count);
                this.streams.FlushAsync().Wait();
            }

            this.buffer = null;
        }

        public void LogEmptyMessage(T time)
        {
            this.StartSegment(time, -1, -1);
        }

        public int LogMessage<S>(T time, int channelId, int vertexId, NaiadSerialization<S> recordSerializer, ArraySegment<S> payload)
        {
            this.EnsureSegment(time, channelId, vertexId);

            int numSerialized = recordSerializer.TrySerializeMany(ref this.bufferAsSubarray, payload);
            this.currentCount += numSerialized;

            if (numSerialized > 0 && numSerialized < payload.Count)
            {
                // we wrote something but not everything: send what we wrote and the caller will try again with the next batch
                this.SendBuffer();
            }
            else if (numSerialized == 0)
            {
                // We didn't manage to send any elements.

                // The first possibility is that the page was full, so allocate a new one.
                this.SendBuffer();

                this.StartSegment(time, channelId, vertexId);
                numSerialized = recordSerializer.TrySerializeMany(ref this.bufferAsSubarray, payload);

                // If writing to an empty page failed, repeatedly double the size of the page until at least
                // one record fits.
                while (numSerialized == 0)
                {
                    this.buffer = new byte[this.buffer.Length * 2];
                    this.StartBuffer();
                    this.StartSegment(time, channelId, vertexId);
                    numSerialized = recordSerializer.TrySerializeMany(ref this.bufferAsSubarray, payload);
                }

                this.currentCount += numSerialized;
            }

            return numSerialized;
        }

        public void AddSentCount(int channelId, TVertex senderTime, TMessage messageTime, int receiverVertexId, long count)
        {
            TimeCountBundle<TMessage> counts;
            if (!this.sentRecordCounts[channelId].Second.TryGetValue(senderTime, out counts))
            {
                counts = new TimeCountBundle<TMessage>(this.sentRecordCounts[channelId].First, true);
                this.sentRecordCounts[channelId].Second.Add(senderTime, counts);
            }

            counts.Add(receiverVertexId, messageTime, count);

            TimeExtent extent = this.timeExtents[senderTime];
            extent.sentMessageFrontier.Add(messageTime.ToPointstamp(this.stageId));
            this.timeExtents[senderTime] = extent;
        }

        private void ReadLogEntry(ref RecvBuffer recv, HashSet<TVertex> validTimes)
        {
            T time;
            int channelId;

            if (!this.timeSerializer.TryDeserialize(ref recv, out time))
            {
                throw new ApplicationException("Failed to read time from log");
            }

            if (!this.intSerializer.TryDeserialize(ref recv, out channelId))
            {
                throw new ApplicationException("Failed to read channel Id from log");
            }

            TVertex vertexTime = this.vertexTimeFunction(time);
            bool deliver = validTimes.Contains(vertexTime);

            if (channelId == -1)
            {
                if (deliver)
                {
                    if (this.notificationAction == null)
                    {
                        throw new ApplicationException("Replayer does not support notifications");
                    }
                    this.notificationAction(vertexTime);
                }
            }
            else
            {
                if (!this.messageReplayers.ContainsKey(channelId))
                {
                    throw new ApplicationException("Replayer does not have a receiver for channel " + channelId);
                }

                int vertexId;
                int recordCount;

                if (!this.intSerializer.TryDeserialize(ref recv, out vertexId))
                {
                    throw new ApplicationException("Failed to read send vertex from log");
                }

                if (!this.intSerializer.TryDeserialize(ref recv, out recordCount))
                {
                    throw new ApplicationException("Failed to read record count from log");
                }

                recv = this.messageReplayers[channelId].DeserializeToMessages(recv, time, vertexId, recordCount, deliver);

                this.channelsToFlush.Add(channelId);
            }
        }

        private void ReplayTimes(Stream data, HashSet<TVertex> validTimes)
        {
            byte[] buffer = new byte[Logger<T, TVertex, TMessage>.initialBufferSize];

            int thisLength = StreamSequence.TryReadLength(buffer, data, sizeof(int), this.intSerializer);
            while (thisLength >= 0)
            {
                if (thisLength < sizeof(int))
                {
                    throw new ApplicationException("Log block has invalid length " + thisLength);
                }

                if (thisLength > buffer.Length)
                {
                    buffer = new byte[thisLength];
                }

                int nextLength = StreamSequence.TryReadLength(buffer, data, thisLength, this.intSerializer);
                if (nextLength == -2)
                {
                    throw new ApplicationException("Log block has invalid length " + thisLength);
                }

                RecvBuffer recv = new RecvBuffer(buffer, 0, thisLength - sizeof(int));

                while (recv.Available > 0)
                {
                    this.ReadLogEntry(ref recv, validTimes);
                }

                thisLength = nextLength;
            }
        }

        public void AddEdge(Edge edge)
        {
            this.sentRecordCounts.Add(edge.ChannelId, edge.PairWith(new Dictionary<TVertex, TimeCountBundle<TMessage>>()));
        }

        public void AddNotifier(Action<TVertex> notificationAction)
        {
            this.notificationAction = notificationAction;
        }

        public void UpdateNotifier(Action<TVertex> notificationAction)
        {
            if (this.notificationAction != null)
            {
                this.notificationAction = notificationAction;
            }
        }

        public void AddMessageReader(int channelId, IMessageReplayer<T> messageReplayer)
        {
            this.messageReplayers.Add(channelId, messageReplayer);
        }

        public void ReplaceMessageReader(int channelId, IMessageReplayer<T> messageReplayer)
        {
            this.messageReplayers[channelId] = messageReplayer;
        }

        public void Shutdown()
        {
            foreach (TVertex time in this.timeExtents
                .Where(t => t.Value.end.Equals(this.streams.UnboundedIndex))
                .Select(t => t.Key))
            {
                this.MarkTimeComplete(time);
            }

            this.SendBuffer();

            // this will block until the outstanding write completes, if any
            this.SendFinalBuffer();
        }

        public Task FlushAsync()
        {
            this.SendBuffer();

            return this.streams.FlushAsync();
        }

        public void MarkFrontier(FTFrontier upTo)
        {
            foreach (TVertex time in this.timeExtents
                .Where(t => t.Value.end.Equals(this.streams.UnboundedIndex))
                .Select(t => t.Key)
                .Where(t =>
                    {
                        Pointstamp stamp = t.ToPointstamp(this.stageId);
                        return upTo.Contains(stamp);
                    }))
            {
                this.MarkTimeComplete(time);
            }
        }

        public void MarkTime(TVertex time)
        {
            if (!this.timeExtents.ContainsKey(time))
            {
                this.timeExtents.Add(time, new TimeExtent(this.streams));
            }
        }

        public void MarkTimeComplete(TVertex time)
        {
            this.timesToSeal.Add(time);
        }

        public IEnumerable<GarbageCollectionWrapper> GarbageCollectStreams()
        {
            HashSet<int> usedIndices = new HashSet<int>();
            foreach (TimeExtent extent in this.timeExtents.Values)
            {
                foreach (int index in this.streams.ExtentRange(extent.start.PairWith(extent.end)))
                {
                    usedIndices.Add(index);
                }
            }

            if (!usedIndices.Contains(this.streams.CurrentIndex))
            {
                this.streams.CloseCurrentStream();
            }

            return this.streams.FindStreams()
                .Where(i => !usedIndices.Contains(i))
                .Select(i => new GarbageCollectionWrapper(this.streams, i));
        }

        private void FilterExtents(FTFrontier frontier, bool keep, bool seal)
        {
            Dictionary<TVertex, TimeExtent> newExtents = new Dictionary<TVertex, TimeExtent>();

            foreach (KeyValuePair<TVertex, TimeExtent> extent in this.timeExtents)
            {
                if (keep == frontier.Contains(extent.Key.ToPointstamp(this.stageId)))
                {
                    if (seal && extent.Value.end.Equals(this.streams.UnboundedIndex))
                    {
                        this.MarkTimeComplete(extent.Key);
                    }

                    newExtents.Add(extent.Key, extent.Value);
                }
            }

            this.timeExtents = newExtents;

            HashSet<TVertex> newSeals = new HashSet<TVertex>();
            foreach (TVertex time in this.timesToSeal)
            {
                if (keep == frontier.Contains(time.ToPointstamp(this.stageId)))
                {
                    newSeals.Add(time);
                }
            }

            this.timesToSeal = newSeals;
        }

        private void FilterCounts(FTFrontier frontier, bool keep)
        {
            Dictionary<int, Pair<Edge, Dictionary<TVertex, TimeCountBundle<TMessage>>>> newCounts =
                new Dictionary<int, Pair<Edge, Dictionary<TVertex, TimeCountBundle<TMessage>>>>();

            foreach (KeyValuePair<int, Pair<Edge, Dictionary<TVertex, TimeCountBundle<TMessage>>>> edge in this.sentRecordCounts)
            {
                Dictionary<TVertex, TimeCountBundle<TMessage>> newBundle = new Dictionary<TVertex, TimeCountBundle<TMessage>>();

                foreach (KeyValuePair<TVertex, TimeCountBundle<TMessage>> count in edge.Value.Second)
                {
                    if (keep == frontier.Contains(count.Key.ToPointstamp(this.stageId)))
                    {
                        newBundle.Add(count.Key, count.Value);
                    }
                }

                newCounts.Add(edge.Key, edge.Value.First.PairWith(newBundle));
            }

            this.sentRecordCounts = newCounts;
        }

        public void SetRetentionFrontier(FTFrontier frontier, bool sealEagerly)
        {
            this.retentionFrontier = frontier;
            this.FilterExtents(frontier, true, true);
            this.FilterCounts(frontier, true);

            if (sealEagerly && this.timesToSeal.Count > 0)
            {
                StreamSequenceIndex maxExtent = this.streams.MaxIndex;

                foreach (TVertex time in this.timesToSeal)
                {
                    TimeExtent extent = this.timeExtents[time];
                    extent.end = maxExtent;
                    this.timeExtents[time] = extent;
                }

                this.timesToSeal.Clear();
            }
        }

        public void GarbageCollectForReceivers(FTFrontier frontier)
        {
            var staleTimes = this.timeExtents.Where(e => frontier.Contains(e.Value.sentMessageFrontier)).ToArray();
            foreach (KeyValuePair<TVertex, TimeExtent> extent in staleTimes)
            {
                this.timeExtents.Remove(extent.Key);
                if (this.timesToSeal.Contains(extent.Key))
                {
                    this.timesToSeal.Remove(extent.Key);
                }
            }

            Dictionary<int, Pair<Edge, Dictionary<TVertex, TimeCountBundle<TMessage>>>> newCounts =
                new Dictionary<int, Pair<Edge, Dictionary<TVertex, TimeCountBundle<TMessage>>>>();

            foreach (KeyValuePair<int, Pair<Edge, Dictionary<TVertex, TimeCountBundle<TMessage>>>> edge in this.sentRecordCounts)
            {
                Dictionary<TVertex, TimeCountBundle<TMessage>> newBundle = new Dictionary<TVertex, TimeCountBundle<TMessage>>();

                foreach (KeyValuePair<TVertex, TimeCountBundle<TMessage>> count in edge.Value.Second)
                {
                    TimeCountBundle<TMessage> newEdgeCounts = count.Value.Filter(this.stageId, frontier);
                    if (newEdgeCounts != null)
                    {
                        newBundle.Add(count.Key, count.Value);
                    }
                }

                newCounts.Add(edge.Key, edge.Value.First.PairWith(newBundle));
            }

            this.sentRecordCounts = newCounts;
        }

        internal struct RangeBoundary : IComparable<RangeBoundary>
        {
            public StreamSequenceIndex location;
            public TVertex time;
            public bool isStart;

            public int CompareTo(RangeBoundary other)
            {
                return this.location.CompareTo(other.location);
            }
        }

        public IEnumerable<RangeBoundary> GetCurrentTimeExtents()
        {
            List<RangeBoundary> boundaries = new List<RangeBoundary>();
            foreach (KeyValuePair<TVertex, TimeExtent> extent in this.timeExtents)
            {
                boundaries.Add(new RangeBoundary { time = extent.Key, location = extent.Value.start, isStart = true });
                StreamSequenceIndex endIndex = extent.Value.end;
                if (endIndex.Equals(this.streams.UnboundedIndex))
                {
                    endIndex = this.streams.Index;
                }
                boundaries.Add(new RangeBoundary { time = extent.Key, location = endIndex, isStart = false });
            }

            boundaries.Sort();

            return boundaries;
        }

        public void ReplayLog(IEnumerable<RangeBoundary> boundaries)
        {
            HashSet<TVertex> currentTimes = new HashSet<TVertex>();
            StreamSequenceIndex previousIndex = new StreamSequenceIndex();
            foreach (RangeBoundary boundary in boundaries)
            {
                if (currentTimes.Count > 0 && !previousIndex.Equals(boundary.location))
                {
                    this.ReplayTimes(this.streams.Read(previousIndex, boundary.location), currentTimes);
                }

                if (boundary.isStart)
                {
                    currentTimes.Add(boundary.time);
                }
                else
                {
                    currentTimes.Remove(boundary.time);
                }

                previousIndex = boundary.location;
            }

            foreach (int channel in this.channelsToFlush)
            {
                this.messageReplayers[channel].Flush();
            }

            this.channelsToFlush.Clear();
        }

        public void Initialize()
        {
            this.SetRetentionFrontier(new FTFrontier(false), false);
            foreach (var wrapper in this.GarbageCollectStreams())
            {
                wrapper.Collect();
            }
        }

        public bool TrySerialize(FTFrontier upTo, FTFrontier lastCheckpoint, SendBufferPage buffer, SerializationFormat format)
        {
            IEnumerable<KeyValuePair<TVertex, TimeExtent>> times = this.timeExtents
                .Where(t =>
                {
                    Pointstamp stamp = t.Key.ToPointstamp(this.stageId);
                    return upTo.Contains(stamp) && !lastCheckpoint.Contains(stamp);
                });
            bool fit = buffer.Write(this.intSerializer, times.Count());
            foreach (KeyValuePair<TVertex, TimeExtent> extent in times)
            {
                fit = fit && buffer.Write(this.vertexTimeSerializer, extent.Key);
                fit = fit && buffer.Write(this.intSerializer, extent.Value.start.index);
                fit = fit && buffer.Write(this.longSerializer, extent.Value.start.position);
                fit = fit && buffer.Write(this.intSerializer, extent.Value.end.index);
                fit = fit && buffer.Write(this.longSerializer, extent.Value.end.position);
                fit = fit && extent.Value.sentMessageFrontier.TrySerialize(buffer, format);
            }

            foreach (KeyValuePair<int, Pair<Edge, Dictionary<TVertex, TimeCountBundle<TMessage>>>> counts in this.sentRecordCounts.OrderBy(b => b.Key))
            {
                IEnumerable<KeyValuePair<TVertex, TimeCountBundle<TMessage>>> countTimes =
                    counts.Value.Second.Where(t =>
                    {
                        Pointstamp stamp = t.Key.ToPointstamp(this.stageId);
                        return upTo.Contains(stamp) && !lastCheckpoint.Contains(stamp);
                    });

                fit = fit && buffer.Write(this.intSerializer, countTimes.Count());
                foreach (KeyValuePair<TVertex, TimeCountBundle<TMessage>> time in countTimes)
                {
                    fit = fit && buffer.Write(this.vertexTimeSerializer, time.Key);
                    fit = fit && time.Value.TrySerialize(buffer, format);
                }
            }

            return fit;
        }

        public bool TryDeserialize(ref RecvBuffer source, SerializationFormat format)
        {
            int numberOfTimes = 0;
            bool success = this.intSerializer.TryDeserialize(ref source, out numberOfTimes);
            for (int i = 0; success && i < numberOfTimes; ++i)
            {
                TVertex time = default(TVertex);
                StreamSequenceIndex start = new StreamSequenceIndex();
                StreamSequenceIndex end = new StreamSequenceIndex();
                FTFrontier frontier = new FTFrontier(false);
                success = success && this.vertexTimeSerializer.TryDeserialize(ref source, out time);
                success = success && this.intSerializer.TryDeserialize(ref source, out start.index);
                success = success && this.longSerializer.TryDeserialize(ref source, out start.position);
                success = success && this.intSerializer.TryDeserialize(ref source, out end.index);
                success = success && this.longSerializer.TryDeserialize(ref source, out end.position);
                success = success && frontier.TryDeserialize(ref source, format);
                if (success)
                {
                    this.timeExtents.Add(time, new TimeExtent(start, end, frontier));
                }
            }

            foreach (KeyValuePair<int, Pair<Edge, Dictionary<TVertex, TimeCountBundle<TMessage>>>> counts in this.sentRecordCounts.OrderBy(b => b.Key))
            {
                int numberOfCounts = 0;
                success = success && this.intSerializer.TryDeserialize(ref source, out numberOfCounts);
                for (int i = 0; success && i < numberOfCounts; ++i)
                {
                    TVertex time = default(TVertex);
                    TimeCountBundle<TMessage> bundle = new TimeCountBundle<TMessage>(counts.Value.First, true);
                    success = success && this.vertexTimeSerializer.TryDeserialize(ref source, out time);
                    success = success && bundle.TryDeserialize(ref source, format);
                    if (success)
                    {
                        counts.Value.Second.Add(time, bundle);
                    }
                }
            }

            return success;
        }

        public Logger(Func<T, TVertex> vertexTimeFunction, int stageId, StreamSequence streams,
            SerializationFormat serializationFormat, Action<string, object[]> writeLogEntry)
        {
            this.currentChannelId = -1;
            this.currentVertexId = -1;
            this.currentTime = default(T);

            this.buffer = new byte[Logger<T, TVertex, TMessage>.initialBufferSize];
            this.countOffset = -1;
            this.currentCount = 0;

            this.serializationFormat = serializationFormat;
            this.intSerializer = this.serializationFormat.GetSerializer<int>();
            this.longSerializer = this.serializationFormat.GetSerializer<long>();
            this.timeSerializer = this.serializationFormat.GetSerializer<T>();
            this.vertexTimeSerializer = this.serializationFormat.GetSerializer<TVertex>();
            this.vertexTimeFunction = vertexTimeFunction;
            this.streams = streams;
            this.stageId = stageId;

            this.writeLogEntry = writeLogEntry;

            this.StartBuffer();
        }
    }

    internal class Logger<T> : Logger<T, T, T> where T : Time<T>
    {
        public Logger(int stageId, StreamSequence streams, SerializationFormat serializationFormat,
            Action<string, object[]> writeLogEntry)
            : base(t => t, stageId, streams, serializationFormat, writeLogEntry)
        {
        }
    }

    internal class CountLogger<TVertex, TMessage> : Logger<Pair<TVertex, TMessage>, TVertex, TMessage>
        where TVertex : Time<TVertex>
        where TMessage : Time<TMessage>
    {
        private readonly Dictionary<int, IProgressUpdater<TMessage>> progressUpdaters = new Dictionary<int,IProgressUpdater<TMessage>>();

        public Pointstamp? UpdateProgressForLoggedMessages<R>(int senderVertexId, FullyTypedStageOutput<TVertex, R, TMessage> stageOutput)
        {
            Pointstamp? maxSentTime = null;

            foreach (var channel in this.sentRecordCounts)
            {
                IProgressUpdater<TMessage> updater = this.progressUpdaters[channel.Key];
                updater.PrepareForRestoration();

                foreach (var time in channel.Value.Second)
                {
                    foreach (int receiverVertexId in time.Value.Vertices(senderVertexId))
                    {
                        foreach (KeyValuePair<TMessage, long> count in time.Value.Counts(receiverVertexId).Counts)
                        {
                            updater.UpdateProgress(count.Key, receiverVertexId, count.Value);
                        }
                    }

                    Pointstamp senderTime = time.Key.ToPointstamp(this.stageId);
                    if (!maxSentTime.HasValue || FTFrontier.IsLessThanOrEqualTo(maxSentTime.Value, senderTime))
                    {
                        maxSentTime = senderTime;
                    }
                }

                updater.FlushUpdates();
            }

            return maxSentTime;
        }

        public void AddProgressUpdater(int channelId, IOutgoingMessageReplayer<TVertex, TMessage> replayer)
        {
            this.AddMessageReader(channelId, replayer);
            this.progressUpdaters[channelId] = replayer;
        }

        public void ReplaceProgressUpdaterForRollback(int channelId, IOutgoingMessageReplayer<TVertex, TMessage> replayer)
        {
            this.ReplaceMessageReader(channelId, replayer);
            this.progressUpdaters[channelId] = replayer;
        }

        public CountLogger(int stageId, StreamSequence streams, SerializationFormat serializationFormat,
            Action<string, object[]> writeLogEntry)
            : base(t => t.First, stageId, streams, serializationFormat, writeLogEntry)
        {
        }
    }

    /// <summary>
    /// Interface for replaying logged incoming messages
    /// </summary>
    /// <typeparam name="K">Key (e.g. time) that messages are batched under</typeparam>
    public interface IMessageReplayer<K>
    {
        /// <summary>
        /// Deserialize a batch of records and forward them to the replayer
        /// </summary>
        /// <param name="recv">buffer from which records are deserialized</param>
        /// <param name="key">key associated with the records</param>
        /// <param name="vertexId">vertex that sent or should receive the records</param>
        /// <param name="recordCount">number of records to deserialize</param>
        /// <param name="deliver">flag indicating whether the deserialized messages should be delivered or not;
        /// this may be set to false during recovery to ensure the state of adjacent stages matches</param>
        /// <returns>updated buffer with the read cursor advanced past the records</returns>
        RecvBuffer DeserializeToMessages(RecvBuffer recv, K key, int vertexId, int recordCount, bool deliver);

        /// <summary>
        /// Flush the replayed messages
        /// </summary>
        void Flush();
    }

    /// <summary>
    /// Interface for updating a progress tracker
    /// </summary>
    /// <typeparam name="T">time type</typeparam>
    public interface IProgressUpdater<T> where T : Time<T>
    {
        /// <summary>
        /// Prepare for replaying updates by installing the appropriate filters according to the current checkpoints
        /// </summary>
        void PrepareForRestoration();
        /// <summary>
        /// Update the progress tracker to indicate that logged messages are being re-inserted into the computation
        /// </summary>
        /// <param name="time">the time of the records being replayed</param>
        /// <param name="receiverVertexId">the vertex the records are being delivered to</param>
        /// <param name="count">the number of records at that time</param>
        void UpdateProgress(T time, int receiverVertexId, long count);
        /// <summary>
        /// Push updated values into the progress tracker
        /// </summary>
        void FlushUpdates();
    }

    /// <summary>
    /// Interface for replaying logged outgoing messages
    /// </summary>
    /// <typeparam name="TSenderTime">Time type at sender vertex</typeparam>
    /// <typeparam name="TMessageTime">Time type of messages</typeparam>
    public interface IOutgoingMessageReplayer<TSenderTime, TMessageTime>
        : IMessageReplayer<Pair<TSenderTime, TMessageTime>>, IProgressUpdater<TMessageTime>
        where TSenderTime : Time<TSenderTime>
        where TMessageTime : Time<TMessageTime>
    {
    }
}
