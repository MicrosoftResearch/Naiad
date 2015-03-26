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
using Microsoft.Research.Naiad.Dataflow.Channels;
using Microsoft.Research.Naiad.Serialization;

namespace Microsoft.Research.Naiad.Runtime.FaultTolerance
{
    /// <summary>
    /// Type of checkpoint used by a stage
    /// </summary>
    public enum CheckpointType
    {
        /// <summary>
        /// For stages that don't need to participate in recovery
        /// </summary>
        None,
        /// <summary>
        /// The default: the stage logs all incoming messages and notifications
        /// </summary>
        Logging,
        /// <summary>
        /// The stage doesn't log anything, or checkpoint any state
        /// </summary>
        Stateless,
        /// <summary>
        /// The stage doesn't checkpoint any state but logs its outgoing messages
        /// </summary>
        StatelessLogAll,
        /// <summary>
        /// The stage doesn't checkpoint any state or log its outgoing messages, but they won't need to be replayed
        /// </summary>
        StatelessLogEphemeral,
        /// <summary>
        /// The stage checkpoints, and logs messages sent to the future
        /// </summary>
        Stateful,
        /// <summary>
        /// The stage checkpoints, and logs all messages
        /// </summary>
        StatefulLogAll,
        /// <summary>
        /// The stage doesn't checkpoint any state or log its outgoing messages, but they won't need to be replayed
        /// </summary>
        StatefulLogEphemeral,
        /// <summary>
        /// The stage caches inputs in memory, releasing them when downstream stages have
        /// persisted them
        /// </summary>
        CachingInput
    }

    /// <summary>
    /// Helper methods to reason about properties of different checkpoint types
    /// </summary>
    public static class CheckpointProperties
    {
        /// <summary>
        /// true if the checkpoint saves state for particular frontiers
        /// </summary>
        /// <param name="type">the type of checkpoint</param>
        /// <returns>true if checkpoints with <paramref name="type"/> save state</returns>
        public static bool IsStateful(CheckpointType type)
        {
            return type == CheckpointType.Stateful || type == CheckpointType.StatefulLogAll;
        }
    }

    /// <summary>
    /// class used to store the state of a vertex that is needed for checkpointing or restoration
    /// </summary>
    /// <typeparam name="T">time type of the vertex</typeparam>
    public class CheckpointState<T> where T : Time<T>
    {
        internal interface DiscardedTimes
        {
            bool ManagesChannel(int channelId);
            IEnumerable<KeyValuePair<T, TimeSetBundle>> Bundles(int channelId);
            bool TrySerialize(FTFrontier upTo, FTFrontier lastCheckpoint, int stageId, SendBufferPage buffer, SerializationFormat serializationFormat);
            bool TryDeserialize(ref RecvBuffer buffer, SerializationFormat serializationFormat);
            void IntersectAll(int stageId, FTFrontier frontier);
            IEnumerable<Pair<Edge, IEnumerable<Pair<Pointstamp, TimeSetBundle>>>> Times(int nodeId);
        };

        internal class DiscardedTimes<TMessage> : DiscardedTimes where TMessage : Time<TMessage>
        {
            private Dictionary<int, Pair<Edge, Dictionary<T, TimeSetBundle<TMessage>>>> discardedMessageTimes;

            public bool ManagesChannel(int channelId)
            {
                return this.discardedMessageTimes.ContainsKey(channelId);
            }

            public IEnumerable<KeyValuePair<T, TimeSetBundle>> Bundles(int channelId)
            {
                return this.discardedMessageTimes[channelId].Second.Select(b => new KeyValuePair<T, TimeSetBundle>(b.Key, b.Value));
            }

            public IEnumerable<Pair<Edge, IEnumerable<Pair<Pointstamp, TimeSetBundle>>>> Times(int nodeId)
            {
                return this.discardedMessageTimes.Select(b =>
                    b.Value.First.PairWith(b.Value.Second.Select(t =>
                        t.Key.ToPointstamp(nodeId).PairWith(t.Value as TimeSetBundle))));
            }

            public void IntersectAll(int stageId, FTFrontier frontier)
            {
                Dictionary<int, Pair<Edge, Dictionary<T, TimeSetBundle<TMessage>>>> newTimes =
                    new Dictionary<int, Pair<Edge, Dictionary<T, TimeSetBundle<TMessage>>>>();

                foreach (KeyValuePair<int, Pair<Edge, Dictionary<T, TimeSetBundle<TMessage>>>> channel in this.discardedMessageTimes)
                {
                    Dictionary<T, TimeSetBundle<TMessage>> pruned = new Dictionary<T,TimeSetBundle<TMessage>>();
                    foreach (var bundle in channel.Value.Second.Where(b => frontier.Contains(b.Key.ToPointstamp(stageId))))
                    {
                        pruned.Add(bundle.Key, bundle.Value);
                    }

                    newTimes.Add(channel.Key, channel.Value.First.PairWith(pruned));
                }

                this.discardedMessageTimes = newTimes;
            }

            public bool TrySerialize(FTFrontier upTo, FTFrontier lastCheckpoint, int stageId, SendBufferPage sendPage, SerializationFormat serializationFormat)
            {
                FTFrontier bottom = new FTFrontier(false);
                FTFrontier top = new FTFrontier(true);

                bool fits = true;

                foreach (var bundle in this.discardedMessageTimes.OrderBy(b => b.Key))
                {
                    var times = bundle.Value.Second.Where(t =>
                    {
                        Pointstamp stamp = t.Key.ToPointstamp(stageId);
                        return upTo.Contains(stamp) && !lastCheckpoint.Contains(stamp);
                    });

                    fits = fits && sendPage.Write(serializationFormat.GetSerializer<int>(), times.Count());
                    foreach (var time in times)
                    {
                        fits = fits && sendPage.Write(serializationFormat.GetSerializer<T>(), time.Key);
                        fits = fits && time.Value.TrySerialize(-1, top, bottom, sendPage, serializationFormat);
                    }
                }

                return fits;
            }

            public bool TryDeserialize(ref RecvBuffer buffer, SerializationFormat serializationFormat)
            {
                bool success = true;
                foreach (var channel in this.discardedMessageTimes.OrderBy(c => c.Key))
                {
                    int numberOfTimes = -1;
                    success = success && serializationFormat.GetSerializer<int>().TryDeserialize(ref buffer, out numberOfTimes);
                    for (int i = 0; success && i < numberOfTimes; ++i)
                    {
                        T time = default(T);
                        success = success && serializationFormat.GetSerializer<T>().TryDeserialize(ref buffer, out time);

                        TimeSetBundle<TMessage> bundle;
                        if (!channel.Value.Second.TryGetValue(time, out bundle))
                        {
                            bundle = new TimeSetBundle<TMessage>(channel.Value.First, true);
                            channel.Value.Second.Add(time, bundle);
                        }

                        success = success && bundle.TryDeserialize(ref buffer, serializationFormat);
                    }
                }
                return success;
            }

            public void AddDiscardedTime(int channelId, T senderTime, TMessage messageTime, int receiverVertexId)
            {
                TimeSetBundle<TMessage> bundle;
                if (!this.discardedMessageTimes[channelId].Second.TryGetValue(senderTime, out bundle))
                {
                    bundle = new TimeSetBundle<TMessage>(this.discardedMessageTimes[channelId].First, true);
                    this.discardedMessageTimes[channelId].Second.Add(senderTime, bundle);
                }

                bundle.Add(receiverVertexId, messageTime);
            }

            public DiscardedTimes(StageOutputForVertex<T> stageOutput)
            {
                this.discardedMessageTimes = new Dictionary<int, Pair<Edge, Dictionary<T, TimeSetBundle<TMessage>>>>();

                foreach (Edge edge in stageOutput.OutputChannels)
                {
                    this.discardedMessageTimes.Add(edge.ChannelId, edge.PairWith(new Dictionary<T, TimeSetBundle<TMessage>>()));
                }
            }
        }

        internal readonly Stage<T> stage;
        internal TimeCountDictionary<T> requestedNotifications;
        internal TimeCount<T> deliveredNotifications;
        internal Dictionary<int, Pair<int,TimeSetBundle<T>>> receivedMessageTimes;
        internal List<DiscardedTimes> discardedOutboundMessageBundles = new List<DiscardedTimes>();
        internal FTFrontier lastCheckpoint = new FTFrontier(false);
        internal FTFrontier lastGarbageCollection = new FTFrontier(false);
        internal FTFrontier lastUpdateSent = new FTFrontier(false);

        internal bool TryDeserialize(ref RecvBuffer buffer, SerializationFormat serializationFormat)
        {
            FTFrontier thisFrontier = new FTFrontier(false);
            // get the latest frontier we are keeping data up to
            bool success = thisFrontier.TryDeserialize(ref buffer, serializationFormat);

            if (success && (!thisFrontier.Contains(this.lastCheckpoint) || thisFrontier.Equals(this.lastCheckpoint)))
            {
                throw new ApplicationException("Badly nested partial checkpoints");
            }

            FTFrontier thisGarbageCollection = new FTFrontier(false);
            success = thisGarbageCollection.TryDeserialize(ref buffer, serializationFormat);
            if (success && !thisGarbageCollection.Equals(this.lastGarbageCollection))
            {
                if (!thisGarbageCollection.Contains(this.lastGarbageCollection))
                {
                    throw new ApplicationException("Badly nested garbage collections");
                }
                this.lastGarbageCollection = thisGarbageCollection;
            }

            success = success && this.requestedNotifications.TryDeserialize(ref buffer, serializationFormat);
            success = success && this.deliveredNotifications.TryDeserialize(ref buffer, serializationFormat);

            foreach (var channel in this.receivedMessageTimes.OrderBy(c => c.Key))
            {
                success = success && channel.Value.Second.TryDeserialize(ref buffer, serializationFormat);
            }

            foreach (var stageOutput in this.discardedOutboundMessageBundles)
            {
                success = success && stageOutput.TryDeserialize(ref buffer, serializationFormat);
            }

            // there may be more state remaining in the buffer depending on the checkpointer, but that will be dealt with elsewhere

            if (success)
            {
                this.lastCheckpoint = thisFrontier;
            }

            return success;
        }

        internal IEnumerable<KeyValuePair<T, TimeSetBundle>> OutgoingDiscardedMessages(int channelId)
        {
            foreach (DiscardedTimes bundle in this.discardedOutboundMessageBundles)
            {
                if (bundle.ManagesChannel(channelId))
                {
                    return bundle.Bundles(channelId);
                }
            }
            throw new ApplicationException("Looked up nonexistent channel");
        }

        internal void FilterMetaData(FTFrontier frontier)
        {
            // the next call contains a hack for inputs, which request a notification before the computation starts,
            // so that request is retained even if frontier is empty
            this.requestedNotifications = this.requestedNotifications.Intersect(this.stage.StageId, frontier, new FTFrontier(false));
            this.deliveredNotifications = this.deliveredNotifications.Intersect(this.stage.StageId, frontier);
            foreach (var bundle in this.receivedMessageTimes.Values)
            {
                bundle.Second.IntersectAll(this.stage.StageId, frontier);
            }
            foreach (var bundle in this.discardedOutboundMessageBundles)
            {
                bundle.IntersectAll(this.stage.StageId, frontier);
            }
        }

        internal bool GarbageCollectMetaData(FTFrontier frontier)
        {
            // garbage collection frontiers can be delivered out of order
            if (frontier.Contains(this.lastGarbageCollection))
            {
                if (frontier.Equals(this.lastGarbageCollection))
                {
                    throw new ApplicationException("Duplicate garbage collection");
                }
                this.requestedNotifications.RemoveStale(this.stage.StageId, frontier);
                this.deliveredNotifications = this.deliveredNotifications.Except(this.stage.StageId, frontier);
                foreach (var bundle in this.receivedMessageTimes.Values)
                {
                    bundle.Second.ExceptAll(this.stage.StageId, frontier);
                }
                this.lastGarbageCollection = frontier;
                return true;
            }
            else
            {
                return false;
            }
        }

        internal CheckpointState(Stage<T> stage, bool makeDiscardedTimes)
        {
            this.stage = stage;

            this.requestedNotifications = new TimeCountDictionary<T>();
            this.deliveredNotifications = new TimeCount<T>();

            this.receivedMessageTimes = new Dictionary<int, Pair<int,TimeSetBundle<T>>>();
            foreach (Edge edge in stage.InternalComputation.Edges.Select(kv => kv.Value).Where(e => e.TargetStage == stage))
            {
                this.receivedMessageTimes.Add(edge.ChannelId, edge.SourceStage.StageId.PairWith(new TimeSetBundle<T>(edge, false)));
            }

            this.discardedOutboundMessageBundles = new List<DiscardedTimes>();
            if (makeDiscardedTimes)
            {
                foreach (StageOutputForVertex<T> output in stage.StageOutputs)
                {
                    this.discardedOutboundMessageBundles.Add(output.MakeDiscardedTimesBundle());
                }
            }
        }
    }

    internal abstract class CheckpointPersistedAction
    {
        public abstract void Execute();
    }

    internal class CheckpointPersistedAction<T> : CheckpointPersistedAction where T : Time<T>
    {
        private readonly Checkpointer<T> checkpointer;
        private readonly long epoch;
        private readonly GarbageCollectionWrapper[] collections;
        private readonly FTFrontier frontier;
        private readonly long startTicks;
        public long QueueTicks;

        public override void Execute()
        {
            foreach (var wrapper in this.collections)
            {
                wrapper.Collect();
            }

            long endTicks = this.checkpointer.vertex.scheduler.Controller.Stopwatch.ElapsedTicks;
            long queueMicroSeconds = ((QueueTicks - startTicks) * 1000000L) / System.Diagnostics.Stopwatch.Frequency;
            long microSeconds = ((endTicks - startTicks) * 1000000L) / System.Diagnostics.Stopwatch.Frequency;
            long totalMicroSeconds = (endTicks * 1000000L) / System.Diagnostics.Stopwatch.Frequency;
            this.checkpointer.vertex.scheduler.WriteLogEntry("{0:D3}.{1:D3} S {2:D7} {3:D11} {4:D7}  {5}",
                this.checkpointer.vertex.Stage.StageId, this.checkpointer.vertex.VertexId,
                microSeconds, totalMicroSeconds, queueMicroSeconds, this.frontier);

            this.checkpointer.ReceivePersistenceNotification(this.epoch, this.frontier);
        }

        public CheckpointPersistedAction(
            Checkpointer<T> checkpointer, long epoch, 
            GarbageCollectionWrapper[] collections, FTFrontier frontier, long startTicks)
        {
            this.checkpointer = checkpointer;
            this.epoch = epoch;
            this.collections = collections;
            this.frontier = frontier;
            this.startTicks = startTicks;
        }
    }

    /// <summary>
    /// class used for writing checkpoints and logs of the vertex
    /// </summary>
    /// <typeparam name="T">time type of the vertex</typeparam>
    public abstract class Checkpointer<T> : CheckpointState<T> where T : Time<T>
    {
        internal static Checkpointer<T> Create(Vertex<T> vertex, Func<string, IStreamSequence> streamSequenceFactory)
        {
            Checkpointer<T> checkpointer;

            switch (vertex.Stage.CheckpointType)
            {
                case CheckpointType.None:
                    throw new NotImplementedException();

                case CheckpointType.Logging:
                    checkpointer = new LoggingCheckpointer<T>(vertex, streamSequenceFactory);
                    break;

                case CheckpointType.Stateless:
                    checkpointer = new StatelessCheckpointer<T>(vertex, streamSequenceFactory, OutgoingMessageLogSelection.LogNever);
                    break;

                case CheckpointType.StatelessLogAll:
                    checkpointer = new StatelessCheckpointer<T>(vertex, streamSequenceFactory, OutgoingMessageLogSelection.LogAlways);
                    break;

                case CheckpointType.StatelessLogEphemeral:
                    checkpointer = new StatelessCheckpointer<T>(vertex, streamSequenceFactory, OutgoingMessageLogSelection.LogEphemeral);
                    break;

                case CheckpointType.Stateful:
                    checkpointer = new StatefulCheckpointer<T>(vertex, streamSequenceFactory, OutgoingMessageLogSelection.LogFuture);
                    break;

                case CheckpointType.StatefulLogAll:
                    checkpointer = new StatefulCheckpointer<T>(vertex, streamSequenceFactory, OutgoingMessageLogSelection.LogAlways);
                    break;

                case CheckpointType.StatefulLogEphemeral:
                    checkpointer = new StatefulCheckpointer<T>(vertex, streamSequenceFactory, OutgoingMessageLogSelection.LogEphemeral);
                    break;

                case CheckpointType.CachingInput:
                    checkpointer = new CachingInputCheckpointer<T>(vertex, streamSequenceFactory);
                    break;

                default:
                    throw new NotImplementedException();
            }

            checkpointer.checkpointWriter = checkpointer.CreateCheckpointWriter();

            return checkpointer;
        }

        internal long RecoveryEpoch { get; private set; }

        internal Vertex<T> vertex;
        internal Vertex Vertex { get { return this.vertex; } }

        internal readonly StreamSequenceManager<T> StreamManager;

        private readonly List<ILogger> loggers = new List<ILogger>();
        internal void AddLogger(ILogger logger)
        {
            this.loggers.Add(logger);
        }

        internal readonly Dictionary<int, OutgoingLogger<T>> outgoingLoggers = new Dictionary<int, OutgoingLogger<T>>();

        internal ICheckpointWriter<T> checkpointWriter = null;

        private Action<CheckpointUpdate> updateCheckpoint = null;

        internal IOutgoingMessageLogger<T, S, TMessage> CreateLogger<S, TMessage>(
            FullyTypedStageOutput<T, S, TMessage> stageOutput, IEnumerable<Cable<T, S, TMessage>> receivers)
            where TMessage : Time<TMessage>
        {
            OutgoingLogger<T, S, TMessage> logger = this.NewOutgoingLogger<S, TMessage>(stageOutput);

            this.discardedOutboundMessageBundles.Add(logger.DiscardedTimes);

            foreach (Cable<T, S, TMessage> cable in receivers)
            {
                logger.AttachToLogger(cable);
            }

            this.outgoingLoggers.Add(stageOutput.StageOutputIndex, logger);

            return logger.CreateMessageLogger(this.vertex.SerializationFormat);
        }

        internal abstract OutgoingLogger<T, S, TMessage> NewOutgoingLogger<S, TMessage>(FullyTypedStageOutput<T, S, TMessage> stageOutput)
            where TMessage : Time<TMessage>;

        internal abstract IMessageLogger<S, T> CreateIncomingMessageLogger<S>(
            int channelId, int senderStageId, Action<Message<S, T>, ReturnAddress> replayReceive, BufferPool<S> bufferPool);

        internal abstract IInputLogger<S, T> CreateInputLogger<S>(Action<T, S[]> replayInputAction);

        internal abstract INotificationLogger<T> CreateNotificationLogger(Action<T> replayNotificationAction);

        internal abstract void UpdateNotifier(Action<T> replayNotificationAction);

        internal abstract ICheckpointWriter<T> CreateCheckpointWriter();

        internal void RegisterReceivedMessageTime(int fromVertexId, int channelId, T time)
        {
            if (this.stage.CheckpointType != CheckpointType.StatelessLogEphemeral)
            {
                this.receivedMessageTimes[channelId].Second.Add(fromVertexId, time);
            }
            if (this.checkpointPolicy.CheckpointWhenPossible)
            {
                this.vertex.PushEventTime(time);
                this.vertex.NotifyAt(time);
                this.vertex.PopEventTime();
            }
        }

        internal void RegisterRequestedNotification(T baseTime, T requirement, T capability)
        {
            this.requestedNotifications.Add(baseTime, requirement, capability, 1);
        }

        internal void RegisterDeliveredNotificationTime(T time)
        {
            this.deliveredNotifications.Add(time, 1);
        }

        internal abstract void ReplayVertex();

        internal abstract void ResendLoggedMessages();

        internal void SendInstantaneousFaultToleranceFrontier()
        {
            if (!this.lastCheckpoint.Complete && !this.vertex.Stage.InternalComputation.Controller.HasFailed)
            {
                this.SendUpdate(new FTFrontier(true), true);
            }
        }

        private ICheckpointPolicy checkpointPolicy = null;

        internal void ConsiderCheckpointing(T completedTime)
        {
            if (this.stage.InternalComputation.IsRestoring)
            {
                return;
            }

            Pointstamp[] completedStamp = new Pointstamp[] { completedTime.ToPointstamp(this.vertex.Stage.StageId) };

            FTFrontier proposed = FTFrontier.FromPointstamps(completedStamp);
            if (!this.lastCheckpoint.Equals(proposed))
            {
                Pointstamp[] toCheckpoint = this.checkpointPolicy.ShouldCheckpointAt(completedStamp, true);
                if (toCheckpoint.Length > 0)
                {
                    this.Persist(FTFrontier.FromPointstamps(toCheckpoint));
                }
            }
        }

        internal void ConsiderCheckpointing(List<Pointstamp> reachablePointstamps)
        {
            if (this.stage.InternalComputation.IsRestoring)
            {
                return;
            }

            if (reachablePointstamps == null)
            {
                if (!this.lastCheckpoint.Equals(new FTFrontier(true)))
                {
                    // we are complete, so write a final checkpoint
                    this.Persist(new FTFrontier(true));
                }
                return;
            }

            Pointstamp minimumReachablePointstamp = new Pointstamp();
            for (int i = 0; i < reachablePointstamps.Count; i++)
            {
                if (i == 0 || FTFrontier.IsLessThanOrEqualTo(reachablePointstamps[i], minimumReachablePointstamp))
                {
                    minimumReachablePointstamp = reachablePointstamps[i];
                }
            }

            FTFrontier proposed = FTFrontier.SetBelow(minimumReachablePointstamp);
            if (!this.lastCheckpoint.Equals(proposed))
            {
                Pointstamp[] toCheckpoint = this.checkpointPolicy.ShouldCheckpointAt(
                    proposed.ToPointstamps<T>(this.stage.StageId), false);
                if (toCheckpoint.Length > 0)
                {
                    this.Persist(FTFrontier.FromPointstamps(toCheckpoint));
                }
            }
        }

        SortedDictionary<FTFrontier, bool> awaitingPersistence = new SortedDictionary<FTFrontier, bool>();

        internal void ReceivePersistenceNotification(long epoch, FTFrontier persisted)
        {
            // do nothing if it's a stale persistence
            if (epoch == this.RecoveryEpoch)
            {
                if (this.awaitingPersistence[persisted])
                {
                    throw new ApplicationException("Duplicate persistence notifications");
                }
                this.awaitingPersistence[persisted] = true;

                // only report that a checkpoint is persisted when all previous ones also are
                while (this.awaitingPersistence.Count > 0 && this.awaitingPersistence.First().Value)
                {
                    FTFrontier toSend = this.awaitingPersistence.First().Key;
                    this.SendUpdate(toSend, false);
                    this.awaitingPersistence.Remove(toSend);
                }
            }
        }

        internal void SendUpdate(FTFrontier highWatermark, bool temporary)
        {
            if (this.lastUpdateSent.Contains(highWatermark))
            {
                return;
            }

            //Console.WriteLine("Sending " + this.vertex + " " + highWatermark + " " + temporary + " " + this.lastUpdateSent);

            CheckpointUpdate update = new CheckpointUpdate
            {
                stageId = this.stage.StageId,
                vertexId = this.vertex.VertexId,
                frontier = highWatermark,
                isTemporary = temporary,
                notifications = this.deliveredNotifications
                    .Intersect(this.stage.StageId, highWatermark)
                    .Except(this.stage.StageId, this.lastUpdateSent)
                    .ToPointstamps(this.stage.StageId)
                    .ToArray(),
                deliveredMessages = this.receivedMessageTimes
                    .Select(c => c.Value.First.PairWith(
                        c.Value.Second.TimeBundle(this.stage.StageId, this.vertex.VertexId)
                            .SelectMany(b => b.Second
                                .Where(p => highWatermark.Contains(p) && !this.lastUpdateSent.Contains(p)))
                                .Distinct()
                                .ToArray()))
                    .ToArray(),
                discardedMessages = this.discardedOutboundMessageBundles
                    .SelectMany(t => t.Times(this.stage.StageId)
                        .Select(e => e.First.TargetStage.StageId.PairWith(e.Second
                            .Where(sendTime =>
                                highWatermark.Contains(sendTime.First) &&
                                !this.lastUpdateSent.Contains(sendTime.First))
                            .Select(sendTime => sendTime.First.PairWith(
                                sendTime.Second.TimeBundle(e.First.TargetStage.StageId, this.vertex.VertexId)
                                    .SelectMany(targetVertex => targetVertex.Second)
                                    .Distinct()
                                    .ToArray()))
                            .ToArray())))
                    .ToArray()
            };

            this.updateCheckpoint(update);

            if (!temporary)
            {
                this.lastUpdateSent = highWatermark;
            }
        }

        private long nextGarbageCollectionTime;

        private void Persist(FTFrontier upTo)
        {
            var stopwatch = this.vertex.scheduler.Controller.Stopwatch;
            SerializationFormat serializationFormat = this.vertex.SerializationFormat;
            NaiadSerialization<int> intSerializer = serializationFormat.GetSerializer<int>();
            NaiadSerialization<T> timeSerializer = serializationFormat.GetSerializer<T>();

            this.checkpointPolicy.SetLastCheckpoint(upTo.ToPointstamps<T>(this.stage.StageId));

            if (this.StreamManager.MetaData.NeedToBreak)
            {
                // checkpoint everything again
                this.lastCheckpoint = new FTFrontier(false);
                this.lastGarbageCollection = new FTFrontier(false);
            }

            if (!upTo.Contains(this.lastCheckpoint))
            {
                // the restoration hasn't finished replaying everything yet
                return;
            }

            if (upTo.Equals(this.lastCheckpoint))
            {
                // we don't need to checkpoint again
                return;
            }

            if (this.lastCheckpoint.Contains(upTo))
            {
                throw new ApplicationException("Out of order checkpoint");
            }

            long startTicks = stopwatch.ElapsedTicks;

            List<Task> toWait = new List<Task>();

            foreach (ILogger logger in this.loggers)
            {
                // make sure all the loggers have written out their buffered data
                logger.MarkFrontier(upTo);
                toWait.Add(logger.FlushAsync());
            }

            // write our state, if we are stateful
            toWait.Add(this.checkpointWriter.Checkpoint(upTo));

            int pageSize = 16 * 1024;
            while (true)
            {
                SendBufferPage sendPage = new SendBufferPage(GlobalBufferPool<byte>.pool, pageSize);
                sendPage.ReserveBytes(sizeof(int));

                // write out the frontier we are keeping data up to
                bool fits = upTo.TrySerialize(sendPage, serializationFormat);

                // write out the most recent garbage collection we did, so we can redo it on deserialization
                fits = lastGarbageCollection.TrySerialize(sendPage, serializationFormat);

                // write out the notifications requested since the last checkpoint
                fits = fits && this.requestedNotifications
                    .TrySerialize(this.stage.StageId, upTo, this.lastCheckpoint, sendPage, serializationFormat);

                // write out the notifications delivered since the last checkpoint
                fits = fits && this.deliveredNotifications
                    .Intersect(this.stage.StageId, upTo)
                    .Except(this.stage.StageId, this.lastCheckpoint)
                    .TrySerialize(sendPage, serializationFormat);

                // write out the times since the last checkpoint at which we have received incoming messages
                foreach (var channel in this.receivedMessageTimes.OrderBy(c => c.Key))
                {
                    fits = fits && channel.Value.Second.TrySerialize(this.stage.StageId, upTo, this.lastCheckpoint, sendPage, serializationFormat);
                }

                // for each outgoing channel, for each time since the last checkpoint, write out the times of
                // messages we have chosen not to log
                foreach (var stageOutput in this.discardedOutboundMessageBundles)
                {
                    fits = fits && stageOutput.TrySerialize(upTo, this.lastCheckpoint, this.vertex.Stage.StageId, sendPage, serializationFormat);
                }

                fits = fits && this.checkpointWriter.TrySerialize(upTo, this.lastCheckpoint, sendPage, serializationFormat);

                // for each logger, write out the metadata that will let it replay messages
                // if asked
                foreach (var logger in this.loggers)
                {
                    fits = fits && logger.TrySerialize(upTo, lastCheckpoint, sendPage, serializationFormat);
                }

                foreach (var outgoingLogger in this.outgoingLoggers.OrderBy(l => l.Key).Select(o => o.Value))
                {
                    fits = fits && outgoingLogger.TrySerialize(sendPage, serializationFormat);
                }

                // pad to a multiple of 512 bytes
                int paddedLength = (sendPage.producerPointer + 511) & (~511);
                int toPad = paddedLength - sendPage.producerPointer;
                byte[] padBuffer = new byte[512];
                fits = fits && (sendPage.WriteElements<byte>(
                                    serializationFormat.GetSerializer<byte>(),
                                    new ArraySegment<byte>(padBuffer, 0, toPad)) == toPad);

                if (fits)
                {
                    sendPage.WriteReserved(intSerializer, sendPage.producerPointer);
                    sendPage.FinalizeRawWrites();

                    BufferSegment segment = sendPage.Consume();
                    ArraySegment<byte> data = segment.ToArraySegment();

                    if ((data.Count & 511) != 0)
                    {
                        throw new ApplicationException("Failed to pad");
                    }

                    if (!this.checkpointPolicy.NoStatePersistence)
                    {
                        toWait.Add(this.StreamManager.MetaData.Write(data.Array, data.Offset, data.Count));
                    }

                    this.lastCheckpoint = upTo;

                    IEnumerable<GarbageCollectionWrapper> toCollect = new GarbageCollectionWrapper[0];
                    if (stopwatch.ElapsedMilliseconds > this.nextGarbageCollectionTime)
                    {
                        toCollect = this.StreamManager.MetaData.GarbageCollectNonCurrent();
                        foreach (ILogger logger in this.loggers)
                        {
                            toCollect = toCollect.Concat(logger.GarbageCollectStreams());
                        }
                        toCollect = toCollect.Concat(this.checkpointWriter.GarbageCollectStreams());
                        // check to garbage collect every two minutes
                        this.nextGarbageCollectionTime = stopwatch.ElapsedMilliseconds + 1000L * 60L * 2L;
                    }

                    long endTicks = stopwatch.ElapsedTicks;
                    long microSeconds = ((endTicks - startTicks) * 1000000L) / System.Diagnostics.Stopwatch.Frequency;
                    long totalMicroSeconds = (endTicks * 1000000L) / System.Diagnostics.Stopwatch.Frequency;
                    this.vertex.scheduler.WriteLogEntry("{0:D3}.{1:D3} M {2:D7} {3:D11} {4}",
                        this.vertex.Stage.StageId, this.vertex.VertexId, microSeconds, totalMicroSeconds, upTo);

                    Task toPersist = Task.WhenAll(toWait);
                    if (toPersist.IsCompleted)
                    {
                        this.SendUpdate(this.lastCheckpoint, false);
                    }
                    else
                    {
                        if (this.checkpointPolicy.NoStatePersistence)
                        {
                            throw new ApplicationException("No persistence but had to wait");
                        }

                        this.awaitingPersistence.Add(this.lastCheckpoint, false);

                        var persistedAction = new CheckpointPersistedAction<T>(
                            this, this.RecoveryEpoch, toCollect.ToArray(), this.lastCheckpoint, startTicks);

                        Task waitForPersistence = Task.WhenAll(toWait).ContinueWith(t =>
                        {
                            if (t.IsFaulted)
                            {
                                // we have to get the task onto a real thread to throw its exception
                                this.vertex.scheduler.NotifyCheckpointPersistenceFailed(t);
                            }
                            else
                            {
                                persistedAction.QueueTicks = stopwatch.ElapsedTicks;
                                this.vertex.scheduler.NotifyCheckpointPersisted(persistedAction);
                            }
                        });
                    }

                    return;
                }

                pageSize *= 2;
            }
        }

        internal void Initialize(CheckpointTracker tracker, bool deleteOldCheckpoints)
        {
            if (deleteOldCheckpoints)
            {
                this.StreamManager.MetaData.Initialize();
                foreach (ILogger logger in this.loggers)
                {
                    logger.Initialize();
                }
                this.checkpointWriter.Initialize();
            }
            else
            {
                this.TryDeserialize();
            }

            VertexLocation location = this.vertex.TypedStage.Placement.Where(p => p.VertexId == this.vertex.VertexId).Single();
            this.updateCheckpoint = tracker.GetUpdateReceiver(location);
        }

        internal void SetRetentionFrontier()
        {
            FTFrontier frontier = this.vertex.TypedStage.CurrentCheckpoint(this.vertex.VertexId).GetRestorationFrontier();

            if (!frontier.Complete)
            {
                // make sure stale any checkpoint persistence events get ignored
                this.RecoveryEpoch = System.DateTime.Now.Ticks;

                this.awaitingPersistence.Clear();

                // don't bother calling these if the frontier is complete since they don't do anything
                this.FilterMetaData(frontier);
                foreach (ILogger logger in this.loggers)
                {
                    logger.SetRetentionFrontier(frontier, true);
                }
                this.checkpointWriter.SetRetentionFrontier(frontier);
            }
        }

        internal void RepairProgress()
        {
            // logging checkpointers repair progress by replaying everything
            if (this.stage.CheckpointType != CheckpointType.Logging)
            {
                foreach (var requested in this.requestedNotifications.Requests())
                {
                    long deliveredCount = this.deliveredNotifications.GetCountIfAny(requested.Second.Second.First);
                    long outstandingCount = requested.Second.Second.Second - deliveredCount;
                    if (outstandingCount == 1)
                    {
                        Console.WriteLine(this.vertex + " replacing " + requested.Second.First);
                        this.vertex.PushEventTime(requested.First);
                        this.vertex.NotifyAt(requested.Second.First, requested.Second.Second.First);
                        this.vertex.PopEventTime();
                    }
                    else if (outstandingCount != 0)
                    {
                        throw new ApplicationException("Bad outstanding notification count " + outstandingCount);
                    }
                }

                foreach (var output in this.outgoingLoggers.Values)
                {
                    output.UpdateProgressForLoggedMessages();
                    output.MarkLoggedMessagesToSend();
                }
            }
        }

        internal Pair<bool, T> RepairInputProgress(ProgressUpdateProducer producer)
        {
            Pointstamp? maxSentStamp = null;
            foreach (var output in this.outgoingLoggers.Values)
            {
                Pointstamp? stamp = output.UpdateProgressForLoggedMessages();
                if (stamp.HasValue && 
                    (!maxSentStamp.HasValue || FTFrontier.IsLessThanOrEqualTo(maxSentStamp.Value, stamp.Value)))
                {
                    maxSentStamp = stamp.Value;
                }
                output.MarkLoggedMessagesToSend();
            }

            Pair<bool, T> maxSentTime = false.PairWith(default(T));
            if (maxSentStamp.HasValue)
            {
                maxSentTime = true.PairWith(default(T).InitializeFrom(maxSentStamp.Value, maxSentStamp.Value.Timestamp.Length));
            }

            return maxSentTime;
        }

        internal void RestoreToFrontier()
        {
            FTFrontier frontier = this.vertex.TypedStage.CurrentCheckpoint(this.vertex.VertexId).GetRestorationFrontier();

            if (!frontier.Complete)
            {
                // we don't have to do anything if we're restoring to a complete frontier
                this.checkpointWriter.RestoreFromCheckpoint(frontier);

                // start a new metadata file
                this.StreamManager.MetaData.StartNewStream();

                this.lastUpdateSent = this.lastUpdateSent.Intersect(frontier);

                this.Persist(frontier);
            }
        }

        private void ReceiveGCUpdate(int dstStageId, int dstVertexId, FTFrontier lowWatermark)
        {
            if (dstStageId == -1)
            {
                long startTicks = this.vertex.scheduler.Controller.Stopwatch.ElapsedTicks;

                if (this.GarbageCollectMetaData(lowWatermark))
                {
                    this.checkpointWriter.GarbageCollect(lowWatermark);
                }
                this.vertex.NotifyGarbageCollectionFrontier(lowWatermark.ToPointstamps<T>(this.stage.StageId));
                this.vertex.NotifyPotentialRollbackRange(
                    new FrontierCheckpointTester<T>(new FTFrontier(false), this.lastGarbageCollection, this.stage.StageId),
                    new FrontierCheckpointTester<T>(this.lastGarbageCollection, new FTFrontier(true), this.stage.StageId));

                long endTicks = this.vertex.scheduler.Controller.Stopwatch.ElapsedTicks;
                long totalMicroSeconds = (endTicks * 1000000L) / System.Diagnostics.Stopwatch.Frequency;
                long microSeconds = ((endTicks - startTicks) * 1000000L) / System.Diagnostics.Stopwatch.Frequency;
                this.vertex.scheduler.WriteLogEntry("{0:D3}.{1:D3} G {2:D7} {3:D11} {4}",
                    this.vertex.Stage.StageId, this.vertex.VertexId, microSeconds, totalMicroSeconds, lowWatermark);
            }
            else
            {
                foreach (var logger in this.outgoingLoggers.Values)
                {
                    logger.UpdateReceiverFrontier(dstStageId, dstVertexId, lowWatermark.ProjectBasic(this.stage.StageId));
                }
            }
        }

        internal bool TryDeserialize()
        {
            bool success = true;
            foreach (RecvBuffer fixedBuffer in this.StreamManager.MetaData.Reinitialize(this.vertex.SerializationFormat))
            {
                RecvBuffer buffer = fixedBuffer;
                success = success && this.TryDeserialize(ref buffer, this.vertex.SerializationFormat);

                success = success && this.checkpointWriter.TryDeserialize(ref buffer, this.vertex.SerializationFormat);

                foreach (ILogger logger in this.loggers)
                {
                    success = success && logger.TryDeserialize(ref buffer, this.vertex.SerializationFormat);
                }

                foreach (var outgoingLogger in this.outgoingLoggers.OrderBy(l => l.Key).Select(o => o.Value))
                {
                    success = success && outgoingLogger.TryDeserialize(ref buffer, this.vertex.SerializationFormat);
                }
            }

            // clean up any metadata that was read from an incremental checkpoint but should have been collected
            if (!this.lastGarbageCollection.Empty)
            {
                this.ReceiveGCUpdate(-1, -1, this.lastGarbageCollection);
            }
            foreach (var outgoingLogger in this.outgoingLoggers.Values)
            {
                outgoingLogger.RefreshGarbageCollection();
            }

            return success;
        }

        internal void ShutDown()
        {
            this.Persist(new FTFrontier(true));

            foreach (ILogger logger in this.loggers)
            {
                logger.Shutdown();
            }

            this.loggers.Clear();

            this.StreamManager.Shutdown();
        }

        internal void UpdateVertexForRollback(Vertex<T> newVertex)
        {
            this.vertex = newVertex;
        }

        internal Checkpointer(Vertex<T> vertex, Func<string, IStreamSequence> streamSequenceFactory) : base(vertex.TypedStage, false)
        {
            this.RecoveryEpoch = System.DateTime.Now.Ticks;

            this.vertex = vertex;

            this.StreamManager = new StreamSequenceManager<T>(streamSequenceFactory, this.vertex.VertexId, this.vertex.Stage.StageId);
            this.vertex.Stage.InternalComputation.CheckpointTracker.RegisterCheckpointer(
                this.stage.StageId, this.vertex.VertexId, this.ReceiveGCUpdate);

            if (this.vertex.Stage.CheckpointPolicyFactory == null)
            {
                this.checkpointPolicy = new CheckpointByTime(this.vertex.Stage.Computation.Controller.Configuration.DefaultCheckpointInterval);
            }
            else
            {
                this.checkpointPolicy = this.vertex.Stage.CheckpointPolicyFactory(this.vertex.VertexId);
            }
        }
    }

    /// <summary>
    /// Checkpointer for a stage that logs all incoming messages and notifications, so can be replayed to any time, but cannot
    /// garbage collect any state: the log, and recovery time, grows without bound. This is the default checkpointer since it
    /// does not require the stage/vertex to implement any checkpointing code
    /// </summary>
    /// <typeparam name="T">time type of the stage</typeparam>
    public class LoggingCheckpointer<T> : Checkpointer<T> where T : Time<T>
    {
        private Logger<T> eventLogger = null;

        private Logger<T> Logger { get { return this.eventLogger; } }

        internal override OutgoingLogger<T, S, TMessage> NewOutgoingLogger<S, TMessage>(FullyTypedStageOutput<T, S, TMessage> stageOutput)
        {
            return new NullOutgoingLogger<T, S, TMessage>(this, stageOutput);
        }

        internal override IInputLogger<S, T> CreateInputLogger<S>(Action<T, S[]> replayInputAction)
        {
            var logger = new DummyInputLogger<S, T>();
            return logger;
        }

        internal override IMessageLogger<S, T> CreateIncomingMessageLogger<S>(
            int channelId, int senderStageId, Action<Message<S, T>, ReturnAddress> replayReceive, BufferPool<S> bufferPool)
        {
            this.Logger.AddMessageReader(channelId,
                new LogMessageReader<S, T, T>(
                    senderStageId, replayReceive, () => { }, k => k, (t, i) => true, this.vertex.SerializationFormat.GetSerializer<S>(), bufferPool));
            return new MessageLogger<S, T>(this, channelId, this.Logger);
        }

        internal override INotificationLogger<T> CreateNotificationLogger(Action<T> replayNotificationAction)
        {
            this.Logger.AddNotifier(replayNotificationAction);
            return new NotificationLogger<T>(this, this.Logger);
        }

        internal override void UpdateNotifier(Action<T> replayNotificationAction)
        {
            this.Logger.UpdateNotifier(replayNotificationAction);
        }

        internal override ICheckpointWriter<T> CreateCheckpointWriter()
        {
            return new NullCheckpointWriter<T>(this);
        }

        internal override void ReplayVertex()
        {
            Console.WriteLine("Replaying vertex");
            // make sure we have written out any pending stuff
            this.Logger.FlushAsync().Wait();
            Console.WriteLine("Replaying vertex log");
            this.Logger.ReplayLog(this.Logger.GetCurrentTimeExtents());
        }

        internal override void ResendLoggedMessages()
        {
        }

        /// <summary>
        /// Create a new logging checkpointer
        /// </summary>
        /// <param name="vertex">vertex being checkpointed</param>
        /// <param name="streamSequenceFactory">a factory to create streams for logging</param>
        public LoggingCheckpointer(Vertex<T> vertex, Func<string, IStreamSequence> streamSequenceFactory)
            : base(vertex, streamSequenceFactory)
        {
            this.eventLogger = new Logger<T>(this.vertex.Stage.StageId, this.StreamManager.EventLog,
                vertex.SerializationFormat, (f,e) => this.vertex.scheduler.WriteLogEntry(f, e));
            this.AddLogger(this.eventLogger);
        }
    }

    /// <summary>
    /// Checkpointer for stateless stages that do not log any incoming messages or notifications.
    /// </summary>
    /// <typeparam name="T">time type of the stage</typeparam>
    internal class StatelessCheckpointer<T> : Checkpointer<T> where T : Time<T>
    {
        private readonly OutgoingMessageLogSelection logSelection;

        internal override IMessageLogger<S, T> CreateIncomingMessageLogger<S>(
            int channelId, int senderStageId, Action<Message<S, T>, ReturnAddress> replayReceive, BufferPool<S> bufferPool)
        {
            return new DummyMessageLogger<S, T>(this, channelId);
        }

        internal override IInputLogger<S, T> CreateInputLogger<S>(Action<T, S[]> replayInputAction)
        {
            var logger = new DummyInputLogger<S, T>();
            return logger;
        }

        internal override INotificationLogger<T> CreateNotificationLogger(Action<T> replayNotificationAction)
        {
            return new DummyNotificationLogger<T>(this);
        }

        internal override void UpdateNotifier(Action<T> replayNotificationAction)
        {
        }

        internal override ICheckpointWriter<T> CreateCheckpointWriter()
        {
            return new NullCheckpointWriter<T>(this);
        }

        internal override OutgoingLogger<T, S, TMessage> NewOutgoingLogger<S, TMessage>(FullyTypedStageOutput<T, S, TMessage> stageOutput)
        {
            if (this.logSelection == OutgoingMessageLogSelection.LogEphemeral)
            {
                return new NullOutgoingLogger<T, S, TMessage>(this, stageOutput);
            }
            else
            {
                return new SelectiveOutgoingLogger<T, S, TMessage>(this, stageOutput, this.logSelection);
            }
        }

        internal override void ReplayVertex()
        {
        }

        internal override void ResendLoggedMessages()
        {
            foreach (var output in this.outgoingLoggers.Values)
            {
                output.ResendLoggedMessages();
            }
        }

        /// <summary>
        /// create a new stateless checkpointer
        /// </summary>
        /// <param name="vertex">vertex to checkpoint</param>
        /// <param name="streamSequenceFactory">a factory to create streams for logging</param>
        /// <param name="logSelection">type of logging to use for outgoing messages</param>
        public StatelessCheckpointer(Vertex<T> vertex, Func<string, IStreamSequence> streamSequenceFactory, OutgoingMessageLogSelection logSelection)
            : base(vertex, streamSequenceFactory)
        {
            this.logSelection = logSelection;
        }
    }

    /// <summary>
    /// Checkpointer for stateful stages that do not log any incoming messages or notifications.
    /// </summary>
    /// <typeparam name="T">time type of the stage</typeparam>
    internal class StatefulCheckpointer<T> : Checkpointer<T> where T : Time<T>
    {
        private readonly OutgoingMessageLogSelection logSelection;

        internal override IMessageLogger<S, T> CreateIncomingMessageLogger<S>(
            int channelId, int senderStageId, Action<Message<S, T>, ReturnAddress> replayReceive, BufferPool<S> bufferPool)
        {
            return new DummyMessageLogger<S, T>(this, channelId);
        }

        internal override IInputLogger<S, T> CreateInputLogger<S>(Action<T, S[]> replayInputAction)
        {
            var logger = new DummyInputLogger<S, T>();
            return logger;
        }

        internal override INotificationLogger<T> CreateNotificationLogger(Action<T> replayNotificationAction)
        {
            return new DummyNotificationLogger<T>(this);
        }

        internal override void UpdateNotifier(Action<T> replayNotificationAction)
        {
        }

        internal override ICheckpointWriter<T> CreateCheckpointWriter()
        {
            return new CheckpointWriter<T>(this);
        }

        internal override OutgoingLogger<T, S, TMessage> NewOutgoingLogger<S, TMessage>(FullyTypedStageOutput<T, S, TMessage> stageOutput)
        {
            if (this.logSelection == OutgoingMessageLogSelection.LogEphemeral)
            {
                return new NullOutgoingLogger<T, S, TMessage>(this, stageOutput);
            }
            else
            {
                return new SelectiveOutgoingLogger<T, S, TMessage>(this, stageOutput, this.logSelection);
            }
        }

        internal override void ReplayVertex()
        {
        }

        internal override void ResendLoggedMessages()
        {
            foreach (var output in this.outgoingLoggers.Values)
            {
                output.ResendLoggedMessages();
            }
        }

        /// <summary>
        /// create a new stateless checkpointer
        /// </summary>
        /// <param name="vertex">vertex to checkpoint</param>
        /// <param name="streamSequenceFactory">a factory to create streams for logging</param>
        /// <param name="logSelection">type of logging to use for outgoing messages</param>
        public StatefulCheckpointer(Vertex<T> vertex, Func<string, IStreamSequence> streamSequenceFactory, OutgoingMessageLogSelection logSelection)
            : base(vertex, streamSequenceFactory)
        {
            this.logSelection = logSelection;
        }
    }

    /// <summary>
    /// Checkpointer for input stages that cache inputs in memory until they can be garbage-collected
    /// </summary>
    /// <typeparam name="T">time type of the stage</typeparam>
    internal class CachingInputCheckpointer<T> : Checkpointer<T> where T : Time<T>
    {
        private InputCacheCheckpointWriter<T> inputReplayer;

        internal override IMessageLogger<S, T> CreateIncomingMessageLogger<S>(
            int channelId, int senderStageId, Action<Message<S, T>, ReturnAddress> replayReceive, BufferPool<S> bufferPool)
        {
            return new DummyMessageLogger<S, T>(this, channelId);
        }

        internal override IInputLogger<S, T> CreateInputLogger<S>(Action<T, S[]> replayInputAction)
        {
            InputCacheCheckpointWriter<S, T> inputCheckpointWriter = new InputCacheCheckpointWriter<S, T>(this);
            this.checkpointWriter = inputCheckpointWriter;
            this.inputReplayer = inputCheckpointWriter;
            inputCheckpointWriter.UpdateReplayAction(replayInputAction);
            return inputCheckpointWriter;
        }

        internal override INotificationLogger<T> CreateNotificationLogger(Action<T> replayNotificationAction)
        {
            return new DummyNotificationLogger<T>(this);
        }

        internal override void UpdateNotifier(Action<T> replayNotificationAction)
        {
        }

        internal override ICheckpointWriter<T> CreateCheckpointWriter()
        {
            // this will get created with the right record type in a call to CreateInputLogger later
            return null;
        }

        internal override OutgoingLogger<T, S, TMessage> NewOutgoingLogger<S, TMessage>(FullyTypedStageOutput<T, S, TMessage> stageOutput)
        {
            return new SelectiveOutgoingLogger<T, S, TMessage>(this, stageOutput, OutgoingMessageLogSelection.LogNever);
        }

        internal override void ReplayVertex()
        {
            this.inputReplayer.ReplayInputs();
        }

        internal override void ResendLoggedMessages()
        {
        }

        /// <summary>
        /// create a new stateless checkpointer
        /// </summary>
        /// <param name="vertex">vertex to checkpoint</param>
        /// <param name="streamSequenceFactory">a factory to create streams for logging</param>
        public CachingInputCheckpointer(Vertex<T> vertex, Func<string, IStreamSequence> streamSequenceFactory)
            : base(vertex, streamSequenceFactory)
        {
        }
    }

    /// <summary>
    /// An interface that lets a vertex decide when to checkpoint
    /// </summary>
    public interface ICheckpointPolicy
    {
        /// <summary>
        /// Called when the system might checkpoint a vertex
        /// </summary>
        /// <param name="newFrontier">the new complete frontier</param>
        /// <param name="isEagerCompletion">true if this is called eagerly from a notification</param>
        /// <returns>the frontier the system should checkpoint to, or an empty list if it should not checkpoint</returns>
        Pointstamp[] ShouldCheckpointAt(Pointstamp[] newFrontier, bool isEagerCompletion);
        /// <summary>
        /// Called when the system takes a new checkpoint or rolls back to an old one
        /// </summary>
        /// <param name="lastFrontier">checkpoint that was made</param>
        void SetLastCheckpoint(Pointstamp[] lastFrontier);
        /// <summary>
        /// If this is true then all vertices in the stage try to checkpoint at the end of every time
        /// </summary>
        bool CheckpointWhenPossible { get; }
        /// <summary>
        /// If this is true then checkpoints are announced to the monitor but not persisted. This should not
        /// be used if any messages are being logged or state is being saved
        /// </summary>
        bool NoStatePersistence { get; }
    }

    /// <summary>
    /// A checkpoint policy that makes a checkpoint every so often according to wall clock time
    /// </summary>
    public class CheckpointByTime : ICheckpointPolicy
    {
        private readonly int checkpointPeriod;
        private readonly System.Diagnostics.Stopwatch timer;
        private long lastCheckpointTime;

        /// <summary>
        /// Called when the system might checkpoint a vertex.
        /// </summary>
        /// <param name="newFrontier">the new complete frontier</param>
        /// <param name="isEagerCompletion">true if this is called eagerly from a notification</param>
        /// <returns>the frontier that was passed in if enough time has elapsed since the last checkpoint, otherwise an empty list</returns>
        public Pointstamp[] ShouldCheckpointAt(Pointstamp[] newFrontier, bool isEagerCompletion)
        {
            if (timer.ElapsedMilliseconds > this.lastCheckpointTime + this.checkpointPeriod)
            {
                return newFrontier;
            }
            else
            {
                return new Pointstamp[0];
            }
        }

        /// <summary>
        /// Called when the system takes a new checkpoint or rolls back to an old one
        /// </summary>
        /// <param name="lastFrontier">checkpoint that was made</param>
        public void SetLastCheckpoint(Pointstamp[] lastFrontier)
        {
            this.lastCheckpointTime = this.timer.ElapsedMilliseconds;
        }

        /// <summary>
        /// If this is true then all vertices in the stage try to checkpoint at the end of every time
        /// </summary>
        public bool CheckpointWhenPossible { get { return false; } }

        /// <summary>
        /// If this is true then checkpoints are announced to the monitor but not persisted. This should not
        /// be used if any messages are being logged or state is being saved
        /// </summary>
        public bool NoStatePersistence { get { return false; } }

        /// <summary>
        /// Create a new policy that checkpoints based on elapsed time
        /// </summary>
        /// <param name="checkpointPeriodInMilliseconds">number of milliseconds between checkpoints</param>
        public CheckpointByTime(int checkpointPeriodInMilliseconds)
        {
            this.checkpointPeriod = checkpointPeriodInMilliseconds;
            this.timer = new System.Diagnostics.Stopwatch();
            this.timer.Start();
            this.lastCheckpointTime = 0;
        }
    }

    /// <summary>
    /// A checkpoint policy that makes a new checkpoint every epoch
    /// </summary>
    public class CheckpointAtEpoch : ICheckpointPolicy
    {
        private int lastCheckpointEpoch;

        /// <summary>
        /// Called when the system might checkpoint a vertex.
        /// </summary>
        /// <param name="newFrontier">the new complete frontier</param>
        /// <param name="isEagerCompletion">true if this is called eagerly from a notification</param>
        /// <returns>the beginning of the epoch of the frontier that was passed in if it is a new epoch since the last checkpoint, otherwise an empty list</returns>
        public Pointstamp[] ShouldCheckpointAt(Pointstamp[] newFrontier, bool isEagerCompletion)
        {
            if (newFrontier.Length != 1)
            {
                throw new ApplicationException("Unexpected frontier");
            }

            if (newFrontier[0].Timestamp[0] != lastCheckpointEpoch)
            {
                if (newFrontier[0].Timestamp[0] < lastCheckpointEpoch)
                {
                    throw new ApplicationException("Epochs out of order");
                }

                // checkpoint at the first time in the frontier regardless of when we got called
                for (int i=1; i<newFrontier[0].Timestamp.Length; ++i)
                {
                    newFrontier[i].Timestamp[i] = 0;
                }

                return newFrontier;
            }
            else
            {
                return new Pointstamp[0];
            }
        }

        /// <summary>
        /// Called when the system takes a new checkpoint or rolls back to an old one
        /// </summary>
        /// <param name="lastFrontier">checkpoint that was made</param>
        public void SetLastCheckpoint(Pointstamp[] lastFrontier)
        {
            if (lastFrontier.Length == 0)
            {
                this.lastCheckpointEpoch = -1;
            }
            else
            {
                if (lastFrontier.Length != 1)
                {
                    throw new ApplicationException("Unexpected frontier");
                }

                this.lastCheckpointEpoch = lastFrontier[0].Timestamp[0];
            }
        }

        /// <summary>
        /// If this is true then all vertices in the stage try to checkpoint at the end of every time
        /// </summary>
        public bool CheckpointWhenPossible { get { return false; } }

        /// <summary>
        /// If this is true then checkpoints are announced to the monitor but not persisted. This should not
        /// be used if any messages are being logged or state is being saved
        /// </summary>
        public bool NoStatePersistence { get { return false; } }

        /// <summary>
        /// Create a new policy that checkpoints every new epoch
        /// </summary>
        public CheckpointAtEpoch()
        {
            this.lastCheckpointEpoch = -1;
        }
    }

    /// <summary>
    /// A checkpoint policy that makes a new checkpoint whenever it can
    /// </summary>
    public class CheckpointEagerly : ICheckpointPolicy
    {
        /// <summary>
        /// Called when the system might checkpoint a vertex.
        /// </summary>
        /// <param name="newFrontier">the new complete frontier</param>
        /// <param name="isEagerCompletion">true if this is called eagerly from a notification</param>
        /// <returns>the frontier that was passed in</returns>
        public Pointstamp[] ShouldCheckpointAt(Pointstamp[] newFrontier, bool isEagerCompletion)
        {
            return newFrontier;
        }

        /// <summary>
        /// Called when the system takes a new checkpoint or rolls back to an old one
        /// </summary>
        /// <param name="lastFrontier">checkpoint that was made</param>
        public void SetLastCheckpoint(Pointstamp[] lastFrontier)
        {
        }

        /// <summary>
        /// If this is true then all vertices in the stage try to checkpoint at the end of every time
        /// </summary>
        public bool CheckpointWhenPossible { get { return true; } }

        /// <summary>
        /// If this is true then checkpoints are announced to the monitor but not persisted. This should not
        /// be used if any messages are being logged or state is being saved
        /// </summary>
        public bool NoStatePersistence { get { return false; } }

        /// <summary>
        /// Create a new policy that checkpoints whenever it can
        /// </summary>
        public CheckpointEagerly()
        {
        }
    }

    /// <summary>
    /// A checkpoint policy that makes a new checkpoint whenever it can and does not persist any state
    /// </summary>
    public class CheckpointWithoutPersistence : ICheckpointPolicy
    {
        /// <summary>
        /// Called when the system might checkpoint a vertex.
        /// </summary>
        /// <param name="newFrontier">the new complete frontier</param>
        /// <param name="isEagerCompletion">true if this is called eagerly from a notification</param>
        /// <returns>the frontier that was passed in</returns>
        public Pointstamp[] ShouldCheckpointAt(Pointstamp[] newFrontier, bool isEagerCompletion)
        {
            return newFrontier;
        }

        /// <summary>
        /// Called when the system takes a new checkpoint or rolls back to an old one
        /// </summary>
        /// <param name="lastFrontier">checkpoint that was made</param>
        public void SetLastCheckpoint(Pointstamp[] lastFrontier)
        {
        }

        /// <summary>
        /// If this is true then all vertices in the stage try to checkpoint at the end of every time
        /// </summary>
        public bool CheckpointWhenPossible { get { return true; } }

        /// <summary>
        /// If this is true then checkpoints are announced to the monitor but not persisted. This should not
        /// be used if any messages are being logged or state is being saved
        /// </summary>
        public bool NoStatePersistence { get { return true; } }

        /// <summary>
        /// Create a new policy that checkpoints whenever it can
        /// </summary>
        public CheckpointWithoutPersistence()
        {
        }
    }

    /// <summary>
    /// Interface used when checkpointing state
    /// </summary>
    /// <typeparam name="T">time type</typeparam>
    public interface ICheckpoint<T> : IEquatable<ICheckpoint<T>> where T : Time<T>
    {
        /// <summary>
        /// True if this is a checkpoint of the full state, false if it is an incremental checkpoint
        /// </summary>
        bool IsFullCheckpoint { get; }

        /// <summary>
        /// Returns true if the checkpoint being written should contain time <paramref name="time"/>
        /// </summary>
        /// <param name="time">time that should be checked</param>
        /// <returns>true if the checkpoint should contain <paramref name="time"/></returns>
        bool ContainsTime(T time);
    }

    /// <summary>
    /// Class to encapsulate a set of times
    /// </summary>
    /// <typeparam name="T"></typeparam>
    public class FrontierCheckpointTester<T> : ICheckpoint<T> where T : Time<T>
    {
        private readonly FTFrontier previousIncremental;
        private readonly FTFrontier frontier;
        private readonly int stageId;

        /// <summary>
        /// create a checkpoint that contains all times inside a minimal antichain of pointstamps
        /// </summary>
        /// <param name="frontier">the minimal antichain</param>
        /// <returns>a checkpoint containing times within the frontier</returns>
        public static ICheckpoint<T> CreateDownwardClosed(Pointstamp[] frontier)
        {
            if (frontier.Length == 0)
            {
                // we can use a dummy stageId since the tests will ignore it
                return new FrontierCheckpointTester<T>(new FTFrontier(false), new FTFrontier(false), -1);
            }
            else
            {
                // assume all the pointstamps have the same location
                return new FrontierCheckpointTester<T>(
                    new FTFrontier(false), FTFrontier.FromPointstamps(frontier), frontier.First().Location);
            }
        }

        /// <summary>
        /// create an empty checkpoint
        /// </summary>
        /// <returns>an empty checkpoint</returns>
        public static ICheckpoint<T> CreateEmpty()
        {
            return new FrontierCheckpointTester<T>(new FTFrontier(false), new FTFrontier(false), -1);
        }

        /// <summary>
        /// True if this is a checkpoint of the full state, false if it is an incremental checkpoint
        /// </summary>
        public bool IsFullCheckpoint { get { return this.previousIncremental.Empty; } }

        /// <summary>
        /// Returns true if the checkpoint being written should contain time <paramref name="time"/>
        /// </summary>
        /// <param name="time">time that should be checked</param>
        /// <returns>true if the checkpoint should contain <paramref name="time"/></returns>
        public bool ContainsTime(T time)
        {
            Pointstamp stamp = time.ToPointstamp(this.stageId);
            return frontier.Contains(stamp) && !previousIncremental.Contains(stamp);
        }

        /// <summary>
        /// True if two checkpoints contain the same times
        /// </summary>
        /// <param name="other">checkpoint to compare</param>
        /// <returns>true if the checkpoints contain the same times</returns>
        public bool Equals(ICheckpoint<T> other)
        {
            if (other is FrontierCheckpointTester<T>)
            {
                FrontierCheckpointTester<T> typed = other as FrontierCheckpointTester<T>;
                return
                    this.previousIncremental.Equals(typed.previousIncremental) &&
                    this.frontier.Equals(typed.frontier);
            }
            else
            {
                return false;
            }
        }

        internal FrontierCheckpointTester(FTFrontier previousIncremental, FTFrontier frontier, int stageId)
        {
            this.previousIncremental = previousIncremental;
            this.frontier = frontier;
            this.stageId = stageId;
        }
    }

    internal abstract class Checkpoint<T> where T : Time<T>
    {
        public abstract bool RestorationContainsTime(T time);
        public abstract Func<TReceiverTime, int, bool> ShouldSendFunc<TReceiverTime>(
            Stage<TReceiverTime> receiverStage, DiscardManager discardManager)
            where TReceiverTime : Time<TReceiverTime>;
        public abstract FTFrontier GetRestorationFrontier();
    }

    internal class NullCheckpoint<T> : Checkpoint<T> where T : Time<T>
    {
        public override bool RestorationContainsTime(T time)
        {
            return true;
        }

        public override Func<TReceiverTime, int, bool> ShouldSendFunc<TReceiverTime>(
            Stage<TReceiverTime> receiverStage, DiscardManager discardManager)
        {
            return (t, v) => true;
        }

        public override FTFrontier GetRestorationFrontier()
        {
            return new FTFrontier(true);
        }
    }

    internal class FrontierCheckpoint<T> : Checkpoint<T> where T : Time<T>
    {
        private readonly Stage<T> stage;
        private FTFrontier restorationFrontier;

        public override bool RestorationContainsTime(T time)
        {
            return this.restorationFrontier.Contains(time.ToPointstamp(this.stage.StageId));
        }

        public override Func<TReceiverTime, int, bool> ShouldSendFunc<TReceiverTime>(
            Stage<TReceiverTime> receiverStage, DiscardManager discardManager)
        {
            int receiverStageId = receiverStage.StageId;

            FTFrontier projected = this.GetRestorationFrontier().Project(this.stage, receiverStageId);
            Dictionary<int, FTFrontier[]> discardFrontiers = discardManager.DiscardFrontiers;

            return (t, v) =>
                {
                    Pointstamp p = t.ToPointstamp(receiverStageId);
                    if (!projected.Contains(p))
                    {
                        return true;
                    }
                    else
                    {
                        FTFrontier[] frontiers = null;
                        if (discardFrontiers.TryGetValue(receiverStageId, out frontiers) == false)
                        {
                            return true;
                        }

                        return !frontiers[v].Contains(p);
                    }
                };
        }

        public override FTFrontier GetRestorationFrontier()
        {
            return this.restorationFrontier;
        }

        internal FrontierCheckpoint(Stage<T> stage, FTFrontier restorationFrontier)
        {
            this.stage = stage;
            this.restorationFrontier = restorationFrontier;
        }

        internal FrontierCheckpoint(Stage<T> stage)
        {
            this.stage = stage;
            this.restorationFrontier = new FTFrontier(true);
        }
    }

    internal interface ICheckpointCollection
    {
        IEnumerable<FTFrontier> GetAvailableFrontiers();
        FTFrontier GetRestorationFrontier();
        FTFrontier ReduceRestorationFrontierBelow(FTFrontier target);
        FTFrontier GetNotificationFrontier();
        void SetNotificationFrontier(FTFrontier newFrontier);
        TimeSet DeliveredNotificationTimes();
        TimeSet DeliveredMessageTimes(int channelId, int senderVertexId);
        IEnumerable<Pair<Pointstamp, TimeSet>> DiscardedMessageTimes(int channelId, int receiverStageId, int receiverVertexId);
        bool DiscardedMessageTimesNeededAtReceiver(FTFrontier receiverFrontier, int channelId, int receiverStageId, int receiverVertexId);
        FTFrontier ReduceRestorationFrontierForDiscardedMessages(FTFrontier receiverFrontier, int channelId, int receiverStageId, int receiverVertexId);
        void InstallCurrentCheckpoint();
    }

    internal abstract class CheckpointCollection<T> : CheckpointState<T>, ICheckpointCollection where T : Time<T>
    {
        internal readonly SerializationFormat SerializationFormat;

        internal readonly StreamSequenceManager<T> StreamManager;
        internal readonly int vertexId;

        public abstract IEnumerable<FTFrontier> GetAvailableFrontiers();
        public abstract FTFrontier GetRestorationFrontier();
        public abstract FTFrontier ReduceRestorationFrontierBelow(FTFrontier target);
        public abstract FTFrontier GetNotificationFrontier();
        public abstract void SetNotificationFrontier(FTFrontier newFrontier);
        public abstract TimeSet DeliveredNotificationTimes();
        public abstract TimeSet DeliveredMessageTimes(int channelId, int senderVertexId);
        public abstract IEnumerable<Pair<Pointstamp, TimeSet>> DiscardedMessageTimes(int channelId, int receiverStageId, int receiverVertexId);
        public abstract bool DiscardedMessageTimesNeededAtReceiver(FTFrontier receiverFrontier, int channelId, int receiverStageId, int receiverVertexId);
        public abstract FTFrontier ReduceRestorationFrontierForDiscardedMessages(FTFrontier receiverFrontier, int channelId, int receiverStageId, int receiverVertexId);
        public abstract void InstallCurrentCheckpoint();
        internal abstract void Initialize();

        public static CheckpointCollection<T> InitializeFrom(Func<string, IStreamSequence> streamSequenceFactory, Stage<T> stage, int vertexId)
        {
            CheckpointCollection<T> collection = null;

            switch (stage.CheckpointType)
            {
                case CheckpointType.None:
                    collection = new NullCheckpointCollection<T>(streamSequenceFactory, stage, vertexId);
                    break;

                case CheckpointType.Logging:
                case CheckpointType.Stateless:
                case CheckpointType.StatelessLogAll:
                case CheckpointType.StatelessLogEphemeral:
                case CheckpointType.CachingInput:
                    collection = new StatelessCheckpointCollection<T>(streamSequenceFactory, stage, vertexId);
                    break;

                case CheckpointType.Stateful:
                case CheckpointType.StatefulLogAll:
                case CheckpointType.StatefulLogEphemeral:
                    collection = new PersistedCheckpointCollection<T>(streamSequenceFactory, stage, vertexId);
                    break;

                default:
                    throw new ApplicationException("Unknown checkpoint type");
            }

            collection.Initialize();

            return collection;
        }

        public CheckpointCollection(Func<string, IStreamSequence> streamSequenceFactory, Stage<T> stage, int vertexId)
            : base(stage, true)
        {
            this.SerializationFormat = stage.InternalComputation.Controller.SerializationFormat;
            this.vertexId = vertexId;
            this.StreamManager = new StreamSequenceManager<T>(streamSequenceFactory, vertexId, stage.StageId);
        }
    }

    internal class NullCheckpointCollection<T> : CheckpointCollection<T> where T : Time<T>
    {
        public override IEnumerable<FTFrontier> GetAvailableFrontiers()
        {
            return new FTFrontier[] { new FTFrontier(true) };
        }

        public override FTFrontier GetRestorationFrontier()
        {
            return new FTFrontier(true);
        }

        public override FTFrontier ReduceRestorationFrontierBelow(FTFrontier target)
        {
            throw new NotImplementedException();
        }

        public override FTFrontier GetNotificationFrontier()
        {
            return new FTFrontier(false);
        }

        public override void SetNotificationFrontier(FTFrontier newFrontier)
        {
            throw new NotImplementedException();
        }

        public override TimeSet DeliveredNotificationTimes()
        {
            return new TimeSet<Empty>(-1);
        }

        public override TimeSet DeliveredMessageTimes(int channelId, int receiverVertexId)
        {
            return new TimeSet<T>(this.stage.StageId);
        }

        public override IEnumerable<Pair<Pointstamp, TimeSet>> DiscardedMessageTimes(int channelId, int receiverStageId, int receiverVertexId)
        {
            return new Pair<Pointstamp, TimeSet>[0];
        }

        public override bool DiscardedMessageTimesNeededAtReceiver(FTFrontier receiverFrontier, int channelId, int receiverStageId, int receiverVertexId)
        {
            return false;
        }

        public override FTFrontier ReduceRestorationFrontierForDiscardedMessages(FTFrontier receiverFrontier, int channelId, int receiverStageId, int receiverVertexId)
        {
            throw new NotImplementedException();
        }

        public override void InstallCurrentCheckpoint()
        {
        }

        internal override void Initialize()
        {
        }

        public NullCheckpointCollection(Func<string, IStreamSequence> streamSequenceFactory, Stage<T> stage, int vertexId)
            : base(streamSequenceFactory, stage, vertexId)
        {
        }
    }

    internal class StatelessCheckpointCollection<T> : CheckpointCollection<T> where T : Time<T>
    {
        private FTFrontier notificationFrontier;
        private FTFrontier restorationFrontier;

        public override bool DiscardedMessageTimesNeededAtReceiver(FTFrontier receiverFrontier, int channelId, int receiverStageId, int receiverVertexId)
        {
            FTFrontier senderFrontier = this.restorationFrontier;
            foreach (TimeSetBundle filteredTimes in this.OutgoingDiscardedMessages(channelId)
                .Where(t => senderFrontier.Contains(t.Key.ToPointstamp(this.stage.StageId)))
                .Select(t => t.Value))
            {
                if (!filteredTimes.TimeSet(receiverVertexId).Projected(receiverStageId).ContainedIn(receiverFrontier))
                {
                    return true;
                }
            }

            return false;
        }

        public override FTFrontier ReduceRestorationFrontierForDiscardedMessages(FTFrontier receiverFrontier, int channelId, int receiverStageId, int receiverVertexId)
        {
            foreach (var bundle in this.OutgoingDiscardedMessages(channelId).OrderBy(t => t.Key))
            {
                if (!bundle.Value.TimeSet(receiverVertexId).Projected(receiverStageId).ContainedIn(receiverFrontier))
                {
                    // this is the first time that has discarded messages which are not contained in the receiver frontier, so our frontier needs to be less than
                    // this time
                    this.ReduceRestorationFrontierBelow(FTFrontier.SetBelow(bundle.Key.ToPointstamp(this.stage.StageId)));
                    return this.restorationFrontier;
                }
            }

            throw new ApplicationException("Logic bug reducing restoration frontiers");
        }

        public override IEnumerable<FTFrontier> GetAvailableFrontiers()
        {
            return new FTFrontier[] { new FTFrontier(true) };
        }

        public override FTFrontier GetRestorationFrontier()
        {
            return this.restorationFrontier;
        }

        public override FTFrontier ReduceRestorationFrontierBelow(FTFrontier target)
        {
            this.restorationFrontier = target;
            return target;
        }

        public override FTFrontier GetNotificationFrontier()
        {
            return this.notificationFrontier;
        }

        public override void SetNotificationFrontier(FTFrontier newFrontier)
        {
            this.notificationFrontier = newFrontier;
        }

        public override TimeSet DeliveredNotificationTimes()
        {
            return this.deliveredNotifications.ToSet(this.stage.StageId);
        }

        public override TimeSet DeliveredMessageTimes(int channelId, int senderVertexId)
        {
            return this.receivedMessageTimes[channelId].Second.TimeSet(senderVertexId);
        }

        public override IEnumerable<Pair<Pointstamp, TimeSet>> DiscardedMessageTimes(int channelId, int receiverStageId, int receiverVertexId)
        {
            return this.OutgoingDiscardedMessages(channelId).Select(t =>
                t.Key.ToPointstamp(this.stage.StageId).PairWith(t.Value.TimeSet(receiverVertexId).Projected(receiverStageId)));
        }

        public override void InstallCurrentCheckpoint()
        {
            this.stage.SetCurrentCheckpoint(
                this.vertexId, new FrontierCheckpoint<T>(this.stage, this.GetRestorationFrontier()));
        }

        internal virtual bool DeserializeForInitialization(ref RecvBuffer buffer, SerializationFormat serializationFormat)
        {
            return this.TryDeserialize(ref buffer, serializationFormat);
        }

        internal override void Initialize()
        {
            bool success = true;
            foreach (RecvBuffer fixedBuffer in this.StreamManager.MetaData.Reinitialize(stage.InternalComputation.Controller.SerializationFormat))
            {
                RecvBuffer buffer = fixedBuffer;
                success = success && this.DeserializeForInitialization(ref buffer, stage.InternalComputation.Controller.SerializationFormat);
            }
            if (!success)
            {
                throw new ApplicationException("Failed to read checkpoint for reconciliation");
            }
        }

        internal StatelessCheckpointCollection(Func<string, IStreamSequence> streamSequenceFactory, Stage<T> stage, int vertexId)
            : base(streamSequenceFactory, stage, vertexId)
        {
            this.restorationFrontier = new FTFrontier(true);
            this.notificationFrontier = this.restorationFrontier;
        }
    }

    internal class PersistedCheckpointCollection<T> : StatelessCheckpointCollection<T> where T : Time<T>
    {
        private readonly List<FTFrontier> availableFrontiers = new List<FTFrontier>();

        public override IEnumerable<FTFrontier> GetAvailableFrontiers()
        {
            return availableFrontiers;
        }

        private bool DeserializeAvailableCheckpoints(ref RecvBuffer source, SerializationFormat format)
        {
            NaiadSerialization<int> intSerializer = format.GetSerializer<int>();
            NaiadSerialization<long> longSerializer = format.GetSerializer<long>();

            int numberOfRegions = 0;
            bool success = intSerializer.TryDeserialize(ref source, out numberOfRegions);
            for (int i = 0; success && i < numberOfRegions; ++i)
            {
                StreamSequenceIndex start = new StreamSequenceIndex();
                StreamSequenceIndex end = new StreamSequenceIndex();
                StreamSequenceIndex previous = new StreamSequenceIndex();
                success = success && intSerializer.TryDeserialize(ref source, out start.index);
                success = success && longSerializer.TryDeserialize(ref source, out start.position);
                success = success && intSerializer.TryDeserialize(ref source, out end.index);
                success = success && longSerializer.TryDeserialize(ref source, out end.position);
                success = success && intSerializer.TryDeserialize(ref source, out previous.index);
                success = success && longSerializer.TryDeserialize(ref source, out previous.position);
                FTFrontier frontier = new FTFrontier(false);
                success = success && frontier.TryDeserialize(ref source, format);
                if (success)
                {
                    this.availableFrontiers.Add(frontier);
                }
            }

            return success;
        }

        internal override bool DeserializeForInitialization(ref RecvBuffer buffer, SerializationFormat serializationFormat)
        {
            bool success = base.DeserializeForInitialization(ref buffer, serializationFormat);
            success = success && this.DeserializeAvailableCheckpoints(ref buffer, serializationFormat);
            return success;
        }

        public override FTFrontier ReduceRestorationFrontierBelow(FTFrontier target)
        {
            FTFrontier largestAvailable = new FTFrontier(false);
            foreach (FTFrontier frontier in this.availableFrontiers)
            {
                if (target.Contains(frontier) && frontier.Contains(largestAvailable))
                {
                    largestAvailable = frontier;
                }
            }

            return base.ReduceRestorationFrontierBelow(largestAvailable);
        }

        internal PersistedCheckpointCollection(Func<string, IStreamSequence> streamSequenceFactory, Stage<T> stage, int vertexId)
            : base(streamSequenceFactory, stage, vertexId)
        {
        }
    }
}
