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

using Microsoft.Research.Naiad;
using Microsoft.Research.Naiad.Runtime.Progress;
using Microsoft.Research.Naiad.Dataflow;
using Microsoft.Research.Naiad.Dataflow.Channels;
using Microsoft.Research.Naiad.Serialization;

namespace Microsoft.Research.Naiad.Runtime.FaultTolerance
{
    /// <summary>
    /// Lets a vertex output tell the logging machinery when it sends a message
    /// </summary>
    /// <typeparam name="TVertex">time type of the sender vertex</typeparam>
    /// <typeparam name="S">type of the records being sent</typeparam>
    /// <typeparam name="TMessage">time type of the message being sent</typeparam>
    public interface IOutgoingMessageLogger<TVertex, S, TMessage>
        where TVertex : Time<TVertex>
        where TMessage : Time<TMessage>
    {
        /// <summary>
        /// Indicate a vertex has sent a message to the downstream stage
        /// </summary>
        /// <param name="time">the time at the sender from which records are being sent</param>
        /// <param name="message">the message that was sent</param>
        /// <param name="channelId">channel record is being sent on</param>
        /// <param name="receiverVertexId">vertex in the stage that received the message</param>
        void LogMessage(TVertex time, Message<S, TMessage> message, int channelId, int receiverVertexId);
    }

    internal class NullOutgoingMessageLogger<TVertex, S, TMessage> : IOutgoingMessageLogger<TVertex, S, TMessage>
        where TVertex : Time<TVertex>
        where TMessage : Time<TMessage>
    {
        public void LogMessage(TVertex sendTime, Message<S, TMessage> message, int channelId, int receiverVertexId)
        {
        }

        public NullOutgoingMessageLogger()
        {
        }
    }

    internal abstract class OutgoingMessageLogger<TVertex, S, TMessage> : IOutgoingMessageLogger<TVertex, S, TMessage>
        where TVertex : Time<TVertex>
        where TMessage : Time<TMessage>
    {
        private readonly OutgoingLogger<TVertex, S, TMessage> parent;
        private readonly NaiadSerialization<S> recordSerializer;
        private readonly CountLogger<TVertex, TMessage> logger;

        protected abstract bool ShouldLogMessage(TVertex sendTime, TMessage projectedSendTime, TMessage messageTime);

        public void LogMessage(TVertex sendTime, Message<S, TMessage> message, int channelId, int receiverVertexId)
        {
            if (this.ShouldLogMessage(sendTime, parent.StageOutput.SendTimeProjection(sendTime), message.time))
            {
                int numWritten = 0;
                while (numWritten < message.length)
                {
                    ArraySegment<S> toWrite = new ArraySegment<S>(message.payload, numWritten, message.length - numWritten);
                    numWritten += this.logger.LogMessage(sendTime.PairWith(message.time), channelId, receiverVertexId, this.recordSerializer, toWrite);
                }

                this.logger.AddSentCount(channelId, sendTime, message.time, receiverVertexId, message.length);
            }
            else
            {
                this.parent.AddDiscardedTime(channelId, sendTime, message.time, receiverVertexId);
            }
        }

        public OutgoingMessageLogger(OutgoingLogger<TVertex, S, TMessage> parent, SerializationFormat serializationFormat)
        {
            this.parent = parent;
            this.logger = this.parent.MessageLogger;

            this.recordSerializer = serializationFormat.GetSerializer<S>();
        }
    }

    internal enum OutgoingMessageLogSelection
    {
        LogNever,
        LogFuture,
        LogAlways,
        LogEphemeral
    }

    internal class SelectiveOutgoingMessageLogger<TVertex, S, TMessage> : OutgoingMessageLogger<TVertex, S, TMessage>
        where TVertex : Time<TVertex>
        where TMessage : Time<TMessage>
    {
        private readonly OutgoingMessageLogSelection selector;

        protected override bool ShouldLogMessage(TVertex sendTime, TMessage projectedSendTime, TMessage messageTime)
        {
            switch (selector)
            {
                case OutgoingMessageLogSelection.LogNever:
                    return false;

                case OutgoingMessageLogSelection.LogFuture:
                    return messageTime.CompareTo(projectedSendTime) > 0;

                case OutgoingMessageLogSelection.LogAlways:
                    return true;

                default:
                    return false;
            }
        }

        public SelectiveOutgoingMessageLogger(
            OutgoingLogger<TVertex, S, TMessage> parent, SerializationFormat serializationFormat,
            OutgoingMessageLogSelection selector)
            : base(parent, serializationFormat)
        {
            this.selector = selector;
        }
    }

    internal interface OutgoingLogger<TVertex> where TVertex : Time<TVertex>
    {
        Pointstamp? UpdateProgressForLoggedMessages();
        void MarkLoggedMessagesToSend();
        void ResendLoggedMessages();
        bool PendingLoggedMessages { get; }
        void UpdateReceiverFrontier(int stageId, int vertexId, FTFrontier frontier);
        bool TrySerialize(SendBufferPage sendPage, SerializationFormat serializationFormat);
        bool TryDeserialize(ref RecvBuffer source, SerializationFormat serializationFormat);
        void RefreshGarbageCollection();
        void UpdateParentForRollback(Checkpointer<TVertex> newParent);
    }

    internal abstract class OutgoingLogger<TVertex, S, TMessage> : OutgoingLogger<TVertex>
        where TVertex : Time<TVertex>
        where TMessage : Time<TMessage>
    {
        private Checkpointer<TVertex> parent;
        private readonly CountLogger<TVertex, TMessage> logger;
        internal readonly FullyTypedStageOutput<TVertex, S, TMessage> StageOutput;
        private readonly Checkpointer<TVertex>.DiscardedTimes<TMessage> discardedTimes;
        private IEnumerable<CountLogger<TVertex, TMessage>.RangeBoundary> pendingLoggedBoundaries;

        private readonly Dictionary<Pair<int, int>, FTFrontier> currentReceiverFrontiers;
        private readonly SortedDictionary<FTFrontier, int> sortedReceiverFrontiers;
        private FTFrontier collectedFrontier;

        public Pointstamp? UpdateProgressForLoggedMessages()
        {
            Pointstamp? maxTime = this.logger.UpdateProgressForLoggedMessages(this.parent.vertex.VertexId, this.StageOutput);

            foreach (var time in this.discardedTimes
                .Times(this.StageOutput.ForStage.StageId)
                .SelectMany(edge => edge.Second.Select(bundle => bundle.First)))
            {
                if (!maxTime.HasValue || FTFrontier.IsLessThanOrEqualTo(maxTime.Value, time))
                {
                    maxTime = time;
                }
            }

            return maxTime;
        }

        public void MarkLoggedMessagesToSend()
        {
            this.pendingLoggedBoundaries = this.logger.GetCurrentTimeExtents();
        }

        public void ResendLoggedMessages()
        {
            this.logger.FlushAsync().Wait();
            this.logger.ReplayLog(this.pendingLoggedBoundaries);
            this.pendingLoggedBoundaries = null;
            // re-do garbage collection, since this will have been suppressed while we were waiting to replay the log
            FTFrontier lowWatermark = this.sortedReceiverFrontiers.Keys.First();
            if (lowWatermark.Contains(this.collectedFrontier))
            {
                this.logger.GarbageCollectForReceivers(lowWatermark);
                this.collectedFrontier = lowWatermark;
            }
        }

        public bool PendingLoggedMessages { get { return this.pendingLoggedBoundaries != null; } }

        internal CountLogger<TVertex, TMessage> MessageLogger { get { return this.logger; } }

        internal void AttachToLogger(Cable<TVertex, S, TMessage> receiver)
        {
            logger.AddEdge(receiver.Edge);
            logger.AddProgressUpdater(receiver.Edge.ChannelId, receiver.GetSendChannel(parent.vertex.VertexId).GetMessageReplayer());
        }

        public void AddDiscardedTime(int channelId, TVertex senderTime, TMessage messageTime, int receiverVertexId)
        {
            this.discardedTimes.AddDiscardedTime(channelId, senderTime, messageTime, receiverVertexId);
        }

        public Checkpointer<TVertex>.DiscardedTimes<TMessage> DiscardedTimes { get { return this.discardedTimes; } }

        public abstract IOutgoingMessageLogger<TVertex, S, TMessage> CreateMessageLogger(SerializationFormat serializationFormat);

        public bool TrySerialize(SendBufferPage sendPage, SerializationFormat serializationFormat)
        {
            return this.collectedFrontier.TrySerialize(sendPage, serializationFormat);
        }

        public bool TryDeserialize(ref RecvBuffer source, SerializationFormat serializationFormat)
        {
            return this.collectedFrontier.TryDeserialize(ref source, serializationFormat);
        }

        public void RefreshGarbageCollection()
        {
            this.logger.GarbageCollectForReceivers(this.collectedFrontier);
        }

        public void UpdateReceiverFrontier(int stageId, int vertexId, FTFrontier frontier)
        {
            FTFrontier oldFrontier;
            if (this.currentReceiverFrontiers.TryGetValue(stageId.PairWith(vertexId), out oldFrontier))
            {
                if (oldFrontier.Contains(frontier))
                {
                    if (oldFrontier.Equals(frontier))
                    {
                        throw new ApplicationException("Duplicate garbage collection");
                    }

                    // garbage collection updates can be delivered out of order
                    return;
                }

                this.currentReceiverFrontiers[stageId.PairWith(vertexId)] = frontier;

                FTFrontier currentLowest = this.sortedReceiverFrontiers.Keys.First();

                int oldCount = this.sortedReceiverFrontiers[oldFrontier];
                if (oldCount == 1)
                {
                    this.sortedReceiverFrontiers.Remove(oldFrontier);
                }
                else
                {
                    this.sortedReceiverFrontiers[oldFrontier] = oldCount - 1;
                }

                if (this.sortedReceiverFrontiers.TryGetValue(frontier, out oldCount))
                {
                    this.sortedReceiverFrontiers[frontier] = oldCount + 1;
                }
                else
                {
                    this.sortedReceiverFrontiers[frontier] = 1;
                }

                FTFrontier newLowest = this.sortedReceiverFrontiers.Keys.First();
                // if we have pending logged messages to write out, don't garbage collect yet
                if (!newLowest.Equals(currentLowest) && !this.PendingLoggedMessages)
                {
                    // this.collectedFrontier can be larger than newLowest if we are restarting and haven't yet
                    // heard about everyone else's low watermarks
                    if (newLowest.Contains(this.collectedFrontier) && !newLowest.Equals(this.collectedFrontier))
                    {
                        this.logger.GarbageCollectForReceivers(newLowest);
                        this.collectedFrontier = newLowest;
                    }
                }
            }
        }

        public void UpdateParentForRollback(Checkpointer<TVertex> newParent)
        {
            this.parent = newParent;
        }

        public OutgoingLogger(Checkpointer<TVertex> parent, FullyTypedStageOutput<TVertex, S, TMessage> stageOutput)
        {
            this.parent = parent;
            this.StageOutput = stageOutput;
            this.discardedTimes = new Checkpointer<TVertex>.DiscardedTimes<TMessage>(this.StageOutput);
            this.pendingLoggedBoundaries = null;
            this.logger = new CountLogger<TVertex, TMessage>(
                this.StageOutput.ForStage.StageId,
                this.parent.StreamManager.MessageLog(this.StageOutput.StageOutputIndex),
                this.parent.Vertex.SerializationFormat,
                (f,e) => this.parent.vertex.scheduler.WriteLogEntry(f,e));
            this.parent.AddLogger(this.logger);

            this.currentReceiverFrontiers = new Dictionary<Pair<int, int>, FTFrontier>();
            foreach (Pair<int,int> vertex in this.StageOutput.OutputChannels
                .SelectMany(e => (e.Exchanges) ?
                    e.TargetStage.Placement.Select(v => e.TargetStage.StageId.PairWith(v.VertexId)) :
                    new Pair<int,int>[] { e.TargetStage.StageId.PairWith(this.parent.vertex.VertexId) })
                .Distinct())
            {
                this.currentReceiverFrontiers.Add(vertex, new FTFrontier(false));
            }

            this.sortedReceiverFrontiers = new SortedDictionary<FTFrontier,int>();
            this.sortedReceiverFrontiers.Add(new FTFrontier(false), this.currentReceiverFrontiers.Count);
            this.collectedFrontier = new FTFrontier(false);
        }
    }

    internal class NullOutgoingLogger<TVertex, S, TMessage> : OutgoingLogger<TVertex, S, TMessage>
        where TVertex : Time<TVertex>
        where TMessage : Time<TMessage>
    {
        public override IOutgoingMessageLogger<TVertex, S, TMessage> CreateMessageLogger(SerializationFormat serializationFormat)
        {
            return new NullOutgoingMessageLogger<TVertex, S, TMessage>();
        }

        public NullOutgoingLogger(Checkpointer<TVertex> parent, FullyTypedStageOutput<TVertex, S, TMessage> stageOutput)
            : base(parent, stageOutput)
        {
        }
    }

    internal class SelectiveOutgoingLogger<TVertex, S, TMessage> : OutgoingLogger<TVertex, S, TMessage>
        where TVertex : Time<TVertex>
        where TMessage : Time<TMessage>
    {
        private readonly OutgoingMessageLogSelection selector;

        public override IOutgoingMessageLogger<TVertex, S, TMessage> CreateMessageLogger(SerializationFormat serializationFormat)
        {
            return new SelectiveOutgoingMessageLogger<TVertex, S, TMessage>(this, serializationFormat, this.selector);
        }

        public SelectiveOutgoingLogger(
            Checkpointer<TVertex> parent, FullyTypedStageOutput<TVertex, S, TMessage> stageOutput,
            OutgoingMessageLogSelection selector)
            : base(parent, stageOutput)
        {
            this.selector = selector;
        }
    }
}
