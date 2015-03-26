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
using Microsoft.Research.Naiad.Serialization;

namespace Microsoft.Research.Naiad.Runtime.FaultTolerance
{
    internal interface IMessageLogger<S, T> where T : Time<T>
    {
        void LogMessage(Message<S, T> message, ReturnAddress from);
    }

    internal class DummyMessageLogger<S, T> : IMessageLogger<S, T> where T : Time<T>
    {
        private readonly Checkpointer<T> checkpointer;
        private readonly int channelId;

        public void LogMessage(Message<S, T> message, ReturnAddress from)
        {
            this.checkpointer.RegisterReceivedMessageTime(from.VertexID, this.channelId, message.time);
        }

        public DummyMessageLogger(Checkpointer<T> checkpointer, int channelId)
        {
            this.checkpointer = checkpointer;
            this.channelId = channelId;
        }
    }

    internal class MessageLogger<S, T> : IMessageLogger<S, T> where T : Time<T>
    {
        private readonly NaiadSerialization<S> recordSerializer;
        private readonly Checkpointer<T> checkpointer;
        private readonly int channelId;
        private readonly Logger<T> vertexLogger;

        public void LogMessage(Message<S, T> message, ReturnAddress from)
        {
            this.checkpointer.RegisterReceivedMessageTime(from.VertexID, this.channelId, message.time);

            int numWritten = 0;
            while (numWritten < message.length)
            {
                ArraySegment<S> toWrite = new ArraySegment<S>(message.payload, numWritten, message.length - numWritten);
                numWritten += this.vertexLogger.LogMessage(message.time, this.channelId, from.VertexID, this.recordSerializer, toWrite);
            }
        }

        public MessageLogger(Checkpointer<T> checkpointer, int channelId, Logger<T> vertexLogger)
        {
            this.checkpointer = checkpointer;
            this.recordSerializer = this.checkpointer.Vertex.SerializationFormat.GetSerializer<S>();
            this.channelId = channelId;
            this.vertexLogger = vertexLogger;
        }
    }

    internal interface IInputLogger<S, T> where T : Time<T>
    {
        void LogInput(T time, S[] payload);
        void LogInputProcessing(T time, S[] payload);
        void UpdateReplayAction(Action<T, S[]> replayAction);
        Pointstamp? MaxCompleted();
    }

    internal class DummyInputLogger<S, T> : IInputLogger<S, T> where T : Time<T>
    {
        public void LogInput(T time, S[] payload)
        {
        }

        public void LogInputProcessing(T time, S[] payload)
        {
        }

        public void UpdateReplayAction(Action<T, S[]> replayAction)
        {
        }

        public Pointstamp? MaxCompleted()
        {
            return null;
        }

        public DummyInputLogger()
        {
        }
    }

    internal interface INotificationLogger<T> where T : Time<T>
    {
        void LogNotificationRequest(T requestTime, T requirement, T capability);
        void LogNotification(T time);
    }

    internal class DummyNotificationLogger<T> : INotificationLogger<T> where T : Time<T>
    {
        private readonly Checkpointer<T> checkpointer;

        public void LogNotificationRequest(T requestTime, T requirement, T capability)
        {
            this.checkpointer.RegisterRequestedNotification(requestTime, requirement, capability);
        }

        public void LogNotification(T time)
        {
            this.checkpointer.RegisterDeliveredNotificationTime(time);
        }

        public DummyNotificationLogger(Checkpointer<T> checkpointer)
        {
            this.checkpointer = checkpointer;
        }
    }

    internal class NotificationLogger<T> : INotificationLogger<T> where T : Time<T>
    {
        private readonly Checkpointer<T> checkpointer;
        private readonly Logger<T> vertexLogger;

        public void LogNotificationRequest(T requestTime, T requirement, T capability)
        {
            // no need to log this since the event that generates it will always be replayed on rollback
        }

        public void LogNotification(T time)
        {
            this.checkpointer.RegisterDeliveredNotificationTime(time);
            this.vertexLogger.LogEmptyMessage(time);
        }

        public NotificationLogger(Checkpointer<T> checkpointer, Logger<T> vertexLogger)
        {
            this.checkpointer = checkpointer;
            this.vertexLogger = vertexLogger;
        }
    }

    internal class LogMessageReader<S, K, TTime> : IMessageReplayer<K> where TTime : Time<TTime>
    {
        private readonly int sendStageId;
        private readonly Action<Message<S, TTime>, ReturnAddress> messageAction;
        private readonly Action flushAction;
        private readonly Func<K, TTime> messageTimeFunction;
        protected Func<TTime, int, bool> deliverMessageFunction;
        private readonly NaiadSerialization<S> recordSerializer;
        private readonly BufferPool<S> bufferPool;

        public RecvBuffer DeserializeToMessages(RecvBuffer recv, K key, int sendVertex, int recordCount, bool deliver)
        {
            TTime messageTime = this.messageTimeFunction(key);

            ReturnAddress from = new ReturnAddress(this.sendStageId, -1, sendVertex, -1);

            deliver = deliver && this.deliverMessageFunction(messageTime, sendVertex);

            int recordsLeft = recordCount;
            while (recordsLeft > 0)
            {
                Message<S, TTime> msg = new Message<S, TTime>();
                msg.Allocate(AllocationReason.Deserializer, this.bufferPool);
                msg.time = messageTime;

                int numToRead = Math.Min(msg.payload.Length, recordsLeft);
                int numReadThisTime = this.recordSerializer.TryDeserializeMany(ref recv, new ArraySegment<S>(msg.payload, 0, numToRead));
                msg.length = numReadThisTime;

                if (numReadThisTime != numToRead)
                {
                    throw new ApplicationException("Failed to read records from log");
                }

                if (deliver)
                {
                    this.messageAction(msg, from);
                }
                recordsLeft -= numReadThisTime;
            }

            return recv;
        }

        public void Flush()
        {
            this.flushAction();
        }

        public LogMessageReader(
            int sendStageId,
            Action<Message<S, TTime>, ReturnAddress> messageAction, Action flushAction,
            Func<K, TTime> messageTimeFunction,
            Func<TTime, int, bool> deliverMessageFunction,
            NaiadSerialization<S> recordSerializer, BufferPool<S> bufferPool)
        {
            this.sendStageId = sendStageId;
            this.messageAction = messageAction;
            this.flushAction = flushAction;
            this.messageTimeFunction = messageTimeFunction;
            this.deliverMessageFunction = deliverMessageFunction;
            this.recordSerializer = recordSerializer;
            this.bufferPool = bufferPool;
        }
    }

    internal class OutgoingMessageLogReader<TSenderTime, S, TMessageTime>
        : LogMessageReader<S, Pair<TSenderTime, TMessageTime>, TMessageTime>, IOutgoingMessageReplayer<TSenderTime, TMessageTime>
        where TSenderTime : Time<TSenderTime>
        where TMessageTime : Time<TMessageTime>
    {
        private readonly Edge<TSenderTime, S, TMessageTime> edge;
        private readonly int senderVertexId;
        private readonly ProgressUpdateBuffer<TMessageTime> progressUpdate;

        public void PrepareForRestoration()
        {
            this.deliverMessageFunction = edge.Source.TypedStage.CurrentCheckpoint(senderVertexId).ShouldSendFunc(
                edge.Target.ForStage,
                edge.Source.GetFiber(senderVertexId).Vertex.Scheduler.State(edge.Source.ForStage.InternalComputation).DiscardManager);
        }

        public void UpdateProgress(TMessageTime time, int receiverVertexId, long count)
        {
            if (this.deliverMessageFunction(time, receiverVertexId))
            {
                this.progressUpdate.Update(time, count);
            }
        }

        public void FlushUpdates()
        {
            this.progressUpdate.Flush();
        }

        public OutgoingMessageLogReader(Edge<TSenderTime, S, TMessageTime> edge, int senderVertexId,
            Action<Message<S, TMessageTime>, ReturnAddress> messageAction, Action flushAction,
            BufferPool<S> bufferPool, ProgressUpdateBuffer<TMessageTime> progressUpdate)
            : base(
                edge.SourceStage.StageId, messageAction, flushAction, k => k.Second, null,
                edge.SourceStage.InternalComputation.Controller.SerializationFormat.GetSerializer<S>(), bufferPool)
        {
            this.edge = edge;
            this.senderVertexId = senderVertexId;
            this.progressUpdate = progressUpdate;
        }
    }

    internal class LogInputReader<S, T> : IMessageReplayer<T> where T : Time<T>
    {
        private static int inputArraySize = 1024;
        private readonly Action<T, S[]> inputAction;
        private readonly Action flushAction;
        private readonly NaiadSerialization<S> recordSerializer;

        public RecvBuffer DeserializeToMessages(RecvBuffer recv, T time, int sendVertex, int recordCount, bool deliver)
        {
            int recordsLeft = recordCount;
            while (recordsLeft > 0)
            {
                S[] payload = new S[Math.Min(LogInputReader<S, T>.inputArraySize, recordsLeft)];
                int numReadThisTime = this.recordSerializer.TryDeserializeMany(ref recv, new ArraySegment<S>(payload));

                if (numReadThisTime != payload.Length)
                {
                    throw new ApplicationException("Failed to read input records from log");
                }

                if (deliver)
                {
                    this.inputAction(time, payload);
                }
                recordsLeft -= numReadThisTime;
            }

            return recv;
        }

        public void Flush()
        {
            this.flushAction();
        }

        public LogInputReader(Action<T, S[]> inputAction, Action flushAction, NaiadSerialization<S> recordSerializer)
        {
            this.inputAction = inputAction;
            this.flushAction = flushAction;
            this.recordSerializer = recordSerializer;
        }
    }
}
