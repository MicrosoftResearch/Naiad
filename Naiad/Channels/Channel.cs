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

using System.IO;
using System.Collections.Concurrent;
using Microsoft.Research.Naiad.DataStructures;
using Microsoft.Research.Naiad.Dataflow.Channels;
using Microsoft.Research.Naiad.Runtime.FaultTolerance;
using System.Diagnostics;
using System.Linq.Expressions;

namespace Microsoft.Research.Naiad.Dataflow
{
    /// <summary>
    /// A message containing typed records all with a common time
    /// </summary>
    /// <typeparam name="TRecord">record type</typeparam>
    /// <typeparam name="TTime">time type</typeparam>
    public struct Message<TRecord, TTime>
        where TTime : Time<TTime>
    {
        internal const int DefaultMessageLength = 256;

        /// <summary>
        /// Payload of typed records
        /// </summary>
        public TRecord[] payload;

        /// <summary>
        /// Number of valid typed records
        /// </summary>
        public int length;

        /// <summary>
        /// Time common to all records
        /// </summary>
        public TTime time;

        /// <summary>
        /// Tests whether the message points at valid data or not
        /// </summary>
        internal bool Unallocated { get { return this.payload == null; } }

        /// <summary>
        /// Causes an unallocated message to point at empty valid data
        /// </summary>
        /// <param name="tag">indication of why the allocation was done, for tracing reasons</param>
        /// <param name="bufferPool">pool to use for allocation</param>
        internal void Allocate(AllocationReason tag, BufferPool<TRecord> bufferPool)
        {
            Debug.Assert(this.Unallocated);

            // acquire from a shared queue.
            this.payload = bufferPool.CheckOut(DefaultMessageLength);
        }

        /// <summary>
        /// Releases the memory held by an allocated message back to a pool. Only call when no other references to the message are held.
        /// </summary>
        /// <param name="tag">indication of why the release was done, for tracing reasons</param>
        /// <param name="bufferPool">pool used for allocation</param>
        internal void Release(AllocationReason tag, BufferPool<TRecord> bufferPool)
        {
            Debug.Assert(!this.Unallocated);

            Array.Clear(this.payload, 0, this.length);

            // return to a shared queue.
            bufferPool.CheckIn(this.payload);

            this.payload = null;
            this.length = 0;
        }

        /// <summary>
        /// Constructs an unallocated message for a specified time
        /// </summary>
        /// <param name="time"></param>
        internal Message(TTime time)
        {
            this.payload = null;
            this.length = 0;
            this.time = time;
        }
    }

    /// <summary>
    /// Represents a channel that can be flushed of sent items
    /// </summary>
    public interface FlushableChannel
    {
        /// <summary>
        /// Flushes any buffered messages to the receiver.
        /// </summary>
        void Flush();
    }

    /// <summary>
    /// Represents the recipient of a <see cref="Message{TRecord,TTime}"/>
    /// </summary>
    /// <typeparam name="TSender">time type of the sending vertex</typeparam>
    /// <typeparam name="TRecord">record type</typeparam>
    /// <typeparam name="TTime">time type</typeparam>
    public interface SendChannel<TSender, TRecord, TTime> : FlushableChannel
        where TSender : Time<TSender>
        where TTime : Time<TTime>
    {
        /// <summary>
        /// Inserts the given <paramref name="message"/> into the channel.
        /// </summary>
        /// <param name="vertexTime">The time of the event sending the message.</param>
        /// <param name="message">The message to be sent.</param>
        void Send(TSender vertexTime, Message<TRecord, TTime> message);

        /// <summary>
        /// Install the class that handles checkpointing/logging for outgoing messages
        /// </summary>
        /// <param name="logger"></param>
        void EnableLogging(IOutgoingMessageLogger<TSender, TRecord, TTime> logger);

        /// <summary>
        /// Get an object that can be used to replay logged messages
        /// </summary>
        /// <returns>the replayer</returns>
        IOutgoingMessageReplayer<TSender,TTime> GetMessageReplayer();
    }
}

namespace Microsoft.Research.Naiad.Dataflow.Channels
{ 


    internal static class Channel
    {
        [Flags]
        public enum Flags
        {
            None = 0x0,
            DisableProgressProtocol = 0x1,
            SoftBarrier = 0x2,
            SpillOnRecv = 0x4
        }

        internal static bool Pipelineable<S, T>(StageOutput<S, T> sender, StageInput<S, T> receiver, Action<S[], int[], int> key)
            where T : Time<T>
        {
            return sender.ForStage.Placement.Equals(receiver.ForStage.Placement) && key == null;
        }

        internal static bool PartitionedEquivalently<S, T>(StageOutput<S, T> sender, StageInput<S, T> receiver)
            where T : Time<T>
        {
            return sender.ForStage.Placement.Equals(receiver.ForStage.Placement) 
                && sender.PartitionedBy != null 
                && receiver.PartitionedBy != null
                && Utilities.ExpressionComparer.Instance.Equals(sender.PartitionedBy, receiver.PartitionedBy);
        }
    }

    internal interface Cable<TSender, S, T>
        where TSender : Time<TSender>
        where T : Time<T>
    {
        SendChannel<TSender, S, T> GetSendChannel(int i);

        Dataflow.Stage<TSender> SourceStage { get; }
        Dataflow.Stage<T> DestinationStage { get; }
        Edge<TSender, S, T> Edge { get; }

        void EnableReceiveLogging();

        void ReMaterializeForRollback(Dictionary<int, Vertex> newSourceVertices, Dictionary<int, Vertex> newTargetVertices);
    }
}
