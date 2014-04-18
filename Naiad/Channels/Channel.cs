/*
 * Naiad ver. 0.4
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
        private const int DEFAULT_MESSAGE_LENGTH = 256;
        internal static TRecord[] Empty = new TRecord[] { };

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
        public bool Unallocated { get { return this.payload == null || this.payload == Message<TRecord,TTime>.Empty; } }

        /// <summary>
        /// Causes an unallocated message to point at empty valid data
        /// </summary>
        public void Allocate()
        {
            if (!this.Unallocated)
                throw new Exception("Attempting to allocate an already allocated message.");

            // acquire from a shared queue.
            this.payload = ThreadLocalBufferPools<TRecord>.pool.Value.CheckOut(DEFAULT_MESSAGE_LENGTH);                
        }

        /// <summary>
        /// Releases the memory held by an allocated message back to a pool. Only call when no other references to the message are held.
        /// </summary>
        public void Release()
        {
            if (this.Unallocated)
                throw new Exception("Attempting to release an unallocated message.");

            // return to a shared queue.
            ThreadLocalBufferPools<TRecord>.pool.Value.CheckIn(this.payload);
            
            this.payload = Message<TRecord, TTime>.Empty;
            this.length = 0;
        }

        /// <summary>
        /// Constructs an unallocated message for a specified time
        /// </summary>
        /// <param name="time"></param>
        internal Message(TTime time)
        {
            this.payload = Message<TRecord, TTime>.Empty;
            this.length = 0;
            this.time = time;
        }
    }

    /// <summary>
    /// Represents the recipient of a <see cref="Message{TRecord,TTime}"/>
    /// </summary>
    /// <typeparam name="TRecord">record type</typeparam>
    /// <typeparam name="TTime">time type</typeparam>
    public interface SendChannel<TRecord, TTime>
        where TTime : Time<TTime>
    {
        /// <summary>
        /// Inserts the given <paramref name="message"/>into the channel.
        /// </summary>
        /// <param name="message">The message to be sent.</param>
        void Send(Message<TRecord, TTime> message);

        /// <summary>
        /// Flushes any buffered messages to the receiver.
        /// </summary>
        void Flush();
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

        internal static bool Pipelineable<S, T>(StageOutput<S, T> sender, StageInput<S, T> receiver, Expression<Func<S, int>> key)
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

    internal interface Cable<S, T>
        where T : Time<T>
    {
        SendChannel<S, T> GetSendChannel(int i);

        Dataflow.Stage SourceStage { get; }
        Dataflow.Stage DestinationStage { get; }
        int ChannelId { get; }
    }
}
