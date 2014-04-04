/*
 * Naiad ver. 0.2
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
    /// <typeparam name="S">record type</typeparam>
    /// <typeparam name="T">time type</typeparam>
    public struct Message<S, T>
        where T : Time<T>
    {
        private const int DEFAULT_MESSAGE_LENGTH = 256;
        internal static S[] Empty = new S[] { };

        /// <summary>
        /// Payload of typed records
        /// </summary>
        public S[] payload;

        /// <summary>
        /// Number of valid typed records
        /// </summary>
        public int length;

        /// <summary>
        /// Time common to all records
        /// </summary>
        public T time;

        internal bool Unallocated { get { return this.payload == null || this.payload == Message<S,T>.Empty; } }

        internal void Allocate()
        {
            if (!this.Unallocated)
                throw new Exception("Attempting to allocate an already allocated message.");

            // acquire from a shared queue.
            this.payload = ThreadLocalBufferPools<S>.pool.Value.CheckOut(DEFAULT_MESSAGE_LENGTH);                
        }

        internal void Release()
        {
            if (this.Unallocated)
                throw new Exception("Attempting to release an unallocated message.");

            // return to a shared queue.
            ThreadLocalBufferPools<S>.pool.Value.CheckIn(this.payload);
            
            this.payload = Message<S, T>.Empty;
            this.length = 0;
        }

        internal Message(T time)
        {
            this.payload = Message<S, T>.Empty;
            this.length = 0;
            this.time = time;
        }
    }
}

namespace Microsoft.Research.Naiad.Dataflow.Channels
{    
    /// <summary>
    /// The external interface for the receiver side of a channel.
    /// 
    /// The consumer of channel data (which may be a channels ifself) Subscribes to
    /// the RecvChannel, and invokes the Recv method to obtain data from the channel.
    /// </summary>
    /// <typeparam name="T">The element type in a channel message.</typeparam>
    public interface RecvChannel<T>
    {
        void Drain();
    }

    public interface SendChannel
    {
        /// <summary>
        /// Flushes any buffered messages to the receiver.
        /// </summary>
        void Flush();
    }

    /// <summary>
    /// The external interface for the sender side of a channel.
    /// 
    /// The producer of channel data (which may be a channel itself) invokes a Send method to insert
    /// data into the channel.
    /// </summary>
    /// <typeparam name="T">The element type in a channel message.</typeparam>
    public interface SendChannel<T> : SendChannel
    {        
        /// <summary>
        /// Inserts a single element into the channel.
        /// </summary>
        /// <param name="elem">The element to be inserted.</param>
        void Send(T elem);
    }



    public interface SendWire<S, T> : SendChannel<Message<S, T>>
        where T : Time<T>
    {
        int RecordSizeHint { get; }
    }

    public interface RecvWire<S, T> : RecvChannel<Message<S, T>>
        where T : Time<T>
    { 
    }

    public static class Channel
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
                && CodeGeneration.ExpressionComparer.Instance.Equals(sender.PartitionedBy, receiver.PartitionedBy);
        }
    }

    public interface Cable
    {
        Dataflow.Stage SourceStage{ get; }
        Dataflow.Stage DestinationStage{ get; }
        int ChannelId { get; }
    }

    public interface Cable<S, T> : Cable
        where T : Time<T>
    {
        SendWire<S, T> GetSendFiber(int i);
        RecvWire<S, T> GetRecvFiber(int i);
    }

}
