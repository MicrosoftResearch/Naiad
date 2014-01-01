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
using Naiad.DataStructures;
using Naiad.Dataflow.Channels;
using System.Diagnostics;
using System.Linq.Expressions;

namespace Naiad.Dataflow
{
    public struct Message<T>
    {
        private const int DEFAULT_MESSAGE_LENGTH = 256;

        public T[] payload;
        public int length;

        public IEnumerable<T> AsEnumerable() { return this.payload.Take(this.length); }

        /// <summary>
        /// Construct a typed message using preallocated buffers
        /// </summary>
        /// <param name="payload">Message body</param>
        /// 
        /// 
        public Message(T[] payload)
        {
            this.payload = payload;
            this.length = 0;
        }

        /// <summary>
        /// Construct a typed message of default size
        /// </summary>
        /// <param name="pool">Pool from which to acquire message buffers</param>
        /// 
        /// 
        public Message(BufferPool<T> pool)
            : this(pool, DEFAULT_MESSAGE_LENGTH)
        {

        }

        /// <summary>
        /// Construct a typed message of specified size
        /// </summary>
        /// <param name="pool">Pool from which to acquire message buffers</param>
        /// <param name="size">Required size in number of elements</param>
        /// 
        /// 
        public Message(BufferPool<T> pool, int size)
        {
            this.payload = pool != null ? pool.CheckOut(size) : new T[size];
            this.length = 0;
        }

        public void Enable(int size = DEFAULT_MESSAGE_LENGTH, BufferPool<T> bufferPool = null)
        {
            if (bufferPool == null)
                bufferPool = ThreadLocalBufferPools<T>.pool.Value;

            this.payload = bufferPool.CheckOut(size);
            this.length = 0;
        }

        public void Disable()
        {
            this.payload = ThreadLocalBufferPools<T>.pool.Value.Empty;
            this.length = 0;
        }
    }
}

namespace Naiad.Dataflow.Channels
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
        
        /// <summary>
        /// Inserts multilpe elements into the channel.
        /// </summary>
        /// <param name="elems"></param>
        void Send(Message<T> elems);
    
    }



    public interface SendWire<S, T> : SendChannel<Pair<S, T>>
        where T : Time<T>
    {
        int RecordSizeHint { get; }
    }

    public interface RecvWire<S, T> : RecvChannel<Pair<S, T>>
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
