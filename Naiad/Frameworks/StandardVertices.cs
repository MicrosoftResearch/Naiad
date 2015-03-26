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
using System.Linq.Expressions;
using System.Text;
using Microsoft.Research.Naiad;
using Microsoft.Research.Naiad.Dataflow.Channels;
using Microsoft.Research.Naiad.Runtime.FaultTolerance;
using Microsoft.Research.Naiad.Serialization;
using Microsoft.Research.Naiad.DataStructures;

using Microsoft.Research.Naiad.Dataflow;
using Microsoft.Research.Naiad.Scheduling;

namespace Microsoft.Research.Naiad.Dataflow
{
    /// <summary>
    /// A repository for input records, stored indexed by time. Calls NotifyAt on record receipt.
    /// </summary>
    /// <typeparam name="TRecord">The type of records in this buffer.</typeparam>
    /// <typeparam name="TTime">The type of timestamp by which this buffer is indexed.</typeparam>
    public class VertexInputBuffer<TRecord, TTime> where TTime : Time<TTime>
    {
        /// <summary>
        /// Indicates available entrancy; always 1 as this class buffers everything.
        /// </summary>
        public int AvailableEntrancy { get { return 1; } set { } }

        private readonly Dictionary<TTime, SpinedList<TRecord>> recordsToProcess = new Dictionary<TTime, SpinedList<TRecord>>();

        private readonly Vertex<TTime> vertex;

        /// <summary>
        /// Enumerates (and destroys) input records associated with the given <paramref name="time"/>.
        /// </summary>
        /// <param name="time">time</param>
        /// <returns>The sequence of input records associated with the given <paramref name="time"/>.</returns>
        public IEnumerable<TRecord> GetRecordsAt(TTime time)
        {
            var result = default(SpinedList<TRecord>);

            if (recordsToProcess.TryGetValue(time, out result))
                recordsToProcess.Remove(time);
            else
                result = new SpinedList<TRecord>();

            return result.DequeueAllAndInvalidate();
        }

        /// <summary>
        /// Remove all the state from this buffer
        /// </summary>
        public void Clear()
        {
            this.recordsToProcess.Clear();
        }

        /// <summary>
        /// Constructs new input buffer for the given <paramref name="vertex"/>.
        /// </summary>
        /// <param name="vertex">The vertex to which this buffer will belong.</param>
        public VertexInputBuffer(Vertex<TTime> vertex)
        {
            this.vertex = vertex;
        }

        /// <summary>
        /// The vertex to which this buffer belongs.
        /// </summary>
        public Vertex Vertex { get { return this.vertex; } }

        /// <summary>
        /// Flushes the associated vertex.
        /// </summary>
        public void Flush() { this.vertex.Flush(); }

        /// <summary>
        /// Buffers the content of the given <paramref name="message"/>, and schedules
        /// a corresponding notification on the owning <see cref="Vertex"/>.
        /// </summary>
        /// <param name="message">The message.</param>
        public void OnReceive(Message<TRecord, TTime> message)
        {
            if (!this.recordsToProcess.ContainsKey(message.time))
            {
                this.recordsToProcess.Add(message.time, new SpinedList<TRecord>());
                this.vertex.NotifyAt(message.time);
            }

            var list = this.recordsToProcess[message.time];
            for (int i = 0; i < message.length; ++i)
                list.Add(message.payload[i]);
        }

        /// <summary>
        /// Returns a string representation of this buffer.
        /// </summary>
        /// <returns>A string representation of this buffer.</returns>
        public override string ToString()
        {
            return string.Format("<{0}B>", this.vertex.Stage.StageId);
        }

        /// <summary>
        /// Restores this buffer from the given <see cref="NaiadReader"/>.
        /// </summary>
        /// <param name="reader">The reader.</param>
        public void Restore(NaiadReader reader)
        {
            var timeSerializer = this.Vertex.SerializationFormat.GetSerializer<TTime>();
            var valueSerializer = this.Vertex.SerializationFormat.GetSerializer<TRecord>();
            var intSerializer = this.Vertex.SerializationFormat.GetSerializer<Int32>();

            int readCount = reader.Read(intSerializer);
            for (int i = 0; i < readCount; ++i)
            {
                TTime time = reader.Read(timeSerializer);
                SpinedList<TRecord> records = new SpinedList<TRecord>();
                records.Restore(reader, valueSerializer, intSerializer);
                this.recordsToProcess[time] = records;
            }
        }

        /// <summary>
        /// Checkpoints the contents of this buffer to the given <see cref="NaiadWriter"/>.
        /// </summary>
        /// <param name="writer">The writer.</param>
        public void Checkpoint(NaiadWriter writer)
        {
            var timeSerializer = this.Vertex.SerializationFormat.GetSerializer<TTime>();
            var valueSerializer = this.Vertex.SerializationFormat.GetSerializer<TRecord>();
            var intSerializer = this.Vertex.SerializationFormat.GetSerializer<Int32>();

            writer.Write(this.recordsToProcess.Count, intSerializer);
            foreach (KeyValuePair<TTime, SpinedList<TRecord>> kvp in this.recordsToProcess)
            {
                writer.Write(kvp.Key, timeSerializer);
                kvp.Value.Checkpoint(writer, valueSerializer, intSerializer);
            }

        }

    }

    /// <summary>
    /// An intermediate buffer for records sent by a <see cref="Vertex"/>.
    /// </summary>
    /// <typeparam name="TRecord">The type of records to be sent.</typeparam>
    /// <typeparam name="TTime">The type of timestamp on the records to be sent.</typeparam>
    /// <typeparam name="TVertexTime">The type of timestamp on the vertex sending records.</typeparam>
    public class VertexOutputBufferForInterestingTime<TVertexTime, TRecord, TTime> : VertexOutput<TVertexTime, TRecord, TTime>
        where TTime : Time<TTime>
        where TVertexTime : Time<TVertexTime>
    {
        private Dataflow.SendChannel<TVertexTime, TRecord, TTime>[] sendChannels;

        private readonly Dictionary<Pair<TTime, TVertexTime>, VertexOutputBufferPerInterestingTime<TVertexTime, TRecord, TTime>> Buffers;

        private bool MustFlushChannels;

        internal BufferPool<TRecord> BufferPool;

        private readonly Queue<Pair<TTime, TVertexTime>> TimesToFlush = new Queue<Pair<TTime, TVertexTime>>();
        private readonly Queue<VertexOutputBufferPerInterestingTime<TVertexTime, TRecord, TTime>> SpareBuffers =
            new Queue<VertexOutputBufferPerInterestingTime<TVertexTime, TRecord, TTime>>();

        internal virtual VertexOutputBufferPerInterestingTime<TVertexTime, TRecord, TTime> MakeBuffer(Pair<TTime, TVertexTime> timePair)
        {
            return new VertexOutputBufferPerInterestingTime<TVertexTime, TRecord, TTime>(this, timePair, this.BufferPool);
        }

        /// <summary>
        /// Adds a recipient for records handled by this buffer.
        /// </summary>
        /// <param name="recipient">The recipient.</param>
        public void AddReceiver(Dataflow.SendChannel<TVertexTime, TRecord, TTime> recipient)
        {
            this.sendChannels = this.sendChannels.Concat(new[] { recipient }).ToArray();
        }

        /// <summary>
        /// Sends a full message.
        /// </summary>
        /// <param name="message">The message.</param>
        public void Send(Message<TRecord, TTime> message)
        {
            this.Send(message, this.vertex.CurrentEventTime);
        }

        internal void Send(Message<TRecord, TTime> message, TVertexTime vertexTime)
        {
            if (this.sendChannels.Length > 0)
            {
                this.MustFlushChannels = true;
                for (int i = 0; i < this.sendChannels.Length; i++)
                {
                    this.sendChannels[i].Send(vertexTime, message);
                }
            }
        }

        /// <summary>
        /// Flushes all internal buffers associated with this buffer.
        /// </summary>
        public void Flush()
        {
            while (this.TimesToFlush.Count > 0)
            { 
                var time = this.TimesToFlush.Dequeue();
                
                var buffer = this.Buffers[time];
                this.Buffers.Remove(time);

                buffer.SendBufferNoReallocate();
                this.SpareBuffers.Enqueue(buffer);
            }

            if (this.MustFlushChannels)
            {
                this.MustFlushChannels = false;
                for (int i = 0; i < this.sendChannels.Length; i++)
                    this.sendChannels[i].Flush();
            }
        }

        /// <summary>
        /// Returns a per-time buffer with for records with a single time, using which records may be sent.
        /// </summary>
        /// <param name="time">The constant time.</param>
        /// <returns>A new per-time buffer.</returns>
        public VertexOutputBufferPerInterestingTime<TVertexTime, TRecord, TTime> GetBufferForInterestingTime(TTime time)
        {
            Pair<TTime, TVertexTime> timePair = time.PairWith(this.vertex.CurrentEventTime);
            if (!this.Buffers.ContainsKey(timePair))
            {
                if (this.SpareBuffers.Count > 0)
                { 
                    var buffer = this.SpareBuffers.Dequeue();
                    buffer.ReinitializeFor(timePair);
                    this.Buffers.Add(timePair, buffer);
                }
                else
                {
                    this.Buffers.Add(timePair, this.MakeBuffer(timePair));
                }

                this.TimesToFlush.Enqueue(timePair);
            }
            return this.Buffers[timePair];
        }

        /// <summary>
        /// The vertex to which this buffer belongs.
        /// </summary>
        public Vertex<TVertexTime> Vertex { get { return this.vertex; } }

        private readonly Vertex<TVertexTime> vertex;

        /// <summary>
        /// Constructs a VertexOutputBuffer for the given vertex.
        /// </summary>
        /// <param name="vertex">The vertex to which this buffer will belong.</param>
        public VertexOutputBufferForInterestingTime(Vertex<TVertexTime> vertex)
        {
            this.vertex = vertex;

            this.BufferPool = vertex.Scheduler.GetBufferPool<TRecord>();

            this.sendChannels = new Dataflow.SendChannel<TVertexTime, TRecord, TTime>[] { };
            this.Buffers = new Dictionary<Pair<TTime, TVertexTime>, VertexOutputBufferPerInterestingTime<TVertexTime, TRecord, TTime>>();

            vertex.AddOnFlushAction(() => this.Flush());
        }
    }

    /// <summary>
    /// An intermediate buffer for records sent by a <see cref="Vertex"/>.
    /// </summary>
    /// <typeparam name="TRecord">The type of records to be sent.</typeparam>
    /// <typeparam name="TTime">The type of timestamp on the records to be sent.</typeparam>
    public class VertexOutputBuffer<TRecord, TTime> : VertexOutputBufferForInterestingTime<TTime, TRecord, TTime>
        where TTime : Time<TTime>
    {
        internal override VertexOutputBufferPerInterestingTime<TTime, TRecord, TTime> MakeBuffer(Pair<TTime, TTime> timePair)
        {
            return new VertexOutputBufferPerTime<TRecord, TTime>(this, timePair, this.BufferPool);
        }
        /// <summary>
        /// Returns a per-time buffer with for records with a single time, using which records may be sent.
        /// </summary>
        /// <param name="time">The constant time.</param>
        /// <returns>A new per-time buffer.</returns>
        public VertexOutputBufferPerTime<TRecord, TTime> GetBufferForTime(TTime time)
        {
            return this.GetBufferForInterestingTime(time) as VertexOutputBufferPerTime<TRecord, TTime>;
        }

        /// <summary>
        /// Constructs a VertexOutputBuffer for the given vertex.
        /// </summary>
        /// <param name="vertex">The vertex to which this buffer will belong.</param>
        public VertexOutputBuffer(Vertex<TTime> vertex) : base(vertex)
        {
        }
    }

    /// <summary>
    /// Represents a per-time buffer for sending records with a single time.
    /// </summary>
    /// <typeparam name="TRecord">The type of records to be sent.</typeparam>
    /// <typeparam name="TTime">The type of timestamp on the records to be sent.</typeparam>
    /// <typeparam name="TVertexTime">The type of timestamp on the vertex sending records.</typeparam>
    public class VertexOutputBufferPerInterestingTime<TVertexTime, TRecord, TTime>
        where TTime : Time<TTime>
        where TVertexTime : Time<TVertexTime>
    {
        private readonly VertexOutputBufferForInterestingTime<TVertexTime, TRecord, TTime> parent;
        private Pair<TTime, TVertexTime> Time;

        private Message<TRecord, TTime> Buffer;
        private BufferPool<TRecord> BufferPool;

        /// <summary>
        /// Sends the given record.
        /// </summary>
        /// <param name="record">The record.</param>
        [System.Runtime.CompilerServices.MethodImplAttribute(System.Runtime.CompilerServices.MethodImplOptions.AggressiveInlining)]
        public void Send(TRecord record)
        {
            this.Buffer.payload[Buffer.length++] = record;
            if (this.Buffer.length == this.Buffer.payload.Length)
                this.SendBuffer();
        }

        private void SendBuffer()
        {
            if (Buffer.length > 0)
            {
                var temp = Buffer;

                this.Buffer = new Message<TRecord, TTime>();
                this.Buffer.Allocate(AllocationReason.VertexOutputBuffer, this.BufferPool);
                this.Buffer.time = this.Time.First;

                parent.Send(temp, this.Time.Second);

                temp.Release(AllocationReason.VertexOutputBuffer, this.BufferPool);
            }
        }


        internal void SendBufferNoReallocate()
        {
            if (Buffer.length > 0)
                parent.Send(Buffer, this.Time.Second);

            Buffer.Release(AllocationReason.VertexOutputBuffer, this.BufferPool);
        }

        internal void ReinitializeFor(Pair<TTime, TVertexTime> time)
        {
            this.Time = time;

            if (!this.Buffer.Unallocated)
                Console.WriteLine("!!!!!!!!!!!!!!!!");

            this.Buffer = new Message<TRecord, TTime>(time.First);
            this.Buffer.Allocate(AllocationReason.VertexOutputBuffer, this.BufferPool);
        }

        /// <summary>
        /// Constructs a new buffer from its parent <see cref="VertexOutputBuffer{TRecord,TTime}"/> and a constant logical time.
        /// </summary>
        /// <param name="parent">The parent buffer.</param>
        /// <param name="time">The constant time.</param>
        /// <param name="bufferPool">The buffer pool to use</param>
        internal VertexOutputBufferPerInterestingTime(VertexOutputBufferForInterestingTime<TVertexTime, TRecord, TTime> parent, Pair<TTime, TVertexTime> time, BufferPool<TRecord> bufferPool)
        {
            this.BufferPool = bufferPool;
            this.parent = parent;
            this.Time = time;

            this.Buffer = new Message<TRecord, TTime>(time.First);
            this.Buffer.Allocate(AllocationReason.VertexOutputBuffer, this.BufferPool);
        }
    }

    /// <summary>
    /// Represents a per-time buffer for sending records with a single time.
    /// </summary>
    /// <typeparam name="TRecord">The type of records to be sent.</typeparam>
    /// <typeparam name="TTime">The type of timestamp on the records to be sent.</typeparam>
    public class VertexOutputBufferPerTime<TRecord, TTime> : VertexOutputBufferPerInterestingTime<TTime, TRecord, TTime>
        where TTime : Time<TTime>
    {
        internal VertexOutputBufferPerTime(VertexOutputBufferForInterestingTime<TTime, TRecord, TTime> parent, Pair<TTime,TTime> time, BufferPool<TRecord> bufferPool)
            : base(parent, time, bufferPool)
        {
        }
    }
}


namespace Microsoft.Research.Naiad.Dataflow.StandardVertices
{
    #region Streaming Vertices and Stages

    /// <summary>
    /// Vertex with one input, no outputs, which accumulates inputs and schedules itself for each time seen.
    /// </summary>
    /// <typeparam name="TOutput">Source type</typeparam>
    /// <typeparam name="TTime">Time type</typeparam>
    public abstract class SinkVertex<TOutput, TTime> : Vertex<TTime>
        where TTime : Time<TTime>
    {
        /// <summary>
        /// Called when a message is received.
        /// </summary>
        /// <param name="message">The message.</param>
        public abstract void OnReceive(Message<TOutput, TTime> message);

        /// <summary>
        /// Constructs a new Sink stage
        /// </summary>
        /// <param name="stream">source stream</param>
        /// <param name="factory">vertex factory</param>
        /// <param name="partitionedBy">partitioning requirement</param>
        /// <param name="name">stage name</param>
        /// <returns>the sink stage</returns>
        public static Stage<SinkVertex<TOutput,TTime>, TTime> MakeStage(Stream<TOutput, TTime> stream, Func<int, Stage<TTime>, SinkVertex<TOutput, TTime>> factory, Expression<Func<TOutput, int>> partitionedBy, string name)
        {
            var stage = Foundry.NewStage(stream.Context, factory, name);

            stage.NewInput(stream, (message, vertex) => vertex.OnReceive(message), partitionedBy);

            return stage;
        }

        /// <summary>
        /// Constructs a SinkVertex from an index and stage
        /// </summary>
        /// <param name="index">index</param>
        /// <param name="stage">stage</param>
        public SinkVertex(int index, Stage<TTime> stage)
            : base(index, stage)
        {
        }
    }

    /// <summary>
    /// Vertex with one input, one output, which calls OnRecv for each record, and OnNotify(time) if it invokes ScheduleAt(time).
    /// </summary>
    /// <typeparam name="TInput">Source record type</typeparam>
    /// <typeparam name="TOutput">Result record type</typeparam>
    /// <typeparam name="TTime">Time type</typeparam>
    public abstract class UnaryVertex<TInput, TOutput, TTime> : Vertex<TTime>
        where TTime : Time<TTime>
    {
        /// <summary>
        /// Manages the list of intended recipients, and the buffering and sending of output.
        /// </summary>
        public readonly VertexOutputBuffer<TOutput, TTime> Output;

        /// <summary>
        /// A programmer-supplied action to be performed on each message receipt.
        /// </summary>
        /// <param name="message">Received message</param>
        public abstract void OnReceive(Message<TInput, TTime> message);

        /// <summary>
        /// Factory to produce a stage consisting of these vertices.
        /// </summary>
        /// <param name="stream">Source data stream</param>
        /// <param name="factory">Function from index and stage to a UnaryVertex</param>
        /// <param name="inputPartitionBy">input partitioning requirement</param>
        /// <param name="outputPartitionBy">output partitioning guarantee</param>
        /// <param name="name">console-friendly name</param>
        /// <returns>stream of records from the vertices</returns>
        public static Stream<TOutput, TTime> MakeStage(Stream<TInput, TTime> stream, Func<int, Stage<TTime>, UnaryVertex<TInput, TOutput, TTime>> factory, Expression<Func<TInput, int>> inputPartitionBy, Expression<Func<TOutput, int>> outputPartitionBy, string name)
        {
            var stage = Foundry.NewStage(stream.Context, factory, name);

            var input1 = stage.NewInput(stream, (message, vertex) => vertex.OnReceive(message), inputPartitionBy);
            var output = stage.NewOutput(vertex => vertex.Output, outputPartitionBy);

            return output;
        }

        /// <summary>
        /// Factory to produce a stage consisting of these vertices.
        /// </summary>
        /// <param name="placement">Placement to use for vertices in the stage</param>
        /// <param name="stream">Source data stream</param>
        /// <param name="factory">Function from index and stage to a UnaryVertex</param>
        /// <param name="inputPartitionBy">input partitioning requirement</param>
        /// <param name="outputPartitionBy">output partitioning guarantee</param>
        /// <param name="name">console-friendly name</param>
        /// <returns>stream of records from the vertices</returns>
        public static Stream<TOutput, TTime> MakeStage(Placement placement, Stream<TInput, TTime> stream, Func<int, Stage<TTime>, UnaryVertex<TInput, TOutput, TTime>> factory, Expression<Func<TInput, int>> inputPartitionBy, Expression<Func<TOutput, int>> outputPartitionBy, string name)
        {
            var stage = Foundry.NewStage(placement, stream.Context, factory, name);

            var input1 = stage.NewInput(stream, (message, vertex) => vertex.OnReceive(message), inputPartitionBy);
            var output = stage.NewOutput(vertex => vertex.Output, outputPartitionBy);

            return output;
        }

        /// <summary>
        /// Creates a new UnaryVertex
        /// </summary>
        /// <param name="index">vertex index</param>
        /// <param name="stage">host stage</param>
        public UnaryVertex(int index, Stage<TTime> stage) : base(index, stage)
        {
            this.Output = new VertexOutputBuffer<TOutput, TTime>(this);
        }
    }

    /// <summary>
    /// Vertex with two inputs, one output, which calls OnRecv1/OnRecv2 for each input, and OnNotify(time) if ScheduleAt(time) is ever called.
    /// </summary>
    /// <typeparam name="TInput1">Source 1 record type</typeparam>
    /// <typeparam name="TInput2">Source 2 record type</typeparam>
    /// <typeparam name="TOutput">Result record type</typeparam>
    /// <typeparam name="TTime">Time type</typeparam>
    public abstract class BinaryVertex<TInput1, TInput2, TOutput, TTime> : Vertex<TTime>
        where TTime : Time<TTime>
    {
        /// <summary>
        /// The buffer for output records.
        /// </summary>
        protected VertexOutputBuffer<TOutput, TTime> Output;

        /// <summary>
        /// Called when a message is received on the first input.
        /// </summary>
        /// <param name="message">The message.</param>
        public abstract void OnReceive1(Message<TInput1, TTime> message);

        /// <summary>
        /// Called when a message is received on the second input.
        /// </summary>
        /// <param name="message">The message.</param>
        public abstract void OnReceive2(Message<TInput2, TTime> message);

        /// <summary>
        /// Creates a new stream from the output of a stage of BinaryVertex objects.
        /// </summary>
        /// <param name="stream1">first input stream</param>
        /// <param name="stream2">second input stream</param>
        /// <param name="factory">factory from index and stage to BinaryVertex</param>
        /// <param name="input1PartitionBy">first input partitioning requirement</param>
        /// <param name="input2PartitionBy">second input partitioning requirement</param>
        /// <param name="outputPartitionBy">output partitioning guarantee</param>
        /// <param name="name">friendly name</param>
        /// <returns>the output stream of the corresponding binary stage.</returns>
        public static Stream<TOutput, TTime> MakeStage(Stream<TInput1, TTime> stream1, Stream<TInput2, TTime> stream2, Func<int, Stage<TTime>, BinaryVertex<TInput1, TInput2, TOutput, TTime>> factory, Expression<Func<TInput1, int>> input1PartitionBy, Expression<Func<TInput2, int>> input2PartitionBy, Expression<Func<TOutput, int>> outputPartitionBy, string name)
        {
            var stage = Foundry.NewStage(stream1.Context, factory, name);

            var input1 = stage.NewInput(stream1, (message, vertex) => vertex.OnReceive1(message), input1PartitionBy);
            var input2 = stage.NewInput(stream2, (message, vertex) => vertex.OnReceive2(message), input2PartitionBy);

            var output = stage.NewOutput(vertex => vertex.Output, outputPartitionBy);

            return output;
        }

        /// <summary>
        /// Creates a new stream from the output of a stage of BinaryVertex objects.
        /// </summary>
        /// <param name="placement">Placement to use for vertices in the stage</param>
        /// <param name="stream1">first input stream</param>
        /// <param name="stream2">second input stream</param>
        /// <param name="factory">factory from index and stage to BinaryVertex</param>
        /// <param name="input1PartitionBy">first input partitioning requirement</param>
        /// <param name="input2PartitionBy">second input partitioning requirement</param>
        /// <param name="outputPartitionBy">output partitioning guarantee</param>
        /// <param name="name">friendly name</param>
        /// <returns>the output stream of the corresponding binary stage.</returns>
        public static Stream<TOutput, TTime> MakeStage(Placement placement, Stream<TInput1, TTime> stream1, Stream<TInput2, TTime> stream2, Func<int, Stage<TTime>, BinaryVertex<TInput1, TInput2, TOutput, TTime>> factory, Expression<Func<TInput1, int>> input1PartitionBy, Expression<Func<TInput2, int>> input2PartitionBy, Expression<Func<TOutput, int>> outputPartitionBy, string name)
        {
            var stage = Foundry.NewStage(placement, stream1.Context, factory, name);

            var input1 = stage.NewInput(stream1, (message, vertex) => vertex.OnReceive1(message), input1PartitionBy);
            var input2 = stage.NewInput(stream2, (message, vertex) => vertex.OnReceive2(message), input2PartitionBy);

            var output = stage.NewOutput(vertex => vertex.Output, outputPartitionBy);

            return output;
        }

        /// <summary>
        /// Creates a new BinaryVertex
        /// </summary>
        /// <param name="index">vertex index</param>
        /// <param name="stage">host stage</param>
        public BinaryVertex(int index, Stage<TTime> stage)
            : base(index, stage)
        {
            this.Output = new VertexOutputBuffer<TOutput, TTime>(this);
        }
    }

    #endregion

    #region Buffering Vertices and Stages

    /// <summary>
    /// Vertex with one input, which accumulates inputs and schedules itself for each time seen. 
    /// </summary>
    /// <typeparam name="TOutput">Source record type</typeparam>
    /// <typeparam name="TTime">Time type</typeparam>
    public class SinkBufferingVertex<TOutput, TTime> : SinkVertex<TOutput, TTime>
        where TTime : Time<TTime>
    {
        /// <summary>
        /// Input buffer
        /// </summary>
        protected VertexInputBuffer<TOutput, TTime> Input;

        readonly Action<IEnumerable<TOutput>> Action;

        /// <summary>
        /// Prune internal state to include only events within a rollback frontier
        /// </summary>
        /// <param name="frontier">the frontier to roll back to</param>
        /// <param name="lastFullCheckpoint">the last full checkpoint contained within frontier</param>
        /// <param name="lastIncrementalCheckpoint">the last incremental checkpoint contained within frontier</param>
        public override void RollBackPreservingState(Runtime.Progress.Pointstamp[] frontier, ICheckpoint<TTime> lastFullCheckpoint, ICheckpoint<TTime> lastIncrementalCheckpoint)
        {
            base.RollBackPreservingState(frontier, lastFullCheckpoint, lastIncrementalCheckpoint);

            this.Input.Clear();
        }

        /// <summary>
        /// Called when a message is received.
        /// </summary>
        /// <param name="message">The message.</param>
        public override void OnReceive(Message<TOutput, TTime> message)
        {
            this.Input.OnReceive(message);
        }

        /// <summary>
        /// Indicates that all messages bearing the given time (or earlier) have been delivered.
        /// </summary>
        /// <param name="time">The timestamp of the notification.</param>
        public override void OnNotify(TTime time)
        {
            this.Action(this.Input.GetRecordsAt(time));
        }

        /// <summary>
        /// Creates a new SinkBufferingStage
        /// </summary>
        /// <param name="index">vertex index</param>
        /// <param name="stage">host stage</param>
        /// <param name="action">action on input collection</param>
        public SinkBufferingVertex(int index, Stage<TTime> stage, Expression<Action<IEnumerable<TOutput>>> action)
            : base(index, stage)
        {
            this.Input = new VertexInputBuffer<TOutput, TTime>(this);

            if (action != null)
                this.Action = action.Compile();
        }
    }

    /// <summary>
    /// Vertex with one input, one output, which accumulates inputs and schedules itself for each time seen. 
    /// </summary>
    /// <typeparam name="TInput">Source record type</typeparam>
    /// <typeparam name="TOutput">Result record type</typeparam>
    /// <typeparam name="TTime">Time type</typeparam>
    public class UnaryBufferingVertex<TInput, TOutput, TTime> : UnaryVertex<TInput, TOutput, TTime>
        where TTime : Time<TTime>
    {
        /// <summary>
        /// Input buffer
        /// </summary>
        protected VertexInputBuffer<TInput, TTime> Input;

        readonly Func<IEnumerable<TInput>, IEnumerable<TOutput>> Transformation;

        /// <summary>
        /// Called when a message is received.
        /// </summary>
        /// <param name="message">The message.</param>
        public override void OnReceive(Message<TInput, TTime> message)
        {
            this.Input.OnReceive(message);
        }

        /// <summary>
        /// Indicates that all messages bearing the given time (or earlier) have been delivered.
        /// </summary>
        /// <param name="time">The timestamp of the notification.</param>
        public override void OnNotify(TTime time)
        {
            var records = this.Input.GetRecordsAt(time);
            var output = this.Output.GetBufferForTime(time);
            foreach (var result in this.Transformation(records))
                output.Send(result);
        }

        /// <summary>
        /// Prune internal state to include only events within a rollback frontier
        /// </summary>
        /// <param name="frontier">the frontier to roll back to</param>
        /// <param name="lastFullCheckpoint">the last full checkpoint contained within frontier</param>
        /// <param name="lastIncrementalCheckpoint">the last incremental checkpoint contained within frontier</param>
        public override void RollBackPreservingState(Runtime.Progress.Pointstamp[] frontier, ICheckpoint<TTime> lastFullCheckpoint, ICheckpoint<TTime> lastIncrementalCheckpoint)
        {
            base.RollBackPreservingState(frontier, lastFullCheckpoint, lastIncrementalCheckpoint);

            this.Input.Clear();
        }

        /// <summary>
        /// Constructs a new UnaryBufferingVertex.
        /// </summary>
        /// <param name="index">vertex index</param>
        /// <param name="stage">host stage</param>
        /// <param name="transformation">transformation from input collection to output collection</param>
        public UnaryBufferingVertex(int index, Stage<TTime> stage, Expression<Func<IEnumerable<TInput>, IEnumerable<TOutput>>> transformation)
            : base(index, stage)
        {
            this.Input = new VertexInputBuffer<TInput, TTime>(this);

            if (transformation != null)
                this.Transformation = transformation.Compile();
        }
    }

    /// <summary>
    /// Vertex with two inputs, one output, which accumulates inputs and schedules itself for each time seen.
    /// </summary>
    /// <typeparam name="TInput1">Source 1 record type</typeparam>
    /// <typeparam name="TInput2">Source 2 record type</typeparam>
    /// <typeparam name="TOutput">Result type</typeparam>
    /// <typeparam name="TTime">Time type</typeparam>
    public class BinaryBufferingVertex<TInput1, TInput2, TOutput, TTime> : BinaryVertex<TInput1, TInput2, TOutput, TTime>
        where TTime : Time<TTime>
    {
        /// <summary>
        /// First input buffer
        /// </summary>
        protected VertexInputBuffer<TInput1, TTime> Input1;

        /// <summary>
        /// Second input buffer
        /// </summary>
        protected VertexInputBuffer<TInput2, TTime> Input2;

        readonly Func<IEnumerable<TInput1>, IEnumerable<TInput2>, IEnumerable<TOutput>> Transformation;

        /// <summary>
        /// Called when a message is received on the first input.
        /// </summary>
        /// <param name="message">The message.</param>
        public override void OnReceive1(Message<TInput1, TTime> message)
        {
            this.Input1.OnReceive(message);
        }

        /// <summary>
        /// Called when a message is received on the second input.
        /// </summary>
        /// <param name="message">The message.</param>
        public override void OnReceive2(Message<TInput2, TTime> message)
        {
            this.Input2.OnReceive(message);
        }

        /// <summary>
        /// Indicates that all messages bearing the given time (or earlier) have been delivered.
        /// </summary>
        /// <param name="time">The timestamp of the notification.</param>
        public override void OnNotify(TTime time)
        {
            var records1 = this.Input1.GetRecordsAt(time);
            var records2 = this.Input2.GetRecordsAt(time);

            var outputBuffer = this.Output.GetBufferForTime(time);

            foreach (var result in this.Transformation(records1, records2))
                outputBuffer.Send(result);
        }

        /// <summary>
        /// Prune internal state to include only events within a rollback frontier
        /// </summary>
        /// <param name="frontier">the frontier to roll back to</param>
        /// <param name="lastFullCheckpoint">the last full checkpoint contained within frontier</param>
        /// <param name="lastIncrementalCheckpoint">the last incremental checkpoint contained within frontier</param>
        public override void RollBackPreservingState(Runtime.Progress.Pointstamp[] frontier, ICheckpoint<TTime> lastFullCheckpoint, ICheckpoint<TTime> lastIncrementalCheckpoint)
        {
            base.RollBackPreservingState(frontier, lastFullCheckpoint, lastIncrementalCheckpoint);

            this.Input1.Clear();
            this.Input2.Clear();
        }

        /// <summary>
        /// Constructs a new BinaryBufferingVertex
        /// </summary>
        /// <param name="index">vertex index</param>
        /// <param name="stage">host stage</param>
        /// <param name="transformation">transformation from two input collections to an output collection</param>
        public BinaryBufferingVertex(int index, Stage<TTime> stage, Expression<Func<IEnumerable<TInput1>, IEnumerable<TInput2>, IEnumerable<TOutput>>> transformation)
            : base(index, stage)
        {
            this.Input1 = new VertexInputBuffer<TInput1, TTime>(this);
            this.Input2 = new VertexInputBuffer<TInput2, TTime>(this);

            if (transformation != null)
                this.Transformation = transformation.Compile();
        }
    }

    #endregion

    /// <summary>
    /// Methods to instantiate stages based on factories.
    /// </summary>
    public static class Foundry
    {
        /// <summary>
        /// Creates a new stage with one input and no outputs.
        /// </summary>
        /// <typeparam name="TOutput">Source type</typeparam>
        /// <typeparam name="TTime">Time type</typeparam>
        /// <param name="source">Source of records</param>
        /// <param name="factory">Vertex factory</param>
        /// <param name="inputPartitionBy">Partitioning requirement</param>
        /// <param name="name">Descriptive name</param>
        /// <returns>the sink stage</returns>
        public static Stage<SinkVertex<TOutput,TTime>, TTime> NewSinkStage<TOutput, TTime>(this Stream<TOutput, TTime> source, Func<int, Stage<TTime>, SinkVertex<TOutput, TTime>> factory, Expression<Func<TOutput, int>> inputPartitionBy, string name)
            where TTime : Time<TTime>
        {
            return SinkVertex<TOutput, TTime>.MakeStage(source, factory, inputPartitionBy, name);
        }

        /// <summary>
        /// Creates a stage with one input and one output.
        /// </summary>
        /// <typeparam name="TInput">Source type</typeparam>
        /// <typeparam name="TOutput">Result type</typeparam>
        /// <typeparam name="TTime">Time type</typeparam>
        /// <param name="source">Source of records</param>
        /// <param name="factory">Vertex factory</param>
        /// <param name="inputPartitionBy">Partitioning requirement</param>
        /// <param name="outputPartitionBy">Partitioning guarantee</param>
        /// <param name="name">Descriptive name</param>
        /// <returns>The stage's output</returns>
        public static Stream<TOutput, TTime> NewUnaryStage<TInput, TOutput, TTime>(this Stream<TInput, TTime> source, Func<int, Stage<TTime>, UnaryVertex<TInput, TOutput, TTime>> factory, Expression<Func<TInput, int>> inputPartitionBy, Expression<Func<TOutput, int>> outputPartitionBy, string name)
            where TTime : Time<TTime>
        {
            return UnaryVertex<TInput, TOutput, TTime>.MakeStage(source, factory, inputPartitionBy, outputPartitionBy, name);
        }

        /// <summary>
        /// Creates a new stage with two inputs and one output
        /// </summary>
        /// <typeparam name="TInput1">First source type</typeparam>
        /// <typeparam name="TInput2">Second source type</typeparam>
        /// <typeparam name="TOutput">Result type</typeparam>
        /// <typeparam name="TTime">Time type</typeparam>
        /// <param name="source">First source of records</param>
        /// <param name="other">Second source of records</param>
        /// <param name="factory">Vertex factory</param>
        /// <param name="input1PartitionBy">First partitioning requirement</param>
        /// <param name="input2PartitionBy">Second partitioning requirement</param>
        /// <param name="outputPartitionBy">Partitioning guarantee</param>
        /// <param name="name">Descriptive name</param>
        /// <returns>The stage's output</returns>
        public static Stream<TOutput, TTime> NewBinaryStage<TInput1, TInput2, TOutput, TTime>(this Stream<TInput1, TTime> source, Stream<TInput2, TTime> other, Func<int, Stage<TTime>, BinaryVertex<TInput1, TInput2, TOutput, TTime>> factory, Expression<Func<TInput1, int>> input1PartitionBy, Expression<Func<TInput2, int>> input2PartitionBy, Expression<Func<TOutput, int>> outputPartitionBy, string name)
            where TTime : Time<TTime>
        {
            return BinaryVertex<TInput1, TInput2, TOutput, TTime>.MakeStage(source, other, factory, input1PartitionBy, input2PartitionBy, outputPartitionBy, name);
        }

        /// <summary>
        /// Creates a stage from a vertex factory
        /// </summary>
        /// <typeparam name="TVertex">Vertex type</typeparam>
        /// <typeparam name="TTime">Time type</typeparam>
        /// <param name="context">Time context</param>
        /// <param name="factory">Vertex factory</param>
        /// <param name="name">Descriptive name</param>
        /// <returns>Constructed stage</returns>
        public static Stage<TVertex, TTime> NewStage<TVertex, TTime>(StreamContext context, Func<int, Stage<TTime>, TVertex> factory, string name)
            where TTime : Time<TTime>
            where TVertex : Vertex<TTime>
        {
            return new Stage<TVertex, TTime>(context, factory, name);
        }

        /// <summary>
        /// Creates a stage from a vertex factory
        /// </summary>
        /// <typeparam name="TVertex">Vertex type</typeparam>
        /// <typeparam name="TTime">Time type</typeparam>
        /// <param name="placement">Placement</param>
        /// <param name="context">Time context</param>
        /// <param name="factory">Vertex factory</param>
        /// <param name="name">Descriptive name</param>
        /// <returns>Constructed stage</returns>
        public static Stage<TVertex, TTime> NewStage<TVertex, TTime>(Placement placement, StreamContext context, Func<int, Stage<TTime>, TVertex> factory, string name)
            where TTime : Time<TTime>
            where TVertex : Vertex<TTime>
        {
            return new Stage<TVertex, TTime>(placement, context.Computation, Stage.OperatorType.Default, factory, name);
        }
    }

    /// <summary>
    /// Methods to create and apply LINQ expressions on a time-by-time basis.
    /// </summary>
    public static class ExtensionMethods
    {
        /// <summary>
        /// Constructs a stream from an input and a function to apply to collections on a time-by-time basis.
        /// </summary>
        /// <typeparam name="TInput">Input type</typeparam>
        /// <typeparam name="TOutput">Output type</typeparam>
        /// <typeparam name="TTime">Time type</typeparam>
        /// <param name="stream">input stream</param>
        /// <param name="keyFunction">partitioning requirement</param>
        /// <param name="transformation">collection transformation</param>
        /// <param name="name">descriptive name</param>
        /// <returns>a new stream representing independent application of transformation to each time in the input stream</returns>
        public static Stream<TOutput, TTime> UnaryExpression<TInput, TOutput, TTime>(this Stream<TInput, TTime> stream, Expression<Func<TInput, int>> keyFunction, Expression<Func<IEnumerable<TInput>, IEnumerable<TOutput>>> transformation, string name)
            where TTime : Time<TTime>
        {
            return UnaryBufferingVertex<TInput, TOutput, TTime>.MakeStage(stream, (i, v) => new UnaryBufferingVertex<TInput, TOutput, TTime>(i, v, transformation), keyFunction, null, name);
        }

        /// <summary>
        /// Constructs a stream from two inputs and a function to apply to collections on a time-by-time basis.
        /// </summary>
        /// <typeparam name="TInput1">First input type</typeparam>
        /// <typeparam name="TInput2">Second input type</typeparam>
        /// <typeparam name="TOutput">Output type</typeparam>
        /// <typeparam name="TTime">Time type</typeparam>
        /// <param name="stream1">first input stream</param>
        /// <param name="stream2">second input stream</param>
        /// <param name="keyFunction1">first partitioning requirement</param>
        /// <param name="keyFunction2">second partitioning requirement</param>
        /// <param name="transformation">collection transformation</param>
        /// <param name="name">descriptive name</param>
        /// <returns>a new stream representing independent application of transformation to each time in the input streams</returns>
        public static Stream<TOutput, TTime> BinaryExpression<TInput1, TInput2, TOutput, TTime>(this Stream<TInput1, TTime> stream1, Stream<TInput2, TTime> stream2, Expression<Func<TInput1, int>> keyFunction1, Expression<Func<TInput2, int>> keyFunction2, Expression<Func<IEnumerable<TInput1>, IEnumerable<TInput2>, IEnumerable<TOutput>>> transformation, string name)
            where TTime : Time<TTime>
        {
            return BinaryBufferingVertex<TInput1, TInput2, TOutput, TTime>.MakeStage(stream1, stream2, (i, v) => new BinaryBufferingVertex<TInput1, TInput2, TOutput, TTime>(i, v, transformation), keyFunction1, keyFunction2, null, name);
        }
    }
}
