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

﻿using System;
using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;
using System.Text;
using Naiad;
using Naiad.Dataflow.Channels;
using Naiad.CodeGeneration;
using Naiad.Runtime.Controlling;
using Naiad.DataStructures;
using Naiad.FaultTolerance;

using Naiad.Dataflow;
using Naiad.Scheduling;

namespace Naiad.Frameworks
{
    /// <summary>
    /// A repository for input records, stored indexed by time. Calls NotifyAt on record receipt.
    /// </summary>
    /// <typeparam name="TRecord">Record type</typeparam>
    /// <typeparam name="TTime">Time type</typeparam>
    public class VertexInputBuffer<TRecord, TTime> : Naiad.Dataflow.VertexInput<TRecord, TTime>, ICheckpointable
        where TTime : Time<TTime>
    {
        public bool LoggingEnabled { get { return false; } set { throw new NotImplementedException("Logging for RecvFiberBank"); } }

        // permissive re-entrancy because we only ever queue up records.
        public int AvailableEntrancy { get { return 1; } set { } }

        protected Dictionary<TTime, SpinedList<TRecord>> recordsToProcess = new Dictionary<TTime, SpinedList<TRecord>>();

        private readonly Vertex<TTime> Shard;

        public IEnumerable<TRecord> GetRecordsAt(TTime time)
        {
            var result = default(SpinedList<TRecord>);

            if (recordsToProcess.TryGetValue(time, out result))
                recordsToProcess.Remove(time);
            else
                result = new SpinedList<TRecord>();

            return result.AsEnumerable();
        }

        public VertexInputBuffer(Vertex<TTime> shard)
        {
            this.Shard = shard;
        }

        public Naiad.Dataflow.Vertex Vertex { get { return this.Shard; } }

        public void Flush() { this.Shard.Flush(); }

        public void RecordReceived(Pair<TRecord, TTime> record, RemotePostbox sender)
        {
            SpinedList<TRecord> list;
            if (!recordsToProcess.TryGetValue(record.v2, out list))
            {
                list = new SpinedList<TRecord>();
                recordsToProcess.Add(record.v2, list);

                this.Shard.NotifyAt(record.v2);
            }

            list.Add(record.v1);
        }

        public void MessageReceived(Message<Pair<TRecord, TTime>> message, RemotePostbox sender)
        {
            for (int i = 0; i < message.length; ++i)
            {
                var record = message.payload[i];

                SpinedList<TRecord> list;
                if (!recordsToProcess.TryGetValue(record.v2, out list))
                {
                    list = new SpinedList<TRecord>();
                    recordsToProcess.Add(record.v2, list);

                    this.Shard.NotifyAt(record.v2);
                }

                list.Add(record.v1);
            }
        }

        private AutoSerializedMessageDecoder<TRecord, TTime> decoder = null;
        public void SerializedMessageReceived(SerializedMessage message, RemotePostbox sender)
        {
            if (this.decoder == null) this.decoder = new AutoSerializedMessageDecoder<TRecord, TTime>();
            foreach (Pair<TRecord, TTime> record in this.decoder.Elements(message))
                this.RecordReceived(record, sender);
        }

        public override string ToString()
        {
            return string.Format("<{0}L>", this.Shard.Stage.StageId);
        }

        public void Restore(NaiadReader reader)
        {
            var timeSerializer = AutoSerialization.GetSerializer<TTime>();
            var valueSerializer = AutoSerialization.GetSerializer<TRecord>();

            int readCount = reader.Read(PrimitiveSerializers.Int32);
            for (int i = 0; i < readCount; ++i)
            {
                TTime time = reader.Read(timeSerializer);
                SpinedList<TRecord> records = new SpinedList<TRecord>();
                records.Restore(reader, valueSerializer);
                this.recordsToProcess[time] = records;
            }
        }

        public void Checkpoint(NaiadWriter writer)
        {
            var timeSerializer = AutoSerialization.GetSerializer<TTime>();
            var valueSerializer = AutoSerialization.GetSerializer<TRecord>();

            writer.Write(this.recordsToProcess.Count, PrimitiveSerializers.Int32);
            foreach (KeyValuePair<TTime, SpinedList<TRecord>> kvp in this.recordsToProcess)
            {
                writer.Write(kvp.Key, timeSerializer);
                kvp.Value.Checkpoint(writer, valueSerializer);
            }

        }

        public bool Stateful { get { throw new NotImplementedException(); } }
    }

    /// <summary>
    /// A helper class which both manages incoming requests to read from an output, and buffers sends along that output.
    /// </summary>
    /// <typeparam name="TRecord">Record type</typeparam>
    /// <typeparam name="TTime">Time type</typeparam>
    public class VertexOutputBuffer<TRecord, TTime> : Naiad.Dataflow.VertexOutput<TRecord, TTime>
        where TTime : Time<TTime>
    {
        private bool mustFlush = false;
        private Dataflow.Channels.SendWire<TRecord, TTime>[] SendChannels;
        public Message<Pair<TRecord, TTime>> Buffer;

        private bool loggingEnabled = false;
        public bool LoggingEnabled { get { return this.loggingEnabled; } set { this.loggingEnabled = value; } }

        private void LogMessage(Message<Pair<TRecord, TTime>> message)
        {
            var encoder = new AutoSerializedMessageEncoder<TRecord, TTime>(this.Vertex.VertexId, 0, DummyBufferPool<byte>.Pool, this.Vertex.Stage.InternalGraphManager.Controller.Configuration.SendPageSize, AutoSerializationMode.OneTimePerMessage);
            encoder.CompletedMessage += (o, a) =>
            {
                ArraySegment<byte> messageSegment = a.Segment.ToArraySegment();
                this.Vertex.LoggingOutput.Write(messageSegment.Array, messageSegment.Offset, messageSegment.Count);
            };

            for (int i = 0; i < message.length; ++i)
                encoder.Write(message.payload[i]);

            encoder.Flush();
        }

        /// <summary>
        /// The size in bytes of the largest single record that can be sent in a (default-sized) serialized message on this channel.
        /// Writes of larger objects will succeed nevertheless, but may be less efficient (usually in the form of more allocations).
        /// N.B. Returns int.MaxValue in the case that serialization will not occur on this channel, so be careful when you use the return value.
        /// </summary>
        public int RecordSizeHint
        {
            get
            {
                return this.SendChannels.Min(x => x.RecordSizeHint);
            }
        }

        private Queue<Message<Pair<TRecord, TTime>>> SpareBuffers; // used for reentrancy.

        public void AddReceiver(Dataflow.Channels.SendWire<TRecord, TTime> sendFiber)
        {
            SendChannels = SendChannels.Concat(new[] { sendFiber }).ToArray();
        }

        public void Send(TRecord record, TTime time)
        {
            mustFlush = true;

            Buffer.payload[Buffer.length++] = new Pair<TRecord, TTime>(record, time);

            if (Buffer.length == Buffer.payload.Length)
                SendBuffer();
        }

        public void Send(Message<Pair<TRecord, TTime>> message)
        {
            if (this.loggingEnabled)
                this.LogMessage(Buffer);
            mustFlush = true;
            for (int i = 0; i < SendChannels.Length; i++)
                SendChannels[i].Send(message);
        }

        public void SendBuffer()
        {
            if (Buffer.length > 0)
            {

                var temp = Buffer;
                Buffer.Disable();

                if (SpareBuffers.Count > 0)
                    Buffer = SpareBuffers.Dequeue();
                else
                    Buffer.Enable();

                Send(temp);

                temp.length = 0;
                SpareBuffers.Enqueue(temp);
            }
        }

        public void Flush()
        {
            if (Buffer.length > 0)
            {
                var temp = Buffer;
                Buffer.Disable();

                if (SpareBuffers.Count > 0)
                    Buffer = SpareBuffers.Dequeue();
                else
                    Buffer.Enable();

                Send(temp);

                temp.length = 0;
                SpareBuffers.Enqueue(temp);
            }

            if (mustFlush)  // avoids perpetuating cycle of flushing.
            {
                mustFlush = false;
                for (int i = 0; i < SendChannels.Length; i++)
                    SendChannels[i].Flush();
            }

            System.Diagnostics.Debug.Assert(this.Buffer.length == 0);
        }

        public Vertex Vertex { get { return this.shard; } }

        private readonly Vertex shard;

        public VertexOutputBuffer(Vertex shard)
        {
            this.shard = shard;
            this.SendChannels = new Dataflow.Channels.SendWire<TRecord, TTime>[] { };
            this.Buffer = new Message<Pair<TRecord, TTime>>();
            this.Buffer.Enable();
            shard.AddOnFlushAction(() => this.Flush());

            this.SpareBuffers = new Queue<Message<Pair<TRecord, TTime>>>();
        }
    }

    #region Streaming Vertices and Stages

    /// <summary>
    /// Vertex with one input, no outputs, which accumulates inputs and schedules itself for each time seen.
    /// </summary>
    /// <typeparam name="TOutput">Source type</typeparam>
    /// <typeparam name="TTime">Time type</typeparam>
    public abstract class SinkVertex<TOutput, TTime> : Vertex<TTime>
        where TTime : Time<TTime>
    {
        public abstract void MessageReceived(Message<Pair<TOutput, TTime>> record);

        public static void MakeStage(Stream<TOutput, TTime> stream, Func<int, Stage<TTime>, SinkVertex<TOutput, TTime>> factory, Expression<Func<TOutput, int>> partitionedBy, string name)
        {
            var stage = Foundry.NewStage(stream.Context, factory, name);

            stage.NewInput(stream, (message, shard) => shard.MessageReceived(message), partitionedBy);
        }

        public SinkVertex(int index, Stage<TTime> stage)
            : base(index, stage)
        {
        }
    }

    /// <summary>
    /// Vertex with one input, one output, which calls OnRecv for each record, and OnDone(time) if it invokes ScheduleAt(time).
    /// </summary>
    /// <typeparam name="TInput">Source record type</typeparam>
    /// <typeparam name="TOutput">Result record type</typeparam>
    /// <typeparam name="TTime">Time type</typeparam>
    public abstract class UnaryVertex<TInput, TOutput, TTime> : Vertex<TTime>
        where TTime : Time<TTime>
    {
        protected readonly VertexOutputBuffer<TOutput, TTime> Output;

        public abstract void MessageReceived(Message<Pair<TInput, TTime>> message);

        public static Stream<TOutput, TTime> MakeStage(Stream<TInput, TTime> stream, Func<int, Stage<TTime>, UnaryVertex<TInput, TOutput, TTime>> factory, Expression<Func<TInput, int>> inputPartitionBy, Expression<Func<TOutput, int>> outputPartitionBy, string name)
        {
            var stage = Foundry.NewStage(stream.Context, factory, name);

            var input1 = stage.NewInput(stream, (message, shard) => shard.MessageReceived(message), inputPartitionBy);
            var output = stage.NewOutput(shard => shard.Output, outputPartitionBy);

            return output;
        }

        public UnaryVertex(int index, Stage<TTime> stage)
            : base(index, stage)
        {
            this.Output = new VertexOutputBuffer<TOutput, TTime>(this);
        }
    }

    /// <summary>
    /// Vertex with two inputs, one output, which calls OnRecv1/OnRecv2 for each input, and OnDone(time) if ScheduleAt(time) is ever called.
    /// </summary>
    /// <typeparam name="TInput1">Source 1 record type</typeparam>
    /// <typeparam name="TInput2">Source 2 record type</typeparam>
    /// <typeparam name="TOutput">Result record type</typeparam>
    /// <typeparam name="TTime">Time type</typeparam>
    public abstract class BinaryVertex<TInput1, TInput2, TOutput, TTime> : Vertex<TTime>
        where TTime : Time<TTime>
    {
        protected VertexOutputBuffer<TOutput, TTime> Output;

        public abstract void MessageReceived1(Message<Pair<TInput1, TTime>> message);
        public abstract void MessageReceived2(Message<Pair<TInput2, TTime>> message);

        public static Stream<TOutput, TTime> MakeStage(Stream<TInput1, TTime> stream1, Stream<TInput2, TTime> stream2, Func<int, Stage<TTime>, BinaryVertex<TInput1, TInput2, TOutput, TTime>> factory, Expression<Func<TInput1, int>> input1PartitionBy, Expression<Func<TInput2, int>> input2PartitionBy, Expression<Func<TOutput, int>> outputPartitionBy, string name)
        {
            var stage = Foundry.NewStage(stream1.Context, factory, name);

            var input1 = stage.NewInput(stream1, (message, shard) => shard.MessageReceived1(message), input1PartitionBy);
            var input2 = stage.NewInput(stream2, (message, shard) => shard.MessageReceived2(message), input2PartitionBy);

            var output = stage.NewOutput(shard => shard.Output, outputPartitionBy);

            return output;
        }

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
        protected VertexInputBuffer<TOutput, TTime> Input;

        readonly Action<IEnumerable<TOutput>> Action;

        public override void MessageReceived(Message<Pair<TOutput, TTime>> record)
        {
            this.Input.MessageReceived(record, new RemotePostbox());
        }

        public override void OnDone(TTime time)
        {
            this.Action(this.Input.GetRecordsAt(time));
        }

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
        protected VertexInputBuffer<TInput, TTime> Input;

        readonly Func<IEnumerable<TInput>, IEnumerable<TOutput>> Transformation;

        public override void MessageReceived(Message<Pair<TInput, TTime>> message)
        {
            this.Input.MessageReceived(message, new RemotePostbox());
        }

        public override void OnDone(TTime time)
        {
            var records = this.Input.GetRecordsAt(time);

            foreach (var result in this.Transformation(records))
                this.Output.Send(result, time);
        }

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
        protected VertexInputBuffer<TInput1, TTime> Input1;
        protected VertexInputBuffer<TInput2, TTime> Input2;

        readonly Func<IEnumerable<TInput1>, IEnumerable<TInput2>, IEnumerable<TOutput>> Transformation;

        public override void MessageReceived1(Message<Pair<TInput1, TTime>> message)
        {
            this.Input1.MessageReceived(message, new RemotePostbox());
        }

        public override void MessageReceived2(Message<Pair<TInput2, TTime>> message)
        {
            this.Input2.MessageReceived(message, new RemotePostbox());
        }

        public override void OnDone(TTime time)
        {
            var records1 = this.Input1.GetRecordsAt(time);
            var records2 = this.Input2.GetRecordsAt(time);

            foreach (var result in this.Transformation(records1, records2))
                this.Output.Send(result, time);
        }

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
        public static void NewStage<TOutput, TTime>(this Stream<TOutput, TTime> source, Func<int, Stage<TTime>, SinkVertex<TOutput, TTime>> factory, Expression<Func<TOutput, int>> inputPartitionBy, string name)
            where TTime : Time<TTime>
        {
            SinkVertex<TOutput, TTime>.MakeStage(source, factory, inputPartitionBy, name);
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
        public static Stream<TOutput, TTime> NewStage<TInput, TOutput, TTime>(this Stream<TInput, TTime> source, Func<int, Stage<TTime>, UnaryVertex<TInput, TOutput, TTime>> factory, Expression<Func<TInput, int>> inputPartitionBy, Expression<Func<TOutput, int>> outputPartitionBy, string name)
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
        public static Stream<TOutput, TTime> NewStage<TInput1, TInput2, TOutput, TTime>(this Stream<TInput1, TTime> source, Stream<TInput2, TTime> other, Func<int, Stage<TTime>, BinaryVertex<TInput1, TInput2, TOutput, TTime>> factory, Expression<Func<TInput1, int>> input1PartitionBy, Expression<Func<TInput2, int>> input2PartitionBy, Expression<Func<TOutput, int>> outputPartitionBy, string name)
            where TTime : Time<TTime>
        {
            return BinaryVertex<TInput1, TInput2, TOutput, TTime>.MakeStage(source, other, factory, input1PartitionBy, input2PartitionBy, outputPartitionBy, name);
        }

        /// <summary>
        /// Creates a stage from a shard factory
        /// </summary>
        /// <typeparam name="TVertex">Shard type</typeparam>
        /// <typeparam name="TTime">Time type</typeparam>
        /// <param name="context">Time context</param>
        /// <param name="factory">Shard factory</param>
        /// <param name="name">Descriptive name</param>
        /// <returns>Constructed stage</returns>
        public static Stage<TVertex, TTime> NewStage<TVertex, TTime>(OpaqueTimeContext<TTime> context, Func<int, Stage<TTime>, TVertex> factory, string name)
            where TTime : Time<TTime>
            where TVertex : Vertex<TTime>
        {
            return new Stage<TVertex, TTime>(context, factory, name);
        }

        /// <summary>
        /// Creates a stage from a shard factory
        /// </summary>
        /// <typeparam name="TVertex">Shard type</typeparam>
        /// <typeparam name="TTime">Time type</typeparam>
        /// <param name="context">Time context</param>
        /// <param name="factory">Shard factory</param>
        /// <param name="name">Descriptive name</param>
        /// <returns>Constructed stage</returns>
        public static Stage<TVertex, TTime> NewStage<TVertex, TTime>(Placement placement, OpaqueTimeContext<TTime> context, Func<int, Stage<TTime>, TVertex> factory, string name)
            where TTime : Time<TTime>
            where TVertex : Vertex<TTime>
        {
            return new Stage<TVertex, TTime>(placement, context, Stage.OperatorType.Default, factory, name);
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
