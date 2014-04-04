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
using System.Linq.Expressions;
using System.Text;
using Microsoft.Research.Naiad;
using Microsoft.Research.Naiad.Dataflow.Channels;
using Microsoft.Research.Naiad.CodeGeneration;
using Microsoft.Research.Naiad.Runtime.Controlling;
using Microsoft.Research.Naiad.DataStructures;
using Microsoft.Research.Naiad.FaultTolerance;

using Microsoft.Research.Naiad.Dataflow;
using Microsoft.Research.Naiad.Scheduling;

namespace Microsoft.Research.Naiad.Frameworks
{
    /// <summary>
    /// A repository for input records, stored indexed by time. Calls NotifyAt on record receipt.
    /// </summary>
    /// <typeparam name="TRecord">Record type</typeparam>
    /// <typeparam name="TTime">Time type</typeparam>
    public class VertexInputBuffer<TRecord, TTime> : VertexInput<TRecord, TTime>, ICheckpointable
        where TTime : Time<TTime>
    {
        public bool LoggingEnabled { get { return false; } set { throw new NotImplementedException("Logging for RecvFiberBank"); } }

        // permissive re-entrancy because we only ever queue up records.
        public int AvailableEntrancy { get { return 1; } set { } }

        protected Dictionary<TTime, SpinedList<TRecord>> recordsToProcess = new Dictionary<TTime, SpinedList<TRecord>>();

        private readonly Vertex<TTime> vertex;

        /// <summary>
        /// Enumerates (and destroys) input records associated with a specified time.
        /// </summary>
        /// <param name="time">time</param>
        /// <returns></returns>
        public IEnumerable<TRecord> GetRecordsAt(TTime time)
        {
            var result = default(SpinedList<TRecord>);

            if (recordsToProcess.TryGetValue(time, out result))
                recordsToProcess.Remove(time);
            else
                result = new SpinedList<TRecord>();

            return result.AsEnumerable();
        }

        public VertexInputBuffer(Vertex<TTime> vertex)
        {
            this.vertex = vertex;
        }

        public Microsoft.Research.Naiad.Dataflow.Vertex Vertex { get { return this.vertex; } }

        public void Flush() { this.vertex.Flush(); }

        public void OnReceive(Message<TRecord, TTime> message, RemotePostbox sender)
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

        private AutoSerializedMessageDecoder<TRecord, TTime> decoder = null;

        public void SerializedMessageReceived(SerializedMessage serializedMessage, RemotePostbox from)
        {
            System.Diagnostics.Debug.Assert(this.AvailableEntrancy >= -1);
            if (this.decoder == null) this.decoder = new AutoSerializedMessageDecoder<TRecord, TTime>(this.Vertex.CodeGenerator);

            foreach (Message<TRecord, TTime> message in this.decoder.AsTypedMessages(serializedMessage))
            {
                this.OnReceive(message, from);
                message.Release();
            }
        }

        public override string ToString()
        {
            return string.Format("<{0}L>", this.vertex.Stage.StageId);
        }

        public void Restore(NaiadReader reader)
        {
            var timeSerializer = this.Vertex.CodeGenerator.GetSerializer<TTime>();
            var valueSerializer = this.Vertex.CodeGenerator.GetSerializer<TRecord>();
            var intSerializer = this.Vertex.CodeGenerator.GetSerializer<Int32>();

            int readCount = reader.Read(intSerializer);
            for (int i = 0; i < readCount; ++i)
            {
                TTime time = reader.Read(timeSerializer);
                SpinedList<TRecord> records = new SpinedList<TRecord>();
                records.Restore(reader, valueSerializer, intSerializer);
                this.recordsToProcess[time] = records;
            }
        }

        public void Checkpoint(NaiadWriter writer)
        {
            var timeSerializer = this.Vertex.CodeGenerator.GetSerializer<TTime>();
            var valueSerializer = this.Vertex.CodeGenerator.GetSerializer<TRecord>();
            var intSerializer = this.Vertex.CodeGenerator.GetSerializer<Int32>();

            writer.Write(this.recordsToProcess.Count, intSerializer);
            foreach (KeyValuePair<TTime, SpinedList<TRecord>> kvp in this.recordsToProcess)
            {
                writer.Write(kvp.Key, timeSerializer);
                kvp.Value.Checkpoint(writer, valueSerializer, intSerializer);
            }

        }

        public bool Stateful { get { throw new NotImplementedException(); } }
    }

    public class VertexOutputBuffer<TRecord, TTime> : Microsoft.Research.Naiad.Dataflow.VertexOutput<TRecord, TTime>
        where TTime : Time<TTime>
    {
        private Dataflow.Channels.SendWire<TRecord, TTime>[] sendChannels;

        private readonly Dictionary<TTime, VertexOutputBufferPerTime<TRecord, TTime>> Buffers;

        private bool MustFlushChannels;

        #region logging

        private bool loggingEnabled = false;
        public bool LoggingEnabled { get { return this.loggingEnabled; } set { this.loggingEnabled = value; } }

        private void LogMessage(Message<TRecord, TTime> message)
        {
            var encoder = new AutoSerializedMessageEncoder<TRecord, TTime>(this.Vertex.VertexId, 0, DummyBufferPool<byte>.Pool, this.Vertex.Stage.InternalGraphManager.Controller.Configuration.SendPageSize, this.Vertex.CodeGenerator);
            encoder.CompletedMessage += (o, a) =>
            {
                ArraySegment<byte> messageSegment = a.Segment.ToArraySegment();
                this.Vertex.LoggingOutput.Write(messageSegment.Array, messageSegment.Offset, messageSegment.Count);
            };

            for (int i = 0; i < message.length; ++i)
                encoder.Write(message.payload[i].PairWith(message.time));

            encoder.Flush();
        }

        #endregion

        public void AddReceiver(Dataflow.Channels.SendWire<TRecord, TTime> sendFiber)
        {
            this.sendChannels = this.sendChannels.Concat(new[] { sendFiber }).ToArray();
        }

        /// <summary>
        /// Sends a fully formed message
        /// </summary>
        /// <param name="message">message</param>
        public void Send(Message<TRecord, TTime> message)
        {
            this.MustFlushChannels = true;
            for (int i = 0; i < this.sendChannels.Length; i++)
                this.sendChannels[i].Send(message);
        }

        /// <summary>
        /// Flushes the associated buffers
        /// </summary>
        public void Flush()
        {
            // the existence of buffers is the "mustFlushChannels == true" test.
            if (this.Buffers.Count > 0)
            {
                while (this.Buffers.Count > 0)
                {
                    var key = this.Buffers.Keys.First();

                    var buffer = this.Buffers[key];
                    this.Buffers.Remove(key);

                    buffer.SendBuffer();
                }
            }

            if (this.MustFlushChannels)
            {
                this.MustFlushChannels = false;
                for (int i = 0; i < this.sendChannels.Length; i++)
                    this.sendChannels[i].Flush();
            }
        }

        /// <summary>
        /// Returns a buffer with a fixed time, with which records may be sent
        /// </summary>
        /// <param name="time">time</param>
        /// <returns>new output buffer</returns>
        public VertexOutputBufferPerTime<TRecord, TTime> GetBufferForTime(TTime time)
        {
            if (!this.Buffers.ContainsKey(time))
                this.Buffers.Add(time, new VertexOutputBufferPerTime<TRecord, TTime>(this, time));

            return this.Buffers[time];
        }

        public Vertex Vertex { get { return this.vertex; } }

        private readonly Vertex vertex;

        public VertexOutputBuffer(Vertex vertex)
        {
            this.vertex = vertex;

            this.sendChannels = new Dataflow.Channels.SendWire<TRecord, TTime>[] { };
            this.Buffers = new Dictionary<TTime, VertexOutputBufferPerTime<TRecord, TTime>>();

            vertex.AddOnFlushAction(() => this.Flush());
        }
    }

    public class VertexOutputBufferPerTime<TRecord, TTime>
        where TTime : Time<TTime>
    {
        private readonly VertexOutputBuffer<TRecord, TTime> parent;
        private readonly TTime Time;

        private Message<TRecord, TTime> Buffer;

        //[System.Runtime.CompilerServices.MethodImplAttribute(System.Runtime.CompilerServices.MethodImplOptions.AggressiveInlining)]
        /// <summary>
        /// Sends a record with the bound time.
        /// </summary>
        /// <param name="record">record to send</param>
        public void Send(TRecord record)
        {
            Buffer.payload[Buffer.length++] = record;
            if (Buffer.length == Buffer.payload.Length)
                SendBuffer();
        }

        internal void SendBuffer()
        {
            if (Buffer.length > 0)
            {
                var temp = Buffer;

                this.Buffer = new Message<TRecord, TTime>();
                this.Buffer.Allocate();
                this.Buffer.time = this.Time;

                parent.Send(temp);

                temp.Release();
            }
        }

        /// <summary>
        /// Constructions a new VertexOutputBufferPerTime from its parent and a fixed time
        /// </summary>
        /// <param name="parent">parent</param>
        /// <param name="time">time</param>
        public VertexOutputBufferPerTime(VertexOutputBuffer<TRecord, TTime> parent, TTime time)
        {
            this.parent = parent;
            this.Time = time;

            this.Buffer = new Message<TRecord, TTime>();
            this.Buffer.Allocate();
            this.Buffer.time = time;
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
        public abstract void OnReceive(Message<TOutput, TTime> record);

        public static void MakeStage(Stream<TOutput, TTime> stream, Func<int, Stage<TTime>, SinkVertex<TOutput, TTime>> factory, Expression<Func<TOutput, int>> partitionedBy, string name)
        {
            var stage = Foundry.NewStage(stream.Context, factory, name);

            stage.NewInput(stream, (message, vertex) => vertex.OnReceive(message), partitionedBy);
        }

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
        protected readonly VertexOutputBuffer<TOutput, TTime> Output;

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
        /// Creates a new UnaryVertex
        /// </summary>
        /// <param name="index">vertex index</param>
        /// <param name="stage">host stage</param>
        public UnaryVertex(int index, Stage<TTime> stage)
            : base(index, stage)
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
        protected VertexOutputBuffer<TOutput, TTime> Output;

        public abstract void OnReceive1(Message<TInput1, TTime> message);
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
        protected VertexInputBuffer<TOutput, TTime> Input;

        readonly Action<IEnumerable<TOutput>> Action;

        public override void OnReceive(Message<TOutput, TTime> record)
        {
            this.Input.OnReceive(record, new RemotePostbox());
        }

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
        protected VertexInputBuffer<TInput, TTime> Input;

        readonly Func<IEnumerable<TInput>, IEnumerable<TOutput>> Transformation;

        public override void OnReceive(Message<TInput, TTime> message)
        {
            this.Input.OnReceive(message, new RemotePostbox());
        }

        public override void OnNotify(TTime time)
        {
            var records = this.Input.GetRecordsAt(time);
            var output = this.Output.GetBufferForTime(time);
            foreach (var result in this.Transformation(records))
                output.Send(result);
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
        protected VertexInputBuffer<TInput1, TTime> Input1;
        protected VertexInputBuffer<TInput2, TTime> Input2;

        readonly Func<IEnumerable<TInput1>, IEnumerable<TInput2>, IEnumerable<TOutput>> Transformation;

        public override void OnReceive1(Message<TInput1, TTime> message)
        {
            this.Input1.OnReceive(message, new RemotePostbox());
        }

        public override void OnReceive2(Message<TInput2, TTime> message)
        {
            this.Input2.OnReceive(message, new RemotePostbox());
        }

        public override void OnNotify(TTime time)
        {
            var records1 = this.Input1.GetRecordsAt(time);
            var records2 = this.Input2.GetRecordsAt(time);

            var outputBuffer = this.Output.GetBufferForTime(time);

            foreach (var result in this.Transformation(records1, records2))
                outputBuffer.Send(result);
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
        public static void NewSinkStage<TOutput, TTime>(this Stream<TOutput, TTime> source, Func<int, Stage<TTime>, SinkVertex<TOutput, TTime>> factory, Expression<Func<TOutput, int>> inputPartitionBy, string name)
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
        public static Stage<TVertex, TTime> NewStage<TVertex, TTime>(OpaqueTimeContext<TTime> context, Func<int, Stage<TTime>, TVertex> factory, string name)
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
        /// <param name="context">Time context</param>
        /// <param name="factory">Vertex factory</param>
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
