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

using Microsoft.Research.Naiad.Dataflow.Channels;
using Microsoft.Research.Naiad.Serialization;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;
using System.Text;
using System.Threading.Tasks;

namespace Microsoft.Research.Naiad.Dataflow
{
    /// <summary>
    /// Represents an output of a vertex, to which zero or more <see cref="SendChannel{TRecord,TTime}"/> (receivers)
    /// can be added.
    /// </summary>
    /// <typeparam name="TRecord">The type of records produced by this output.</typeparam>
    /// <typeparam name="TTime">The type of timestamp on the records produced by this output.</typeparam>
    public interface VertexOutput<TRecord, TTime>
     where TTime : Time<TTime>
    {
        /// <summary>
        /// The vertex hosting the output.
        /// </summary>
        Dataflow.Vertex Vertex { get; }

        /// <summary>
        /// Adds the given receiver to those that will be informed of every messages sent on this output.
        /// </summary>
        /// <param name="receiver">A receiver of messages.</param>
        void AddReceiver(SendChannel<TRecord, TTime> receiver);
    }

    /// <summary>
    /// Defines the input of a vertex, which must process messages and manage re-entrancy for the runtime.
    /// </summary>
    /// <typeparam name="TRecord">The type of records accepted by thie input.</typeparam>
    /// <typeparam name="TTime">The type of timestamp on the records accepted by this input.</typeparam>
    public interface VertexInput<TRecord, TTime>
        where TTime : Time<TTime>
    {
        /// <summary>
        /// Reports and sets the status of logging; infrequently supported.
        /// </summary>
        bool LoggingEnabled { get; set; }

        /// <summary>
        /// Indicates whether the destination vertex can be currently re-entered. Decremented and incremented by Naiad.
        /// </summary>
        int AvailableEntrancy { get; set; }

        /// <summary>
        /// The vertex hosting the input.
        /// </summary>
        Dataflow.Vertex Vertex { get; }

        /// <summary>
        /// Ensures that before returning all messages are sent and all progress traffic has been presented to the worker.
        /// </summary>
        void Flush();

        /// <summary>
        /// Callback for a message containing several records.
        /// </summary>
        /// <param name="message">the message</param>
        /// <param name="from">the source of the message</param>
        void OnReceive(Message<TRecord, TTime> message, ReturnAddress from);

        /// <summary>
        /// Callback for a serialized message. 
        /// </summary>
        /// <param name="message">the serialized message</param>
        /// <param name="from">the source of the serialized message</param>
        void SerializedMessageReceived(SerializedMessage message, ReturnAddress from);
    }

    #region StageInput and friends

    /// <summary>
    /// Represents an input to a dataflow stage.
    /// </summary>
    /// <typeparam name="TRecord">record type</typeparam>
    /// <typeparam name="TTime">time type</typeparam>
    public class StageInput<TRecord, TTime>
        where TTime : Time<TTime>
    {
        internal readonly Stage ForStage;
        internal readonly Expression<Func<TRecord, int>> PartitionedBy;

        private readonly Dictionary<int, VertexInput<TRecord, TTime>> endpointMap;

        internal void Register(VertexInput<TRecord, TTime> endpoint)
        {
            this.endpointMap[endpoint.Vertex.VertexId] = endpoint;
        }

        internal VertexInput<TRecord, TTime> GetPin(int index)
        {
            if (endpointMap.ContainsKey(index))
                return endpointMap[index];
            else
                throw new Exception("Error in StageInput.GetPin()");
        }

        /// <summary>
        /// Returns a string representation of this stage input.
        /// </summary>
        /// <returns>A string representation of this stage input.</returns>
        public override string ToString()
        {
            return String.Format("StageInput[{0}]", this.ForStage);
        }

        internal StageInput(Stage stage, Expression<Func<TRecord, int>> partitionedBy)
        {
            this.PartitionedBy = partitionedBy;
            this.ForStage = stage;
            this.endpointMap = new Dictionary<int, VertexInput<TRecord, TTime>>();
        }
        internal StageInput(Stage stage)
            : this(stage, null)
        { }
    }

#if false
    public class RecvFiberSpillBank<S, T> : VertexInput<S, T>, ICheckpointable
        where T : Time<T>
    {
        private int channelId;
        public int ChannelId { get { return this.channelId; } set { this.channelId = value; } }

        public bool LoggingEnabled { get { return false; } set { throw new NotImplementedException("Logging for RecvFiberSpillBank"); } }

        public int AvailableEntrancy { get { return this.Vertex.Entrancy; } set { this.Vertex.Entrancy = value; } }
        private  SpillFile<Pair<S, T>> spillFile;

        private readonly Vertex<T> vertex;

        public IEnumerable<Pair<S, T>> GetRecords()
        {
            Pair<S, T> record;
            while (this.spillFile.TryGetNextElement(out record))
                yield return record;
        }

        public RecvFiberSpillBank(Vertex<T> vertex)
            : this(vertex, 1 << 20)
        {
        }

        public RecvFiberSpillBank(Vertex<T> vertex, int bufferSize)
        {
            this.vertex = vertex;
            this.spillFile = new SpillFile<Pair<S, T>>(System.IO.Path.GetRandomFileName(), bufferSize, new AutoSerializedMessageEncoder<S, T>(1, 1, DummyBufferPool<byte>.Pool, vertex.Stage.InternalGraphManager.Controller.Configuration.SendPageSize, vertex.CodeGenerator), new AutoSerializedMessageDecoder<S, T>(vertex.CodeGenerator), vertex.Stage.InternalGraphManager.Controller.Configuration.SendPageSize, vertex.CodeGenerator.GetSerializer<MessageHeader>());
        }

        public Microsoft.Research.Naiad.Dataflow.Vertex Vertex { get { return this.vertex; } }

        public void Flush() { this.spillFile.Flush(); }

        public void RecordReceived(Pair<S, T> record, RemotePostbox sender)
        {
            this.spillFile.Write(record);
            this.vertex.NotifyAt(record.v2);
        }

        public void MessageReceived(Message<S, T> message, RemotePostbox sender)
        {
            for (int i = 0; i < message.length; ++i)
                this.RecordReceived(message.payload[i].PairWith(message.time), sender);
        }

        private AutoSerializedMessageDecoder<S, T> decoder = null;
        public void SerializedMessageReceived(SerializedMessage serializedMessage, RemotePostbox sender)
        {
            if (this.decoder == null) this.decoder = new AutoSerializedMessageDecoder<S, T>(this.Vertex.CodeGenerator);
            
            foreach (Message<S, T> message in this.decoder.AsTypedMessages(serializedMessage))
            {
                this.MessageReceived(message, sender);
                message.Release();
            }
        }

        public override string ToString()
        {
            return string.Format("<{0}L>", this.vertex.Stage.StageId);
        }

        public void Restore(NaiadReader reader)
        {
            throw new NotImplementedException();
        }


        public void Checkpoint(NaiadWriter writer)
        {
            throw new NotImplementedException();
        }

        public virtual bool Stateful { get { return true; } }
    }

#endif

    internal abstract class Receiver<S, T> : VertexInput<S, T>
        where T : Time<T>
    {
        private int channelId;
        public int ChannelId { get { return this.channelId; } set { this.channelId = value; } }

        private bool loggingEnabled = false;
        public bool LoggingEnabled { get { return this.loggingEnabled; } set { this.loggingEnabled = value; } }

        public int AvailableEntrancy
        {
            get { return this.Vertex.Entrancy; }
            set { this.Vertex.Entrancy = value; }
        }

        protected Vertex vertex;

        public Vertex Vertex
        {
            get { return this.vertex; }
        }

        public void Flush()
        {
            System.Diagnostics.Debug.Assert(this.AvailableEntrancy >= -1);
            this.vertex.Flush();
        }

        public abstract void OnReceive(Message<S, T> message, ReturnAddress from);

        private Queue<Message<S, T>> SpareBuffers; // used for reentrancy.


        // private Message<S, T> Buffer;
        private AutoSerializedMessageDecoder<S, T> decoder = null;

        public void SerializedMessageReceived(SerializedMessage serializedMessage, ReturnAddress from)
        {
            System.Diagnostics.Debug.Assert(this.AvailableEntrancy >= -1);
            if (this.decoder == null) this.decoder = new AutoSerializedMessageDecoder<S, T>(this.Vertex.SerializationFormat);

            if (this.loggingEnabled)
                this.LogMessage(serializedMessage);

            foreach (Message<S, T> message in this.decoder.AsTypedMessages(serializedMessage))
            {
                this.OnReceive(message, from);
                message.Release();
            }
        }

        protected void LogMessage(Message<S, T> message)
        {
            var encoder = new AutoSerializedMessageEncoder<S, T>(this.Vertex.VertexId, this.channelId, DummyBufferPool<byte>.Pool, this.Vertex.Stage.InternalComputation.Controller.Configuration.SendPageSize, this.Vertex.SerializationFormat);
            encoder.CompletedMessage += (o, a) =>
            {
                ArraySegment<byte> messageSegment = a.Segment.ToArraySegment();
                this.Vertex.LoggingOutput.Write(messageSegment.Array, messageSegment.Offset, messageSegment.Count);
            };

            // XXX : Needs to be fixed up...
            for (int i = 0; i < message.length; ++i)
                encoder.Write(message.payload[i].PairWith(message.time));

            encoder.Flush();
        }

        protected void LogMessage(SerializedMessage message)
        {
            byte[] messageHeaderBuffer = new byte[MessageHeader.SizeOf];
            MessageHeader.WriteHeaderToBuffer(messageHeaderBuffer, 0, message.Header, this.Vertex.SerializationFormat.GetSerializer<MessageHeader>());
            this.Vertex.LoggingOutput.Write(messageHeaderBuffer, 0, messageHeaderBuffer.Length);
            this.Vertex.LoggingOutput.Write(message.Body.Buffer, message.Body.CurrentPos, message.Body.End - message.Body.CurrentPos);
        }

        public Receiver(Vertex vertex)
        {
            this.vertex = vertex;
            this.SpareBuffers = new Queue<Message<S, T>>();
        }
    }

    internal class ActionReceiver<S, T> : Receiver<S, T>
        where T : Time<T>
    {
        private readonly Action<Message<S, T>, ReturnAddress> MessageCallback;

        public override void OnReceive(Message<S, T> message, ReturnAddress from)
        {
            if (this.LoggingEnabled)
                this.LogMessage(message);
            this.MessageCallback(message, from);
        }

        public ActionReceiver(Vertex vertex, Action<Message<S, T>, ReturnAddress> messagecallback)
            : base(vertex)
        {
            this.MessageCallback = messagecallback;
        }
        public ActionReceiver(Vertex vertex, Action<Message<S, T>> messagecallback)
            : base(vertex)
        {
            this.MessageCallback = (m, u) => messagecallback(m);
        }
        public ActionReceiver(Vertex vertex, Action<S, T> recordcallback)
            : base(vertex)
        {
            this.MessageCallback = ((m, u) => { for (int i = 0; i < m.length; i++) recordcallback(m.payload[i], m.time); });
        }
    }

    internal class ActionSubscriber<S, T> : VertexOutput<S, T> where T : Time<T>
    {
        private readonly Action<SendChannel<S, T>> onListener;
        private Vertex<T> vertex;

        public Vertex Vertex
        {
            get { return this.vertex; }
        }

        public void AddReceiver(SendChannel<S, T> receiver)
        {
            this.onListener(receiver);
        }

        public ActionSubscriber(Vertex<T> vertex, Action<SendChannel<S, T>> action)
        {
            this.vertex = vertex;
            this.onListener = action;
        }
    }

    #endregion

    #region StageOutput and friends

    internal class StageOutput<R, T>
        where T : Time<T>
    {
        internal readonly Dataflow.Stage ForStage;
        internal readonly Dataflow.TimeContext<T> Context;

        private readonly Dictionary<int, VertexOutput<R, T>> endpointMap;

        private readonly Expression<Func<R, int>> partitionedBy;
        public Expression<Func<R, int>> PartitionedBy { get { return partitionedBy; } }

        internal void Register(VertexOutput<R, T> endpoint)
        {
            this.endpointMap[endpoint.Vertex.VertexId] = endpoint;
        }

        public VertexOutput<R, T> GetFiber(int index) { return endpointMap[index]; }

        public void AttachBundleToSender(Cable<R, T> bundle)
        {
            foreach (var pair in endpointMap)
            {
                pair.Value.AddReceiver(bundle.GetSendChannel(pair.Key));
            }
        }

        public override string ToString()
        {
            return String.Format("SendPort[{0}]", this.ForStage);
        }

        internal StageOutput(Stage stage, Dataflow.ITimeContext<T> context)
            : this(stage, context, null) { }

        internal StageOutput(Stage stage, Dataflow.ITimeContext<T> context, Expression<Func<R, int>> partitionedBy)
        {
            this.ForStage = stage;
            this.Context = new TimeContext<T>(context);
            endpointMap = new Dictionary<int, VertexOutput<R, T>>();
            this.partitionedBy = partitionedBy;
        }
    }

    #endregion
}
