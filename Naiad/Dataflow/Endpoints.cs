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

using Microsoft.Research.Naiad.Dataflow.Channels;
using Microsoft.Research.Naiad.Runtime.Progress;
using Microsoft.Research.Naiad.Runtime.FaultTolerance;
using Microsoft.Research.Naiad.Serialization;
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Linq.Expressions;
using System.Text;
using System.Threading.Tasks;

namespace Microsoft.Research.Naiad.Dataflow
{
    /// <summary>
    /// Represents an output of a vertex, to which zero or more <see cref="SendChannel{TSender,TRecord,TTime}"/> (receivers)
    /// can be added.
    /// </summary>
    /// <typeparam name="TRecord">The type of records produced by this output.</typeparam>
    /// <typeparam name="TTime">The type of timestamp on the records produced by this output.</typeparam>
    /// <typeparam name="TSender">The type of timestamp on the vertex producing this output.</typeparam>
    public interface VertexOutput<TSender, TRecord, TTime>
        where TTime : Time<TTime>
        where TSender : Time<TSender>
    {
        /// <summary>
        /// The vertex hosting the output.
        /// </summary>
        Dataflow.Vertex<TSender> Vertex { get; }

        /// <summary>
        /// Adds the given receiver to those that will be informed of every messages sent on this output.
        /// </summary>
        /// <param name="receiver">A receiver of messages.</param>
        void AddReceiver(SendChannel<TSender, TRecord, TTime> receiver);
    }

    /// <summary>
    /// Defines the input of a vertex, which must process messages and manage re-entrancy for the runtime.
    /// </summary>
    /// <typeparam name="TRecord">The type of records accepted by this input.</typeparam>
    /// <typeparam name="TTime">The type of timestamp on the records accepted by this input.</typeparam>
    public interface VertexInput<TRecord, TTime>
        where TTime : Time<TTime>
    {
        /// <summary>
        /// Sets the object in charge of making checkpoints
        /// </summary>
        /// <param name="checkpointer">the checkpointing object</param>
        void SetCheckpointer(Checkpointer<TTime> checkpointer);
        /// <summary>
        /// Reports and sets the status of logging.
        /// </summary>
        bool LoggingEnabled { get; }

        /// <summary>
        /// Indicates whether the destination vertex can be currently re-entered. Decremented and incremented by Naiad.
        /// </summary>
        int AvailableEntrancy { get; set; }

        /// <summary>
        /// The ID of the edge feeding in to this input
        /// </summary>
        int ChannelId { get; set; }

        /// <summary>
        /// The ID of the stage feeding in to this input
        /// </summary>
        int SenderStageId { get; set; }

        /// <summary>
        /// The vertex hosting the input.
        /// </summary>
        Dataflow.Vertex<TTime> Vertex { get; }

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
        /// <param name="from">the sender of the message</param>
        /// 
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
        internal int SenderStageId;
        internal int ChannelId;
        internal readonly Stage<TTime> ForStage;
        internal readonly Expression<Func<TRecord, int>> PartitionedBy;

        private readonly Dictionary<int, VertexInput<TRecord, TTime>> endpointMap;

        internal void Register(VertexInput<TRecord, TTime> endpoint)
        {
            this.endpointMap[endpoint.Vertex.VertexId] = endpoint;
            endpoint.ChannelId = this.ChannelId;
            endpoint.SenderStageId = this.SenderStageId;
        }

        internal void SetChannelId(int channelId, int senderStageId)
        {
            this.ChannelId = channelId;
            this.SenderStageId = senderStageId;
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

        internal StageInput(Stage<TTime> stage, Expression<Func<TRecord, int>> partitionedBy)
        {
            this.PartitionedBy = partitionedBy;
            this.ForStage = stage;
            this.endpointMap = new Dictionary<int, VertexInput<TRecord, TTime>>();
        }
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

        private int senderStageId;
        public int SenderStageId { get { return this.senderStageId; } set { this.senderStageId = value; } }

        protected IMessageLogger<S, T> logger = null;
        public void SetCheckpointer(Checkpointer<T> checkpointer)
        {
            this.logger = checkpointer.CreateIncomingMessageLogger<S>(this.channelId, this.senderStageId, this.ReplayReceive, this.BufferPool);
        }
        public bool LoggingEnabled { get { return this.logger != null; } }

        private BufferPool<S> BufferPool;

        public int AvailableEntrancy
        {
            get { return this.Vertex.Entrancy; }
            set { this.Vertex.Entrancy = value; }
        }

        protected Vertex<T> vertex;

        public Vertex<T> Vertex
        {
            get { return this.vertex; }
        }

        public void Flush()
        {
            System.Diagnostics.Debug.Assert(this.AvailableEntrancy >= -1);
            this.vertex.Flush();
        }

        public void ReplayReceive(Message<S, T> message, ReturnAddress from)
        {
            this.OnReceive(message, from);
            message.Release(AllocationReason.PostOfficeChannel, this.BufferPool);
        }

        public abstract void OnReceive(Message<S, T> message, ReturnAddress from);

        private AutoSerializedMessageDecoder<S, T> decoder = null;

        public void SerializedMessageReceived(SerializedMessage serializedMessage, ReturnAddress from)
        {
            System.Diagnostics.Debug.Assert(this.AvailableEntrancy >= -1);
            if (this.decoder == null) this.decoder = new AutoSerializedMessageDecoder<S, T>(this.Vertex.SerializationFormat, this.Vertex.Scheduler.GetBufferPool<S>());

            Stage stage = this.Vertex.Stage;
            InternalComputation computation = stage.InternalComputation;

            Message<S, T> msg = new Message<S, T>();
            msg.Allocate(AllocationReason.Deserializer, this.BufferPool);
            // N.B. At present, AsTypedMessages reuses and yields the same given msg for each batch
            //      of deserialized messages. As a result, the message passed to OnReceive MUST NOT
            //      be queued, because its payload will be overwritten by the next batch of messages.
            foreach (Message<S, T> message in this.decoder.AsTypedMessages(serializedMessage, msg))
            {
                this.OnReceive(message, from);
            }
            msg.Release(AllocationReason.Deserializer, this.BufferPool);
        }

        public Receiver(Vertex<T> vertex)
        {
            this.vertex = vertex;
            this.BufferPool = vertex.Scheduler.GetBufferPool<S>();
        }
    }

    internal class ActionReceiver<S, T> : Receiver<S, T>
        where T : Time<T>
    {
        private readonly Action<Message<S, T>, ReturnAddress> MessageCallback;

        public override void OnReceive(Message<S, T> message, ReturnAddress from)
        {
            this.vertex.PushEventTime(message.time);

            if (this.LoggingEnabled)
                this.logger.LogMessage(message, from);
            this.MessageCallback(message, from);

            T poppedTime = this.vertex.PopEventTime();
            if (poppedTime.CompareTo(message.time) != 0)
            {
                throw new ApplicationException("Time stack mismatch");
            }
        }

        public ActionReceiver(Vertex<T> vertex, Action<Message<S, T>> messagecallback)
            : base(vertex)
        {
            this.MessageCallback = (m, u) => messagecallback(m);
        }
        public ActionReceiver(Vertex<T> vertex, Action<S, T> recordcallback)
            : base(vertex)
        {
            this.MessageCallback = ((m, u) => { for (int i = 0; i < m.length; i++) recordcallback(m.payload[i], m.time); });
        }
    }

    internal class ActionSubscriber<S, T> : VertexOutput<T, S, T> where T : Time<T>
    {
        private readonly Action<SendChannel<T, S, T>> onListener;
        private Vertex<T> vertex;

        public Vertex<T> Vertex
        {
            get { return this.vertex; }
        }

        public void AddReceiver(SendChannel<T, S, T> receiver)
        {
            this.onListener(receiver);
        }

        public ActionSubscriber(Vertex<T> vertex, Action<SendChannel<T, S, T>> action)
        {
            this.vertex = vertex;
            this.onListener = action;
        }
    }

    #endregion

    #region StageOutput and friends

    internal interface UntypedStageOutput
    {
        HashSet<Edge> OutputChannels { get; }
    }

    internal interface StageOutputForVertex<TVertexTime> : UntypedStageOutput where TVertexTime : Time<TVertexTime>
    {
        CheckpointState<TVertexTime>.DiscardedTimes MakeDiscardedTimesBundle();
        void EnableLogging(int vertexId);
    }

    internal abstract class StageOutput<R, TMessageTime>
        where TMessageTime : Time<TMessageTime>
    {
        internal abstract Dataflow.Stage ForStage { get; }

        private readonly HashSet<Edge> outputChannels = new HashSet<Edge>();
        public HashSet<Edge> OutputChannels { get { return this.outputChannels; } }

        private readonly Expression<Func<R, int>> partitionedBy;
        public Expression<Func<R, int>> PartitionedBy { get { return partitionedBy; } }

        public abstract Edge NewEdge(StageInput<R, TMessageTime> recvPort, Action<R[], int[], int> key, Channel.Flags flags);

        public override string ToString()
        {
            return String.Format("SendPort[{0}]", this.ForStage);
        }

        internal StageOutput(Expression<Func<R, int>> partitionedBy)
        {
            this.partitionedBy = partitionedBy;
        }
    }

    internal class FullyTypedStageOutput<TVertexTime, R, TMessageTime> : StageOutput<R, TMessageTime>, StageOutputForVertex<TVertexTime>
        where TVertexTime : Time<TVertexTime>
        where TMessageTime : Time<TMessageTime>
    {
        internal readonly int StageOutputIndex;
        internal readonly Func<TVertexTime, TMessageTime> SendTimeProjection;

        private readonly Dictionary<int, VertexOutput<TVertexTime, R, TMessageTime>> endpointMap;
        private readonly Dictionary<int, Cable<TVertexTime, R, TMessageTime>> receivers;

        private IOutgoingMessageLogger<TVertexTime, R, TMessageTime> messageLogger = null;

        internal readonly Dataflow.Stage<TVertexTime> TypedStage;
        internal override Stage ForStage
        {
            get { return this.TypedStage; }
        }

        internal void Register(VertexOutput<TVertexTime, R, TMessageTime> endpoint)
        {
            this.endpointMap[endpoint.Vertex.VertexId] = endpoint;
        }

        public VertexOutput<TVertexTime, R, TMessageTime> GetFiber(int index) { return endpointMap[index]; }

        public void AttachBundleToSender(Cable<TVertexTime, R, TMessageTime> bundle)
        {
            this.receivers.Add(bundle.Edge.ChannelId, bundle);
            foreach (var pair in endpointMap)
            {
                pair.Value.AddReceiver(bundle.GetSendChannel(pair.Key));
            }
        }

        public void ReAttachBundleForRollback(Cable<TVertexTime, R, TMessageTime> bundle, Dictionary<int, Vertex> newSourceVertices)
        {
            foreach (int vertex in newSourceVertices.Keys)
            {
                endpointMap[vertex].AddReceiver(bundle.GetSendChannel(vertex));
            }
        }

        public override Edge NewEdge(StageInput<R, TMessageTime> recvPort, Action<R[], int[], int> key, Channel.Flags flags)
        {
            return new Edge<TVertexTime, R, TMessageTime>(this, recvPort, key, flags);
        }

        public CheckpointState<TVertexTime>.DiscardedTimes MakeDiscardedTimesBundle()
        {
            return new CheckpointState<TVertexTime>.DiscardedTimes<TMessageTime>(this);
        }

        public void EnableLogging(int vertexId)
        {
            VertexOutput<TVertexTime, R, TMessageTime> endpoint = this.endpointMap[vertexId];

            this.messageLogger =
                endpoint.Vertex.Checkpointer.CreateLogger<R, TMessageTime>(this, this.receivers.Values);

            foreach (Cable<TVertexTime, R, TMessageTime> cable in this.receivers.Values)
            {
                cable.GetSendChannel(endpoint.Vertex.VertexId).EnableLogging(messageLogger);
            }
        }

        internal FullyTypedStageOutput(Stage<TVertexTime> stage, Expression<Func<R, int>> partitionedBy, Func<TVertexTime, TMessageTime> sendTimeProjection)
            : base(partitionedBy)
        {
            this.StageOutputIndex = stage.NewStageOutput(this);
            this.TypedStage = stage;
            this.SendTimeProjection = sendTimeProjection;
            this.endpointMap = new Dictionary<int, VertexOutput<TVertexTime, R, TMessageTime>>();
            this.receivers = new Dictionary<int, Cable<TVertexTime, R, TMessageTime>>();
        }
    }

    #endregion
}
