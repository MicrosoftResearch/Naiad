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
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

using Microsoft.Research.Naiad;
using Microsoft.Research.Naiad.Dataflow;
using Microsoft.Research.Naiad.Dataflow.StandardVertices;
using Microsoft.Research.Naiad.Dataflow.PartitionBy;
using Microsoft.Research.Naiad.Input;
using Microsoft.Research.Naiad.Runtime.Progress;

namespace Microsoft.Research.Naiad.Runtime.FaultTolerance
{
    /// <summary>
    /// A set of times of messages sent to or from a vertex in another stage. This is public so it can be serialized
    /// </summary>
    public struct MessageTimeBundle
    {
        /// <summary>
        /// the vertex in the sender or receiver stage
        /// </summary>
        public int vertexId;
        /// <summary>
        /// the times of the messages
        /// </summary>
        public Pointstamp[] times;
    }

    /// <summary>
    /// The metadata associated with a new checkpoint that has been persisted
    /// </summary>
    public struct CheckpointUpdate
    {
        /// <summary>
        /// The stage that wrote the checkpoint
        /// </summary>
        public int stageId;
        /// <summary>
        /// The vertex that wrote the checkpoint
        /// </summary>
        public int vertexId;
        /// <summary>
        /// The frontier of the new checkpoint
        /// </summary>
        public FTFrontier frontier;
        /// <summary>
        /// true if this is indicating the instantaneous state during a rollback, rather than persisted state
        /// </summary>
        public bool isTemporary;
        /// <summary>
        /// The times of any notifications that were delivered since the last checkpoint
        /// </summary>
        public Pointstamp[] notifications;
        /// <summary>
        /// For each upstream edge, a pair with first element the upstream stageId and second element an array,
        /// for each upstream vertex, of the set of times of delivered messages from that vertex since the last
        /// checkpoint
        /// </summary>
        public Pair<int, Pointstamp[]>[] deliveredMessages;
        /// <summary>
        /// For each downstream edge a pair with first element the downstream stageId and second element an array of pairs
        /// with first element the time of an event at the sender vertex and second element an array, for each downstream
        /// vertex, of the set of times of discarded messages to that vertex since the last checkpoint
        /// </summary>
        public Pair<int, Pair<Pointstamp, Pointstamp[]>[]>[] discardedMessages;
    }

    /// <summary>
    /// The metadata associated with a new low-watermark that allows old checkpoints to be garbage-collected
    /// </summary>
    public struct CheckpointLowWatermark
    {
        /// <summary>
        /// The vertexID of the fault-tolerance manager vertex that manages this checkpoint
        /// </summary>
        public int managerVertex;
        /// <summary>
        /// The stage whose checkpoints can be garbage-collected
        /// </summary>
        public int stageId;
        /// <summary>
        /// The vertex whose checkpoints can be garbage-collected
        /// </summary>
        public int vertexId;
        /// <summary>
        /// If this is an outgoing log garbage-collection, the id of the destination stage with a new low watermark,
        /// otherwise -1
        /// </summary>
        public int dstStageId;
        /// <summary>
        /// If this is an outgoing log garbage-collection, the id of the destination vertex with a new low watermark,
        /// otherwise -1
        /// </summary>
        public int dstVertexId;
        /// <summary>
        /// The new low watermark of dstStageId.dstVertexId if they are not -1.-1, stageId.VertexId otherwise
        /// </summary>
        public FTFrontier frontier;
    }

    internal class CheckpointUpdateVertex : Vertex<Epoch>
    {
        public VertexOutputBuffer<CheckpointUpdate, Epoch> Output;
        public ProgressUpdateBuffer<Epoch> progressHoldBuffer;

        public void OnRecv(CheckpointUpdate update)
        {
            var buffer = this.Output.GetBufferForTime(new Epoch(0));
            buffer.Send(update);
            this.Output.Flush();
        }

        public CheckpointUpdateVertex(int index, Stage<Epoch> stage) : base(index, stage)
        {
            this.Output = new VertexOutputBuffer<CheckpointUpdate, Epoch>(this);
            this.PushEventTime(new Epoch(0));
            this.progressHoldBuffer = new ProgressUpdateBuffer<Epoch>(this.Stage.StageId, this.scheduler.State(this.Stage.InternalComputation).Producer);
        }
    }

    internal class CheckpointUpdateAggregator : SinkVertex<CheckpointUpdate,Epoch>
    {
        private readonly BaseComputation computation;

        public override void OnReceive(Message<CheckpointUpdate, Epoch> message)
        {
            for (int i=0; i<message.length; ++i)
            {
                this.computation.NotifyCheckpointPersisted(message.payload[i]);
            }
        }

        public CheckpointUpdateAggregator(int index, Stage<Epoch> stage, BaseComputation computation) : base(index, stage)
        {
            this.computation = computation;
        }
    }

    internal class CheckpointTracker
    {
        private readonly StreamingDataSource<CheckpointUpdate> updater;
        private readonly Stage updaterStage;
        private readonly Stage<CheckpointUpdateAggregator, Epoch> consumer;
        private readonly StreamingDataSource<CheckpointLowWatermark> gcDataSource;
        private readonly Stage gcStage;
        private readonly Dictionary<Pair<int, int>, Action<int, int, FTFrontier>> updateFunction;
        private readonly Dictionary<int, List<CheckpointUpdate>> pendingUpdates = new Dictionary<int, List<CheckpointUpdate>>();
        private int incompleteFrontierCount;

        public Vertex CentralReceiverVertex
        {
            get
            {
                return consumer.Vertices.Single();
            }
        }

        public int GetUpdateReceiverVertex(VertexLocation location)
        {
            return this.updaterStage.Placement
                .Where(p => p.ProcessId == location.ProcessId && p.ThreadId == location.ThreadId)
                .Select(p => p.VertexId)
                .Single();
        }

        public Action<CheckpointUpdate> GetUpdateReceiver(VertexLocation location)
        {
            System.Threading.Interlocked.Increment(ref this.incompleteFrontierCount);

            if (!this.pendingUpdates.ContainsKey(location.ThreadId))
            {
                this.pendingUpdates.Add(location.ThreadId, new List<CheckpointUpdate>());
            }

            return u =>
            {
                this.pendingUpdates[location.ThreadId].Add(u);
            };
        }

        public void FlushUpdates(int threadIndex)
        {
            if (this.pendingUpdates.ContainsKey(threadIndex))
            {
                List<CheckpointUpdate> toSend = this.pendingUpdates[threadIndex];
                if (toSend.Count > 0)
                {
                    this.pendingUpdates[threadIndex] = new List<CheckpointUpdate>();

                    int newCount = 1;

                    foreach (CheckpointUpdate update in toSend.Where(u => !u.isTemporary && u.frontier.Complete))
                    {
                        newCount = System.Threading.Interlocked.Decrement(ref this.incompleteFrontierCount);
                    }

                    this.updater.OnNext(toSend, threadIndex);

                    if (newCount == 0)
                    {
                        this.updater.OnCompleted();
                    }
                }
            }
        }

        public void RestoreProgress()
        {
            foreach (var v in this.updaterStage.Vertices)
            {
                v.RestoreProgressUpdates(false);
            }

            foreach (var v in this.gcStage.Vertices)
            {
                v.RestoreProgressUpdates(false);
            }
        }

        public int SendDeferredMessages(int maxQueued)
        {
            foreach (var v in this.updaterStage.Vertices)
            {
                maxQueued = Math.Max(maxQueued, v.Replay(Vertex.ReplayMode.SendDeferredMessages));
            }
            return maxQueued;
        }

        private void ReceiveStageUpdate(BaseComputation computation, int stageId, Pointstamp defaultVersion, FTFrontier frontier)
        {
            computation.NotifyStageStable(stageId, frontier.ToPointstamps(defaultVersion));
        }

        private void ReceiveGCUpdates(Message<CheckpointLowWatermark, Epoch> message)
        {
            for (int i = 0; i < message.length; ++i)
            {
                CheckpointLowWatermark update = message.payload[i];
                this.updateFunction[update.stageId.PairWith(update.vertexId)](
                    update.dstStageId, update.dstVertexId, update.frontier);
            }
        }

        public void ReceiveGCUpdates(IEnumerable<CheckpointLowWatermark> updates)
        {
            if (updates == null)
            {
                this.gcDataSource.OnCompleted();
            }
            else
            {
                this.gcDataSource.OnNext(updates);
            }
        }

        public void RegisterCheckpointer(int stageId, int vertexId, Action<int, int, FTFrontier> updateFunction)
        {
            this.updateFunction.Add(stageId.PairWith(vertexId), updateFunction);
        }

        public void RegisterStageStableCallbacks(BaseComputation computation)
        {
            foreach (Stage stage in computation.Stages.Select(s => s.Value))
            {
                this.RegisterCheckpointer(stage.StageId, -1, (ds, dv, frontier) =>
                    this.ReceiveStageUpdate(computation, stage.StageId, stage.DefaultVersion, frontier));
            }
        }

        public CheckpointTracker(BaseComputation computation)
        {
            this.updateFunction = new Dictionary<Pair<int, int>, Action<int, int, FTFrontier>>();

            this.updater = new StreamingDataSource<CheckpointUpdate>();
            var stream = computation.NewInput(this.updater).SetCheckpointType(CheckpointType.None);
            this.updaterStage = stream.ForStage;

            var placement = new Placement.SingleVertex(
                computation.Controller.Configuration.CentralizerProcessId,
                computation.Controller.Configuration.CentralizerThreadId);
            this.consumer = new Stage<CheckpointUpdateAggregator,Epoch>(
                placement, computation, Stage.OperatorType.Default,
                (i, s) => new CheckpointUpdateAggregator(i, s, computation), "CheckpointUpdateAggregator");
            consumer.SetCheckpointType(CheckpointType.None);

            consumer.NewInput(stream, (m, v) => v.OnReceive(m), m => 0);

            this.gcDataSource = new StreamingDataSource<CheckpointLowWatermark>();
            var gcInput = computation.NewInput(this.gcDataSource).SetCheckpointType(CheckpointType.None);
            this.gcStage = gcInput.ForStage;

            var gcReceiver = gcInput
                .PartitionBy(c => c.managerVertex).SetCheckpointType(CheckpointType.None)
                .Subscribe((m, i) => this.ReceiveGCUpdates(m), (e, i) => { }, (i) => { });

            if (computation.Controller.Configuration.ProcessID != 0)
            {
                this.gcDataSource.OnCompleted();
            }
        }
    }
}
