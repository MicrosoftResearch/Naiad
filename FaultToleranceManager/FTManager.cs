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
using System.IO;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

using System.Collections.Concurrent;

using Microsoft.Research.Naiad.Frameworks.DifferentialDataflow;
using Microsoft.Research.Naiad.Frameworks.Lindi;
using Microsoft.Research.Naiad.Runtime.FaultTolerance;
using Microsoft.Research.Naiad.Runtime.Progress;
using Microsoft.Research.Naiad.Dataflow;
using Microsoft.Research.Naiad.Dataflow.StandardVertices;

namespace Microsoft.Research.Naiad.FaultToleranceManager
{
    /// <summary>
    /// The Microsoft.Research.Naiad.FaultToleranceManager namespace provides the classes for incrementally keeping
    /// track of the most recent global checkpoint that is available
    /// </summary>
    class NamespaceDoc
    {
    }

    internal struct NodeState
    {
        public int gcUpdateSendVertexId;
        public bool downwardClosed;
        public FTFrontier currentRestoration;
        public FTFrontier currentNotification;
        public HashSet<FTFrontier> checkpoints;
        public Dictionary<Pointstamp, int[]> deliveredMessages;
        public HashSet<Pointstamp> deliveredNotifications;
        public Dictionary<Pointstamp, Pair<int, Pointstamp>[]> discardedMessages;

        public NodeState(bool downwardClosed, int gcUpdateSendVertexId)
        {
            this.gcUpdateSendVertexId = gcUpdateSendVertexId;
            this.downwardClosed = downwardClosed;
            this.currentRestoration = new FTFrontier(false);
            this.currentNotification = new FTFrontier(false);
            this.checkpoints = new HashSet<FTFrontier>();
            this.checkpoints.Add(currentRestoration);
            this.deliveredMessages = new Dictionary<Pointstamp, int[]>();
            this.deliveredNotifications = new HashSet<Pointstamp>();
            this.discardedMessages = new Dictionary<Pointstamp, Pair<int, Pointstamp>[]>();
        }
    }

    /// <summary>
    /// class to keep track of the most recent global checkpoint for a Naiad computation
    /// </summary>
    public class FTManager
    {
        private Stage[] stages;
        internal Stage[] Stages { get { return this.stages; } }

        private Thread managerThread;

        private readonly Dictionary<SV, NodeState> nodeState = new Dictionary<SV, NodeState>();

        private readonly Dictionary<SV, SV[]> upstreamEdges = new Dictionary<SV, SV[]>();
        private readonly Dictionary<int, SV[]> upstreamStage = new Dictionary<int, SV[]>();
        private readonly Dictionary<int, SortedDictionary<FTFrontier, int>> stageFrontiers = new Dictionary<int, SortedDictionary<FTFrontier, int>>();

        private void AddStageFrontier(int stage, FTFrontier frontier)
        {
            var stageDictionary = this.stageFrontiers[stage];
            int count;
            if (stageDictionary.TryGetValue(frontier, out count))
            {
                stageDictionary[frontier] = count + 1;
            }
            else
            {
                stageDictionary[frontier] = 1;
            }
        }

        private void RemoveStageFrontier(int stage, FTFrontier frontier)
        {
            var stageDictionary = this.stageFrontiers[stage];
            int count = stageDictionary[frontier];
            if (count == 1)
            {
                stageDictionary.Remove(frontier);
            }
            else
            {
                stageDictionary[frontier] = count - 1;
            }
        }

        private FTFrontier StageFrontier(int stageId)
        {
            return this.stageFrontiers[stageId].First().Key;
        }

        private System.Diagnostics.Stopwatch stopwatch = new System.Diagnostics.Stopwatch();
        private FileStream checkpointLogFile = null;
        private StreamWriter checkpointLog = null;
        internal StreamWriter CheckpointLog
        {
            get
            {
                if (checkpointLog == null)
                {
                    string fileName = String.Format("ftmanager.log");
                    this.checkpointLogFile = new FileStream(fileName, FileMode.Create, FileAccess.Write, FileShare.ReadWrite);
                    this.checkpointLog = new StreamWriter(this.checkpointLogFile);
                    var flush = new System.Threading.Thread(new System.Threading.ThreadStart(() => FlushThread()));
                    flush.Start();
                    stopwatch.Start();
                }
                return checkpointLog;
            }
        }

        private void FlushThread()
        {
            while (true)
            {
                Thread.Sleep(1000);
                lock (this)
                {
                    if (this.checkpointLog != null)
                    {
                        this.checkpointLog.Flush();
                        this.checkpointLogFile.Flush(true);
                    }
                }
            }
        }

        public void WriteLog(string entry)
        {
            lock (this)
            {
                long microseconds = this.stopwatch.ElapsedTicks * 1000000L / System.Diagnostics.Stopwatch.Frequency;
                this.CheckpointLog.WriteLine(String.Format("{0:D11}: {1}", microseconds, entry));
            }
        }

        private Computation computation;

        private HashSet<int> stagesToMonitor = new HashSet<int>();

        private enum State
        {
            Incremental,
            PreparingForRollback,
            DrainingForRollback,
            AddedTemporaryForRollback,
            RevokingTemporaryForRollback,
            DrainingForExit,
            Stopping
        }
        private State state = State.Incremental;

        // initialize this to non-null because the first computation is triggered by GetGraph and
        // expects there to be non-null pendingUpdates when it terminates
        private List<CheckpointUpdate> pendingUpdates = new List<CheckpointUpdate>();
        private List<CheckpointUpdate> temporaryUpdates = null;
        private Dictionary<SV,CheckpointLowWatermark> rollbackFrontiers = null;
        private List<CheckpointLowWatermark> pendingGCUpdates = null;
        private ManualResetEventSlim quiescenceBarrier = null;

        private InputCollection<Edge> graph;
        private InputCollection<Checkpoint> checkpointStream;
        private InputCollection<DeliveredMessage> deliveredMessages;
        private InputCollection<Notification> deliveredNotifications;
        private InputCollection<DiscardedMessage> discardedMessages;

        private int epoch = -1;

        private IEnumerable<Checkpoint> InitializeCheckpoints()
        {
            foreach (Stage stage in this.stages.Where(s => s != null))
            {
                for (int vertexId=0; vertexId<stage.Placement.Count; ++vertexId)
                {
                    SV node = new SV { StageId = stage.StageId, VertexId = vertexId};
                    NodeState state = this.nodeState[node];

                    yield return new Checkpoint {
                        node = node, checkpoint = state.currentRestoration, downwardClosed = state.downwardClosed };
                }
            }
        }

        private void GetGraph(object o, Diagnostics.GraphMaterializedEventArgs args)
        {
            this.stages = new Stage[args.stages.Select(s => s.StageId).Max() + 1];
            foreach (Stage stage in args.stages)
            {
                this.stages[stage.StageId] = stage;
                this.stageFrontiers.Add(stage.StageId, new SortedDictionary<FTFrontier, int>());
                this.AddStageFrontier(stage.StageId, new FTFrontier(false));
            }

            foreach (Pair<Pair<int, int>, int> ftVertex in args.ftmanager)
            {
                Stage stage = this.stages[ftVertex.First.First];
                SV node = new SV { StageId = stage.StageId, VertexId = ftVertex.First.Second };
                NodeState state = new NodeState(!CheckpointProperties.IsStateful(stage.CheckpointType), ftVertex.Second);
                this.nodeState.Add(node, state);
            }

            foreach (Pair<SV, SV[]> edgeList in args.edges
                .Select(e => new SV { StageId = e.First.First, VertexId = e.First.Second }.PairWith(
                    new SV { StageId = e.Second.First, VertexId = e.Second.Second }))
                .GroupBy(e => e.Second)
                .Select(e => e.Key.PairWith(e.Select(ee => ee.First).ToArray())))
            {
                this.upstreamEdges.Add(edgeList.First, edgeList.Second);
            }

            foreach (SV node in this.nodeState.Keys.Where(n => !this.upstreamEdges.ContainsKey(n)))
            {
                this.upstreamEdges.Add(node, new SV[0]);
            }

            foreach (Pair<int, SV[]> edgeList in args.edges
                .Select(e => new SV { StageId = e.First.First, VertexId = e.First.Second }.PairWith(e.Second.First))
                .GroupBy(e => e.Second)
                .Select(e => e.Key.PairWith(e.Select(ee => ee.First).ToArray())))
            {
                this.upstreamStage.Add(edgeList.First, edgeList.Second);
            }

            foreach (int stage in this.nodeState.Keys.Select(sv => sv.StageId).Distinct().Where(n => !this.upstreamStage.ContainsKey(n)))
            {
                this.upstreamStage.Add(stage, new SV[0]);
            }

            this.graph.OnNext(args.edges.Select(e => new Edge
            {
                src = new SV { StageId = e.First.First, VertexId = e.First.Second },
                dst = new SV { StageId = e.Second.First, VertexId = e.Second.Second }
            }));
            this.graph.OnCompleted();

            this.checkpointStream.OnNext(this.InitializeCheckpoints());

            this.deliveredMessages.OnNext();
            this.deliveredNotifications.OnNext();
            this.discardedMessages.OnNext();

            ++this.epoch;
        }

        private void AddChangesFromUpdate(
            CheckpointUpdate update,
            int updateWeight,
            ref IEnumerable<Weighted<Checkpoint>> checkpointChanges,
            ref IEnumerable<Weighted<Notification>> notificationChanges,
            ref IEnumerable<Weighted<DeliveredMessage>> deliveredMessageChanges,
            ref IEnumerable<Weighted<DiscardedMessage>> discardedMessageChanges)
        {
            Stage stage = this.stages[update.stageId];
            SV node = new SV { StageId = update.stageId, VertexId = update.vertexId };
            NodeState state = this.nodeState[node];

#if false
#if true
            if (update.stageId == 46 && update.vertexId == 0)
            {
                if (update.frontier.maximalElement.Timestamp[1] > 1)
                {
                    Console.WriteLine("Ignoring update " + stage + "[" + update.vertexId + "] " + update.frontier);
                    this.nodeState[node] = state;
                    return;
                }
            }
#else
            if (update.stageId == 33 && update.vertexId == 0)
            {
                Pointstamp p = new Pointstamp();
                p.Location = update.stageId;
                p.Timestamp.a = 0;
                p.Timestamp.b = 2;
                p.Timestamp.c = 3;
                p.Timestamp.Length = 3;

                if (update.frontier.Contains(p))
                {
                    this.nodeState[node] = state;
                    return;
                }
            }

            if (update.stageId == 49 && update.vertexId == 0)
            {
                Pointstamp p = new Pointstamp();
                p.Location = update.stageId;
                p.Timestamp.a = 3;
                p.Timestamp.Length = 1;

                if (update.frontier.Contains(p))
                {
                    this.nodeState[node] = state;
                    return;
                }
            }
#endif
#endif

            if (state.currentRestoration.Contains(update.frontier))
            {
                throw new ApplicationException("FT checkpoints received out of order");
            }

            if (!update.isTemporary && state.downwardClosed)
            {
                FTFrontier oldFrontier = state.checkpoints.Single();

                if (oldFrontier.Contains(update.frontier))
                {
                    throw new ApplicationException("FT checkpoints received out of order");
                }

                state.checkpoints.Remove(oldFrontier);
                state.checkpoints.Add(update.frontier);

                checkpointChanges = checkpointChanges.Concat(new Weighted<Checkpoint>[]
                    {
                        new Weighted<Checkpoint>(new Checkpoint {
                            node = node, checkpoint = update.frontier, downwardClosed = true }, 1),
                        new Weighted<Checkpoint>(new Checkpoint {
                            node = node, checkpoint = oldFrontier, downwardClosed = true }, -1)
                    });
            }
            else
            {
                if (!update.isTemporary)
                {
                    foreach (var checkpoint in state.checkpoints)
                    {
                        if (checkpoint.Contains(update.frontier))
                        {
                            throw new ApplicationException("FT checkpoints received out of order");
                        }
                    }

                    state.checkpoints.Add(update.frontier);
                }

                checkpointChanges = checkpointChanges.Concat(new Weighted<Checkpoint>[]
                    {
                        new Weighted<Checkpoint>(new Checkpoint {
                            node = node, checkpoint = update.frontier, downwardClosed = false }, updateWeight)
                    });
            }

            IEnumerable<DeliveredMessage> messages =
                update.deliveredMessages.SelectMany(srcStage =>
                    srcStage.Second.Select(time =>
                            new DeliveredMessage
                            {
                                edge = new Edge 
                                {
                                    src = new SV { StageId = srcStage.First, VertexId = -1 },
                                    dst = node
                                },
                                dstTime = new LexStamp { time = time }
                            }));

            if (!update.isTemporary)
            {
                foreach (var time in messages.GroupBy(m => m.dstTime))
                {
                    if (state.currentRestoration.Contains(time.Key.time))
                    {
                        throw new ApplicationException("Stale Delivered message");
                    }
                    state.deliveredMessages.Add(time.Key.time, time.Select(m => m.edge.src.StageId).ToArray());
                }
            }
            deliveredMessageChanges = deliveredMessageChanges
                .Concat(messages.Select(m => new Weighted<DeliveredMessage>(m, updateWeight)));

            if (!update.isTemporary)
            {
                foreach (var time in update.notifications)
                {
                    if (state.currentRestoration.Contains(time))
                    {
                        throw new ApplicationException("Stale Delivered notification");
                    }
                    state.deliveredNotifications.Add(time);
                }
            }
            notificationChanges = notificationChanges
                .Concat(update.notifications.Select(time =>
                    new Weighted<Notification>(new Notification { node = node, time = new LexStamp { time = time } }, updateWeight)));

            IEnumerable<DiscardedMessage> discarded =
                update.discardedMessages.SelectMany(dstStage =>
                    dstStage.Second.SelectMany(nodeTimes =>
                        nodeTimes.Second
                            .Where(dstTime => !this.StageFrontier(dstStage.First).Contains(dstTime))
                            // don't bother keeping track of discarded messages that are already contained
                            // in the destination's restoration frontier
                            .Select(dstTime =>
                                new DiscardedMessage
                                {
                                    edge = new Edge
                                    {
                                        src = node,
                                        dst = new SV { StageId = dstStage.First, VertexId = -1 }
                                    },
                                    srcTime = new LexStamp { time = nodeTimes.First },
                                    dstTime = new LexStamp { time = dstTime }
                                })));

            if (!update.isTemporary)
            {
                foreach (var srcGroup in update.discardedMessages
                    .SelectMany(dstStage =>
                        dstStage.Second.Select(nodeTimes => nodeTimes.First.PairWith(dstStage.First.PairWith(nodeTimes.Second))))
                    .GroupBy(srcTime => srcTime.First))
                {
                    if (state.currentRestoration.Contains(srcGroup.Key))
                    {
                        throw new ApplicationException("Stale Discarded message");
                    }

                    state.discardedMessages.Add(
                        srcGroup.Key,
                        srcGroup.SelectMany(srcTime => srcTime.Second.Second
                            .Select(dstTime => srcTime.Second.First.PairWith(dstTime)))
                            .ToArray());
                }
            }
            discardedMessageChanges = discardedMessageChanges.
                Concat(discarded.Select(m => new Weighted<DiscardedMessage>(m, updateWeight)));

            this.nodeState[node] = state;
        }

        private void InjectChangesFromComputedUpdate(
            SV node, FTFrontier newFrontier, bool isLowWatermark,
            ref IEnumerable<Weighted<Checkpoint>> checkpointChanges,
            ref IEnumerable<Weighted<Notification>> notificationChanges,
            ref IEnumerable<Weighted<DeliveredMessage>> deliveredMessageChanges,
            ref IEnumerable<Weighted<DiscardedMessage>> discardedMessageChanges,
            HashSet<int> newStageFrontiers
            )
        {
            NodeState state = this.nodeState[node];

            var thisCheckpoints = state.checkpoints
                .Where(c => !newFrontier.Equals(c) &&
                            isLowWatermark == newFrontier.Contains(c))
                .ToArray();
            if (state.downwardClosed)
            {
                if (isLowWatermark)
                {
                    if (thisCheckpoints.Length > 0)
                    {
                        throw new ApplicationException("Multiple downward-closed checkpoints");
                    }
                }
                else
                {
                    if (thisCheckpoints.Length > 0)
                    {
                        if (thisCheckpoints.Length > 1 || state.checkpoints.Count != 1)
                        {
                            throw new ApplicationException("Multiple downward-closed checkpoints");
                        }

                        state.checkpoints.Remove(thisCheckpoints[0]);
                        state.checkpoints.Add(newFrontier);

                        checkpointChanges = checkpointChanges
                            .Concat(new Weighted<Checkpoint>[] {
                                new Weighted<Checkpoint>(
                                    new Checkpoint { node = node, checkpoint = newFrontier, downwardClosed = true }, 1),
                                new Weighted<Checkpoint>(
                                    new Checkpoint { node = node, checkpoint = thisCheckpoints[0], downwardClosed = true }, -1) });
                    }
                }
            }
            else
            {
                checkpointChanges = checkpointChanges
                    .Concat(thisCheckpoints
                        .Select(c => new Weighted<Checkpoint>(
                            new Checkpoint { node = node, checkpoint = c, downwardClosed = state.downwardClosed }, -1)));
                foreach (var c in thisCheckpoints)
                {
                    state.checkpoints.Remove(c);
                }
            }

            var thisNotifications = state.deliveredNotifications
                .Where(n => isLowWatermark == newFrontier.Contains(n))
                .ToArray();
            notificationChanges = notificationChanges
                .Concat(thisNotifications
                    .Select(n => new Weighted<Notification>(
                        new Notification { node = node, time = new LexStamp { time = n } }, -1)));
            foreach (var n in thisNotifications)
            {
                state.deliveredNotifications.Remove(n);
            }

            var thisDeliveredMessages = state.deliveredMessages
                .Where(m => isLowWatermark == newFrontier.Contains(m.Key))
                .ToArray();
            deliveredMessageChanges = deliveredMessageChanges
                .Concat(thisDeliveredMessages
                    .SelectMany(t => t.Value
                        .Select(m => new Weighted<DeliveredMessage>(
                            new DeliveredMessage
                            {
                                edge = new Edge { src = new SV(m, -1), dst = node },
                                dstTime = new LexStamp { time = t.Key }
                            }, -1))));
            foreach (var m in thisDeliveredMessages)
            {
                state.deliveredMessages.Remove(m.Key);
            }

            if (isLowWatermark)
            {
                FTFrontier oldStageFrontier = this.StageFrontier(node.StageId);
                this.RemoveStageFrontier(node.StageId, oldStageFrontier);
                this.AddStageFrontier(node.StageId, newFrontier);
                FTFrontier newStageFrontier = this.StageFrontier(node.StageId);
                if (!oldStageFrontier.Equals(newStageFrontier))
                {
                    newStageFrontiers.Add(node.StageId);

                    foreach (SV upstream in this.upstreamStage[node.StageId])
                    {
                        NodeState upstreamState = this.nodeState[upstream];

                        var staleDiscarded = upstreamState.discardedMessages
                            .Select(srcTime =>
                                new KeyValuePair<Pointstamp, Pair<int, Pointstamp>[]>(
                                    srcTime.Key,
                                    srcTime.Value
                                        .Where(dst =>
                                            dst.First.Equals(node.StageId) && newStageFrontier.Contains(dst.Second))
                                        .ToArray()))
                            .Where(srcTime => srcTime.Value.Length > 0)
                            .ToArray();

                        discardedMessageChanges = discardedMessageChanges
                            .Concat(staleDiscarded
                                .SelectMany(srcTime => srcTime.Value
                                    .Select(dst => new Weighted<DiscardedMessage>(
                                        new DiscardedMessage
                                        {
                                            edge = new Edge { src = upstream, dst = new SV(dst.First, -1) },
                                            srcTime = new LexStamp { time = srcTime.Key },
                                            dstTime = new LexStamp { time = dst.Second }
                                        }, -1))));

                        foreach (var staleSrcTime in staleDiscarded)
                        {
                            var newSrcMessages = upstreamState.discardedMessages[staleSrcTime.Key]
                                .Where(dst => !dst.First.Equals(node.StageId) || !newStageFrontier.Contains(dst.Second))
                                .ToArray();

                            if (newSrcMessages.Length == 0)
                            {
                                upstreamState.discardedMessages.Remove(staleSrcTime.Key);
                            }
                            else
                            {
                                upstreamState.discardedMessages[staleSrcTime.Key] = newSrcMessages;
                            }
                        }

                        this.nodeState[upstream] = upstreamState;
                    }
                }
            }
            else
            {
                var thisDiscardedMessages = state.discardedMessages
                    .Where(m => !newFrontier.Contains(m.Key))
                    .ToArray();
                discardedMessageChanges = discardedMessageChanges
                    .Concat(thisDiscardedMessages
                        .SelectMany(srcTime => srcTime.Value
                            .Select(dst => new Weighted<DiscardedMessage>(
                                new DiscardedMessage
                                {
                                    edge = new Edge { src = node, dst = new SV(dst.First, -1) },
                                    srcTime = new LexStamp { time = srcTime.Key },
                                    dstTime = new LexStamp { time = dst.Second }
                                }, -1))));
                foreach (var m in thisDiscardedMessages)
                {
                    state.discardedMessages.Remove(m.Key);
                }
            }

            this.nodeState[node] = state;
        }

        private bool AddChangesFromComputedUpdate(
            IGrouping<SV, Weighted<Frontier>> computedUpdate,
            ref IEnumerable<Weighted<Checkpoint>> checkpointChanges,
            ref IEnumerable<Weighted<Notification>> notificationChanges,
            ref IEnumerable<Weighted<DeliveredMessage>> deliveredMessageChanges,
            ref IEnumerable<Weighted<DiscardedMessage>> discardedMessageChanges,
            List<CheckpointLowWatermark> gcUpdates,
            HashSet<int> newStageUpdates)
        {
            SV node = computedUpdate.Key;
            NodeState state = this.nodeState[node];
            FTFrontier oldRestoration = state.currentRestoration;

            foreach (var change in computedUpdate.GroupBy(u => u.record.isNotification))
            {
                var updates = change.OrderBy(c => c.weight).ToArray();
                if (change.Key)
                {
                    if (!((updates.Length == 2 &&
                           updates[0].weight == -1 && updates[0].record.frontier.Equals(state.currentNotification) &&
                           updates[1].weight == 1) ||
                          (updates.Length == 1 &&
                           updates[0].weight == 1 && updates[0].record.frontier.Empty && state.currentNotification.Empty)))
                    {
                        throw new ApplicationException("Bad incremental logic");
                    }
                    state.currentNotification = updates.Last().record.frontier;
                }
                else
                {
                    if (!((updates.Length == 2 &&
                           updates[0].weight == -1 && updates[0].record.frontier.Equals(state.currentRestoration) &&
                           updates[1].weight == 1 && updates[1].record.frontier.Contains(state.currentRestoration)) ||
                          (updates.Length == 1 &&
                           updates[0].weight == 1 && updates[0].record.frontier.Empty && state.currentRestoration.Empty)))
                    {
                        throw new ApplicationException("Bad incremental logic");
                    }
                    state.currentRestoration = updates.Last().record.frontier;
                }
            }

            this.nodeState[node] = state;

            if (oldRestoration.Equals(state.currentRestoration))
            {
                return false;
            }

            if (oldRestoration.Contains(state.currentRestoration))
            {
                throw new ApplicationException("Bad incremental logic");
            }

            gcUpdates.Add(new CheckpointLowWatermark
            {
                managerVertex = state.gcUpdateSendVertexId,
                stageId = node.StageId, vertexId = node.VertexId,
                dstStageId = -1, dstVertexId = -1,
                frontier = state.currentRestoration
            });

            foreach (SV upstream in this.upstreamEdges[node])
            {
                NodeState upstreamState = this.nodeState[upstream];
                gcUpdates.Add(new CheckpointLowWatermark
                    {
                        managerVertex = upstreamState.gcUpdateSendVertexId,
                        stageId = upstream.StageId, vertexId = upstream.VertexId,
                        dstStageId = node.StageId, dstVertexId = node.VertexId,
                        frontier = state.currentRestoration
                    });
            }

            this.InjectChangesFromComputedUpdate(node, state.currentRestoration, true,
                ref checkpointChanges, ref notificationChanges, ref deliveredMessageChanges, ref discardedMessageChanges, newStageUpdates);

            return true;
        }

        private bool InjectUpdates(
            IEnumerable<CheckpointUpdate> updates,
            IEnumerable<Weighted<Frontier>> changes,
            bool sendGCUpdates)
        {
            IEnumerable<Weighted<Checkpoint>> checkpointChanges = new Weighted<Checkpoint>[0];
            IEnumerable<Weighted<Notification>> notificationChanges = new Weighted<Notification>[0];
            IEnumerable<Weighted<DeliveredMessage>> deliveredMessageChanges = new Weighted<DeliveredMessage>[0];
            IEnumerable<Weighted<DiscardedMessage>> discardedMessageChanges = new Weighted<DiscardedMessage>[0];
            List<CheckpointLowWatermark> gcUpdates;
            HashSet<int> newStageUpdates = new HashSet<int>();
            
            if (sendGCUpdates)
            {
                gcUpdates = new List<CheckpointLowWatermark>();
            }
            else
            {
                gcUpdates = this.pendingGCUpdates;
            }

            bool didAnything = false;

            foreach (CheckpointUpdate update in updates)
            {
                this.WriteLog(update.stageId + "." + update.vertexId + " " + update.frontier + " " + (update.isTemporary ? "TA" : "UA"));
                this.AddChangesFromUpdate(update, 1,
                    ref checkpointChanges, ref notificationChanges, ref deliveredMessageChanges, ref discardedMessageChanges);
                didAnything = true;
            }

            if (changes != null)
            {
                foreach (IGrouping<SV, Weighted<Frontier>> computedUpdate in changes.GroupBy(c => c.record.node))
                {
                    didAnything = this.AddChangesFromComputedUpdate(computedUpdate,
                        ref checkpointChanges, ref notificationChanges, ref deliveredMessageChanges, ref discardedMessageChanges,
                        gcUpdates, newStageUpdates)
                        || didAnything;
                }
            }

            if (didAnything)
            {
                foreach (int stage in newStageUpdates.Where(s => this.stagesToMonitor.Contains(s)))
                {
                    foreach (int sendVertex in this.nodeState.Values.Select(s => s.gcUpdateSendVertexId).Distinct())
                    {
                        gcUpdates.Add(new CheckpointLowWatermark
                        {
                            managerVertex = sendVertex,
                            stageId = stage,
                            vertexId = -1,
                            dstStageId = -1,
                            dstVertexId = -1,
                            frontier = this.StageFrontier(stage)
                        });
                    }
                }

                if (gcUpdates.Count > 0 && this.computation != null && sendGCUpdates)
                {
                    foreach (var update in gcUpdates)
                    {
                        this.WriteLog(update.stageId + "." + update.vertexId + " " + update.frontier + " " + "G" + update.dstStageId + "." + update.dstVertexId);
                    }
                    this.computation.ReceiveCheckpointUpdates(gcUpdates);
                }

                this.WriteLog("START");

                this.checkpointStream.OnNext(checkpointChanges);
                this.deliveredNotifications.OnNext(notificationChanges);
                this.deliveredMessages.OnNext(deliveredMessageChanges);
                this.discardedMessages.OnNext(discardedMessageChanges);

                ++this.epoch;
            }

            return didAnything;
        }

        private void InjectRollbackUpdates(IEnumerable<CheckpointUpdate> updates)
        {
            IEnumerable<Weighted<Checkpoint>> checkpointChanges = new Weighted<Checkpoint>[0];
            IEnumerable<Weighted<Notification>> notificationChanges = new Weighted<Notification>[0];
            IEnumerable<Weighted<DeliveredMessage>> deliveredMessageChanges = new Weighted<DeliveredMessage>[0];
            IEnumerable<Weighted<DiscardedMessage>> discardedMessageChanges = new Weighted<DiscardedMessage>[0];

            foreach (CheckpointUpdate update in updates)
            {
                this.AddChangesFromUpdate(
                    update, -1,
                    ref checkpointChanges, ref notificationChanges, ref deliveredMessageChanges, ref discardedMessageChanges);
            }

            foreach (KeyValuePair<SV, CheckpointLowWatermark> rollback in this.rollbackFrontiers)
            {
                if (!rollback.Value.frontier.Complete)
                {
                    this.InjectChangesFromComputedUpdate(rollback.Key, rollback.Value.frontier, false,
                        ref checkpointChanges, ref notificationChanges, ref deliveredMessageChanges, ref discardedMessageChanges, null);
                }
            }

            this.checkpointStream.OnNext(checkpointChanges);
            this.deliveredNotifications.OnNext(notificationChanges);
            this.deliveredMessages.OnNext(deliveredMessageChanges);
            this.discardedMessages.OnNext(discardedMessageChanges);

            ++this.epoch;
        }

        private void GetUpdate(object o, Diagnostics.CheckpointPersistedEventArgs args)
        {
            CheckpointUpdate update = args.checkpoint;

            this.WriteLog(update.stageId + "." + update.vertexId + " " + update.frontier + " " + (update.isTemporary ? "T" : "U"));

            lock (this)
            {
                if (update.isTemporary)
                {
                    if (!(this.state == State.PreparingForRollback || this.state == State.DrainingForRollback))
                    {
                        throw new ApplicationException("Got temporary update in state " + this.state);
                    }

                    this.temporaryUpdates.Add(update);
                    return;
                }

                if (!(this.state == State.Incremental || this.state == State.PreparingForRollback))
                {
                    throw new ApplicationException("Got update in state " + this.state);
                }

                if (this.pendingUpdates == null)
                {
                    // there is no computation in progress, so we're going to start one

                    // make a list that subsequent updates will be queued in while the new computation is ongoing
                    this.pendingUpdates = new List<CheckpointUpdate>();
                }
                else
                {
                    this.pendingUpdates.Add(update);
                    return;
                }
            }

            // if we got this far, start a new computation with a single update
            this.InjectUpdates(new CheckpointUpdate[] { update }, null, true);
        }

        /// <summary>
        /// Called while lock is held!!!
        /// </summary>
        /// <param name="changes">updates computed for temporary rollback</param>
        private void DealWithComputedRollbackFrontiers(IEnumerable<Weighted<Frontier>> changes)
        {
            // we just computed the necessary frontiers

            // fill in the low watermark for everyone first
            foreach (var state in this.nodeState)
            {
                this.rollbackFrontiers.Add(state.Key, new CheckpointLowWatermark
                    {
                        stageId = state.Key.StageId,
                        vertexId = state.Key.VertexId,
                        managerVertex = state.Value.gcUpdateSendVertexId,
                        frontier = state.Value.currentRestoration,
                        dstStageId = -2,
                        dstVertexId = -2
                    });
            }

            List<Pair<SV, FTFrontier>> rollbacks = new List<Pair<SV, FTFrontier>>();

            foreach (var change in changes.Where(c => !c.record.isNotification))
            {
                CheckpointLowWatermark current = this.rollbackFrontiers[change.record.node];

                if (change.weight == 1)
                {
                    if (change.record.frontier.Equals(current.frontier) || current.frontier.Contains(change.record.frontier))
                    {
                        throw new ApplicationException("Rollback below low watermark");
                    }

                    current.frontier = change.record.frontier;
                    this.rollbackFrontiers[change.record.node] = current;
                }
                else if (change.weight == -1)
                {
                    if (!change.record.frontier.Equals(this.nodeState[change.record.node].currentRestoration))
                    {
                        throw new ApplicationException("Rollback doesn't match state");
                    }
                }
                else
                {
                    throw new ApplicationException("Rollback has weight " + change.weight);
                }

            }

            // now revert the temporary updates and discard any state and deltas that have been invalidated
            // by the rollback
            this.InjectRollbackUpdates(this.temporaryUpdates);
        }

        /// <summary>
        /// Called while lock is held!!!
        /// </summary>
        /// <param name="changes">updates computed for temporary rollback</param>
        private void CleanUpAfterRollback(IEnumerable<Weighted<Frontier>> changes)
        {
            foreach (var change in changes.Where(c => c.weight > 0))
            {
                if (change.weight != 1)
                {
                    throw new ApplicationException("Rollback has weight " + change.weight);
                }

                NodeState current = this.nodeState[change.record.node];

                if (change.record.isNotification && !change.record.frontier.Equals(current.currentNotification))
                {
                    throw new ApplicationException("Bad rollback reversion");
                }
                if (!change.record.isNotification && !change.record.frontier.Equals(current.currentRestoration))
                {
                    throw new ApplicationException("Bad rollback reversion");
                }
            }

            // tell the rollback thread we are ready to proceed
            this.quiescenceBarrier.Set();
            this.quiescenceBarrier = null;
        }

        private void ReactToFrontiers(IEnumerable<Weighted<Frontier>> changes)
        {
            this.WriteLog("COMPLETE");

            while (true)
            {
                List<CheckpointUpdate> queuedUpdates = new List<CheckpointUpdate>();
                State currentState;

                lock (this)
                {
                    currentState = this.state;

                    switch (currentState)
                    {
                        case State.Incremental:
                        case State.PreparingForRollback:
                            if (changes == null && this.pendingUpdates.Count == 0)
                            {
                                // there's nothing more to do, so indicate that there is no computation in progress
                                this.pendingUpdates = null;
                                return;
                            }
                            else
                            {
                                // get hold of any updates that were sent in while we were computing
                                queuedUpdates = this.pendingUpdates;
                                // make sure subsequent updates continue to get queued
                                this.pendingUpdates = new List<CheckpointUpdate>();
                            }
                            break;

                        case State.DrainingForRollback:
                            if (changes == null && this.pendingUpdates.Count == 0)
                            {
                                // there's nothing more to do, so start the rollback computation
                                this.WriteLog("START ROLLBACK");
                                this.state = State.AddedTemporaryForRollback;
                                this.pendingUpdates = null;
                                queuedUpdates = this.temporaryUpdates;
                            }
                            else
                            {
                                // get hold of any updates that were sent in while we were computing
                                queuedUpdates = this.pendingUpdates;
                                // make sure subsequent updates continue to get queued
                                this.pendingUpdates = new List<CheckpointUpdate>();
                            }
                            break;

                        case State.DrainingForExit:
                            // no point in continuing to update things since we are exiting
                            this.computation.ReceiveCheckpointUpdates(null);
                            this.computation = null;
                            return;

                        case State.Stopping:
                            // no point in continuing to update things since we are exiting
                            this.quiescenceBarrier.Set();
                            this.quiescenceBarrier = null;
                            return;

                        case State.AddedTemporaryForRollback:
                            if (this.pendingUpdates!= null)
                            {
                                throw new ApplicationException("New updates during rollback");
                            }

                            this.WriteLog("START REVOKING");
                            this.state = State.RevokingTemporaryForRollback;
                            queuedUpdates = this.temporaryUpdates;
                            break;

                        case State.RevokingTemporaryForRollback:
                            if (this.pendingUpdates!= null)
                            {
                                throw new ApplicationException("New updates during rollback reversion");
                            }

                            this.WriteLog("FINISHED REVOKING");
                            this.state = State.Incremental;
                            break;
                    }
                }

                switch (currentState)
                {
                    case State.Incremental:
                    case State.PreparingForRollback:
                    case State.DrainingForRollback:
                        if (this.InjectUpdates(queuedUpdates, changes, currentState != State.DrainingForRollback))
                        {
                            // we started a new computation, so we don't need to do any more here
                            return;
                        }

                        // we didn't start a new computation so go around the loop in case somebody added a new pending
                        // update in the meantime
                        changes = null;
                        break;

                    case State.AddedTemporaryForRollback:
                        this.DealWithComputedRollbackFrontiers(changes);
                        return;

                    case State.RevokingTemporaryForRollback:
                        this.CleanUpAfterRollback(changes);
                        return;

                    case State.DrainingForExit:
                    case State.Stopping:
                        throw new ApplicationException("Bad case " + currentState);
                }
            }
        }

        private void ShowRollback()
        {
            foreach (var state in this.rollbackFrontiers.OrderBy(s => (s.Key.StageId << 16) + s.Key.VertexId))
            {
                Console.WriteLine(this.stages[state.Key.StageId] + "[" + state.Key.VertexId + "] " +
                    state.Value.frontier);
            }
        }

        private void ShowState(bool fullState)
        {
            foreach (var state in this.nodeState.OrderBy(s => (s.Key.StageId << 16) + s.Key.VertexId))
            {
                Console.WriteLine(this.stages[state.Key.StageId] + "[" + state.Key.VertexId + "] " +
                    state.Value.currentRestoration + "; " + state.Value.currentNotification);

                if (fullState)
                {
                    Console.Write(" ");
                    foreach (var checkpoint in state.Value.checkpoints.OrderBy(c => c))
                    {
                        Console.Write(" " + checkpoint);
                    }
                    Console.WriteLine();


                    Console.Write(" ");
                    foreach (var time in state.Value.deliveredNotifications.OrderBy(t => new LexStamp { time = t }))
                    {
                        Console.Write(" " + time.Timestamp);
                    }
                    Console.WriteLine();

                    Console.Write(" ");
                    foreach (var time in state.Value.deliveredMessages.OrderBy(t => new LexStamp { time = t.Key }))
                    {
                        Console.Write(" " + time.Key.Timestamp + ":");
                        foreach (var src in time.Value)
                        {
                            Console.Write(" " + src);
                        }
                        Console.Write(";");
                    }
                    Console.WriteLine();

                    Console.Write(" ");
                    foreach (var time in state.Value.discardedMessages.OrderBy(t => new LexStamp { time = t.Key }))
                    {
                        Console.Write(" " + time.Key.Timestamp + ":");
                        foreach (var dst in time.Value.OrderBy(d => new LexStamp { time = d.Second }))
                        {
                            Console.Write(" " + dst.First + "=" + dst.Second.Timestamp);
                        }
                        Console.Write(";");
                    }
                    Console.WriteLine();
                }
            }
        }

        private void Manage(ManualResetEventSlim startBarrier, ManualResetEventSlim stopBarrier)
        {
            Configuration config = new Configuration();
            config.MaxLatticeInternStaleTimes = 10;

            using (Computation reconciliation = NewComputation.FromConfig(config))
            {
                this.graph = reconciliation.NewInputCollection<Edge>();
                this.checkpointStream = reconciliation.NewInputCollection<Checkpoint>();
                this.deliveredMessages = reconciliation.NewInputCollection<DeliveredMessage>();
                this.deliveredNotifications = reconciliation.NewInputCollection<Notification>();
                this.discardedMessages = reconciliation.NewInputCollection<DiscardedMessage>();

                Collection<Frontier, Epoch> initial = this.checkpointStream
                    .Max(c => c.node, c => c.checkpoint)
                    .SelectMany(c => new Frontier[] {
                    new Frontier { node = c.node, frontier = c.checkpoint, isNotification = false },
                    new Frontier { node = c.node, frontier = c.checkpoint, isNotification = true } });

                var frontiers = initial
                    .FixedPoint((c, f) =>
                        {
                            var reducedDiscards = f
                                .ReduceForDiscarded(
                                    this.checkpointStream.EnterLoop(c), this.discardedMessages.EnterLoop(c));

                            var reduced = f
                                .Reduce(
                                    this.checkpointStream.EnterLoop(c), this.deliveredMessages.EnterLoop(c),
                                    this.deliveredNotifications.EnterLoop(c), this.graph.EnterLoop(c),
                                    this);

                            return reduced.Concat(reducedDiscards).Concat(f)
                                .Min(ff => ff.node.PairWith(ff.isNotification), ff => ff.frontier);
                        })
                    .Consolidate();

                var sync = frontiers.Subscribe(changes => ReactToFrontiers(changes));

                reconciliation.Activate();

                startBarrier.Set();

                // the streams will now be fed by other threads until the computation exits

                stopBarrier.Wait();

                ManualResetEventSlim finalBarrier = null;
                lock (this)
                {
                    if (this.pendingUpdates != null)
                    {
                        // there is a computation running
                        this.state = State.Stopping;
                        this.quiescenceBarrier = new ManualResetEventSlim(false);
                        finalBarrier = this.quiescenceBarrier;
                    }
                }

                if (finalBarrier != null)
                {
                    finalBarrier.Wait();
                }

                this.checkpointStream.OnCompleted();
                this.deliveredMessages.OnCompleted();
                this.deliveredNotifications.OnCompleted();
                this.discardedMessages.OnCompleted();

                reconciliation.Join();
            }

            this.ShowState(false);
        }

        public void NotifyComputationExiting()
        {
            lock (this)
            {
                if (this.pendingUpdates == null)
                {
                    // there is no computation running, so shut down the update input
                    this.computation.ReceiveCheckpointUpdates(null);
                    this.computation = null;
                }
                else
                {
                    // there is a computation running, so get it to shut down the update input when it completes
                    this.state = State.DrainingForExit;
                }
            }
        }

        private void ComputeRollback()
        {
            this.WriteLog("COMPUTATION START ROLLBACK");
            this.computation.StartRollback();
            this.WriteLog("COMPUTATION STARTED ROLLBACK");

            // once we get here, everybody should have stopped sending any updates though there may
            // still be a final computation going on

            ManualResetEventSlim barrier = new ManualResetEventSlim();

            bool mustStart = false;

            lock (this)
            {
                if (this.pendingUpdates == null)
                {
                    // there is no computation, so we have to start it ourselves
                    this.state = State.AddedTemporaryForRollback;
                    mustStart = true;
                }
                else
                {
                    // there is a computation going on: tell it to start the rollback when it finishes
                    this.state = State.DrainingForRollback;
                }

                // the machinery will now turn over until everything is computed
                this.rollbackFrontiers = new Dictionary<SV, CheckpointLowWatermark>();
                this.quiescenceBarrier = barrier;
            }

            if (mustStart)
            {
                this.InjectUpdates(this.temporaryUpdates, null, false);
            }

            barrier.Wait();

            this.WriteLog("ROLLBACK COMPLETE");

            lock (this)
            {
                // we shouldn't get any more temporary updates
                this.temporaryUpdates = null;

                // open up for business doing incremental updates from the computation again
                this.state = State.Incremental;
            }
        }

        private Random random = new Random();
        private HashSet<int> failedProcesses = new HashSet<int>();
        private ManualResetEventSlim failureRestartEvent = null;

        public void FailProcess(int processId)
        {
            lock (this)
            {
                if (this.failedProcesses.Contains(processId))
                {
                    throw new ApplicationException("Failing process twice");
                }

                this.failedProcesses.Add(processId);
            }

            int restartDelay = 500 + this.random.Next(100);

            Console.WriteLine("Sending failure request to " + processId + " delay " + restartDelay);
            this.computation.SimulateFailure(processId, restartDelay);
        }

        public void OnSimulatedProcessRestart(object o, Diagnostics.ProcessRestartedEventArgs args)
        {
            int processId = args.processId;

            Console.WriteLine("Got process restart message from " + processId);

            lock (this)
            {
                if (!this.failedProcesses.Contains(processId))
                {
                    throw new ApplicationException("Non-failed process has restarted");
                }

                this.failedProcesses.Remove(processId);

                if (this.failedProcesses.Count == 0 && this.failureRestartEvent != null)
                {
                    this.failureRestartEvent.Set();
                    this.failureRestartEvent = null;
                }
            }
        }

        public void WaitForSimulatedFailures()
        {
            ManualResetEventSlim restartEvent = null;
            lock (this)
            {
                if (this.failedProcesses.Count > 0)
                {
                    this.failureRestartEvent = new ManualResetEventSlim(false);
                    restartEvent = this.failureRestartEvent;
                }
            }

            if (restartEvent != null)
            {
                Console.WriteLine("Waiting for failed processes to restart");
                restartEvent.Wait();
                restartEvent.Dispose();
                Console.WriteLine("Failed processes have restarted");
            }
        }

        public void PerformRollback(IEnumerable<int> pauseImmediately, IEnumerable<int> pauseAfterRecovery, IEnumerable<int> pauseLast)
        {
            lock (this)
            {
                this.state = State.PreparingForRollback;
                this.temporaryUpdates = new List<CheckpointUpdate>();
                this.pendingGCUpdates = new List<CheckpointLowWatermark>();
            }

            this.computation.PausePeerProcesses(pauseImmediately);

            this.WaitForSimulatedFailures();

            this.computation.PausePeerProcesses(pauseAfterRecovery);
            this.computation.PausePeerProcesses(pauseLast);

            this.ComputeRollback();

            this.ShowRollback();

            IEnumerable<CheckpointLowWatermark> frontiers;
            List<CheckpointLowWatermark> gcUpdates;
            Computation computation;

            lock (this)
            {
                frontiers = this.rollbackFrontiers.Values;
                gcUpdates = this.pendingGCUpdates;
                computation = this.computation;
                this.rollbackFrontiers = null;
                this.pendingGCUpdates = null;
            }

            if (computation != null)
            {
                computation.RestoreToFrontiers(frontiers);
                if (gcUpdates.Count > 0)
                {
                    computation.ReceiveCheckpointUpdates(gcUpdates);
                }
            }
        }

        /// <summary>
        /// Start monitoring the checkpoints for Naiad computation <paramref name="computation"/>
        /// </summary>
        /// <param name="computation">the computation to be managed</param>
        public void Initialize(Computation computation, IEnumerable<int> stagesToMonitor)
        {
            this.computation = computation;

            foreach (int stage in stagesToMonitor)
            {
                this.stagesToMonitor.Add(stage);
            }

            ManualResetEventSlim startBarrier = new ManualResetEventSlim(false);
            ManualResetEventSlim stopBarrier = new ManualResetEventSlim(false);

            this.managerThread = new Thread(() => this.Manage(startBarrier, stopBarrier));
            this.managerThread.Start();

            // wait for the manager to initialize
            startBarrier.Wait();

            this.computation.OnMaterialized += this.GetGraph;
            this.computation.OnCheckpointPersisted += this.GetUpdate;
            this.computation.OnProcessRestarted += this.OnSimulatedProcessRestart;
            this.computation.OnShutdown += (o, a) => stopBarrier.Set();
        }

        /// <summary>
        /// wait for the manager thread to exit
        /// </summary>
        public void Join()
        {
            this.managerThread.Join();
            this.managerThread = null;
        }
    }
}
