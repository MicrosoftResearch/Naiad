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
using System.Text;
using System.Threading.Tasks;

using Microsoft.Research.Naiad.Frameworks.DifferentialDataflow;
using Microsoft.Research.Naiad.Frameworks.Lindi;
using Microsoft.Research.Naiad.Runtime.FaultTolerance;
using Microsoft.Research.Naiad.Runtime.Progress;
using Microsoft.Research.Naiad.Dataflow;
using Microsoft.Research.Naiad.Dataflow.StandardVertices;

namespace Microsoft.Research.Naiad.FaultToleranceManager
{
    internal struct SV : IEquatable<SV>
    {
        public int StageId;
        public int VertexId;

        public SV(int Stage, int Vertex)
        {
            this.StageId = Stage;
            this.VertexId = Vertex;
        }

        public bool Equals(SV other)
        {
            return this.StageId == other.StageId && this.VertexId == other.VertexId;
        }

        public override int GetHashCode()
        {
            return this.VertexId + (this.StageId << 16);
        }
    }

    internal struct Edge : IEquatable<Edge>
    {
        public SV src;
        public SV dst;

        public bool Equals(Edge other)
        {
            return this.src.Equals(other.src) && this.dst.Equals(other.dst);
        }
    }

    internal struct LexStamp : IEquatable<LexStamp>, IComparable<LexStamp>
    {
        public Pointstamp time;

        public bool Equals(LexStamp other)
        {
            return this.time.Equals(other.time);
        }

        public int CompareTo(LexStamp other)
        {
            if (this.time.Equals(other.time))
            {
                return 0;
            }
            else
            {
                return FTFrontier.IsLessThanOrEqualTo(this.time, other.time) ? -1 : 1;
            }
        }

        public override int GetHashCode()
        {
            return time.GetHashCode();
        }
    }

    internal struct DeliveredMessage : IEquatable<DeliveredMessage>
    {
        public Edge edge;
        public LexStamp dstTime;

        public bool Equals(DeliveredMessage other)
        {
            return
                edge.Equals(other.edge) && dstTime.Equals(other.dstTime);
        }
    }

    internal struct DiscardedMessage : IEquatable<DiscardedMessage>
    {
        public Edge edge;
        public LexStamp srcTime;
        public LexStamp dstTime;

        public bool Equals(DiscardedMessage other)
        {
            return
                edge.Equals(other.edge) && srcTime.Equals(other.srcTime) && dstTime.Equals(other.dstTime);
        }
    }

    internal struct Notification : IEquatable<Notification>
    {
        public SV node;
        public LexStamp time;

        public bool Equals(Notification other)
        {
            return
                node.Equals(other.node) && time.Equals(other.time);
        }
    }

    internal struct Frontier : IEquatable<Frontier>
    {
        public SV node;
        public FTFrontier frontier;
        public bool isNotification;

        public Frontier(SV node, FTFrontier frontier, bool isNotification)
        {
            this.node = node;
            this.frontier = frontier;
            this.isNotification = isNotification;
        }

        public bool Equals(Frontier other)
        {
            return this.node.Equals(other.node)
                && this.frontier.Equals(other.frontier)
                && this.isNotification.Equals(other.isNotification);
        }
    }

    internal struct Checkpoint : IEquatable<Checkpoint>
    {
        public SV node;
        public FTFrontier checkpoint;
        public bool downwardClosed;

        public bool Equals(Checkpoint other)
        {
            return
                node.Equals(other.node) && checkpoint.Equals(other.checkpoint) && downwardClosed == other.downwardClosed;
        }
    }

    internal static class ExtensionMethods
    {
        private static Pair<Checkpoint, LexStamp> PairCheckpointToBeLowerThanTime(Checkpoint checkpoint, LexStamp time)
        {
            if (checkpoint.downwardClosed && checkpoint.checkpoint.Contains(time.time))
            {
                return new Checkpoint
                {
                    node = checkpoint.node,
                    checkpoint = FTFrontier.SetBelow(time.time),
                    downwardClosed = true
                }.PairWith(time);
            }
            else
            {
                return checkpoint.PairWith(time);
            }
        }

        private static Pair<Checkpoint, FTFrontier> PairCheckpointToBeWithinFrontier(Checkpoint checkpoint, FTFrontier frontier)
        {
            if (checkpoint.downwardClosed && checkpoint.checkpoint.Contains(frontier))
            {
                return new Checkpoint
                {
                    node = checkpoint.node,
                    checkpoint = frontier,
                    downwardClosed = true
                }.PairWith(frontier);
            }
            else
            {
                return checkpoint.PairWith(frontier);
            }
        }

        public static Collection<Frontier, T> ReduceForDiscarded<T>(
            this Collection<Frontier, T> frontiers,
            Collection<Checkpoint, T> checkpoints,
            Collection<DiscardedMessage, T> discardedMessages) where T : Time<T>
        {
            return frontiers
                // only take the restoration frontiers
                .Where(f => !f.isNotification)
                // match with all the discarded messages to the node for a given restoration frontier
                .Join(discardedMessages, f => f.node.StageId, m => m.edge.dst.StageId, (f, m) => f.PairWith(m))
                // keep all discarded messages that are outside the restoration frontier at the node
                .Where(p => !p.First.frontier.Contains(p.Second.dstTime.time))
                // we only need the sender node id and the send time of the discarded message
                .Select(p => p.Second.edge.src.PairWith(p.Second.srcTime))
                // keep the sender node and minimum send time of any discarded message outside its destination restoration frontier
                .Min(m => m.First, m => m.Second)
                // for each node that sent a needed discarded message, match it up with all the available checkpoints,
                // reducing downward-closed checkpoints to be less than the time the message was sent
                .Join(
                    checkpoints, m => m.First, c => c.node,
                    (m, c) => PairCheckpointToBeLowerThanTime(c, m.Second))
                // then throw out any checkpoints that included any required but discarded sent messages
                .Where(c => !c.First.checkpoint.Contains(c.Second.time))
                // and just keep the feasible checkpoint
                .Select(c => c.First)
                // now select the largest feasible checkpoint at each node constrained by discarded messages
                .Max(c => c.node, c => c.checkpoint)
                // and convert it to a pair of frontiers
                .SelectMany(c => new Frontier[] {
                    new Frontier { node = c.node, frontier = c.checkpoint, isNotification = false },
                    new Frontier { node = c.node, frontier = c.checkpoint, isNotification = true } });
        }

        public static Collection<Frontier, T> Reduce<T>(
            this Collection<Frontier, T> frontiers,
            Collection<Checkpoint, T> checkpoints,
            Collection<DeliveredMessage, T> deliveredMessageTimes,
            Collection<Notification, T> deliveredNotificationTimes,
            Collection<Edge, T> graph, FTManager manager) where T : Time<T>
        {
            Collection<Pair<Edge, FTFrontier>, T> projectedMessageFrontiers = frontiers
                // only look at the restoration frontiers
                .Where(f => !f.isNotification)
                // project each frontier along each outgoing edge
                .Join(
                    graph, f => f.node, e => e.src,
                    (f, e) => new Edge { src = new SV(e.src.StageId, -1), dst = e.dst }
                        .PairWith(f.frontier.Project(manager.Stages[e.src.StageId], e.dst.StageId)));

            Collection<Frontier, T> intersectedProjectedNotificationFrontiers = frontiers
                // only look at the notification frontiers
                .Where(f => f.isNotification)
                // project each frontier along each outgoing edge to its destination
                .Join(
                    graph, f => f.node, e => e.src,
                    (f, e) => new Frontier {
                        node = e.dst,
                        frontier = f.frontier.Project(manager.Stages[e.src.StageId], e.dst.StageId),
                        isNotification = true })
                // and find the intersection (minimum) of the projections at the destination
                .Min(f => f.node, f => f.frontier);

            Collection<Pair<SV,LexStamp>,T> staleDeliveredMessages = deliveredMessageTimes
                // match up delivered messages with the projected frontier along the delivery edge,
                // keeping the dst node, dst time and projected frontier
                .Join(
                    projectedMessageFrontiers, m => m.edge, f => f.First,
                    (m, f) => f.First.dst.PairWith(m.dstTime.PairWith(f.Second)))
                // filter to keep only messages that fall outside their projected frontiers
                .Where(m => !m.Second.Second.Contains(m.Second.First.time))
                // we only care about the destination node and stale message time
                .Select(m => m.First.PairWith(m.Second.First));

            Collection<Pair<SV,LexStamp>,T> staleDeliveredNotifications = deliveredNotificationTimes
                // match up delivered notifications with the intersected projected notification frontier at the node,
                // keeping node, time and intersected projected frontier
                .Join(
                    intersectedProjectedNotificationFrontiers, n => n.node, f => f.node,
                    (n, f) => n.node.PairWith(n.time.PairWith(f.frontier)))
                // filter to keep only notifications that fall outside their projected frontiers
                .Where(n => !n.Second.Second.Contains(n.Second.First.time))
                // we only care about the node and stale notification time
                .Select(n => n.First.PairWith(n.Second.First));

            Collection<Pair<SV,LexStamp>,T> earliestStaleEvents = staleDeliveredMessages
                .Concat(staleDeliveredNotifications)
                // keep only the earliest stale event at each node
                .Min(n => n.First, n => n.Second);

            var reducedFrontiers = checkpoints
                // for each node that executed a stale, match it up with all the available checkpoints,
                // reducing downward-closed checkpoints to be less than the time the event happened at
                .Join(
                    earliestStaleEvents, c => c.node, e => e.First,
                    (c, e) => PairCheckpointToBeLowerThanTime(c, e.Second))
                // then throw out any checkpoints that included any stale events
                .Where(c => !c.First.checkpoint.Contains(c.Second.time))
                // and select the largest feasible checkpoint at each node
                .Max(c => c.First.node, c => c.First.checkpoint)
                // then convert it to a pair of frontiers
                .SelectMany(c => new Frontier[] {
                    new Frontier { node = c.First.node, frontier = c.First.checkpoint, isNotification = false },
                    new Frontier { node = c.First.node, frontier = c.First.checkpoint, isNotification = true } });

            // return any reduction in either frontier
            return reducedFrontiers.Concat(intersectedProjectedNotificationFrontiers);
        }
    }
}
