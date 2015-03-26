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
using Microsoft.Research.Naiad.Runtime.Controlling;
using Microsoft.Research.Naiad.Runtime.FaultTolerance;
using Microsoft.Research.Naiad.DataStructures;

namespace Microsoft.Research.Naiad.Runtime.Progress
{
    internal class Reachability
    {
        public List<List<int>> ComparisonDepth;

        public int CompareTo(Pointstamp a, Pointstamp b)
        {
            if (a.Timestamp[0] != b.Timestamp[0])
                return a.Timestamp[0] - b.Timestamp[0];

            if (a.Equals(b))
                return 0;

            if (this.LessThan(a, b))
                return -1;

            if (this.LessThan(a, b))
                return 1;

            return (a.Location - b.Location);
        }

        public bool LessThan(Pointstamp a, Pointstamp b)
        {
            // early exit if this.epoch > that.epoch
            if (a.Timestamp[0] > b.Timestamp[0])
                return false;

            var depth = ComparisonDepth[a.Location][b.Location];
            if (depth == 0)
                return false;
            else
            {
                var increment = depth < 0;
                depth = Math.Abs(depth);

                for (int i = 1; i < depth; i++)
                {
                    if (a.Timestamp[i] > b.Timestamp[i])
                        return false;

                    if (i + 1 == depth && increment && a.Timestamp[i] + 1 > b.Timestamp[i])
                        return false;

                    if (a.Timestamp[i] < b.Timestamp[i])
                        return true;
                }

                return true;
            }
        }


        public struct GraphNode
        {
            public readonly int Index;
            public readonly int Depth;
            public readonly int[] Neighbors;

            public readonly bool Ingress;
            public readonly bool Egress;
            public readonly bool Advance;

            public readonly bool Exchanges; // true iff an exchange edge

            public readonly bool IsStage;  // true iff a stage node.

            public GraphNode(Dataflow.Stage stage)
            {
                this.Index = stage.StageId;
                this.Depth = stage.DefaultVersion.Timestamp.Length;

                this.Ingress = stage.IsIterationIngress;
                this.Egress = stage.IsIterationEgress;
                this.Advance = stage.IsIterationAdvance;

                this.Neighbors = new int[stage.Targets.Count];
                for (int i = 0; i < Neighbors.Length; i++)
                    Neighbors[i] = stage.Targets[i].ChannelId;

                this.Exchanges = false;
                this.IsStage = true;
            }
            public GraphNode(Dataflow.Edge edge)
            {
                this.Index = edge.ChannelId;
                this.Depth = edge.SourceStage.DefaultVersion.Timestamp.Length;
                if (edge.SourceStage.IsIterationIngress)
                {
                    ++this.Depth;
                }
                if (edge.SourceStage.IsIterationEgress)
                {
                    --this.Depth;
                }

                this.Ingress = false;
                this.Egress = false;
                this.Advance = false;

                this.Neighbors = new int[] { edge.TargetStage.StageId };

                this.Exchanges = edge.Exchanges;
                this.IsStage = false;
            }
            public GraphNode(int index, int[] neighbors)
            {
                this.Index = index;
                this.Depth = 0;

                this.Ingress = false;
                this.Egress = false;
                this.Advance = false;

                this.Neighbors = neighbors;

                this.Exchanges = false;
                this.IsStage = false;
            }
        }

        public GraphNode[] Graph;

        internal void RegenerateGraph(InternalComputation manager)
        {
            var maxIdentifier = 0;

            if (manager.Stages.Count() > 0)
                maxIdentifier = Math.Max(manager.Stages.Max(x => x.Key), maxIdentifier);

            if (manager.Edges.Count() > 0)
                maxIdentifier = Math.Max(manager.Edges.Max(x => x.Key), maxIdentifier);

            this.Graph = new GraphNode[maxIdentifier + 1];

#if true
            for (int i = 0; i < this.Graph.Length; i++)
                this.Graph[i] = new GraphNode(i, new int[] { });

            foreach (var stage in manager.Stages)
                this.Graph[stage.Key] = new GraphNode(stage.Value);

            foreach (var edge in manager.Edges)
                this.Graph[edge.Key] = new GraphNode(edge.Value);
#else
            for (int i = 0; i < this.Graph.Length; i++)
            {
                if (manager.Stages.ContainsKey(i))
                    this.Graph[i] = new GraphNode(manager.Stages[i]);
                else if (manager.Edges.ContainsKey(i))
                    this.Graph[i] = new GraphNode(manager.Edges[i]);
                else  // else it is processing a progress edge, because they are allocated differently
                    this.Graph[i] = new GraphNode(i, new int[] { });
            }
#endif
        }

        public HashSet<int> NoImpersonation = new HashSet<int>();
        public void DoNotImpersonate(int identifier) { this.NoImpersonation.Add(identifier); }

        public int[][] Impersonations;

        public IEnumerable<Pointstamp> EnumerateImpersonations(Pointstamp time)
        {
            var limits = this.Impersonations[time.Location];

            if (limits == null)
            {
                yield break;
            }
            else
            {
                for (int i = 0; i < limits.Length; i++)
                {
                    var depths = this.ComparisonDepth[time.Location][limits[i]];
                    var coords = this.Graph[limits[i]].Depth;

                    var newVersion = new Pointstamp();
                    newVersion.Location = limits[i];
                    newVersion.Timestamp.Length = coords;

                    for (int j = 0; j < newVersion.Timestamp.Length; j++)
                    {
                        if (j < Math.Abs(depths))
                            newVersion.Timestamp[j] = time.Timestamp[j];
                        else
                            newVersion.Timestamp[j] = 0;
                    }

                    if (depths < 0)
                        newVersion.Timestamp[Math.Abs(depths) - 1] = newVersion.Timestamp[Math.Abs(depths) - 1] + 1;

                    yield return newVersion;
                }
            }
        }

        // populates this.ComparisonDepth, indexed by collection and channel identifiers.
        public void UpdateReachabilityPartialOrder(InternalComputation internalComputation)
        {
            RegenerateGraph(internalComputation);

            var reachableDepths = new List<List<int>>(this.Graph.Length);

            var magicNumber = 37;

            //Console.Error.WriteLine("Updating reachability with {0} objects", Reachability.Graph.Length);
            for (int i = 0; i < this.Graph.Length; i++)
            {
                var reachable = new List<int>(this.Graph.Length);

                var versionList = new Pointstamp[] { new Pointstamp(i, Enumerable.Repeat(magicNumber, this.Graph[i].Depth).ToArray()) };

                var reachabilityResults = this.DetermineReachabilityList(versionList);

                for (int j = 0; j < reachabilityResults.Length; j++)
                {
                    var depth = 0;
                    var increment = false;


                    // for each element of the reachable set
                    if (reachabilityResults[j] != null)
                    {
                        for (int k = 0; k < reachabilityResults[j].Count; k++)
                        {
                            for (int l = 0; l < reachabilityResults[j][k].Timestamp.Length && reachabilityResults[j][k].Timestamp[l] >= magicNumber; l++)
                            {
                                if (l + 1 > depth || l + 1 == depth && increment)
                                {
                                    depth = l + 1;
                                    increment = (reachabilityResults[j][k].Timestamp[l] > magicNumber);
                                }
                            }
                        }
                    }

                    reachable.Add(increment ? -depth : depth);
                }

                reachableDepths.Add(reachable);
            }

            this.ComparisonDepth = reachableDepths;

            #region Set up impersonation

            // consider each stage / edge
            this.Impersonations = new int[this.Graph.Length][];

            for (int i = 0; i < this.Graph.Length; i++)
            {
                // not applicable to exchange edges.
                if (!this.Graph[i].Exchanges && !this.NoImpersonation.Contains(i))
                {
                    var reached = new HashSet<int>();
                    var limits = new HashSet<int>();
                    var queue = new List<int>();

                    //reached.Add(i);
                    queue.Add(i);

                    for (int j = 0; j < queue.Count; j++)
                    {
                        var candidate = queue[j];

                        // check if queue[j] is interested in masquerading
                        var available = true;
                        for (int k = 0; k < this.Graph[candidate].Neighbors.Length; k++)
                        {
                            var target = this.Graph[candidate].Neighbors[k];
                            if (this.Graph[target].Exchanges)
                                available = false;
                        }

                        if (!reached.Contains(candidate))
                        {
                            reached.Add(candidate);

                            if (available)
                            {
                                for (int k = 0; k < this.Graph[candidate].Neighbors.Length; k++)
                                    queue.Add(this.Graph[candidate].Neighbors[k]);
                            }
                            else
                            {
                                limits.Add(candidate);
                            }
                        }
                    }

                    // if we found someone who wants to masquerade
                    if (!limits.Contains(i) && limits.Count > 0)
                    {
                        Impersonations[i] = limits.ToArray();
                    }
                    else
                        Impersonations[i] = null;
                }
            }

            #endregion
        }

        // For each operator, compute the minimal antichain of times that are reachable from the given list of versions, and 
        // update their state accordingly.
        public void UpdateReachability(
            InternalController controller, Pointstamp[] versions, List<Dataflow.Vertex> vertices,
            DiscardManager discardManager)
        {
            //Console.Error.WriteLine("Updating reachability with versions");
            //foreach (var version in versions)
            //    Console.Error.WriteLine(version);

            var result = DetermineReachabilityList(versions);

            foreach (var vertex in vertices)
                vertex.UpdateReachability(result[vertex.Stage.StageId]);
        }

        // Returns a list (indexed by graph identifier) of lists of Pointstamps that can be reached at each collection, for the 
        // given array of times. Each sub-list will be a minimal antichain of Pointstamps at which a collection is reachable.
        //
        // If the sublist for a collection is null, that collection is not reachable from the given array of times.

        public List<Pointstamp>[] DetermineReachabilityList(Pointstamp[] times)
        {
            // Initially, the result for each collection is null, which corresponds to it not being reachable from the given times.
            var result = new List<Pointstamp>[this.Graph.Length];

            // For each time, perform breadth-first search from that time to each reachable collection.
            for (int time = 0; time < times.Length; time++)
            {
                // To perform the BFS, we need a list, which will act like a FIFO queue.
                var queue = new List<int>();

                // The BFS starts from the current time's stage
                var index = times[time].Location;

                if (result[index] == null)
                    result[index] = new List<Pointstamp>(0);

                // Attempt to add the current time to the antichain for its own collection.
                if (AddToAntiChain(result[index], times[time]))
                {
                    // If this succeeds, commence BFS from that collection.
                    queue.Add(index);

                    // While we haven't visited every element of the queue, move to the next element.
                    for (int i = 0; i < queue.Count; i++)
                    {
                        var collectionId = queue[i];
                        
                        // For each immediately downstream collection from the current collection, attempt to improve the antichain for the downstream.
                        for (int k = 0; k < this.Graph[collectionId].Neighbors.Length; k++)
                        {
                            var target = this.Graph[collectionId].Neighbors[k];

                            var updated = false;

                            if (result[target] == null)
                                result[target] = new List<Pointstamp>(0);

                            // For each element of the current collection's antichain, evaluate the minimal caused version at the downstream collection.
                            for (int j = 0; j < result[collectionId].Count; j++)
                            {
                                // make a new copy so that we can tamper with the contents
                                var localtime = new Pointstamp(result[collectionId][j]);
                                localtime.Location = target;

                                // If the target is a feedback stage, we must increment the last coordinate.
                                if (this.Graph[collectionId].Advance)
                                    localtime.Timestamp[localtime.Timestamp.Length - 1]++;

                                // If the target is an egress stage, we must strip off the last coordinate.
                                if (this.Graph[collectionId].Egress)
                                {
                                    localtime.Timestamp.Length--;
                                    localtime.Timestamp[localtime.Timestamp.Length] = 0;
                                }

                                // If the target is an ingress stage, we must add a new coordinate.
                                if (this.Graph[collectionId].Ingress)
                                {
                                    localtime.Timestamp.Length++;
                                }

                                if (localtime.Timestamp.Length != this.Graph[target].Depth)
                                    throw new Exception("Something is horribly wrong in Reachability");

                                // If the computed minimal time for the downstream collection becomes a member of its antichain, we have updated it
                                // (and must search forward from that collection).
                                if (AddToAntiChain(result[target], localtime))
                                    updated = true;
                            }

                            // Where the antichain has been updated, we must search forward from the downstream collection.
                            if (updated)
                                queue.Add(target);
                        }
                    }
                }
            }

            return result;
        }

        public bool AddToAntiChain(List<Pointstamp> list, Pointstamp time)
        {
            // bail if time can be reached by any element of list
            for (int i = 0; i < list.Count; i++)
                if (ProductOrderLessThan(list[i], time))
                    return false;

            // belongs in; clean out reachable times.
            for (int i = 0; i < list.Count; i++)
                if (ProductOrderLessThan(time, list[i]))
                {
                    list.RemoveAt(i);
                    i--;
                }

            list.Add(time);
            return true;
        }

        // compares two causal orders for reachability. uses controller to determine which lattice elements correspond to loops, and which to prioritizations.
        // for now, the assumption is that the first int is always the input lattice, which has no back edge.
        // for now, this only works if a and b correspond to the same stage. 
        public bool ProductOrderLessThan(Pointstamp a, Pointstamp b)
        {
            if (a.Timestamp.Length != b.Timestamp.Length)
                Console.WriteLine("should have same length!");

            if (a.Location != b.Location)
                Console.WriteLine("meant to be called on pointstamps of the same stage");

            for (int i = 0; i < a.Timestamp.Length; i++)
                if (a.Timestamp[i] > b.Timestamp[i])
                    return false;

            return true;
        }
    }
}
