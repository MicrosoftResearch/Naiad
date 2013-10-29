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
using System.Text;

using Naiad;
using Naiad.Frameworks.DifferentialDataflow;

namespace Examples.DifferentialDataflow
{
    public static class StronglyConnectedComponentsExtensionMethods
    {
        #region Trimming pre-processing

        // iteratively removes all nodes with no outgoing edges, and incident incoming edges.
        public static Collection<SCC.Edge, T> TrimLeavesAndFlip<T>(this Collection<SCC.Edge, T> edges)
            where T : Time<T>
        {
            return edges.FixedPoint((lc, x) => x.Select(edge => edge.source)      // identify nodes with outgoing edges
                                                .Distinct()                       // keep one copy of each
                                                .Join(edges.EnterLoop(lc),       // retain edges pointing at such nodes
                                                      node => node, edge => edge.target, 
                                                      node => node, edge => edge.source,
                                                      (target, node, source) => new SCC.Edge(source, target)))
                        .Consolidate()
                        .Select(edge => new SCC.Edge(edge.target, edge.source));
        }

        #endregion

        #region SCC logic

        // fixed point of the process that snips edges between nodes in confirmed distinct sccs
        public static Collection<SCC.Edge, T> SCC<T>(this Collection<SCC.Edge, T> edges)
            where T : Time<T>
        {
            return edges.FixedPoint((lc, y) => y.TrimAndTranspose()
                                                .TrimAndTranspose());
        }

        // edges are expected to be partitioned by edge => edge.source
        public static Collection<SCC.Edge, T> TrimAndTranspose<T>(this Collection<SCC.Edge, T> edges) where T : Time<T>
        {
            var labels = edges.BroadcastMin()
                              .Consolidate(node => node.name);

            // each endpoint of an edge is labeled, and we retain only edges with matching labels.
            return edges.Select(edge => new SCC.LabeledEdge(edge, 0, 0))
                        .Join(labels, x => x.edge.source, node => node.name, x => x.edge.target, y => y.label, (k, x, y) => new SCC.LabeledEdge(new SCC.Edge(k, x), y, 0))
                        .Join(labels, x => x.edge.target, node => node.name, x => new IntPair(x.edge.source, x.label1), y => y.label, (k, x, y) => new SCC.LabeledEdge(new SCC.Edge(x.s, k), x.t, y))
                        .Where(x => x.label1 == x.label2)
                        .Select(x => new SCC.Edge(x.edge.target, x.edge.source));
        }

        // repeatedly broadcast the minimum value, and accumulate incoming messages to update the min
        public static Collection<SCC.Node, T> BroadcastMin<T>(this Collection<SCC.Edge, T> edges)
            where T : Time<T>
        {
            var nodes = edges.Select(edge => new SCC.Node(edge.source, edge.source)).PartitionBy(node => node.name);//.Distinct();

            var keep = 10;
            Func<SCC.Node, int> priority = node => 65536 * (node.label < keep ? node.label : keep + Convert.ToInt32(Math.Log(1 + node.label)));

            return nodes.AssumePartitionedBy(node => node.name)
                        .Where(x => false)
                        .GeneralFixedPoint((lc, x) => edges.EnterLoop(lc)
                                                           .Join(x, edge => edge.source, node => node.name, edge => edge.target, node => node.label, (source, target, label) => new SCC.Node(target, label), true)
                                                           .Where(node => node.label < node.name)
                                                           .Concat(nodes.EnterLoop(lc, priority))
                                                           .Min(node => node.name, n => n.label, (s, t) => t, (s, t) => new SCC.Node(s, t), true)
                                                           .AssumePartitionedBy(node => node.name),
                                           priority, node => node.name, Int32.MaxValue);
        }

        #endregion
    }

    /// <summary>
    /// Demonstrates an interactive strongly connected components computation using Naiad.
    /// </summary>
    public class SCC : Example
    {
        #region Custom datatypes to make the code cleaner

        public struct Node : IEquatable<Node>
        {
            public int name;
            public int label;

            public bool Equals(Node that)
            {
                return this.name == that.name && this.label == that.label;
            }

            public override int GetHashCode()
            {
                return 13241 * name + 312 * label;
            }

            public override string ToString()
            {
                return string.Format("[{0}, {1}]", name, label);
            }


            public Node(int n, int l)
            {
                name = n;
                label = l;
            }
        }

        public struct Edge : IEquatable<Edge>
        {
            public int source;
            public int target;

            public bool Equals(Edge that)
            {
                return this.source == that.source && this.target == that.target;
            }

            public override int GetHashCode()
            {
                return 12312 * source + 1231 * target;
            }

            public override string ToString()
            {
                return string.Format("[{0}, {1}]", source, target);
            }

            public Edge(int s, int t)
            {
                source = s;
                target = t;
            }
        }

        public struct LabeledEdge : IEquatable<LabeledEdge>
        {
            public Edge edge;
            public int label1;
            public int label2;

            public bool Equals(LabeledEdge that)
            {
                return this.edge.Equals(that.edge) && this.label1 == that.label1 && this.label2 == that.label2;
            }

            public override string ToString()
            {
                return String.Format("[({0}, {1}), {2}, {3}]", edge.source, edge.target, label1, label2);
            }

            public LabeledEdge(Edge e, int l1, int l2)
            {
                edge = e;
                label1 = l1;
                label2 = l2;
            }
        }

        #endregion 

        int nodeCount = 1000000;
        int edgeCount = 2000000;

        public void Execute(string[] args)
        {
            using (var controller = NewController.FromArgs(ref args))
            {
                // establish numbers of nodes and edges from input or from defaults.
                if (args.Length == 3)
                {
                    nodeCount = Convert.ToInt32(args[1]);
                    edgeCount = Convert.ToInt32(args[2]);
                }

                // generate a random graph
                var random = new Random(0);
                var graph = new Edge[edgeCount];
                for (int i = 0; i < edgeCount; i++)
                    graph[i] = new Edge(random.Next(nodeCount), random.Next(nodeCount));

                using (var manager = controller.NewGraph())
                {
                    // set up the CC computation
                    var edges = new IncrementalCollection<Edge>(manager);//.NewInput<Edge>();

                    var stopwatch = System.Diagnostics.Stopwatch.StartNew();

                    var result = edges.TrimLeavesAndFlip()
                                      .TrimLeavesAndFlip()
                                      .SCC()
                                      .Subscribe(x => Console.WriteLine("{1}\tNet edge changes within SCCs: {0}", x.Sum(y => y.weight), stopwatch.Elapsed));

                    Console.WriteLine("Strongly connected components on a random graph ({0} nodes, {1} edges)", nodeCount, edgeCount);
                    Console.WriteLine("Reporting the numbers of edges within SCCs (may take a moment):");

                    manager.Activate();

                    // input graph and wait
                    edges.OnNext(graph);

                    result.Sync(0);

                    Console.WriteLine();
                    Console.WriteLine("Press [enter] repeatedly to rewire random edges in the graph. (\"done\" to exit)");

                    // if we are up for interactive access ...
                    if (controller.Configuration.Processes == 1)
                    {
                        for (int i = 0; i < graph.Length; i++)
                        {
                            var line = Console.ReadLine();
                            if (line == "done")
                                break;
                            stopwatch.Restart();
                            var newEdge = new Edge(random.Next(nodeCount), random.Next(nodeCount));
                            Console.WriteLine("Rewiring edge: {0} -> {1}", graph[i], newEdge);
                            edges.OnNext(new[] { new Weighted<Edge>(graph[i], -1), new Weighted<Edge>(newEdge, 1) });
                            result.Sync(i + 1);
                        }
                    }

                    edges.OnCompleted();
                    manager.Join();
                }

                controller.Join();
            }
        }

        public string Usage { get { return "[nodecount edgecount]"; } }
    }
}
