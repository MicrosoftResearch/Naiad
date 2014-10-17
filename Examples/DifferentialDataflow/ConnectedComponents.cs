/*
 * Naiad ver. 0.5
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
using System.Diagnostics;

using Microsoft.Research.Naiad;
using Microsoft.Research.Naiad.Frameworks.DifferentialDataflow;

namespace Microsoft.Research.Naiad.Examples.DifferentialDataflow
{
    public static class ConnectedComponentsExtensionMethods
    {
        public static Collection<IntPair, T> ConnectedComponents<T>(this Collection<IntPair, T> edges)
            where T : Time<T>
        {
            return edges.ConnectedComponents(x => 0);
        }

        /// <summary>
        /// the node names are now introduced in waves. propagation of each wave completes before the next starts.
        /// </summary>
        public static Collection<IntPair, T> ConnectedComponents<T>(this Collection<IntPair, T> edges, Func<IntPair, int> priorityFunction)
            where T : Time<T>
        {
            // initial labels only needed for min, as the max will be improved on anyhow.
            var nodes = edges.Select(x => new IntPair(Math.Min(x.s, x.t), Math.Min(x.s, x.t)))
                             .Consolidate();

            // symmetrize the graph
            edges = edges.Select(edge => new IntPair(edge.t, edge.s))
                         .Concat(edges);

            // prioritization introduces labels from small to large (in batches).
            return nodes.Where(x => false)
                        .GeneralFixedPoint((lc, x) => x.Join(edges.EnterLoop(lc), n => n.s, e => e.s, (n, e) => new IntPair(e.t, n.t))
                                                             .Concat(nodes.EnterLoop(lc, priorityFunction))
                                                             .Min(n => n.s, n => n.t),
                                            priorityFunction,
                                            n => n.s,
                                            Int32.MaxValue);
        }
    }

    /// <summary>
    /// Demonstrates a connected components computation using Naiad's FixedPoint operator.
    /// </summary>
    public class ConnectedComponents : Example
    {
        int nodeCount = 1000000;
        int edgeCount =  750000;

        public void Execute(string[] args)
        {
            using (var computation = NewComputation.FromArgs(ref args))
            {
                // establish numbers of nodes and edges from input or from defaults.
                if (args.Length == 3)
                {
                    nodeCount = Convert.ToInt32(args[1]);
                    edgeCount = Convert.ToInt32(args[2]);
                }

                // generate a random graph
                var random = new Random(0);
                var graph = new IntPair[edgeCount];
                for (int i = 0; i < edgeCount; i++)
                    graph[i] = new IntPair(random.Next(nodeCount), random.Next(nodeCount));

                var stopwatch = System.Diagnostics.Stopwatch.StartNew();

                // set up the CC computation
                var edges = computation.NewInputCollection<IntPair>();

                //Func<IntPair, int> priorityFunction = node => 0;
                //Func<IntPair, int> priorityFunction = node => Math.Min(node.t, 100);
                Func<IntPair, int> priorityFunction = node => 65536 * (node.t < 10 ? node.t : 10 + Convert.ToInt32(Math.Log(1 + node.t) / Math.Log(2.0)));

                var output = edges.ConnectedComponents(priorityFunction)
                                  .Count(n => n.t, (l, c) => c)  // counts results with each label
                                  .Consolidate()
                                  .Subscribe(l =>
                                             {
                                                 Console.Error.WriteLine("Time to process: {0}", stopwatch.Elapsed);
                                                 foreach (var result in l.OrderBy(x => x.record))
                                                     Console.Error.WriteLine(result);
                                             });

                Console.Error.WriteLine("Running connected components on a random graph ({0} nodes, {1} edges)", nodeCount, edgeCount);
                Console.Error.WriteLine("For each size, the number of components of that size (may take a moment):");

                computation.Activate();

                edges.OnNext(computation.Configuration.ProcessID == 0 ? graph : Enumerable.Empty<IntPair>());

                // if we are up for interactive access ...
                if (computation.Configuration.Processes == 1)
                {
                    output.Sync(0);

                    Console.WriteLine();
                    Console.WriteLine("Next: sequentially rewiring random edges (press [enter] each time):");

                    for (int i = 0; true; i++)
                    {
                        Console.ReadLine();
                        stopwatch.Restart();
                        var newEdge = new IntPair(random.Next(nodeCount), random.Next(nodeCount));
                        Console.WriteLine("Rewiring edge: {0} -> {1}", graph[i], newEdge);
                        edges.OnNext(new[] { new Weighted<IntPair>(graph[i], -1), new Weighted<IntPair>(newEdge, 1) });
                        output.Sync(i + 1);
                    }
                }

                edges.OnCompleted();
                computation.Join();
            }

        }

        public string Usage { get { return "[nodecount edgecount]"; } }


        public string Help
        {
            get { return "Demonstrates an iterative differential dataflow computation using GeneralizedFixedPoint, which supports introducing records in iterations other than the first. This is used, with several choices of initial iteration function, to circulate smaller labels first, resulting is less redundant work at the expense of more sequential dependencies."; }
        }
    }
}
