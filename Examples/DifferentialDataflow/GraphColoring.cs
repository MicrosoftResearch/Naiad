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
using System.Text;

using Naiad;
using Naiad.Dataflow;
using Naiad.Frameworks;
using Naiad.Frameworks.DifferentialDataflow;

namespace Examples.DifferentialDataflow
{
    public static class GraphColoringExtensionMethods
    {
        // returns a pair of node identifier and the first integer not in colors.
        public static IEnumerable<IntPair> UpdateColors(int node, IEnumerable<int> colors)
        {
            var array = colors.ToArray();

            var unset = 0;
            for (int i = 0; i < array.Length; i++)
                if (array[i] == Int32.MaxValue)
                    unset++;

            if (unset > 1)
                yield return new IntPair(node, int.MaxValue);
            else
            {
                array = array.Distinct().ToArray();

                Array.Sort(array);
                
                var index = 0;
                while (index < array.Length && array[index] == index)
                    index++;

                yield return new IntPair(node, index);
            }
        }

        // takes a graph, returns a coloring of the graph (or diverges).
        public static Collection<IntPair, T> Color<T>(this Collection<IntPair, T> graph)
            where T : Time<T>
        {
            var uncolored = graph.SelectMany(edge => new[] { edge.s, edge.t })
                                 .Distinct()
                                 .Select(n => new IntPair(n, Int32.MaxValue));

            return uncolored.FixedPoint((lc, color) => graph.EnterLoop(lc)
                                                      .Select(edge => new IntPair(Math.Min(edge.s, edge.t), Math.Max(edge.s, edge.t)))
                                                      .Join(color, e => e.s, c => c.s, e => e.t, c => c.t, (n, e, c) => new IntPair(e, c))
                                                      .Concat(uncolored.EnterLoop(lc))
                                                      .GroupBy(c => c.s, c => c.t, (n, cs) => UpdateColors(n, cs))
                                                      //.Monitor(null)
                                                      .AssumePartitionedBy(c => c.s)
                                                      , c => c.s);
        }
    }

    public class GraphColoring : Example
    {
        int nodeCount = 100000;
        int edgeCount = 200000;

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
                var graph = new IntPair[edgeCount];
                for (int i = 0; i < edgeCount; i++)
                    graph[i] = new IntPair(random.Next(nodeCount), random.Next(nodeCount));

                using (var manager = controller.NewGraph())
                {
                    // set up the CC computation
                    var edges = new IncrementalCollection<IntPair>(manager);

                    var stopwatch = System.Diagnostics.Stopwatch.StartNew();

                    var colors = edges.Where(x => x.s != x.t)
                                      .Color();

                    var output = colors.Select(x => x.t)          // just keep the color (to count)
                                       .Output
                                       .Subscribe((i, l) => Console.WriteLine("Time to process: {0}", stopwatch.Elapsed));

                    // set to enable a correctness test, at the cost of more memory and computation.
                    var testCorrectness = false;
                    if (testCorrectness)
                    {
                        edges.Where(x => x.s != x.t)
                             .Join(colors, e => e.s, c => c.s, e => e.t, c => c.t, (s, t, c) => new IntPair(c, t))
                             .Join(colors, e => e.t, c => c.s, e => e.s, c => c.t, (t, s, c) => new IntPair(s, c))
                             .Where(p => p.s == p.t)
                             .Consolidate()
                             .Subscribe(l => Console.WriteLine("Coloring errors: {0}", l.Length));
                    }

                    Console.WriteLine("Running graph coloring on a random graph ({0} nodes, {1} edges)", nodeCount, edgeCount);
                    Console.WriteLine("For each color, the nodes with that color:");

                    manager.Activate();

                    edges.OnNext(controller.Configuration.ProcessID == 0 ? graph : Enumerable.Empty<IntPair>());

                    output.Sync(0);

                    // if we are up for interactive access ...
                    if (controller.Configuration.Processes == 1)
                    {
                        Console.WriteLine();
                        Console.WriteLine("Next: sequentially rewiring random edges (press [enter] each time):");

                        for (int i = 0; i < graph.Length; i++)
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

                    manager.Join();
                }

                controller.Join();
            }
        }

        public string Usage { get { return "[nodecount edgecount]"; } }
    }
}
