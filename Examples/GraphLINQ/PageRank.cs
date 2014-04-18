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

using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

using Microsoft.Research.Naiad;
using Microsoft.Research.Naiad.Input;
using Microsoft.Research.Naiad.Frameworks.Lindi;
using Microsoft.Research.Naiad.Frameworks.GraphLINQ;
using Microsoft.Research.Naiad.Dataflow;
using Microsoft.Research.Naiad.Dataflow.Iteration;
using Microsoft.Research.Naiad.Dataflow.PartitionBy;

namespace Microsoft.Research.Naiad.Examples.GraphLINQ
{
    public static class ExtensionMethods
    {
        // performs one step of pagerank, scaling ranks by (1.0 - reset) / degree, tranmitting to neighbors, aggregating along the way.
        public static Stream<NodeWithValue<float>, IterationIn<Epoch>> PageRankStep(this Stream<NodeWithValue<float>, IterationIn<Epoch>> ranks,
                                                                                         Stream<Edge, Epoch> edges,
                                                                                         Stream<NodeWithValue<Int64>, Epoch> degrees,
                                                                                         LoopContext<Epoch> loopContext)
        {
            // join ranks with degrees, scaled down. then "graphreduce", meaning accumulate over graph edges.
            return ranks.NodeJoin(loopContext.EnterLoop(degrees), (rank, degree) => degree > 0 ? rank * (0.85f / degree) : 0.0f)
                        .GraphReduce(loopContext.EnterLoop(edges), (x, y) => x + y, true);
        }

        public static IEnumerable<string> ReadLinesOfText(this string filename)
        {
            if (System.IO.File.Exists(filename))
            {
                var file = System.IO.File.OpenText(filename);

                while (!file.EndOfStream)
                    yield return file.ReadLine();
            }
            else
                Console.WriteLine("File not found! {0}", filename);
        }
    }

    public class PageRank : Example
    {
        public string Usage
        {
            get { return "[edgefilename]"; }
        }

        public void Execute(string[] args)
        {
            using (var computation = NewComputation.FromArgs(ref args))
            {
                var stopwatch = System.Diagnostics.Stopwatch.StartNew();

                Edge[] edges;

                if (args.Length > 1)
                {
                    edges = args[1].ReadLinesOfText()
                                   .Where(x => !x.StartsWith("#"))
                                   .Select(x => x.Split())
                                   .Select(x => new Edge(new Node(Int32.Parse(x[0])), new Node(Int32.Parse(x[1]))))
                                   .ToArray();
                }
                else
                {
                    edges = GenerateEdges(1000000, 2000000);
                }

                Console.WriteLine("{0}\tData loaded", stopwatch.Elapsed);

                PageRankMain(computation, edges, 10);

                Console.WriteLine("{0}\tDone", stopwatch.Elapsed);
            }
        }

        public static void PageRankMain(Computation computation, IEnumerable<Edge> graph, int iterations)
        {
            // initializes a graph and converts it to a Stream<Pair<int, int>, Epoch>.
            var edges = graph.AsNaiadStream(computation);

            // capture degrees before trimming leaves.
            var degrees = edges.Select(x => x.source)
                               .CountNodes();

            // removes edges to pages with zero out-degree.
            var trim = false;
            if (trim)
                edges = edges.Select(x => x.target.WithValue(x.source))
                             .FilterBy(degrees.Select(x => x.node))
                             .Select(x => new Edge(x.value, x.node));

            // initial distribution of ranks.
            var start = degrees.Select(x => x.node.WithValue(0.15f))
                               .PartitionBy(x => x.node.index);

            // define an iterative pagerank computation, add initial values, aggregate up the results and print them to the screen.
            var ranks = start.IterateAndAccumulate((lc, deltas) => deltas.PageRankStep(edges, degrees, lc),
                                                    x => x.node.index,
                                                    iterations,
                                                    "PageRank")
                             .Concat(start)
                             .NodeAggregate((x, y) => x + y);
            //.Subscribe(x => { foreach (var y in x.OrderBy(z => z.node.index)) Console.WriteLine(y.value); });

            // start computation, and block until completion.
            computation.Activate();
            computation.Join();

        }

        public static Edge[] GenerateEdges(int nodes, int edges)
        {
            var random = new Random(0);

            var result = new Edge[edges];
            for (int i = 0; i < result.Length; i++)
                result[i] = new Edge(new Node(random.Next(nodes)), new Node(random.Next(nodes)));

            return result;
        }


        public string Help
        {
            get { return "Demonstrates pagerank computation using iterative dataflow and GraphLINQ primitives. Repeatedly applies NodeJoin on ranks and degrees to scale values down by degree, followed by GraphReduce to accumulate the ranks along the graph edges."; }
        }
    }

}
