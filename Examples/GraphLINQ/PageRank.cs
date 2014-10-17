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

using Microsoft.Research.Naiad;
using Microsoft.Research.Naiad.Input;
using Microsoft.Research.Naiad.Frameworks.Lindi;
using Microsoft.Research.Naiad.Frameworks.GraphLINQ;

using Microsoft.Research.Naiad.Dataflow;
using Microsoft.Research.Naiad.Dataflow.Iteration;
using Microsoft.Research.Naiad.Dataflow.PartitionBy;

namespace Microsoft.Research.Naiad.Examples.GraphLINQ
{
    public class PageRank : Example
    {
        public string Usage
        {
            get { return "[edgefilenames ...]"; }
        }

        public void Execute(string[] args)
        {
            using (var computation = NewComputation.FromArgs(ref args))
            {
                // either read inputs from a file, or generate them randomly.
                Stream<Edge, Epoch> edges;
                if (args.Length == 1)
                {
                    // generate a random graph in each process; pagerank computation is performed on the union of edges.
                    edges = GenerateEdges(1000000, 20000000, computation.Configuration.ProcessID).AsNaiadStream(computation);
                }
                else
                {
                    var text = args.Skip(1)
                                   .AsNaiadStream(computation)
                                   .Distinct()
                                   .SelectMany(x => x.ReadLinesOfText());

                    edges = text.Where(x => !x.StartsWith("#"))
                                .Select(x => x.Split())
                                .Select(x => new Edge(new Node(Int32.Parse(x[0])), new Node(Int32.Parse(x[1]))));
                }

                Console.Out.WriteLine("Started up!");
                Console.Out.Flush();

                edges = edges.PartitionBy(x => x.source);

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
                var iterations = 10;
                var ranks = start.IterateAndAccumulate((lc, deltas) => deltas.PageRankStep(lc.EnterLoop(degrees),
                                                                                           lc.EnterLoop(edges)),
                                                        x => x.node.index,
                                                        iterations,
                                                        "PageRank")
                                 .Concat(start)                             // add initial ranks in for correctness.
                                 .NodeAggregate((x, y) => x + y)            // accumulate up the ranks.
                                 .Where(x => x.value > 0.0f);               // report only positive ranks.

                var stopwatch = System.Diagnostics.Stopwatch.StartNew();
                computation.OnFrontierChange += (x, y) => { Console.WriteLine(stopwatch.Elapsed + "\t" + string.Join(", ", y.NewFrontier)); Console.Out.Flush(); };

                // start computation, and block until completion.
                computation.Activate();
                computation.Join();
            }
        }
        
        public static IEnumerable<Edge> GenerateEdges(int nodes, int edges, int seed)
        {
            var random = new Random(seed);
            for (int i = 0; i < edges; i++)
                yield return new Edge(new Node(random.Next(nodes)), new Node(random.Next(nodes)));
        }


        public string Help
        {
            get { return "Demonstrates pagerank computation using iterative dataflow and GraphLINQ primitives. Repeatedly applies NodeJoin on ranks and degrees to scale values down by degree, followed by GraphReduce to accumulate the ranks along the graph edges."; }
        }
    }

    public static class ExtensionMethods
    {
        // performs one step of pagerank, scaling ranks by (1.0 - reset) / degree, tranmitting to neighbors, aggregating along the way.
        public static Stream<NodeWithValue<float>, T> PageRankStep<T>(this Stream<NodeWithValue<float>, T> ranks,
                                                                           Stream<NodeWithValue<Int64>, T> degrees,
                                                                           Stream<Edge, T> edges)
            where T : Time<T>
        {
            // join ranks with degrees, scaled down. then "graphreduce", accumulating ranks over graph edges.
            return ranks.NodeJoin(degrees, (rank, degree) => degree > 0 ? rank * (0.85f / degree) : 0.0f)
                        .GraphReduce(edges, (x, y) => x + y, false)
                        .Where(x => x.value > 0.0f);
        }

        public static IEnumerable<string> ReadLinesOfText(this string filename)
        {
            Console.WriteLine("Reading file {0}", filename);

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
}
