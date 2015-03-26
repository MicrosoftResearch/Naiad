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

using Microsoft.Research.Naiad;
using Microsoft.Research.Naiad.Input;
using Microsoft.Research.Naiad.Frameworks.Lindi;
using Microsoft.Research.Naiad.Frameworks.GraphLINQ;
using Microsoft.Research.Naiad.Frameworks.DifferentialDataflow;

using Microsoft.Research.Naiad.Dataflow;
using Microsoft.Research.Naiad.Dataflow.PartitionBy;
using Microsoft.Research.Naiad.Dataflow.Iteration;

using Microsoft.Research.Naiad.Frameworks.WebHdfs;

using System.IO;
using Microsoft.Research.Naiad.Frameworks;


namespace PageRank
{
    public static class Program
    {
        public static IEnumerable<string> ReadLinesOfText(string filename)
        {
            if (File.Exists(filename))
            {
                var file = File.OpenText(filename);

                while (!file.EndOfStream)
                    yield return file.ReadLine();
            }
            else
                Console.WriteLine("File not found! {0}", filename);
        }

        static IEnumerable<Edge> WebGraphLineToEdges(string line)
        {
            string[] fields = line.Split('\t');
            int vertexId = int.Parse(fields[0]);
            for (int i = 1; i < fields.Length; ++i)
            {
                yield return new Edge(new Node(vertexId), new Node(int.Parse(fields[1])));
            }
        }

        static void Main(string[] args)
        {
            using (var controller = NewController.FromArgs(ref args))
            {
                var stopwatch = System.Diagnostics.Stopwatch.StartNew();

                string impl = args[0].ToLower();
                string dataset = args[1].ToLower();
                string location = args[2].ToLower();

                Func<Computation, Stream<Edge, Epoch>> generator = null;

                switch (dataset)
                {
                    case "lj":
                        switch (location)
                        {
                            case "file":
                                generator = c => new string[] { @"H:\soc-LiveJournal1.txt" }.AsNaiadStream(c).Distinct().SelectMany(ReadLinesOfText).Where(x => !x.StartsWith("#")).Select(x => x.Split('\t')).Select(x => new Edge(new Node(int.Parse(x[0])), new Node(int.Parse(x[1]))));
                                break;
                            case "hdfs":
                                generator = c => c.ReadWebHdfsTextCollection(new Uri("hdfs://msr-hdp-nn1/user/derekmur/soc-LiveJournal1.txt")).Where(x => !x.StartsWith("#")).Select(x => x.Split('\t')).Select(x => new Edge(new Node(int.Parse(x[0])), new Node(int.Parse(x[1]))));
                                break;
                        }
                        break;
                    case "twitter":
                        switch (location)
                        {
                            case "file":
                                generator = c => new string[] { @"H:\twitter_rv.net" }.AsNaiadStream(c).Distinct().SelectMany(ReadLinesOfText).Where(x => !x.StartsWith("#")).Select(x => x.Split('\t')).Select(x => new Edge(new Node(int.Parse(x[0])), new Node(int.Parse(x[1]))));
                                break;
                            case "hdfs":
                                generator = c => c.ReadWebHdfsTextCollection(new Uri("hdfs://msr-hdp-nn1/user/derekmur/twitter_rv.net")).Where(x => !x.StartsWith("#")).Select(x => x.Split('\t')).Select(x => new Edge(new Node(int.Parse(x[0])), new Node(int.Parse(x[1]))));
                                break;
                        }
                        break;
                    case "uk":
                        switch (location)
                        {
                            case "file":
                                generator = c => new string[] { @"H:\uk-2007-05\uk-2007-05.txt" }.AsNaiadStream(c).Distinct().SelectMany(ReadLinesOfText).SelectMany(x => WebGraphLineToEdges(x));
                                break;
                            case "hdfs":
                                generator = c => c.ReadWebHdfsTextCollection(new Uri("hdfs://msr-hdp-nn1/user/fetterly/uk-2007-05-hc.txt")).SelectMany(x => WebGraphLineToEdges(x));
                                break;
                        }
                        break;
                }

                if (generator == null && !(impl == "vertex" || impl == "unicorn"))
                {
                    Console.Error.WriteLine("USAGE: PageRank.exe (vertex|unicorn) (lj|twitter|uk) (file|hdfs)");
                    System.Environment.Exit(-1);
                }

                var edges = new Edge[0];
                    
                    /*ReadLinesOfText(filename).Where(x => !x.StartsWith("#"))
                                                     .Select(x => x.Split())
                                                     .Select(x => new Edge(new Node(Int32.Parse(x[0])), new Node(Int32.Parse(x[1]))))
                                                     .ToArray();
                    */
                //Console.WriteLine("{0}\tData loaded", stopwatch.Elapsed);

                switch (impl)
                {
                    case "vertex":
                        PageRankMain(controller, generator, 10);
                        break;
                    case "unicorn":
                        int totalWorkers = controller.Configuration.WorkerCount * controller.Configuration.Processes;

                        int rows = (int)Math.Sqrt(totalWorkers);
                        int cols = (int)Math.Sqrt(totalWorkers);

                        if (rows * cols != totalWorkers)
                        {
                            Console.Error.WriteLine("Total number of workers must be a square: {0}", totalWorkers);
                            System.Environment.Exit(-1);
                        }

                        int rowsPerProcess = (int)Math.Sqrt(controller.Configuration.WorkerCount);
                        int colsPerProcess = (int)Math.Sqrt(controller.Configuration.WorkerCount);

                        if (rowsPerProcess * colsPerProcess != controller.Configuration.WorkerCount)
                        {
                            Console.Error.WriteLine("Number of workers in a process must be a square: {0}", controller.Configuration.WorkerCount);
                            System.Environment.Exit(-1);
                        }

                        Console.Error.WriteLine("r = {0}, c = {1}, rpp = {2}, cpp = {3}", rows, cols, rowsPerProcess, colsPerProcess);

                        Geometry geo = new Geometry(rows, cols, rowsPerProcess, colsPerProcess, 1024);

                        UnicornMain(controller, generator, 10, geo);

                        break;
                }

                //

                

                controller.Join();

                Console.WriteLine("{0}\tDone", stopwatch.Elapsed);
            }
        }

        public static void PageRankMain(Controller controller, Func<Computation, Stream<Edge, Epoch>> generator, int iterations)
        {
            using (var manager = controller.NewComputation())
            {
                var edges = generator(manager);

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
                                                        "Pagerank")
                                 .Concat(start)
                                 .NodeAggregate((x, y) => x + y);
                                 //.Subscribe(x => { foreach (var y in x.OrderBy(z => z.node.index)) Console.WriteLine(y.value); });

                manager.OnFrontierChange += (x, y) => { Console.WriteLine("{1} Frontier changed to {0}", string.Join(", ", y.NewFrontier), DateTime.Now); };

                // start computation, and block until completion.
                manager.Activate();
                manager.Join();

                Console.WriteLine();
            }
        }

        public static void UnicornMain(Controller controller, Func<Computation, Stream<Edge, Epoch>> generator, int iterations, Geometry geometry)
        {
            //TestCentrally(nodeCount, edgeCount, 1, iterations + 1);

            // arguments are rows, cols, rowsperprocess, colsperprocess, where rows * cols = workers, and rpp * cpp = workers per process. 
            // e.g. 8 8 2 2 means 4 workers per process, and consequently 16 processes. 16 16 2 2 should be 64 processes and 4 workers per.
            using (var manager = controller.NewComputation())
            {
                manager.OnFrontierChange += (x, y) => { Console.WriteLine("{1} Frontier changed to {0}", string.Join(", ", y.NewFrontier), DateTime.Now.ToString("hh:mm:ss.fff")); };

                // initializes a graph and converts it to a Stream<Pair<int, int>, Epoch>.
                var edges = generator(manager);


                var nodes = edges.SelectMany(x => new[] { x.source, x.target }).Distinct();

                // capture degrees before trimming leaves.
                var degrees = edges.Select(x => x.source)
                                   .CountNodes();
#if true
                degrees = edges.Select(x => x.target)
                               .Distinct()
                               .Select(x => x.WithValue(1L))
                               .Concat(degrees)
                               .GroupBy(x => x.node, (k, l) => new[] { k.WithValue(l.Sum(x => x.value)) });
#endif
                // removes edges to pages with zero out-degree.
                var trim = false;
                if (trim)
                    edges = edges.Select(x => x.target.WithValue(x.source))
                                 .FilterBy(degrees.Select(x => x.node))
                                 .Select(x => new Edge(x.value, x.node));

                // initial distribution of ranks.
                var start = nodes.Select(x => x.WithValue(1.0f))
                                 .PartitionBy(x => x.node.index)
                                 .ConvertToVector(geometry);

                // define an iterative pagerank computation, add initial values, aggregate up the results and print them to the screen.
                var ranks = start.IterateAndAccumulate((lc, deltas) => deltas.PageRankStep(edges, degrees.ConvertToVector(geometry), lc, geometry),
                                                        null,
                                                        iterations,
                                                        "Pagerank")
                                 .Concat(start)
                                 .NodeAggregate((x, y) => x + y);
                                 //.Subscribe(x => { foreach (var y in x) Console.WriteLine(y); });

                // start computation, and block until completion.
                manager.Activate();
                manager.Join();
            }
        }

        // performs one step of pagerank, scaling ranks by (1.0 - reset) / degree, tranmitting to neighbors, aggregating along the way.
        public static Stream<NodeWithValue<float>, IterationIn<Epoch>> PageRankStep(this Stream<NodeWithValue<float>, IterationIn<Epoch>> ranks, 
                                                                                         Stream<Edge, Epoch> edges, 
                                                                                         Stream<NodeWithValue<Int64>, Epoch> degrees, 
                                                                                         LoopContext<Epoch> loopContext)
        {
            // join ranks with degrees, scaled down. then "graphreduce", meaning accumulate over graph edges.
            return ranks.NodeJoin(loopContext.EnterLoop(degrees), (rank, degree) => degree > 0 ? rank * (0.85f / degree) : 0.0f)
                        .GraphReduce(loopContext.EnterLoop(edges), (x, y) => x + y, false);
        }

        // performs one step of pagerank, scaling ranks by (1.0 - reset) / degree, tranmitting to neighbors, aggregating along the way.
        public static Stream<NodeValueBlock<float>, IterationIn<Epoch>> PageRankStep(this Stream<NodeValueBlock<float>, IterationIn<Epoch>> ranks,
                                                                                         Stream<Edge, Epoch> edges,
                                                                                         Stream<NodeValueBlock<Int64>, Epoch> degrees,
                                                                                         LoopContext<Epoch> loopContext,
                                                                                         Geometry geometry)
        {
            // join ranks with degrees, scaled down. then "graphreduce", meaning accumulate over graph edges.
            return ranks.NodeJoin(loopContext.EnterLoop(degrees), (rank, degree) =>
            {
                if (rank > 0 && degree == 0)
                    Console.WriteLine("rank: {0}, degree; {1}", rank, degree);

                if (degree > 0)
                    return rank * (0.85f / degree);
                else
                    return 0.0f;

            })
                        .GraphReduce(loopContext.EnterLoop(edges), (x, y) => x + y, geometry);
        }

        // test delta-based pagerank computation using for loops, rather than Naiad.
        public static void TestCentrally(int nodeCount, int edgeCount, int workers, int iterations)
        {
            // this is the pattern usually used by distributed Naiad graphgen.
            var edges = GenerateEdges(nodeCount, edgeCount);

            if (edges.Length < 20)
                for (int i = 0; i < edges.Length; i++)
                    Console.WriteLine(edges[i]);

            // tracks degrees for each node
            var degrees = new float[nodeCount];
            for (int i = 0; i < edges.Length; i++)
                degrees[edges[i].source]++;

            // note: only for nodes that exist.
            var oldRanks = new float[nodeCount];
            for (int i = 0; i < edges.Length; i++)
            {
                oldRanks[edges[i].source] = 1.0f;
                oldRanks[edges[i].target] = 1.0f;
            }

            // accumulation of deltas.
            var totRanks = new float[nodeCount];
            for (int i = 0; i < oldRanks.Length; i++)
                totRanks[i] = oldRanks[i];

            for (int iteration = 0; iteration < iterations; iteration++)
            {
                // scale down all deltas.
                for (int i = 0; i < degrees.Length; i++)
                    oldRanks[i] = 0.85f * oldRanks[i];

                // accumulate deltas scaled down by degree.
                var newRanks = new float[oldRanks.Length];
                for (int i = 0; i < edges.Length; i++)
                    newRanks[edges[i].target] += oldRanks[edges[i].source] / degrees[edges[i].source];

                // copy over the accumulations.
                for (int i = 0; i < degrees.Length; i++)
                    oldRanks[i] = newRanks[i];

                // add the accumulation in to the total.
                for (int i = 0; i < degrees.Length; i++)
                    totRanks[i] += oldRanks[i];
            }

            // take a peek at the top 10, for validation purposes.
            for (int i = 0; i < Math.Min(10, totRanks.Length); i++)
                Console.WriteLine(totRanks[i]);

            Console.WriteLine();
        }

        public static Edge[] GenerateEdges(int nodes, int edges)
        {
            var random = new Random(0);

            var result = new Edge[edges];
            for (int i = 0; i < result.Length; i++)
                result[i] = new Edge(new Node(random.Next(nodes)), new Node(random.Next(nodes)));

            return result;
        }

        #region PageRank
        public static IEnumerable<Weighted<Node>> Multiply(Node name, IEnumerable<Weighted<Node>> records, int scale)
        {
            foreach (var record in records)
                yield return new Weighted<Node>(name, scale);
        }

        public static IEnumerable<Weighted<Node>> ScaleDown(Node name, IEnumerable<Weighted<Node>> names, IEnumerable<Weighted<Node>> degree, double scale)
        {
            foreach (var n in names)
                foreach (var d in degree)
                    yield return new Weighted<Node>(name, (Int32)((scale * n.weight) / d.weight));
        }

        // Normal pagerank
        public static Collection<Pair<Node, Int64>, T> TestPageRank<T>(this Collection<Edge, T> edges, float resetProbability = 0.15f)
            where T : Time<T>
        {
            // determine out-degree of each vertex
            var degrees = edges.Count(x => x.source);

            // initially all ranks are equal
            var ranks = degrees.Select(x => x.First.PairWith(63356L));

            // repeatedly: divide rank by out-degree, send along out-edges, add in some uniform, add everything up, print number of diffs.
            return ranks.FixedPoint((lc, x) => x.Join(degrees.EnterLoop(lc).AssumeImmutable(), r => r.First, d => d.First, r => r.Second, d => d.Second, (n, r, d) => n.PairWith((Int64) ((1.0f - resetProbability) * r / d)))
                                                .Join(edges.EnterLoop(lc).AssumeImmutable(), r => r.First, e => e.source, r => r.Second, e => e.target, (s, r, t) => t.PairWith(r))
                                                .Concat(ranks.Select(r => r.First.PairWith((Int64) (resetProbability * 65536))).EnterLoop(lc))
                                                .Sum(r => r.First, r => r.Second, (name, sum) => name.PairWith(sum))
                                                .Monitor(null)
                                   , 100);
        }

        // Fast pagerank, questionable accuracy
        public static Collection<Node, Epoch> TestPageRank(this Collection<Edge, Epoch> edges, int iterations, bool trimLeaves)
        {
            edges = edges.AssumeImmutable();

            // count outgoing edges for each source; identify present nodes.
            var degrees = edges.Select(x => x.source)
                               .Consolidate(x => x);

            var sources = degrees.Distinct();

            if (trimLeaves)   // can trim edges; importantly, done after degrees are determined.
                edges = edges.Join(sources, x => x.target.index, x => x.index, x => x.source, x => x, (ti, s, d) => new Edge(s, new Node(ti)), true);

            //return degrees;

            // just want node with weight 65536.  //l1.Select(x => new Weighted<int>(n, 65536)))//
            var start = sources.CoGroupBy(sources, x => x, x => x, x => x, x => x, (n, l1, l2) => Multiply(n, l1, 65536))
                               .AssumePartitionedBy(r => r);

            // repeatedly: scale by 0.85 / degree, join with neighbors.
            return start.Where(x => false)
                        .FixedPoint((ic, y) => y.CoGroupBy(degrees.EnterLoop(ic).AssumeImmutable(),
                                                     r => r, x => x,
                                                     r => r, x => x,
                                                     (name, r, x) => ScaleDown(name, r, x, 0.85))
                                          .Monitor(null)
                                          .AssumePartitionedBy(r => r)
                                          .Join(edges.EnterLoop(ic).AssumeImmutable(),
                                                r => r, e => e.source.index,
                                                r => r, e => e.target.index,
                                                (name, names, link) => new Node(link), true)
                                          .Concat(start.EnterLoop(ic)),
                                    r => r,
                                    iterations);
        }

        public static void DDPageRankMain(Controller controller, IEnumerable<Edge> graph, int iterations)
        {
            using (var manager = controller.NewComputation())
            {
                var input = manager.NewInputCollection<Edge>();

                var edges = input//.TestPageRank(100, false)
                                 .TestPageRank()
                                 .Consolidate()                 // collapses multiple rounds into one score per page
                                 .Count(x => x)                 // turns <name,weight> into <(name,weight), 1>
                                 .Select(x => x.Second)         // should be a multiset of scores now
                                 .Consolidate(x => x)           // should be a set of <score, count>
                                 .Count(x => x)                 // should be a set of <(score,count),1>
                                 .Where(x => x.First > 1 << 17) // those scores twice normal
                                 .Subscribe(l => { foreach (var pair in l.OrderByDescending(x => x.record.First).Take(10)) Console.WriteLine("{0}\t{1}", pair.record.First, pair.record.Second); });



                if (controller.Configuration.ProcessID == 0)
                    input.OnCompleted(graph);
                else
                    input.OnCompleted();

                manager.Activate();
                manager.Join();
            }
        }

        #endregion

    }
}
