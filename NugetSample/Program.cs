/*
 * Naiad ver. 0.3
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
using Microsoft.Research.Naiad.Frameworks.Azure;
using Microsoft.Research.Naiad.Frameworks.DifferentialDataflow;
using Microsoft.WindowsAzure.Storage;
using Microsoft.WindowsAzure;

namespace Microsoft.Research.Naiad.Examples
{
    public static class ConnectedComponentsExtensionMethods
    {

    }

    public static class Program
    {
        public static void Main(string[] args)
        {
            using (var controller = NewController.FromArgs(ref args))
            {
                if (args.Length < 3)
                {
                    Console.Error.WriteLine("Usage: results_container_name node_count edge_count");
                    System.Environment.Exit(0);
                }

                string containerName = args[0];
                int nodeCount = Convert.ToInt32(args[1]);
                int edgeCount = Convert.ToInt32(args[2]);

                // For Azure storage emulator:
                string connectionString = "UseDevelopmentStorage=true;";
                
                // To use Windows Azure Storage, uncomment this line, and substitute your account name and key:
                // string connectionString = "DefaultEndpointsProtocol=https;AccountName=[Account name];AccountKey=[Account key]";

                Console.Error.WriteLine("Running connected components on a random graph ({0} nodes, {1} edges).", nodeCount, edgeCount);
                if (connectionString != null) 
                {
                    Console.Error.WriteLine("Writing results to container {0}", containerName);
                }
                else
                {
                    Console.Error.WriteLine("Writing results to console");
                }

                // generate a random graph
                var random = new Random(0);
                var graph = new IntPair[edgeCount];
                for (int i = 0; i < edgeCount; i++)
                    graph[i] = new IntPair(random.Next(nodeCount), random.Next(nodeCount));

                var stopwatch = System.Diagnostics.Stopwatch.StartNew();

                using (var computation = controller.NewComputation())
                {
                    // set up the CC computation
                    var edges = new IncrementalCollection<IntPair>(computation);

#if false
                    // 
                    Func<IntPair, int> priorityFunction = node => 0;
#else
                    // Introduce labels in priority order. Labels 0 through 9 inclusive are introduced sequentially,
                    // following by exponentially-growing sets of labels.
                    Func<IntPair, int> priorityFunction = node => 65536 * (node.t < 10 ? node.t : 10 + Convert.ToInt32(Math.Log(1 + node.t) / Math.Log(2.0)));
#endif

                    // Perform the connected components algorithm on the collection of edges.
                    var labeledVertices = edges.ConnectedComponents(priorityFunction);

                    // Count the number of vertices in each component.
                    var componentSizes = labeledVertices.Count(n => n.t, (l, c) => new Pair<int, long>(l, c)); // counts results with each label

                    // Ignore the labels and consolidate to find the number of components having each size.
                    var sizeDistribution = componentSizes.Select(x => x.v2).Consolidate();


                    if (connectionString != null)
                    {
                        var account = CloudStorageAccount.Parse(connectionString);
                        var container = account.CreateCloudBlobClient().GetContainerReference(containerName);
                        container.CreateIfNotExists();

                        // Write the results to the given Azure blob container, with filename "componentSizes-part-i" for process i.
                        sizeDistribution.Output
                                        .WriteTextToAzureBlobs(container, "componentSizes-part-{0}-{1}");
                    }
                    else
                    {
                        // Write the results to the console.
                        sizeDistribution.Subscribe(xs => { foreach (var x in xs) Console.WriteLine(x); });
                    }
                    
                    computation.Activate();

                    edges.OnCompleted(controller.Configuration.ProcessID == 0 ? graph : Enumerable.Empty<IntPair>());

                    computation.Join();
                }

                controller.Join();
            }
       
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


}
