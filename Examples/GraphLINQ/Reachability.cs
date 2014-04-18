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
using Microsoft.Research.Naiad.Frameworks.GraphLINQ;
using Microsoft.Research.Naiad.Frameworks.Lindi;
using Microsoft.Research.Naiad.Dataflow;

namespace Microsoft.Research.Naiad.Examples.GraphLINQ
{
    public class Reachability : Example
    {
        public string Usage
        {
            get { return "edgesfilename rootsfilename"; }
        }

        public void Execute(string[] args)
        {
            // a controller manages an instance of Naiad
            using (var computation = NewComputation.FromArgs(ref args))
            {
                // define a graph input from a filename and some transformations.
                var edgeStrings = new[] { args[1] }.AsNaiadStream(computation)
                                                   .SelectMany(x => ReadLines(x))
                                                   .Select(x => x.Split())
                                                   .Select(x => x[0].PairWith(x[1]));

                // define reachability roots from a second filename.
                var rootStrings = new[] { args[2] }.AsNaiadStream(computation)
                                                   .SelectMany(x => ReadLines(x));

                // collect all names of known vertices in the graph we want.
                var nodes = rootStrings.Concat(edgeStrings.Select(x => x.First))
                                       .Concat(edgeStrings.Select(x => x.Second));

                // create a string->node mapping.
                var names = nodes.GenerateDenseNameMapping();

                // converts (string, string) -> edge and string -> node.
                var edges = edgeStrings.RenameEdges(names);
                var roots = rootStrings.RenameNodes(names);

                // iteratively expand reachable set as pairs (node, isReachable).
                var limit = roots.Select(x => x.WithValue(true))
                                 .IterateAndAccumulate((lc, x) => x.TransmitAlong(lc.EnterLoop(edges))      // transmit (node, true) values along edges
                                                                   .StateMachine((bool b, bool s) => true), // any received value sets the state to true
                                                        x => x.node.index,                                  // partitioning information
                                                        Int32.MaxValue,                                     // the number of iterations
                                                        "Reachability")                                     // a nice descriptive name
                                 .Concat(roots.Select(x => x.WithValue(true)))                              // add the original trusted nodes
                                 .NodeAggregate((a, b) => true);                                            // aggregate, for the originals

                // print the results onto the screen (or write to file, as appopriate)
                limit.Select(x => x.node.index)
                     .Subscribe(x => Console.WriteLine(x.Count()));

                // enable some diagnostic information about execution times.
                var stopwatch = System.Diagnostics.Stopwatch.StartNew();
                computation.OnFrontierChange += (x, y) => Console.WriteLine("{0}: {1}", stopwatch.Elapsed, string.Join(", ", y.NewFrontier));

                // start the computation and wait until it finishes
                computation.Activate();
                computation.Join();
            }

        }

        public static IEnumerable<string> ReadLines(string filename)
        {
            using (var reader = System.IO.File.OpenText(filename))
            {
                for (var line = reader.ReadLine(); !reader.EndOfStream; line = reader.ReadLine())
                    yield return line;
            }
        }

        public string Help
        {
            get { return "Demonstrates iterative computation using GraphLINQ primitives. Loads an edge set and a node set from files, and then iteratively expands the set of nodes reachable from the initial node set."; }
        }
    }
}
