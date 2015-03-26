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
using Microsoft.Research.Naiad.Diagnostics;

namespace Microsoft.Research.Naiad.Examples
{
    /// <summary>
    /// An example runnable as a sample Naiad program.
    /// </summary>
    public interface Example
    {
        /// <summary>
        /// Describes arguments used by the example
        /// </summary>
        string Usage { get; }

        /// <summary>
        /// Describes the intended behavior for the example, with rich descriptive text.
        /// </summary>
        string Help { get; }

        /// <summary>
        /// Executes the example with all of the supplied arguments (including the example name).
        /// </summary>
        /// <param name="args"></param>
        void Execute(string[] args);
    }

    class Program
    {
        static void Main(string[] args)
        {
            // map from example names to code to run in each case
            var examples = new Dictionary<string, Example>();

            // loading up several examples
            examples.Add("wordcount", new WordCount.WordCount());
            examples.Add("lookup", new KeyValueLookup.KeyValueLookup());
            examples.Add("connectedcomponents", new ConnectedComponents.ConnectedComponents());

            // two examples capable of stressing Naiad's performance
            examples.Add("benchmark-throughput", new Throughput.Throughput());
            examples.Add("benchmark-latency", new Latency.Latency());

            // some differential dataflow examples
            examples.Add("dd-stronglyconnectedcomponents", new DifferentialDataflow.SCC());
            examples.Add("dd-connectedcomponents", new DifferentialDataflow.ConnectedComponents());
            examples.Add("dd-wordcount", new DifferentialDataflow.WordCount());
            examples.Add("dd-searchindex", new DifferentialDataflow.SearchIndex());
            examples.Add("dd-graphcoloring", new DifferentialDataflow.GraphColoring());
            
            // some GraphLINQ examples
            examples.Add("graphlinq-reachability", new Examples.GraphLINQ.Reachability());
            examples.Add("graphlinq-pagerank", new Examples.GraphLINQ.PageRank());

            // determine which exmample was asked for
            if (args.Length == 0 || !examples.ContainsKey(args[0].ToLower()))
            {
                Console.Error.WriteLine("First argument not found in list of examples");
                Console.Error.WriteLine("Choose from the following exciting options:");
                foreach (var pair in examples)
                    Console.Error.WriteLine("\tExamples.exe {0} {1} [naiad options]", pair.Key, pair.Value.Usage);

                Console.Error.WriteLine();
                Configuration.Usage();
            }
            else
            {
                var example = args[0].ToLower();
                if (args.Contains("--help") || args.Contains("/?") || args.Contains("--usage"))
                {
                    Console.Error.WriteLine("Usage: Examples.exe {0} {1} [naiad options]", example, examples[example].Usage);
                    Configuration.Usage();
                }
                else
                {
                    Logging.LogLevel = LoggingLevel.Error;
                    Logging.LogStyle = LoggingStyle.Console;

                    examples[example].Execute(args);
                }
            }
        }
    }
}
