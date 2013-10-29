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
using System.Threading.Tasks;

using Naiad;

namespace Examples
{

    public interface Example
    {
        string Usage { get; }
        void Execute(string[] args);
    }

    class Program
    {
        static void Main(string[] args)
        {
            // map from example names to code to run in each case
            var examples = new Dictionary<string, Example>();

            // loading up as many examples as we can think of
            examples.Add("wordcount", new WordCount.WordCount());
            examples.Add("lookup", new KeyValueLookup.KeyValueLookup());
            examples.Add("connectedcomponents", new ConnectedComponents.ConnectedComponents());

            // also load up some differential dataflow examples
            examples.Add("dd-stronglyconnectedcomponents", new DifferentialDataflow.SCC());
            examples.Add("dd-connectedcomponents", new DifferentialDataflow.ConnectedComponents());
            examples.Add("dd-wordcount", new DifferentialDataflow.WordCount());
            examples.Add("dd-searchindex", new DifferentialDataflow.SearchIndex());
            examples.Add("dd-graphcoloring", new DifferentialDataflow.GraphColoring());
            
            // determine which exmample was asked for
            if (args.Length == 0 || !examples.ContainsKey(args[0].ToLower()))
            {
                Console.Error.WriteLine("First argument not found in list of examples");
                Console.Error.WriteLine("Choose from the following exciting options:");
                foreach (var pair in examples.OrderBy(x => x.Key))
                    Console.Error.WriteLine("\tExamples.exe {0} {1} [naiad options]", pair.Key, pair.Value.Usage);

                Console.Error.WriteLine();
                Configuration.Usage();
            }
            else
            {
                var example = args[0].ToLower();
                if (args.Contains("--help") || args.Contains("/?") || args.Contains("--usage"))
                {
                    Console.Error.WriteLine("Usage: NaiadExamples.exe {0} {1} [naiad options]", example, examples[example].Usage);
                    Configuration.Usage();
                }
                else
                {
                    Logging.LogLevel = LoggingLevel.Off;

                    examples[example].Execute(args);
                }
            }
        }
    }
}
