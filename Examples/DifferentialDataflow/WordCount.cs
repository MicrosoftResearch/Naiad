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
    /// <summary>
    /// Demonstrates an interactive Naiad computation.
    /// </summary>
    public class WordCount : Example
    {
        /// <summary>
        /// Executes a word counting Naiad program.
        /// </summary>
        /// <param name="config">Naiad controller configuration</param>
        /// <param name="args">Remaining arguments</param>
        public void Execute(string[] args)
        {
            // first, construct a Naiad controller.
            using (var controller = NewController.FromArgs(ref args))
            {
                using (var graph = controller.NewGraph())
                {
                    // create an incrementally updateable collection
                    var text = new IncrementalCollection<string>(graph);//.NewInput<string>();

                    // segment strings, count, and print
                    text.SelectMany(x => x.Split(' '))
                        .Count(y => y, (k, c) => k + ":" + c)   // yields "word:count" for each word
                        .Subscribe(l => { foreach (var element in l) Console.WriteLine(element); });

                    graph.Activate();

                    Console.WriteLine("Start entering lines of text. An empty line will exit the program.");
                    Console.WriteLine("Naiad will display counts (and changes in counts) of words you type.");

                    var line = Console.ReadLine();
                    for (int i = 0; line != ""; i++ )
                    {
                        text.OnNext(line);
                        graph.Sync(i);
                        line = Console.ReadLine();
                    }

                    text.OnCompleted(); // closes input
                    graph.Join();
                }

                controller.Join();  // blocks until flushed
            }
        }

        public string Usage { get { return ""; } }
    }
}
