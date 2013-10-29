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
using Naiad.Dataflow;

namespace Examples.WordCount
{
    public static class ExtensionMethods
    {
        /// <summary>
        /// Counts records in the input stream, emitting new counts as they change.
        /// </summary>
        /// <typeparam name="TRecord">record type</typeparam>
        /// <param name="stream">input stream</param>
        /// <returns>stream of counts</returns>
        public static Naiad.Dataflow.Stream<Pair<TRecord, Int64>, Epoch> StreamingCount<TRecord>(this Naiad.Dataflow.Stream<TRecord, Epoch> stream)
        {
            return Naiad.Frameworks.Foundry.NewStage(stream, (i, s) => new CountVertex<TRecord>(i, s), x => x.GetHashCode(), null, "Count");
        }

        /// <summary>
        /// A Naiad vertex for counting records of type S. Each epoch, changed counts are produced as output.
        /// </summary>
        internal class CountVertex<TRecord> : Naiad.Frameworks.UnaryVertex<TRecord, Pair<TRecord, Int64>, Epoch>
        {
            // we maintain all the current counts, as well as recently changed keys.
            private readonly Dictionary<TRecord, Int64> Counts = new Dictionary<TRecord, long>();
            private readonly HashSet<TRecord> Changed = new HashSet<TRecord>();

            // Each batch of records of type TRecord we receive, we must update counts.
            public override void MessageReceived(Naiad.Dataflow.Message<Pair<TRecord, Epoch>> message)
            {
                // a message contains length valid records.
                for (int i = 0; i < message.length; i++)
                {
                    var data = message.payload[i].v1;
                    var time = message.payload[i].v2;

                    this.NotifyAt(time);

                    if (!this.Counts.ContainsKey(data))
                        this.Counts[data] = 0;

                    this.Counts[data] += 1;

                    this.Changed.Add(data);
                }
            }

            // once all records of an epoch are received, we should send the counts along.
            public override void OnDone(Epoch time)
            {
                foreach (var record in this.Changed)
                    this.Output.Send(new Pair<TRecord, Int64>(record, this.Counts[record]), time);

                // reset observed records
                this.Changed.Clear();
            }

            // the UnaryVertex base class needs to know the index and stage of the vertex. 
            public CountVertex(int index, Naiad.Dataflow.Stage<Epoch> stage) : base(index, stage) { }
        }
    }

    public class WordCount : Example
    {
        public string Usage { get { return ""; } }

        public void Execute(string[] args)
        {
            // the first thing to do is to allocate a controller from args.
            using (var controller = Naiad.NewController.FromArgs(ref args))
            {
                // controllers allocate graphs, which are where computations are defined.
                using (var graph = controller.NewGraph())
                {
                    // 1. Make a new data source, to which we will supply strings.
                    var source = new BatchedDataSource<string>();

                    // 2. Attach source, and apply count extension method.
                    var counts = graph.NewInput(source).StreamingCount();

                    // 3. Subscribe to the resulting stream with a callback to print the outputs.
                    counts.Subscribe(list => { foreach (var element in list) Console.WriteLine(element); });

                    // with our dataflow graph defined, we can start soliciting strings from the user.
                    Console.WriteLine("Start entering lines of text. An empty line will exit the program.");
                    Console.WriteLine("Naiad will display counts (and changes in counts) of words you type.");

                    graph.Activate();       // activate the execution of this graph (no new stages allowed).

                    // read lines of input and hand them to the input, until an empty line appears.
                    for (var line = Console.ReadLine(); line.Length > 0; line = Console.ReadLine())
                        source.OnNext(line.Split());

                    source.OnCompleted();   // signal the end of the input.
                    graph.Join();           // waits until the graph has finished executing.
                }

                controller.Join();          // waits until all graphs have finished executing.
            }
        }
    }
}