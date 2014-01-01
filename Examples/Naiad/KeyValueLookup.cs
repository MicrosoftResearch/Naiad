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
using System.Threading.Tasks;

using Naiad;
using Naiad.Dataflow;

namespace Examples.KeyValueLookup
{
    public static class ExtensionMethods
    {
        // key-value pairs on the first input are retrieved via the second input.
        public static Stream<Pair<TKey, TValue>, Epoch> KeyValueLookup<TKey, TValue>(this Stream<Pair<TKey, TValue>, Epoch> kvpairs, Stream<TKey, Epoch> requests)
        {
            return Naiad.Frameworks.Foundry.NewStage(kvpairs, requests, (i, s) => new KeyValueLookupVertex<TKey, TValue>(i, s), x => x.v1.GetHashCode(), y => y.GetHashCode(), null, "Lookup");
        }

        /// <summary>
        /// A vertex storing (key, value) pairs received on one input, and reporting pairs requested on the other.
        /// Note: no attempt made at consistency; queueing updates or requests and using OnDone could help out here.
        /// </summary>
        /// <typeparam name="TKey">key type</typeparam>
        /// <typeparam name="TValue">value type</typeparam>
        public class KeyValueLookupVertex<TKey, TValue> : Naiad.Frameworks.BinaryVertex<Pair<TKey, TValue>, TKey, Pair<TKey, TValue>, Epoch>
        {
            private readonly Dictionary<TKey, TValue> Values = new Dictionary<TKey, TValue>();

            public override void MessageReceived1(Message<Pair<Pair<TKey, TValue>, Epoch>> message)
            {
                for (int i = 0; i < message.length; i++)
                    this.Values[message.payload[i].v1.v1] = message.payload[i].v1.v2;
            }

            public override void MessageReceived2(Message<Pair<TKey, Epoch>> message)
            {
                for (int i = 0; i < message.length; i++)
                {
                    var key = message.payload[i].v1;
                    var time = message.payload[i].v2;

                    if (this.Values.ContainsKey(key))
                        this.Output.Send(key.PairWith(this.Values[key]), time);
                }
            }

            public KeyValueLookupVertex(int index, Stage<Epoch> vertex) : base(index, vertex) { }
        }
    }

    public class KeyValueLookup : Example
    {
        public string Usage { get { return ""; } }

        public void Execute(string[] args)
        {
            using (var controller = Naiad.NewController.FromArgs(ref args))
            {
                using (var graph = controller.NewGraph())
                {
                    var keyvals = new BatchedDataSource<Pair<string, string>>();
                    var queries = new BatchedDataSource<string>();

                    graph.NewInput(keyvals)
                         .KeyValueLookup(graph.NewInput(queries))
                         .Subscribe(list => { foreach (var l in list) Console.WriteLine("value[\"{0}\"]:\t\"{1}\"", l.v1, l.v2); });

                    graph.Activate();

                    if (controller.Configuration.ProcessID == 0)
                    {
                        Console.WriteLine("Enter two strings to insert/overwrite a (key, value) pairs.");
                        Console.WriteLine("Enter one string to look up a key.");

                        // repeatedly read lines and introduce records based on their structure.
                        // note: it is important to advance both inputs in order to make progress.
                        for (var line = Console.ReadLine(); line.Length > 0; line = Console.ReadLine())
                        {
                            var split = line.Split();

                            if (split.Length == 1)
                            {
                                queries.OnNext(line);
                                keyvals.OnNext();
                            }
                            if (split.Length == 2)
                            {
                                queries.OnNext();
                                keyvals.OnNext(split[0].PairWith(split[1]));
                            }
                            if (split.Length > 2)
                                Console.Error.WriteLine("error: lines with three or more strings are not understood.");
                        }
                    }

                    keyvals.OnCompleted();
                    queries.OnCompleted();

                    graph.Join();
                }

                controller.Join();
            }
        }
    }
}
