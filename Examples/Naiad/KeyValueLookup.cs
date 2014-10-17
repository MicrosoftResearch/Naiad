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
using System.Threading.Tasks;

using Microsoft.Research.Naiad;
using Microsoft.Research.Naiad.Dataflow;
using Microsoft.Research.Naiad.Input;
using Microsoft.Research.Naiad.Dataflow.StandardVertices;

namespace Microsoft.Research.Naiad.Examples.KeyValueLookup
{
    public static class ExtensionMethods
    {
        // key-value pairs on the first input are retrieved via the second input.
        public static Stream<Pair<TKey, TValue>, Epoch> KeyValueLookup<TKey, TValue>(this Stream<Pair<TKey, TValue>, Epoch> kvpairs, Stream<TKey, Epoch> requests)
        {
            return Foundry.NewBinaryStage(kvpairs, requests, (i, s) => new KeyValueLookupVertex<TKey, TValue>(i, s), x => x.First.GetHashCode(), y => y.GetHashCode(), null, "Lookup");
        }

        /// <summary>
        /// A vertex storing (key, value) pairs received on one input, and reporting pairs requested on the other.
        /// Note: no attempt made at consistency; queueing updates or requests and using OnNotify could help out here.
        /// </summary>
        /// <typeparam name="TKey">key type</typeparam>
        /// <typeparam name="TValue">value type</typeparam>
        public class KeyValueLookupVertex<TKey, TValue> : BinaryVertex<Pair<TKey, TValue>, TKey, Pair<TKey, TValue>, Epoch>
        {
            private readonly Dictionary<TKey, TValue> Values = new Dictionary<TKey, TValue>();

            public override void OnReceive1(Message<Pair<TKey, TValue>, Epoch> message)
            {
                for (int i = 0; i < message.length; i++)
                    this.Values[message.payload[i].First] = message.payload[i].Second;
            }

            public override void OnReceive2(Message<TKey, Epoch> message)
            {
                var output = this.Output.GetBufferForTime(message.time);

                for (int i = 0; i < message.length; i++)
                {
                    var key = message.payload[i];
                    if (this.Values.ContainsKey(key))
                        output.Send(key.PairWith(this.Values[key]));
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
            using (var computation = NewComputation.FromArgs(ref args))
            {
                var keyvals = new BatchedDataSource<Pair<string, string>>();
                var queries = new BatchedDataSource<string>();

                computation.NewInput(keyvals)
                       .KeyValueLookup(computation.NewInput(queries))
                       .Subscribe(list => { foreach (var l in list) Console.WriteLine("value[\"{0}\"]:\t\"{1}\"", l.First, l.Second); });

                computation.Activate();

                if (computation.Configuration.ProcessID == 0)
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

                computation.Join();
            }

        }


        public string Help
        {
            get { return "Provides interactive get and set functionality for a distributed key-value store.\nDemonstrates how general data stores can be described in dataflow"; }
        }
    }
}
