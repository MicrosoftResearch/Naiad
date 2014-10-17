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

using Microsoft.Research.Naiad;
using Microsoft.Research.Naiad.Dataflow;
using Microsoft.Research.Naiad.Frameworks.Lindi;
using Microsoft.Research.Naiad.Frameworks.Azure;
using Microsoft.Research.Naiad.Dataflow.PartitionBy;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Microsoft.WindowsAzure.Storage;
using Microsoft.WindowsAzure.Storage.Table;
using Microsoft.Research.Naiad.Dataflow.StandardVertices;

namespace Microsoft.Research.Naiad.AzureExamples
{
    class ConnectedComponents : Example
    {
        public string Usage
        {
            get { return "containername directoryname outputblobname"; }
        }

        public void Execute(string[] args)
        {
            var containerName = args[1];
            var directoryName = args[2];
            var outputblobName = args[3];

            CloudStorageAccount storageAccount = CloudStorageAccount.DevelopmentStorageAccount;

            var container = storageAccount.CreateCloudBlobClient()
                                          .GetContainerReference(containerName);

            if (!container.Exists())
                throw new Exception("No such container exists");

            // allocate a new computation from command line arguments.
            using (var computation = NewComputation.FromArgs(ref args))
            {
                // Set Console.Out to point at an Azure blob bearing the process id. 
                // See the important note at end of method about closing Console.Out.
                computation.Controller.SetConsoleOut(container, "stdout-{0}.txt");

                System.Diagnostics.Stopwatch stopwatch = new System.Diagnostics.Stopwatch();

                // read the edges from azure storage
                var edges = computation.ReadBinaryFromAzureBlobs<Pair<int, int>>(container, directoryName);

                // symmetrize the graph by adding in transposed edges.
                edges = edges.Select(x => new Pair<int, int>(x.Second, x.First))
                             .Concat(edges);

                // invoke director reachability
                var result = edges.DirectedReachability();

                // listen to the output for reporting, and also write the output somewhere in Azure
                result.Subscribe(list => Console.WriteLine("labeled {0} nodes in {1}", list.Count(), stopwatch.Elapsed));
                result.WriteBinaryToAzureBlobs(container, outputblobName);

                stopwatch.Start();

                // start computation and wait.
                computation.Activate();
                computation.Join();
            }


            // very important to close the stream to flush writes to Azure.
            Console.Out.Close();
        }

        public string Help
        {
            get { return "Demonstrates a connected components computation (from Examples.ConnectedComponents) run using Azure data sources, suitable for use in HDInsight. Requires previous use of azure-graphgenerator, or an equivalent source of graph data."; }
        }
    }
        public static class ExtensionMethods
        {
            // takes a graph as an edge stream and produces a stream of pairs (x,y) where y is the least vertex capable of reaching vertex x.
            public static Stream<Pair<TVertex, TVertex>, Epoch> DirectedReachability<TVertex>(this Stream<Pair<TVertex, TVertex>, Epoch> edges)
                where TVertex : IEquatable<TVertex>, IComparable<TVertex>
            {
                // prepartitioning reduces exchanges by one.
                edges = edges.PartitionBy(x => x.First.GetHashCode());

                // initial labels are (node, node).
                var labels = edges.Select(x => x.First)
                                  .Distinct()
                                  .Select(x => new Pair<TVertex, TVertex>(x, x));

                // repeatedly exchange labels with neighbors, keeping the least observed labels.
                // emits all exchanged labels; a BlockingAggregate reduces this to one per node.
                return labels.IterateAndAccumulate((lc, z) => z.GraphJoin(lc.EnterLoop(edges))
                                                               .StreamingAggregate((x, y) => x.CompareTo(y) > 0 ? x : y)
                    //.Synchronize()  // optionally sync up everything each iteration
                                                               ,
                                                   x => x.First.GetHashCode(), Int32.MaxValue, "Iteration")
                             .BlockingAggregate((x, y) => x.CompareTo(y) > 0 ? x : y);
            }

            #region Streaming aggregation

            /// <summary>
            /// Aggregates key-value pairs, producing new outputs whenever an aggregate changes.
            /// </summary>
            /// <typeparam name="TKey">key type</typeparam>
            /// <typeparam name="TValue">value type</typeparam>
            /// <typeparam name="TTime">time type</typeparam>
            /// <param name="input">input key-value stream</param>
            /// <param name="aggregate">aggregation function</param>
            /// <returns>aggregated key-value pairs</returns>
            public static Stream<Pair<TKey, TValue>, IterationIn<TTime>> StreamingAggregate<TKey, TValue, TTime>(this Stream<Pair<TKey, TValue>, IterationIn<TTime>> input, Func<TValue, TValue, TValue> aggregate)
                where TTime : Time<TTime>
                where TValue : IEquatable<TValue>
            {
                return Foundry.NewUnaryStage(input, (i, s) => new StreamingAggregateVertex<TKey, TValue, IterationIn<TTime>>(i, s, aggregate), x => x.First.GetHashCode(), x => x.First.GetHashCode(), "StreamingAggregate");
            }

            /// <summary>
            /// Aggregates key-value pairs, and produces new output whenever the result changes. 
            /// </summary>
            /// <typeparam name="TKey">Key type</typeparam>
            /// <typeparam name="TValue">Value type</typeparam>
            /// <typeparam name="TTime">Time type</typeparam>
            public class StreamingAggregateVertex<TKey, TValue, TTime> : UnaryVertex<Pair<TKey, TValue>, Pair<TKey, TValue>, TTime>
                where TTime : Time<TTime>
                where TValue : IEquatable<TValue>
            {
                private readonly Dictionary<TKey, TValue> Values;
                private readonly Func<TValue, TValue, TValue> Aggregate;

                public override void OnReceive(Message<Pair<TKey, TValue>, TTime> message)
                {
                    var output = this.Output.GetBufferForTime(message.time);
                    for (int j = 0; j < message.length; j++)
                    {
                        var record = message.payload[j];

                        // if the key is new, install the new value.
                        if (!this.Values.ContainsKey(record.First))
                        {
                            this.Values[record.First] = record.Second;
                            output.Send(record);
                        }
                        // else update value and send if it changes.
                        else
                        {
                            var oldValue = this.Values[record.First];
                            var newValue = this.Aggregate(oldValue, record.Second);
                            if (!oldValue.Equals(newValue))
                            {
                                this.Values[record.First] = newValue;
                                output.Send(new Pair<TKey, TValue>(record.First, newValue));
                            }
                        }
                    }
                }

                public StreamingAggregateVertex(int index, Stage<TTime> vertex, Func<TValue, TValue, TValue> aggregate)
                    : base(index, vertex)
                {
                    this.Values = new Dictionary<TKey, TValue>();
                    this.Aggregate = aggregate;

                    this.Entrancy = 5;
                }
            }

            #endregion

            #region Blocking aggregation

            /// <summary>
            /// Aggregates key-value pairs, producing at most one output per key per time.
            /// </summary>
            /// <typeparam name="TKey">key type</typeparam>
            /// <typeparam name="TValue">value type</typeparam>
            /// <typeparam name="TTime">time type</typeparam>
            /// <param name="input">input key-value stream</param>
            /// <param name="aggregate">aggregation function</param>
            /// <returns>aggregated key-value pairs</returns>
            public static Stream<Pair<TKey, TValue>, TTime> BlockingAggregate<TKey, TValue, TTime>(this Stream<Pair<TKey, TValue>, TTime> input, Func<TValue, TValue, TValue> aggregate)
                where TTime : Time<TTime>
                where TValue : IEquatable<TValue>
            {
                return Foundry.NewUnaryStage(input, (i, s) => new BlockingAggregateVertex<TKey, TValue, TTime>(i, s, aggregate), x => x.First.GetHashCode(), x => x.First.GetHashCode(), "BlockingAggregate");
            }

            /// <summary>
            /// Aggregates key-value pairs, and produces one new output whenever for each time in which a value changes.
            /// </summary>
            /// <typeparam name="TKey">Key type</typeparam>
            /// <typeparam name="TValue">Value type</typeparam>
            /// <typeparam name="TTime">Time type</typeparam>
            public class BlockingAggregateVertex<TKey, TValue, TTime> : UnaryVertex<Pair<TKey, TValue>, Pair<TKey, TValue>, TTime>
                where TTime : Time<TTime>
                where TValue : IEquatable<TValue>
            {
                private readonly HashSet<TKey> Active = new HashSet<TKey>();
                private readonly Dictionary<TKey, TValue> Values = new Dictionary<TKey, TValue>();
                private readonly Func<TValue, TValue, TValue> Aggregate;

                public override void OnReceive(Message<Pair<TKey, TValue>, TTime> message)
                {
                    for (int j = 0; j < message.length; j++)
                    {
                        var record = message.payload[j];
                        var time = message.time;

                        // if the key is new, install the value.
                        if (!this.Values.ContainsKey(record.First))
                        {
                            this.Values[record.First] = record.Second;
                            this.Active.Add(record.First);
                            this.NotifyAt(time);
                        }
                        // else update value and send if it changes
                        else
                        {
                            var oldValue = this.Values[record.First];
                            var newValue = this.Aggregate(oldValue, record.Second);
                            if (!oldValue.Equals(newValue))
                            {
                                this.Values[record.First] = newValue;
                                this.Active.Add(record.First);
                                this.NotifyAt(time);
                            }
                        }
                    }
                }

                public override void OnNotify(TTime time)
                {
                    var output = this.Output.GetBufferForTime(time);
                    foreach (var key in this.Active)
                        output.Send(key.PairWith(this.Values[key]));

                    this.Active.Clear();
                }

                public BlockingAggregateVertex(int index, Stage<TTime> vertex, Func<TValue, TValue, TValue> aggregate)
                    : base(index, vertex)
                {
                    this.Aggregate = aggregate;
                }
            }

            #endregion

            #region Graph-based Join

            public static Stream<Pair<TVertex, TState>, TTime> GraphJoin<TVertex, TState, TTime>(this Stream<Pair<TVertex, TState>, TTime> values, Stream<Pair<TVertex, TVertex>, TTime> edges)
                where TTime : Time<TTime>
            {
                return Foundry.NewBinaryStage(edges, values, (i, s) => new GraphJoinVertex<TVertex, TState, TTime>(i, s), x => x.First.GetHashCode(), y => y.First.GetHashCode(), null, "GraphJoin");
            }

            public class GraphJoinVertex<TVertex, TState, TTime> : BinaryVertex<Pair<TVertex, TVertex>, Pair<TVertex, TState>, Pair<TVertex, TState>, TTime>
                where TTime : Time<TTime>
            {
                private readonly Dictionary<TVertex, List<TVertex>> edges = new Dictionary<TVertex, List<TVertex>>();
                private readonly List<Pair<TVertex, TState>> enqueued = new List<Pair<TVertex, TState>>();
                private bool graphReceived = false;

                public override void OnReceive1(Message<Pair<TVertex, TVertex>, TTime> message)
                {
                    this.NotifyAt(message.time);

                    for (int i = 0; i < message.length; i++)
                    {
                        var edge = message.payload[i];

                        if (!this.edges.ContainsKey(edge.First))
                            this.edges.Add(edge.First, new List<TVertex>());

                        this.edges[edge.First].Add(edge.Second);
                    }
                }

                public override void OnReceive2(Message<Pair<TVertex, TState>, TTime> message)
                {
                    if (!this.graphReceived)
                    {
                        // add each record to a queue of work todo.
                        for (int i = 0; i < message.length; i++)
                            this.enqueued.Add(message.payload[i]);
                    }
                    else
                    {
                        var output = this.Output.GetBufferForTime(message.time);

                        for (int i = 0; i < message.length; i++)
                        {
                            var data = message.payload[i];

                            // send data to any matching neighbors.
                            if (this.edges.ContainsKey(data.First))
                                foreach (var destination in this.edges[data.First])
                                    output.Send(destination.PairWith(data.Second));
                        }
                    }
                }

                public override void OnNotify(TTime time)
                {
                    // we've received the entire graph (put this at the end of the method to see re-entrancy at work / crashing!)
                    this.graphReceived = true;

                    var output = this.Output.GetBufferForTime(time);

                    // send matches with enqueued data.
                    foreach (var data in this.enqueued)
                        if (this.edges.ContainsKey(data.First))
                            foreach (var destination in this.edges[data.First])
                                output.Send(destination.PairWith(data.Second));

                    this.enqueued.Clear();
                }

                public GraphJoinVertex(int index, Stage<TTime> vertex)
                    : base(index, vertex)
                {
                    this.Entrancy = 5;
                }
            }

            #endregion
        }
}
