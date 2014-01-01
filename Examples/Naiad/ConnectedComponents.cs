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
using Naiad.Dataflow.PartitionBy;

using Naiad.Frameworks.Lindi;
using System.Diagnostics;

namespace Examples.ConnectedComponents
{
    public static class ExtensionMethods
    {
        // takes a graph as an edge stream and produces a stream of pairs (x,y) where y is the least vertex capable of reaching vertex x.
        public static Stream<Pair<TVertex, TVertex>, Epoch> DirectedReachability<TVertex>(this Stream<Pair<TVertex, TVertex>, Epoch> edges) 
            where TVertex : IEquatable<TVertex>, IComparable<TVertex>
        {
            // prepartitioning reduces exchanges by one.
            edges = edges.PartitionBy(x => x.v1.GetHashCode());

            // initial labels are (node, node).
            var labels = edges.Select(x => x.v1)
                              .Distinct()
                              .Select(x => new Pair<TVertex, TVertex>(x, x));

            // repeatedly exchange labels with neighbors, keeping the least observed labels.
            // emits all exchanged labels; a BlockingAggregate reduces this to one per node.
            return labels.IterateAndAccumulate((lc, z) => z.GraphJoin(lc.EnterLoop(edges))
                                                           .StreamingAggregate((x, y) => x.CompareTo(y) > 0 ? x : y)
                                                        // .Synchronize()  // optionally sync up everything each iteration
                                                           ,
                                               x => x.v1.GetHashCode(), Int32.MaxValue, "Iteration")
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
        public static Stream<Pair<TKey, TValue>, TTime> StreamingAggregate<TKey, TValue, TTime>(this Stream<Pair<TKey, TValue>, TTime> input, Func<TValue, TValue, TValue> aggregate)
            where TTime : Time<TTime>
            where TValue : IEquatable<TValue>
        {
            return Naiad.Frameworks.Foundry.NewStage(input, (i, s) => new StreamingAggregateVertex<TKey, TValue, TTime>(i, s, aggregate), x => x.v1.GetHashCode(), x => x.v1.GetHashCode(), "StreamingAggregate");
        }

        /// <summary>
        /// Aggregates key-value pairs, and produces new output whenever the result changes. 
        /// </summary>
        /// <typeparam name="TKey">Key type</typeparam>
        /// <typeparam name="TValue">Value type</typeparam>
        /// <typeparam name="TTime">Time type</typeparam>
        public class StreamingAggregateVertex<TKey, TValue, TTime> : Naiad.Frameworks.UnaryVertex<Pair<TKey, TValue>, Pair<TKey, TValue>, TTime>
            where TTime : Time<TTime>
            where TValue : IEquatable<TValue>
        {
            private readonly Dictionary<TKey, TValue> Values;
            private readonly Func<TValue, TValue, TValue> Aggregate;

            public override void MessageReceived(Message<Pair<Pair<TKey, TValue>, TTime>> message)
            {
                for (int j = 0; j < message.length; j++)
                {
                    var record = message.payload[j];

                    // if the key is new, install the new value.
                    if (!this.Values.ContainsKey(record.v1.v1))
                    {
                        this.Values[record.v1.v1] = record.v1.v2;
                        this.Output.Send(record.v1, record.v2);
                    }
                    // else update value and send if it changes.
                    else
                    {
                        var oldValue = this.Values[record.v1.v1];
                        var newValue = this.Aggregate(oldValue, record.v1.v2);
                        if (!oldValue.Equals(newValue))
                        {
                            this.Values[record.v1.v1] = newValue;
                            this.Output.Send(new Pair<TKey, TValue>(record.v1.v1, newValue), record.v2);
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
            return Naiad.Frameworks.Foundry.NewStage(input, (i, s) => new BlockingAggregateVertex<TKey, TValue, TTime>(i, s, aggregate), x => x.v1.GetHashCode(), x => x.v1.GetHashCode(), "BlockingAggregate");
        }

        /// <summary>
        /// Aggregates key-value pairs, and produces one new output whenever for each time in which a value changes.
        /// </summary>
        /// <typeparam name="TKey">Key type</typeparam>
        /// <typeparam name="TValue">Value type</typeparam>
        /// <typeparam name="TTime">Time type</typeparam>
        public class BlockingAggregateVertex<TKey, TValue, TTime> : Naiad.Frameworks.UnaryVertex<Pair<TKey, TValue>, Pair<TKey, TValue>, TTime>
            where TTime : Time<TTime>
            where TValue : IEquatable<TValue>
        {
            private readonly HashSet<TKey> Active = new HashSet<TKey>();
            private readonly Dictionary<TKey, TValue> Values = new Dictionary<TKey, TValue>();
            private readonly Func<TValue, TValue, TValue> Aggregate;

            public override void MessageReceived(Message<Pair<Pair<TKey, TValue>, TTime>> message)
            {
                for (int j = 0; j < message.length; j++)
                {
                    var record = message.payload[j].v1;
                    var time = message.payload[j].v2;

                    // if the key is new, install the value.
                    if (!this.Values.ContainsKey(record.v1))
                    {
                        this.Values[record.v1] = record.v2;
                        this.Active.Add(record.v1);
                        this.NotifyAt(time);
                    }
                    // else update value and send if it changes
                    else
                    {
                        var oldValue = this.Values[record.v1];
                        var newValue = this.Aggregate(oldValue, record.v2);
                        if (!oldValue.Equals(newValue))
                        {
                            this.Values[record.v1] = newValue;
                            this.Active.Add(record.v1);
                            this.NotifyAt(time);
                        }
                    }
                }
            }

            public override void OnDone(TTime time)
            {
                foreach (var key in this.Active)
                    this.Output.Send(key.PairWith(this.Values[key]), time);

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
            return Naiad.Frameworks.Foundry.NewStage(edges, values, (i, s) => new GraphJoinVertex<TVertex, TState, TTime>(i, s), x => x.v1.GetHashCode(), y => y.v1.GetHashCode(), null, "GraphJoin");
        }

        public class GraphJoinVertex<TVertex, TState, TTime> : Naiad.Frameworks.BinaryVertex<Pair<TVertex, TVertex>, Pair<TVertex, TState>, Pair<TVertex, TState>, TTime>
            where TTime : Time<TTime>
        {
            private readonly Dictionary<TVertex, List<TVertex>> edges = new Dictionary<TVertex, List<TVertex>>();
            private readonly List<Pair<TVertex, TState>> enqueued = new List<Pair<TVertex, TState>>();
            private bool graphReceived = false;

            public override void MessageReceived1(Message<Pair<Pair<TVertex, TVertex>, TTime>> message)
            {
                for (int i = 0; i < message.length; i++)
                {
                    var edge = message.payload[i].v1;
                    var time = message.payload[i].v2;

                    if (!this.edges.ContainsKey(edge.v1))
                        this.edges.Add(edge.v1, new List<TVertex>());

                    this.edges[edge.v1].Add(edge.v2);

                    this.NotifyAt(time);
                }
            }

            public override void MessageReceived2(Message<Pair<Pair<TVertex, TState>, TTime>> message)
            {
                if (!this.graphReceived)
                {
                    // add each record to a queue of work todo.
                    for (int i = 0; i < message.length; i++)
                        this.enqueued.Add(message.payload[i].v1);
                }
                else
                {
                    for (int i = 0; i < message.length; i++)
                    {
                        var data = message.payload[i].v1;
                        var time = message.payload[i].v2;

                        // send data to any matching neighbors.
                        if (this.edges.ContainsKey(data.v1))
                            foreach (var destination in this.edges[data.v1])
                                this.Output.Send(destination.PairWith(data.v2), time);
                    }
                }
            }

            public override void OnDone(TTime time)
            {
                // we've received the entire graph (put this at the end of the method to see re-entrancy at work / crashing!)
                this.graphReceived = true;

                // send matches with enqueued data.
                foreach (var data in this.enqueued)
                    if (this.edges.ContainsKey(data.v1))
                        foreach (var destination in this.edges[data.v1])
                            this.Output.Send(destination.PairWith(data.v2), time);

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

    public class ConnectedComponents : Example
    {
        public string Usage
        {
            get { return "[nodes edges]"; }
        }

        public void Execute(string[] args)
        {
            // allocate a new controller from command line arguments.
            using (var controller = NewController.FromArgs(ref args))
            {
                var nodeCount = args.Length == 3 ? Convert.ToInt32(args[1]) : 100000;
                var edgeCount = args.Length == 3 ? Convert.ToInt32(args[2]) : 200000;

                #region Generate a local fraction of input data

                var random = new Random(0);
                var processes = controller.Configuration.Processes;
                var graphFragment = new Pair<int, int>[edgeCount / processes];
                for (int i = 0; i < graphFragment.Length; i++)
                    graphFragment[i] = new Pair<int, int>(random.Next(nodeCount), random.Next(nodeCount));

                #endregion

                Console.WriteLine("Computing components of a random graph on {0} nodes and {1} edges", nodeCount, edgeCount);

                Stopwatch stopwatch = new Stopwatch();

                // allocate a new graph manager for the computation.
                using (var manager = controller.NewGraph())
                {
                    // convert array of edges to single-epoch stream.
                    var edges = graphFragment.AsNaiadStream(manager)
                                             .Synchronize();

                    // symmetrize the graph by adding in transposed edges.
                    edges = edges.Select(x => new Pair<int, int>(x.v2, x.v1))
                                 .Concat(edges);

                    edges.DirectedReachability()
                         .Subscribe(list => Console.WriteLine("labeled {0} nodes in {1}", list.Count(), stopwatch.Elapsed));

                    stopwatch.Start();
                    manager.Activate();     // start graph computation
                    manager.Join();         // block until computation completes
                }

                controller.Join();
            }
        }
    }
}