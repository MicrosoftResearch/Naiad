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
using Microsoft.Research.Naiad.Dataflow.Iteration;
using Microsoft.Research.Naiad.Frameworks;
using Microsoft.Research.Naiad.Runtime;
using Microsoft.Research.Naiad.Scheduling;
using Microsoft.Research.Naiad.Input;
using Microsoft.Research.Naiad.Dataflow.StandardVertices;

using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Text;

namespace Microsoft.Research.Naiad.Examples.Latency
{
    internal class Barrier : UnaryVertex<int, int, IterationIn<Epoch>>
    {
        private readonly int Iterations;

        // Send round-robin to destinations to try and get better network utilization
        public override void OnNotify(IterationIn<Epoch> time)
        {
            if (time.iteration < this.Iterations)
                this.NotifyAt(new IterationIn<Epoch>(time.outerTime, time.iteration + 1));
        }

        public static Stream<int, IterationIn<Epoch>> MakeStage(Stream<int, IterationIn<Epoch>> ingress, Stream<int, IterationIn<Epoch>> feedbackOutput, int iterations)
        {
            var stage = new Stage<Barrier, IterationIn<Epoch>>(ingress.Context, (i, s) => new Barrier(i, s, iterations), "Barrier");
            stage.NewInput(ingress, (message, vertex) => { }, null);
            stage.NewInput(feedbackOutput, (message, vertex) => { }, null);
            
            return stage.NewOutput<int>(vertex => vertex.Output);
        }

        public Barrier(int index, Stage<IterationIn<Epoch>> vertex, int iterations)
            : base(index, vertex)
        {
            this.NotifyAt(new IterationIn<Epoch>(new Epoch(0), 0));
            this.Iterations = iterations;
        }

        public override void OnReceive(Message<int, IterationIn<Epoch>> message) { throw new NotImplementedException(); }
    }


    class Latency : Example
    {
        public string Usage { get { return "iterations"; } }

        public void Execute(string[] args)
        {
            using (var computation = NewComputation.FromArgs(ref args))
            {
                int iterations = int.Parse(args[1]);

                // first construct a simple graph with a feedback loop.
                var inputStream = (new int[] { }).AsNaiadStream(computation);

                var loopContext = new LoopContext<Epoch>(inputStream.Context, "loop");
                var feedback = loopContext.Delay<int>();
                var ingress = loopContext.EnterLoop(inputStream);

                feedback.Input = Barrier.MakeStage(ingress, feedback.Output, iterations);

                // prepare measurement callbacks
                var sw = new Stopwatch();
                var lastTime = 0L;
                var times = new List<double>(iterations);

                computation.OnStartup += (c, y) => { sw.Start(); };
                computation.OnFrontierChange += (v, b) =>
                {
                    var now = sw.ElapsedTicks;

                    if (lastTime > 0)
                        times.Add(1000.0 * (now - lastTime) / (double)Stopwatch.Frequency);

                    lastTime = now;
                };

                Console.WriteLine("Running barrier latency test with {0} iterations, vertices={1}", iterations, ingress.ForStage.Placement.Count);

                // start computation and block
                computation.Activate();
                computation.Join();

                // print results
                times.Sort();

                var percentiles = new[] { 0.00, 0.01, 0.05, 0.10, 0.25, 0.50, 0.75, 0.90, 0.95, 0.99 };
                var latencies = percentiles.Select(f => times[(int)(iterations * f)]).ToArray();

                Console.WriteLine("Ran {0} iterations on {1} processes; this is process {2}", times.Count - 1, computation.Configuration.Processes, computation.Configuration.ProcessID);

                Console.WriteLine("%-ile\tLatency (ms)");
                for (int i = 0; i < latencies.Length; i++)
                    Console.WriteLine("{0:0.00}:\t{1:0.00}", percentiles[i], latencies[i]);

                Console.WriteLine("max:\t{0:0.00}", latencies[latencies.Length - 1]);

            }
        }


        public string Help
        {
            get { return "Tests the latency of Naiad by iterating an empty loop as quickly as possible, for a user-specified number of iterations."; }
        }
    }
}
