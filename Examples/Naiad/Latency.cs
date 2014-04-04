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

using Microsoft.Research.Naiad;
using Microsoft.Research.Naiad.Dataflow;
using Microsoft.Research.Naiad.Dataflow.Iteration;
using Microsoft.Research.Naiad.Frameworks;
using Microsoft.Research.Naiad.Runtime;
using Microsoft.Research.Naiad.Scheduling;
using Microsoft.WindowsAzure.Storage;
using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Text;

namespace Examples.Latency
{
    internal class Barrier : UnaryVertex<int, int, IterationIn<Epoch>>
    {
        private readonly int Iterations;

        // Send round-robin to destinations to try and get better network utilization
        public override void OnNotify(IterationIn<Epoch> time)
        {
            if (time.t < this.Iterations)
                this.NotifyAt(new IterationIn<Epoch>(time.s, time.t + 1));
        }

        public static Stream<int, IterationIn<Epoch>> MakeStage(Stream<int, IterationIn<Epoch>> ingress, Stream<int, IterationIn<Epoch>> feedbackOutput, int iterations)
        {
            var stage = new Stage<Barrier, IterationIn<Epoch>>(ingress.Context, (i, s) => new Barrier(i, s, iterations), "Barrier");
            var initialInput = stage.NewInput(ingress, (message, vertex) => { }, null);
            var feedbackInput = stage.NewInput(feedbackOutput, (message, vertex) => { }, null);
            
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
            using (var controller = NewController.FromArgs(ref args))
            {
                Logging.LogLevel = LoggingLevel.Off;

                int iterations = int.Parse(args[1]);

                using (var manager = controller.NewComputation())
                {
                    // first construct a simple graph with a feedback loop.
                    var inputStream = (new int[] { }).AsNaiadStream(manager);

                    var loopContext = new LoopContext<Epoch>(inputStream.Context, "loop");
                    var feedback = loopContext.Delay<int>();
                    var ingress = loopContext.EnterLoop(inputStream);

                    feedback.Input = Barrier.MakeStage(ingress, feedback.Output, iterations);

                    // prepare measurement callbacks
                    var sw = new Stopwatch();
                    var lastTime = 0L;
                    var times = new List<double>(iterations);

                    manager.OnStartup += (c, y) => { sw.Start(); };
                    manager.Frontier.OnFrontierChanged += (v, b) =>
                    {
                        var now = sw.ElapsedTicks;

                        if (lastTime > 0)
                            times.Add(1000.0 * (now - lastTime) / (double)Stopwatch.Frequency);

                        lastTime = now;
                    };

                    Console.Error.WriteLine("Running barrier latency test with {0} iterations, vertices={1}", iterations, ingress.ForStage.Placement.Count);

                    // start computation and block
                    manager.Activate();
                    manager.Join();

                    // print results
                    times.Sort();

                    var percentiles = new[] { 0.00, 0.01, 0.05, 0.10, 0.25, 0.50, 0.75, 0.90, 0.95, 0.99 };
                    var latencies = percentiles.Select(f => times[(int)(iterations * f)]).ToArray();

                    CloudStorageAccount account = CloudStorageAccount.Parse("DefaultEndpointsProtocol=https;AccountName=msrsvc;AccountKey=I4JPlk0bZ6YWypg+RJamyq0us1b+kCcuoeKlPhfiHTcVW7P4xvuzURvlRShSo1O3UDhcL2LiY4kMaarD+p1lKg==");

                    var client = account.CreateCloudBlobClient();
                    var cref = client.GetContainerReference("naiad-jobs");
                    var bref = cref.GetBlockBlobReference(string.Format("foo-{0}", controller.Configuration.ProcessID));

                    bref.UploadText("Done");

                    Console.WriteLine("Ran {0} iterations on {1} processes; this is process {2}", times.Count - 1, controller.Configuration.Processes, controller.Configuration.ProcessID);

                    Console.WriteLine("%-ile\tLatency (ms)");
                    for (int i = 0; i < latencies.Length; i++)
                        Console.WriteLine("{0:0.00}:\t{1:0.00}", percentiles[i], latencies[i]);

                    Console.WriteLine("max:\t{0:0.00}", latencies[latencies.Length - 1]);
                }

                controller.Join();
            }
        }
    }
}
