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
using Microsoft.Research.Naiad.Dataflow.StandardVertices;
using Microsoft.Research.Naiad.Input;
using Microsoft.Research.Naiad.Frameworks;
using Microsoft.Research.Naiad.Runtime;
using Microsoft.Research.Naiad.Scheduling;
using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Text;

namespace Microsoft.Research.Naiad.Examples.Throughput
{
    public class ProducerVertex : Vertex<Epoch>
    {
        private readonly VertexOutputBuffer<Pair<int, int>, Epoch> output;

        private readonly int numberToSend;

        public override void OnNotify(Epoch time)
        {
            var output = this.output.GetBufferForTime(new Epoch(0));
            for (int i = 0; i < this.numberToSend; ++i)
                output.Send(this.VertexId.PairWith(i));
        }

        private ProducerVertex(int id, Stage<Epoch> stage, int numberToSend)
            : base(id, stage)
        {
            this.numberToSend = numberToSend;
            this.output = new VertexOutputBuffer<Pair<int, int>, Epoch>(this);
            this.NotifyAt(new Epoch(0));
        }

        public static Stream<Pair<int, int>, Epoch> MakeStage(int numberToSend, int startProcess, int endProcess, int numberOfWorkers, Stream<Pair<int, int>, Epoch> input)
        {
            var locations = new List<VertexLocation>();
            for (int i = 0; i < endProcess - startProcess; i++)
                for (int j = 0; j < numberOfWorkers; j++)
                    locations.Add(new VertexLocation(locations.Count, i + startProcess, j));

            Placement placement = new Placement.Explicit(locations);

            Stage<ProducerVertex, Epoch> stage = Foundry.NewStage(placement, input.Context, (i, s) => new ProducerVertex(i, s, numberToSend), "Producer");
            stage.NewInput(input, (v, m) => { }, null);
            Stream<Pair<int, int>, Epoch> stream = stage.NewOutput(v => v.output);
            return stream;
        }
    }

    public class ConsumerVertex : Vertex<Epoch>
    {
        private int numReceived = 0;
        private readonly int numberToConsume;
        private Stopwatch stopwatch = new Stopwatch();

        private void OnRecv(Message<Pair<int, int>, Epoch> message)
        {
            //Console.WriteLine("In OnRecv");
            if (!stopwatch.IsRunning)
                stopwatch.Start();

            numReceived += message.length;
        }

        public override void OnNotify(Epoch time)
        {
            Console.WriteLine("Received {0} records in {1}", numReceived, stopwatch.Elapsed);
        }

        private ConsumerVertex(int id, Stage<Epoch> stage, int numberToConsume)
            : base(id, stage)
        {
            this.numberToConsume = numberToConsume;
            this.NotifyAt(new Epoch(0));
        }

        public static Stage<ConsumerVertex, Epoch> MakeStage(int numberToConsume, int startProcess, int endProcess, int numberOfWorkers, bool exchange, Stream<Pair<int, int>, Epoch> stream)
        {
            var locations = new List<VertexLocation>();
            for (int i = 0; i < endProcess - startProcess; i++)
                for (int j = 0; j < numberOfWorkers; j++)
                    locations.Add(new VertexLocation(locations.Count, i + startProcess, j));

            Placement placement = new Placement.Explicit(locations);

            Stage<ConsumerVertex, Epoch> stage = Foundry.NewStage(placement, stream.Context, (i, s) => new ConsumerVertex(i, s, numberToConsume), "Consumer");


            if (exchange)
                stage.NewInput(stream, (m, v) => v.OnRecv(m), x => x.Second);
            else
                stage.NewInput(stream, (m, v) => v.OnRecv(m), x => x.First);

            return stage;
        }
    }

    class Throughput : Example
    {
        public string Usage
        {
            get { return "records producers consumers [exchange]"; }
        }

        public void Execute(string[] args)
        {
            using (OneOffComputation computation = NewComputation.FromArgs(ref args))
            {
                int numToExchange = args.Length > 1 ? int.Parse(args[1]) : 1000000;
                int producers = Int32.Parse(args[2]);
                int consumers = Int32.Parse(args[3]);

                var exchange = args.Length > 4 && args[4] == "exchange";

                var input = new Pair<int, int>[] { }.AsNaiadStream(computation);

                Stream<Pair<int, int>, Epoch> stream = ProducerVertex.MakeStage(numToExchange, 0, producers, computation.Configuration.WorkerCount, input);
                Stage<ConsumerVertex, Epoch> consumer = ConsumerVertex.MakeStage(numToExchange, computation.Configuration.Processes - consumers, computation.Configuration.Processes, computation.Configuration.WorkerCount, exchange, stream);

                computation.Activate();
                computation.Join();
            }
        }


        public string Help
        {
            get { return "Tests the throughput of Naiad by sending a user-specified number of records along a single exchange edge as fast as possible."; }
        }
    }
}
