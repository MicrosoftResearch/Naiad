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

using Naiad;
using Naiad.Dataflow;
using Naiad.Frameworks;
using Naiad.Runtime;
using Naiad.Scheduling;
using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Text;

namespace Examples.Throughput
{
    public class ProducerVertex : Vertex<Epoch>
    {
        private readonly VertexOutputBuffer<int, Epoch> output;

        private readonly int numberToSend;

        public override void OnDone(Epoch time)
        {
            Console.WriteLine("In OnDone");
            for (int i = 0; i < this.numberToSend; ++i)
            {
                this.output.Send(37, 0);
            }
        }

        private ProducerVertex(int id, Stage<Epoch> stage, int numberToSend)
            : base(id, stage)
        {
            Console.WriteLine("Constructing Producer {0}", id);
            this.numberToSend = numberToSend;
            this.output = new VertexOutputBuffer<int,Epoch>(this);
            this.NotifyAt(0);
        }

        public static Stream<int, Epoch> MakeStage(int numberToSend, Stream<int, Epoch> input)
        {
            Stage<ProducerVertex, Epoch> stage = Foundry.NewStage(new SingleVertexPlacement(0, 0), input.Context, (i, s) => new ProducerVertex(i, s, numberToSend), "Producer");
            stage.NewInput(input, (v, m) => { }, null);
            Stream<int, Epoch> stream = stage.NewOutput(v => v.output);
            return stream;
        }
    }

    public class ConsumerVertex : Vertex<Epoch>
    {
        private int numReceived = 0;
        private readonly int numberToConsume;
        private Stopwatch stopwatch = new Stopwatch();

        private void OnRecv(Message<Pair<int, Epoch>> message)
        {
            //Console.WriteLine("In OnRecv");
            if (!stopwatch.IsRunning)
                stopwatch.Start();
            
            numReceived += message.length;
        }

        public override void OnDone(Epoch time)
        {
            Console.WriteLine("Received {0}/{1} in {2}", numReceived, numberToConsume, stopwatch.Elapsed);
        }

        private ConsumerVertex(int id, Stage<Epoch> stage, int numberToConsume)
            : base(id, stage)
        {
            Console.WriteLine("Constructing Consumer {0}", id);
            this.numberToConsume = numberToConsume;
            this.NotifyAt(0);
        }

        public static Stage<ConsumerVertex, Epoch> MakeStage(int numberToConsume, Stream<int, Epoch> stream)
        {
            Stage<ConsumerVertex, Epoch> stage = Foundry.NewStage(new SingleVertexPlacement(1, 0), stream.Context, (i, s) => new ConsumerVertex(i, s, numberToConsume), "Consumer");
            stage.NewInput(stream, (m, v) => v.OnRecv(m), null);
            return stage;
        }
    }

    class Throughput : Example
    {
        public string Usage
        {
            get { throw new NotImplementedException(); }
        }

        public void Execute(string[] args)
        {
            using (Controller controller = NewController.FromArgs(ref args))
            {
                int numToExchange = int.Parse(args[1]);

                using (GraphManager graph = controller.NewGraph())
                {
                    Stream<int, Epoch> input = graph.NewInput(new ConstantDataSource<int>(5));

                    Stream<int, Epoch> stream = ProducerVertex.MakeStage(numToExchange, input);
                    Stage<ConsumerVertex, Epoch> consumer = ConsumerVertex.MakeStage(numToExchange, stream);

                    graph.Activate();

                    graph.Join();
                }
            }
        }
    }
}
