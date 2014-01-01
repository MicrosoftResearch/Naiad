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

using Naiad.Dataflow;
using Naiad.Dataflow.Channels;
using Naiad.Runtime.Controlling;
using Naiad.Scheduling;
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Naiad.Runtime.Progress
{
    internal interface LocalProgressInfo 
    {
        PointstampCountSet PointstampCountSet { get; }
    }

    internal interface ProgressTracker : Frontier
    {
        void BroadcastProgressUpdate(Pointstamp time, int update);

        LocalProgressInfo GetInfoForWorker(int workerId);
        ProgressUpdateAggregator Aggregator { get; }

        void Complain(TextWriter writer);

        void Cancel();

        void BlockUntilComplete();
    }

    internal class DistributedProgressTracker : ProgressTracker
    {
        private ProgressChannel progressChannel;          // single producer -> many consumers

        ProgressUpdateConsumer consumer;
        public LocalProgressInfo GetInfoForWorker(int workerId) { return this.consumer; }

        private ProgressUpdateAggregator aggregator;
        public ProgressUpdateAggregator Aggregator { get { return this.aggregator; } }

        public void BroadcastProgressUpdate(Pointstamp time, int update)
        {
            this.consumer.InjectElement(time, update);
        }

        public event EventHandler<FrontierChangedEventArgs> OnFrontierChanged { add { this.consumer.OnFrontierChanged += value; } remove { this.consumer.OnFrontierChanged -= value; } }

        public void Cancel()
        {
            this.consumer.FrontierEmpty.Set();
        }

        public void BlockUntilComplete()
        {
            this.consumer.FrontierEmpty.WaitOne();
        }

        public DistributedProgressTracker(InternalGraphManager graphManager) 
        {
            var processes = graphManager.Controller.Configuration.Processes;
            var processid = graphManager.Controller.Configuration.ProcessID;

            var context = new OpaqueTimeContext<Pointstamp>(graphManager.ContextManager.MakeRawContextForScope<Pointstamp>("progress context"));

            // construct aggregator stage with unconnected output
            var aggregatorPlacement = new SingleVertexPerProcessPlacement(processes, 0);
            var aggregator = new Stage<ProgressUpdateAggregator, Pointstamp>(aggregatorPlacement, context, Stage.OperatorType.Default, (i, v) => new ProgressUpdateAggregator(i, v), "Aggregator");
            var stream = aggregator.NewOutput(shard => shard.Output);
            aggregator.Materialize();
            this.aggregator = aggregator.GetShard(processid);

            // construct consumer stage with unconnected input
            var consumerPlacement = new SingleVertexPerProcessPlacement(processes, 0);
            var consumer = new Stage<ProgressUpdateConsumer, Pointstamp>(consumerPlacement, context, Stage.OperatorType.Default, (i, v) => new ProgressUpdateConsumer(i, v, this.aggregator), "Consumer");
            var recvPort = consumer.NewUnconnectedInput(shard => shard.Input, null);
            consumer.Materialize();
            this.consumer = consumer.GetShard(processid);

            // connect aggregators to consumers with special progress channel
            this.progressChannel = new ProgressChannel(aggregatorPlacement.Count, this.consumer, stream.StageOutput, recvPort, graphManager.Controller, graphManager.AllocateNewGraphIdentifier());
            stream.StageOutput.AttachBundleToSender(this.progressChannel);

            Logging.Progress("Distributed progress tracker enabled");
        }

        public void Complain(TextWriter writer)
        {
            lock (this.consumer.PCS)
            {
                var frontier = this.consumer.PCS.Frontier;
                for (int i = 0; i < frontier.Length; i++)
                    writer.WriteLine("\tfrontier[{0}]:\t{1}\t{2}", i, frontier[i], this.consumer.PCS.Counts[frontier[i]]);
            }
        }
    }

    internal class CentralizedProgressTracker : ProgressTracker
    {
        public ProgressUpdateAggregator Aggregator { get { return this.aggregator; } }
        private readonly ProgressUpdateAggregator aggregator;
        private readonly ProgressUpdateConsumer consumer;
        private readonly ProgressUpdateCentralizer centralizer;

        public LocalProgressInfo GetInfoForWorker(int workerId) { return this.consumer; }

        public event EventHandler<FrontierChangedEventArgs> OnFrontierChanged { add { this.consumer.OnFrontierChanged += value; } remove { this.consumer.OnFrontierChanged -= value; } }

        public void Cancel()
        {
            this.consumer.FrontierEmpty.Set();
        }

        public void BlockUntilComplete()
        {
            this.consumer.FrontierEmpty.WaitOne();
        }

        public void BroadcastProgressUpdate(Pointstamp time, int update)
        {
            this.consumer.InjectElement(time, 1);

            if (this.centralizer != null)
                this.centralizer.InjectElement(time, update);
        }


        public CentralizedProgressTracker(InternalGraphManager graphManager) 
        {
            var centralizerProcessId = graphManager.Controller.Configuration.CentralizerProcessId;
            var centralizerThreadId = graphManager.Controller.Configuration.CentralizerThreadId;

            var processes = graphManager.Controller.Configuration.Processes;
            var processid = graphManager.Controller.Configuration.ProcessID;

            Logging.Progress("Centralized progress tracker enabled, running on process {0} thread {1}", centralizerProcessId, centralizerThreadId);

            var context = new OpaqueTimeContext<Pointstamp>(graphManager.ContextManager.MakeRawContextForScope<Pointstamp>("progress context"));

            // construct aggregator stage and unconnected output
            var aggregatorPlacement = new SingleVertexPerProcessPlacement(processes, 0);
            var aggregatorStage = new Stage<ProgressUpdateAggregator, Pointstamp>(aggregatorPlacement, context, Stage.OperatorType.Default, (i, v) => new ProgressUpdateAggregator(i, v), "Aggregator");
            var stream = aggregatorStage.NewOutput(shard => shard.Output);
            aggregatorStage.Materialize();
            this.aggregator = aggregatorStage.GetShard(processid);

            // construct centralizer stage and unconnected input and output
            var centralizerPlacement = new SingleVertexPlacement(centralizerProcessId, centralizerThreadId);
            var centralizer = new Stage<ProgressUpdateCentralizer, Pointstamp>(centralizerPlacement, context, Stage.OperatorType.Default, (i, v) => new ProgressUpdateCentralizer(i, v, null), "Centralizer");
            var centralizerRecvPort = centralizer.NewUnconnectedInput<Int64>(shard => shard.Input, null);
            var centralizerSendPort = centralizer.NewOutput(shard => shard.Output, null);
            centralizer.Materialize();
            this.centralizer = (processid == centralizerProcessId) ? centralizer.GetShard(0) : null;

            // construct consumer stage and unconnected input
            var consumerPlacement = new SingleVertexPerProcessPlacement(processes, 0); 
            var consumer = new Stage<ProgressUpdateConsumer, Pointstamp>(consumerPlacement, context, Stage.OperatorType.Default, (i, v) => new Runtime.Progress.ProgressUpdateConsumer(i, v, this.aggregator), "Consumer");
            var consumerRecvPort = consumer.NewUnconnectedInput(shard => shard.Input, null);
            consumer.Materialize();
            this.consumer = consumer.GetShard(processid);

            // connect centralizer to consumers with special progress channel
            var progressChannel = new ProgressChannel(centralizer.Placement.Count, this.consumer, centralizerSendPort.StageOutput, consumerRecvPort, graphManager.Controller, graphManager.AllocateNewGraphIdentifier());
            centralizerSendPort.StageOutput.AttachBundleToSender(progressChannel);

            // connect aggregators to centralizer with special centralized progress channel
            var centralizerChannel = new CentralizedProgressChannel(centralizer, stream.StageOutput, centralizerRecvPort, graphManager.Controller, graphManager.AllocateNewGraphIdentifier());
            stream.StageOutput.AttachBundleToSender(centralizerChannel);

            Logging.Progress("Centralized progress tracker initialization completed");
        }


        public void Complain(TextWriter writer)
        {
            lock (this.consumer.PCS)
            {
                var frontier = this.consumer.PCS.Frontier;
                for (int i = 0; i < frontier.Length; i++)
                    writer.WriteLine("\tfrontier[{0}]:\t{1}\t{2}", i, frontier[i], this.consumer.PCS.Counts[frontier[i]]);
            }

            if (this.centralizer != null)
            {
                lock (this.centralizer.PCS)
                {
                    var frontier = this.centralizer.PCS.Frontier;
                    for (int i = 0; i < frontier.Length; i++)
                        writer.WriteLine("\tcentralized frontier[{0}]:\t{1}\t{2}", i, frontier[i], this.centralizer.PCS.Counts[frontier[i]]);
                }
            }
        }
    }
}
