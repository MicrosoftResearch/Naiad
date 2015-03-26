/*
 * Naiad ver. 0.6
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

using Microsoft.Research.Naiad.Dataflow;
using Microsoft.Research.Naiad.Dataflow.Channels;
using Microsoft.Research.Naiad.Runtime.Controlling;
using Microsoft.Research.Naiad.Scheduling;
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

using Microsoft.Research.Naiad.Diagnostics;
using Microsoft.Research.Naiad.Runtime.FaultTolerance;

namespace Microsoft.Research.Naiad.Runtime.Progress
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

        void ForceFlush();
        void PrepareForRollback(bool preparing);
        void Reset();

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

        public void ForceFlush()
        {
            throw new NotImplementedException();
        }

        public void PrepareForRollback(bool preparing)
        {
            throw new NotImplementedException();
        }

        public void Reset()
        {
            throw new NotImplementedException();
        }

        public DistributedProgressTracker(InternalComputation internalComputation) 
        {
            var processes = internalComputation.Controller.Configuration.Processes;
            var processid = internalComputation.Controller.Configuration.ProcessID;

            // construct aggregator stage with unconnected output
            var aggregatorPlacement = new Placement.SingleVertexPerProcess(processes, 0);
            var aggregator = new Stage<ProgressUpdateAggregator, Empty>(aggregatorPlacement, internalComputation, Stage.OperatorType.Default, (i, v) => new ProgressUpdateAggregator(i, v), "Aggregator");
            aggregator.SetCheckpointType(CheckpointType.None);
            var stream = aggregator.NewTypedOutput(vertex => vertex.Output, null);
            aggregator.Materialize();
            this.aggregator = aggregator.GetVertex(processid);

            // construct consumer stage with unconnected input
            var consumerPlacement = new Placement.SingleVertexPerProcess(processes, 0);
            var consumer = new Stage<ProgressUpdateConsumer, Empty>(consumerPlacement, internalComputation, Stage.OperatorType.Default, (i, v) => new ProgressUpdateConsumer(i, v, this.aggregator), "Consumer");
            consumer.SetCheckpointType(CheckpointType.None);
            var recvPort = consumer.NewUnconnectedInput(vertex => vertex.Input, null);
            consumer.Materialize();
            this.consumer = consumer.GetVertex(processid);

            // connect aggregators to consumers with special progress channel
            this.progressChannel = new ProgressChannel(aggregatorPlacement.Count, this.consumer, stream.TypedStageOutput, recvPort, internalComputation.Controller, new Edge<Empty, Update, Empty>(internalComputation.AllocateNewGraphIdentifier()));
            stream.TypedStageOutput.AttachBundleToSender(this.progressChannel);

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
            // The FrontierEmpty event is signalled on the transition to empty,
            // so check the length in case the computation has no vertices.
            if (this.consumer.PCS.Frontier.Length > 0)
                this.consumer.FrontierEmpty.WaitOne();
        }

        public void BroadcastProgressUpdate(Pointstamp time, int update)
        {
            this.consumer.InjectElement(time, 1);

            if (this.centralizer != null)
                this.centralizer.InjectElement(time, update);
        }

        public void ForceFlush()
        {
            this.aggregator.ConsiderFlushingBufferedUpdates(true);
        }

        public void PrepareForRollback(bool preparing)
        {
            // this may only be called by the process where the centralizer lives
            this.centralizer.PrepareCentralizerForRollback(preparing);
        }

        public void Reset()
        {
            aggregator.Reset();
            if (this.centralizer != null)
                centralizer.Reset();
            consumer.Reset();
        }


        public CentralizedProgressTracker(InternalComputation internalComputation) 
        {
            var centralizerProcessId = internalComputation.Controller.Configuration.CentralizerProcessId;
            var centralizerThreadId = internalComputation.Controller.Configuration.CentralizerThreadId;

            var processes = internalComputation.Controller.Configuration.Processes;
            var processid = internalComputation.Controller.Configuration.ProcessID;

            Logging.Progress("Centralized progress tracker enabled, running on process {0} thread {1}", centralizerProcessId, centralizerThreadId);

            // construct aggregator stage and unconnected output
            var aggregatorPlacement = new Placement.SingleVertexPerProcess(processes, 0);
            var aggregatorStage = new Stage<ProgressUpdateAggregator, Empty>(aggregatorPlacement, internalComputation, Stage.OperatorType.Default, (i, v) => new ProgressUpdateAggregator(i, v), "Aggregator");
            aggregatorStage.SetCheckpointType(CheckpointType.None);
            var stream = aggregatorStage.NewTypedOutput(vertex => vertex.Output, null);
            aggregatorStage.Materialize();
            this.aggregator = aggregatorStage.GetVertex(processid);

            // construct centralizer stage and unconnected input and output
            var centralizerPlacement = new Placement.SingleVertex(centralizerProcessId, centralizerThreadId);
            var centralizer = new Stage<ProgressUpdateCentralizer, Empty>(centralizerPlacement, internalComputation, Stage.OperatorType.Default, (i, v) => new ProgressUpdateCentralizer(i, v, null), "Centralizer");
            centralizer.SetCheckpointType(CheckpointType.None);
            var centralizerRecvPort = centralizer.NewUnconnectedInput<Update>(vertex => vertex.Input, null);
            var centralizerSendPort = centralizer.NewTypedOutput(vertex => vertex.Output, null);
            centralizer.Materialize();
            this.centralizer = (processid == centralizerProcessId) ? centralizer.GetVertex(0) : null;

            // construct consumer stage and unconnected input
            var consumerPlacement = new Placement.SingleVertexPerProcess(processes, 0); 
            var consumer = new Stage<ProgressUpdateConsumer, Empty>(consumerPlacement, internalComputation, Stage.OperatorType.Default, (i, v) => new Runtime.Progress.ProgressUpdateConsumer(i, v, this.aggregator), "Consumer");
            consumer.SetCheckpointType(CheckpointType.None);
            var consumerRecvPort = consumer.NewUnconnectedInput(vertex => vertex.Input, null);
            consumer.Materialize();
            this.consumer = consumer.GetVertex(processid);

            // connect centralizer to consumers with special progress channel
            var progressChannel = new ProgressChannel(centralizer.Placement.Count, this.consumer, centralizerSendPort.TypedStageOutput, consumerRecvPort, internalComputation.Controller, new Edge<Empty, Update, Empty>(internalComputation.AllocateNewGraphIdentifier()));
            centralizerSendPort.TypedStageOutput.AttachBundleToSender(progressChannel);

            // connect aggregators to centralizer with special centralized progress channel
            var centralizerChannel = new CentralizedProgressChannel(centralizer, stream.TypedStageOutput, centralizerRecvPort, internalComputation.Controller, new Edge<Empty, Update, Empty>(internalComputation.AllocateNewGraphIdentifier()));
            stream.TypedStageOutput.AttachBundleToSender(centralizerChannel);

            Logging.Progress("Centralized progress tracker initialization completed");
        }


        public void Complain(TextWriter writer)
        {
            lock (this.consumer.PCS)
            {
                foreach (var count in this.consumer.PCS.Counts.OrderBy(c => c.Key.Location))
                    writer.WriteLine("\tcounts[{0}]:\t{1}", count.Key, count.Value);
                var frontier = this.consumer.PCS.Frontier;
                for (int i = 0; i < frontier.Length; i++)
                    writer.WriteLine("\tfrontier[{0}]:\t{1}\t{2}", i, frontier[i], this.consumer.PCS.Counts[frontier[i]]);
            }

            if (this.centralizer != null)
            {
                lock (this.centralizer.PCS)
                {
                    foreach (var count in this.centralizer.PCS.Counts.OrderBy(c => c.Key.Location))
                    {
                        if (count.Key.Location == 34)
                        {
//                            System.Threading.Thread.Sleep(System.Threading.Timeout.Infinite);
                        }
                        writer.WriteLine("\tcentralized counts[{0}]:\t{1}", count.Key, count.Value);
                    }
                    var frontier = this.centralizer.PCS.Frontier;
                    for (int i = 0; i < frontier.Length; i++)
                        writer.WriteLine("\tcentralized frontier[{0}]:\t{1}\t{2}", i, frontier[i], this.centralizer.PCS.Counts[frontier[i]]);
                }
            }
        }
    }
}
