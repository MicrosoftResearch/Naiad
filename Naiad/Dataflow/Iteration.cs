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
using System.Linq.Expressions;
using System.Text;
using Microsoft.Research.Naiad;
using Microsoft.Research.Naiad.Dataflow.Channels;
using Microsoft.Research.Naiad.Runtime.Controlling;
using Microsoft.Research.Naiad.Dataflow;
using Microsoft.Research.Naiad.DataStructures;
using Microsoft.Research.Naiad.Dataflow.Reporting;
using Microsoft.Research.Naiad.Frameworks;

namespace Microsoft.Research.Naiad.Dataflow.Iteration
{
    internal class IngressVertex<R, T> : Vertex<IterationIn<T>>
        where T : Time<T>
    {
        private readonly VertexOutputBuffer<R, IterationIn<T>> Output;

        private readonly Func<R, int> InitialIteration;

        public void MessageReceived(Message<R, T> message)
        {
            if (this.InitialIteration == null)
            {
                var output = this.Output.GetBufferForTime(new IterationIn<T>(message.time, 0));
                for (int i = 0; i < message.length; i++)
                    output.Send(message.payload[i]);
            }
            else
            {
                for (int i = 0; i < message.length; i++)
                    this.Output.GetBufferForTime(new IterationIn<T>(message.time, InitialIteration(message.payload[i]))).Send(message.payload[i]);
            }
        }

        public override string ToString()
        {
            return "Ingress";
        }

        internal static Stream<R, IterationIn<T>> NewStage(Stream<R, T> input, ITimeContext<IterationIn<T>> internalContext)
        {
            return NewStage(input, internalContext, null);
        }


        internal static Stream<R, IterationIn<T>> NewStage(Stream<R, T> input, ITimeContext<IterationIn<T>> internalContext, Func<R, int> initialIteration)
        {
            var stage = new Stage<IngressVertex<R, T>, IterationIn<T>>(new OpaqueTimeContext<IterationIn<T>>(internalContext), Stage.OperatorType.IterationIngress, (i, v) => new IngressVertex<R, T>(i, v, initialIteration), "FixedPoint.Ingress");

            stage.NewSurprisingTimeTypeInput(input, vertex => new ActionReceiver<R, T>(vertex, m => vertex.MessageReceived(m)), input.PartitionedBy);

            return stage.NewOutput(vertex => vertex.Output, input.PartitionedBy);
        }

        internal IngressVertex(int index, Stage<IterationIn<T>> stage, Func<R, int> initialIteration)
            : base(index, stage)
        {
            Output = new VertexOutputBuffer<R, IterationIn<T>>(this);
            this.InitialIteration = initialIteration;
        }
    }

    public class Feedback<R, T>
        where T : Time<T>
    {
        public Stream<R, IterationIn<T>> Output { get { return this.output; } }
        public Stream<R, IterationIn<T>> Input { set { this.AttachInput(value); } }

        private StageInput<R, IterationIn<T>> input;
        private Stream<R, IterationIn<T>> output;

        private readonly Expression<Func<R, int>> PartitionedBy;
        private readonly int MaxIterations;

        private readonly Stage<AdvanceVertex<R, T>, IterationIn<T>> stage;

        private void AttachInput(Stream<R, IterationIn<T>> stream)
        {
            stage.InternalGraphManager.Connect(stream.StageOutput, this.input, this.PartitionedBy, Channel.Flags.None);
        }

        internal Feedback(ITimeContext<IterationIn<T>> context,
            Expression<Func<R, int>> partitionedBy, int maxIterations)
        {
            this.stage = new Stage<AdvanceVertex<R, T>, IterationIn<T>>(new OpaqueTimeContext<IterationIn<T>>(context), Stage.OperatorType.IterationAdvance, (i, v) => new AdvanceVertex<R, T>(i, v, maxIterations),  "Iterate.Advance");

            this.input = this.stage.NewUnconnectedInput((message, vertex) => vertex.OnReceive(message), partitionedBy);
            this.output = this.stage.NewOutput(vertex => vertex.VertexOutput, partitionedBy);
            
            this.PartitionedBy = partitionedBy;
            this.MaxIterations = maxIterations;
        }
    }

    internal class AdvanceVertex<R, T> : UnaryVertex<R, R, IterationIn<T>>
        where T : Time<T>
    {
        internal VertexOutput<R, IterationIn<T>> VertexOutput { get { return this.Output; } }

        private readonly int MaxIterations;

        public override void OnReceive(Message<R, IterationIn<T>> message)
        {
            if (message.time.t < this.MaxIterations)
            {
                var output = this.Output.GetBufferForTime(new IterationIn<T>(message.time.s, message.time.t + 1));
                for (int i = 0; i < message.length; i++)
                    output.Send(message.payload[i]);
            }
        }

        public override void OnNotify(IterationIn<T> time)
        {
            // nothing to do here
        }

        public override string ToString()
        {
            return "Advance";
        }

        public AdvanceVertex(int index, Stage<IterationIn<T>> stage, int maxIterations)
            : base(index, stage)
        {
            this.MaxIterations = maxIterations;
        }
    }

    internal class EgressVertex<R, T> : Vertex<T>
        where T : Time<T>
    {
        private readonly VertexOutputBuffer<R, T> outputs;
        private readonly int releaseAfter;

        public void MessageReceived(Message<R, IterationIn<T>> message)
        {
            if (message.time.t >= releaseAfter)
            {
                var output = this.outputs.GetBufferForTime(message.time.s);
                for (int i = 0; i < message.length; i++)
                    output.Send(message.payload[i]);
            }
        }

        public override string ToString()
        {
            return "Egress";
        }

        internal static Stream<R, T> NewStage(Stream<R, IterationIn<T>> input, ITimeContext<T> externalContext, int iterationNumber)
        {
            var stage = new Stage<EgressVertex<R, T>, T>(new OpaqueTimeContext<T>(externalContext), Stage.OperatorType.IterationEgress, (i, v) => new EgressVertex<R, T>(i, v, iterationNumber), "FixedPoint.Egress");

            stage.NewSurprisingTimeTypeInput(input, vertex => new ActionReceiver<R, IterationIn<T>>(vertex, m => vertex.MessageReceived(m)), input.PartitionedBy);

            return stage.NewOutput<R>(vertex => vertex.outputs, input.PartitionedBy);
        }

        internal EgressVertex(int index, Stage<T> stage, int iterationNumber)
            : base(index, stage)
        {
            outputs = new VertexOutputBuffer<R, T>(this);
            this.releaseAfter = iterationNumber;
        }
    }


    internal class ReportingEgressVertex<T> : Vertex<T>
        where T : Time<T>
    {
        public void RecordReceived(Pair<string, IterationIn<T>> record)
        {
            Context.Reporting.ForwardLog(new [] { new Pair<string, T>(record.v1, record.v2.s) });
        }

        public void MessageReceived(Message<string, IterationIn<T>> message, RemotePostbox sender)
        {
            var stripped = new Pair<string, T>[message.length];

            for (int i = 0; i < message.length; i++)
            {
                stripped[i].v1 = message.payload[i];
                stripped[i].v2 = message.time.s;
            }

            Context.Reporting.ForwardLog(stripped);
        }

        public void ForwardIntAggregate(Message<Pair<string, ReportingRecord<Int64>>, IterationIn<T>> message, RemotePostbox sender)
        {
            for (int i = 0; i < message.length; ++i)
            {
                ReportingRecord<Int64> r = message.payload[i].v2;
                Context.Reporting.LogAggregate(message.payload[i].v1, r.type, r.payload, r.count, message.time.s);
            }
        }

        public void ForwardDoubleAggregate(Message<Pair<string, ReportingRecord<double>>, IterationIn<T>> message, RemotePostbox sender)
        {
            for (int i = 0; i < message.length; ++i)
            {
                ReportingRecord<double> r = message.payload[i].v2;
                Context.Reporting.LogAggregate(message.payload[i].v1, r.type, r.payload, r.count, message.time.s);
            }
        }

        public override string ToString()
        {
            return "ReportingEgress";
        }

        public ReportingEgressVertex(int index, Stage<T> parent)
            : base(index, parent)
        {
        }
    }

    internal class ReportingEgressStage<T> : IReportingConnector<IterationIn<T>>
        where T : Time<T>
    {
        private readonly Stage<ReportingEgressVertex<T>, T> stage;

        public readonly NaiadList<StageInput<string, IterationIn<T>>> receivers;
        internal IEnumerable<StageInput<string, IterationIn<T>>> Receivers
        {
            get { return receivers; }
        }

        private StageInput<Pair<string, ReportingRecord<Int64>>, IterationIn<T>> intAggregator;
        internal StageInput<Pair<string, ReportingRecord<Int64>>, IterationIn<T>> IntAggregator
        {
            get { return intAggregator; }
        }

        private StageInput<Pair<string, ReportingRecord<double>>, IterationIn<T>> doubleAggregator;
        internal StageInput<Pair<string, ReportingRecord<double>>, IterationIn<T>> DoubleAggregator
        {
            get { return doubleAggregator; }
        }

        public void ConnectInline(Stream<string, IterationIn<T>> sender)
        {
            receivers.Add(stage.NewSurprisingTimeTypeInput(sender, vertex => new ActionReceiver<string, IterationIn<T>>(vertex, (m,p) => vertex.MessageReceived(m, p)), null));
        }

        public void ConnectIntAggregator(Stream<Pair<string, ReportingRecord<Int64>>, IterationIn<T>> sender)
        {
            System.Diagnostics.Debug.Assert(intAggregator == null);
            intAggregator = stage.NewSurprisingTimeTypeInput(sender, vertex => new ActionReceiver<Pair<string, ReportingRecord<Int64>>, IterationIn<T>>(vertex, (m,p) => vertex.ForwardIntAggregate(m, p)), null);
        }

        public void ConnectDoubleAggregator(Stream<Pair<string, ReportingRecord<double>>, IterationIn<T>> sender)
        {
            System.Diagnostics.Debug.Assert(doubleAggregator == null);
            doubleAggregator = stage.NewSurprisingTimeTypeInput(sender, vertex => new ActionReceiver<Pair<string, ReportingRecord<double>>, IterationIn<T>>(vertex, (m,p) => vertex.ForwardDoubleAggregate(m, p)), null);
        }

        internal ReportingEgressStage(ITimeContext<T> externalContext)
        {
            receivers = new NaiadList<StageInput<string, IterationIn<T>>>();
            intAggregator = null;
            doubleAggregator = null;

            this.stage = new Stage<ReportingEgressVertex<T>, T>(new OpaqueTimeContext<T>(externalContext), Stage.OperatorType.IterationEgress, (i, v) => new ReportingEgressVertex<T>(i, v), "FixedPoint.ReportingEgress");
        }
    }

    public class LoopContext<T>
        where T : Time<T>
    {
        private readonly ITimeContext<T> externalContext;
        private readonly ITimeContext<IterationIn<T>> internalContext;

        public Stream<R, IterationIn<T>> EnterLoop<R>(Stream<R, T> stream)
        {
            return IngressVertex<R, T>.NewStage(stream, internalContext);
        }

        public Stream<R, IterationIn<T>> EnterLoop<R>(Stream<R, T> stream, Func<R, int> initialIteration)
        {
            return IngressVertex<R, T>.NewStage(stream, internalContext, initialIteration);
        }

        public Stream<R, T> ExitLoop<R>(Stream<R, IterationIn<T>> stream, int iterationNumber)
        {
            return EgressVertex<R, T>.NewStage(stream, externalContext, iterationNumber);
        }

        public Stream<R, T> ExitLoop<R>(Stream<R, IterationIn<T>> stream)
        {
            return this.ExitLoop(stream, 0);
        }

        public Feedback<R, T> Delay<R>()
        {
            return Delay<R>(null, Int32.MaxValue);
        }

        public Feedback<R, T> Delay<R>(int maxIters)
        {
            return Delay<R>(null, maxIters);
        }

        public Feedback<R, T> Delay<R>(Expression<Func<R, int>> partitionedBy)
        {
            return new Feedback<R, T>(internalContext, partitionedBy, Int32.MaxValue);
        }

        public Feedback<R, T> Delay<R>(Expression<Func<R, int>> partitionedBy, int maxIters)
        {
            return new Feedback<R, T>(internalContext, partitionedBy, maxIters);
        }

        public LoopContext(OpaqueTimeContext<T> s, string name)
        {
            externalContext = s.Context;
            if (this.externalContext.HasReporting && this.externalContext.Manager.RootStatistics.HasInline)
            {
                internalContext = externalContext.Manager.MakeContextForScope<IterationIn<T>>(
                    externalContext.Scope + "." + name, new ReportingEgressStage<T>(externalContext));
            }
            else
            {
                internalContext = externalContext.Manager.MakeContextForScope<IterationIn<T>>(
                    externalContext.Scope + "." + name, null);
            }
        }
    }
}

