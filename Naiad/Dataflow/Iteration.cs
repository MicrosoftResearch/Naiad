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

﻿using System;
using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;
using System.Text;
using Naiad;
using Naiad.Dataflow.Channels;
using Naiad.Runtime.Controlling;
using Naiad.Dataflow;
using Naiad.DataStructures;
using Naiad.Dataflow.Reporting;
using Naiad.Frameworks;

namespace Naiad.Dataflow.Iteration
{
    internal class IngressShard<R, T> : Vertex<IterationIn<T>>
        where T : Time<T>
    {
        private readonly VertexOutputBuffer<R, IterationIn<T>> outputs;

        private readonly Func<R, int> InitialIteration;

        public void MessageReceived(Message<Pair<R, T>> message)
        {
            for (int i = 0; i < message.length; i++)
            {
                this.outputs.Buffer.payload[this.outputs.Buffer.length++] = new Pair<R, IterationIn<T>>(message.payload[i].v1, new IterationIn<T>(message.payload[i].v2, InitialIteration(message.payload[i].v1)));
                if (this.outputs.Buffer.length == this.outputs.Buffer.payload.Length)
                    this.outputs.SendBuffer();
            }
        }

        public override string ToString()
        {
            return "FixedPoint.Ingress";
        }

        internal static Stream<R, IterationIn<T>> NewStage(Stream<R, T> input, ITimeContext<IterationIn<T>> internalContext)
        {
            return NewStage(input, internalContext, x => 0);
        }


        internal static Stream<R, IterationIn<T>> NewStage(Stream<R, T> input, ITimeContext<IterationIn<T>> internalContext, Func<R, int> initialIteration)
        {
            var stage = new Stage<IngressShard<R, T>, IterationIn<T>>(new OpaqueTimeContext<IterationIn<T>>(internalContext), Stage.OperatorType.IterationIngress, (i, v) => new IngressShard<R, T>(i, v, initialIteration), "FixedPoint.Ingress");

            stage.NewSurprisingTimeTypeInput(input, shard => new ActionReceiver<R, T>(shard, m => shard.MessageReceived(m)), input.PartitionedBy);

            return stage.NewOutput(shard => shard.outputs, input.PartitionedBy);
        }

        internal IngressShard(int index, Stage<IterationIn<T>> stage, Func<R, int> initialIteration)
            : base(index, stage)
        {
            outputs = new VertexOutputBuffer<R, IterationIn<T>>(this);
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

        private readonly Stage<AdvanceShard<R, T>, IterationIn<T>> stage;

        private void AttachInput(Stream<R, IterationIn<T>> stream)
        {
            stage.InternalGraphManager.Connect(stream.StageOutput, this.input, this.PartitionedBy, Channel.Flags.None);
        }

        internal Feedback(ITimeContext<IterationIn<T>> context,
            Expression<Func<R, int>> partitionedBy, int maxIterations)
        {
            this.stage = new Stage<AdvanceShard<R, T>, IterationIn<T>>(new OpaqueTimeContext<IterationIn<T>>(context), Stage.OperatorType.IterationAdvance, (i, v) => new AdvanceShard<R, T>(i, v, maxIterations),  "Iterate.Advance");

            this.input = this.stage.NewUnconnectedInput((message, shard) => shard.MessageReceived(message), partitionedBy);
            this.output = this.stage.NewOutput(shard => shard.ShardOutput, partitionedBy);
            
            this.PartitionedBy = partitionedBy;
            this.MaxIterations = maxIterations;
        }
    }

    internal class AdvanceShard<R, T> : UnaryVertex<R, R, IterationIn<T>>
        where T : Time<T>
    {
        //internal ShardInput<R, IterationIn<T>> ShardInput { get { return this.Input; } }
        internal VertexOutput<R, IterationIn<T>> ShardOutput { get { return this.Output; } }

        private readonly int MaxIterations;

        public override void MessageReceived(Message<Pair<R, IterationIn<T>>> message)
        {
            for (int i = 0; i < message.length; i++)
            {
                var record = message.payload[i];
                if (record.v2.t < this.MaxIterations)
                {
                    this.Output.Buffer.payload[this.Output.Buffer.length++] = new Pair<R, IterationIn<T>>(record.v1, new IterationIn<T>(record.v2.s, record.v2.t + 1));
                    if (this.Output.Buffer.length == this.Output.Buffer.payload.Length)
                        this.Output.SendBuffer();
                }
            }
        }

        public override void OnDone(IterationIn<T> time)
        {
            // nothing to do here
        }

        public override string ToString()
        {
            return "FixedPoint.Advance";
        }

        public AdvanceShard(int index, Stage<IterationIn<T>> stage, int maxIterations)
            : base(index, stage)
        {
            this.MaxIterations = maxIterations;
        }
    }

    internal class EgressShard<R, T> : Vertex<T>
        where T : Time<T>
    {
        private readonly VertexOutputBuffer<R, T> outputs;
        private readonly int releaseAfter;

        public void MessageReceived(Message<Pair<R, IterationIn<T>>> message)
        {
            for (int i = 0; i < message.length; i++)
            {
                var record = message.payload[i];
                if (record.v2.t >= releaseAfter)
                {
                    this.outputs.Buffer.payload[this.outputs.Buffer.length++] = new Pair<R, T>(record.v1, record.v2.s);
                    if (this.outputs.Buffer.length == this.outputs.Buffer.payload.Length)
                        this.outputs.SendBuffer();
                }
            }    
        }

        public override string ToString()
        {
            return "FixedPoint.Egress";
        }

        internal static Stream<R, T> NewStage(Stream<R, IterationIn<T>> input, ITimeContext<T> externalContext, int iterationNumber)
        {
            var stage = new Stage<EgressShard<R, T>, T>(new OpaqueTimeContext<T>(externalContext), Stage.OperatorType.IterationEgress, (i, v) => new EgressShard<R, T>(i, v, iterationNumber), "FixedPoint.Egress");

            stage.NewSurprisingTimeTypeInput(input, shard => new ActionReceiver<R, IterationIn<T>>(shard, m => shard.MessageReceived(m)), input.PartitionedBy);

            return stage.NewOutput<R>(shard => shard.outputs, input.PartitionedBy);
        }

        internal EgressShard(int index, Stage<T> stage, int iterationNumber)
            : base(index, stage)
        {
            outputs = new VertexOutputBuffer<R, T>(this);
            this.releaseAfter = iterationNumber;
        }
    }


    internal class ReportingEgressShard<T> : Vertex<T>
        where T : Time<T>
    {
        public void RecordReceived(Pair<string, IterationIn<T>> record)
        {
            Context.Reporting.ForwardLog(new [] { new Pair<string, T>(record.v1, record.v2.s) });
        }

        public void MessageReceived(Message<Pair<string, IterationIn<T>>> message, RemotePostbox sender)
        {
            var stripped = new Pair<string, T>[message.length];

            for (int i = 0; i < message.length; i++)
            {
                stripped[i].v1 = message.payload[i].v1;
                stripped[i].v2 = message.payload[i].v2.s;
            }

            Context.Reporting.ForwardLog(stripped);
        }

        public void ForwardIntAggregate(Message<Pair<Pair<string, ReportingRecord<Int64>>, IterationIn<T>>> message, RemotePostbox sender)
        {
            for (int i = 0; i < message.length; ++i)
            {
                ReportingRecord<Int64> r = message.payload[i].v1.v2;
                Context.Reporting.LogAggregate(message.payload[i].v1.v1, r.type, r.payload, r.count, message.payload[i].v2.s);
            }
        }

        public void ForwardDoubleAggregate(Message<Pair<Pair<string, ReportingRecord<double>>, IterationIn<T>>> message, RemotePostbox sender)
        {
            for (int i = 0; i < message.length; ++i)
            {
                ReportingRecord<double> r = message.payload[i].v1.v2;
                Context.Reporting.LogAggregate(message.payload[i].v1.v1, r.type, r.payload, r.count, message.payload[i].v2.s);
            }
        }

        public override string ToString()
        {
            return "FixedPoint.ReportingEgress";
        }

        public ReportingEgressShard(int index, Stage<T> parent)
            : base(index, parent)
        {
        }
    }

    internal class ReportingEgressStage<T> : IReportingConnector<IterationIn<T>>
        where T : Time<T>
    {
        private readonly Stage<ReportingEgressShard<T>, T> stage;

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
            receivers.Add(stage.NewSurprisingTimeTypeInput(sender, shard => new ActionReceiver<string, IterationIn<T>>(shard, (m,p) => shard.MessageReceived(m, p)), null));
        }

        public void ConnectIntAggregator(Stream<Pair<string, ReportingRecord<Int64>>, IterationIn<T>> sender)
        {
            System.Diagnostics.Debug.Assert(intAggregator == null);
            intAggregator = stage.NewSurprisingTimeTypeInput(sender, shard => new ActionReceiver<Pair<string, ReportingRecord<Int64>>, IterationIn<T>>(shard, (m,p) => shard.ForwardIntAggregate(m, p)), null);
        }

        public void ConnectDoubleAggregator(Stream<Pair<string, ReportingRecord<double>>, IterationIn<T>> sender)
        {
            System.Diagnostics.Debug.Assert(doubleAggregator == null);
            doubleAggregator = stage.NewSurprisingTimeTypeInput(sender, shard => new ActionReceiver<Pair<string, ReportingRecord<double>>, IterationIn<T>>(shard, (m,p) => shard.ForwardDoubleAggregate(m, p)), null);
        }

        internal ReportingEgressStage(ITimeContext<T> externalContext)
        {
            receivers = new NaiadList<StageInput<string, IterationIn<T>>>();
            intAggregator = null;
            doubleAggregator = null;

            this.stage = new Stage<ReportingEgressShard<T>, T>(new OpaqueTimeContext<T>(externalContext), Stage.OperatorType.IterationEgress, (i, v) => new ReportingEgressShard<T>(i, v), "FixedPoint.ReportingEgress");
        }
    }

    public class LoopContext<T>
        where T : Time<T>
    {
        private readonly ITimeContext<T> externalContext;
        private readonly ITimeContext<IterationIn<T>> internalContext;

        public Stream<R, IterationIn<T>> EnterLoop<R>(Stream<R, T> stream)
        {
            return IngressShard<R, T>.NewStage(stream, internalContext);
        }

        public Stream<R, IterationIn<T>> EnterLoop<R>(Stream<R, T> stream, Func<R, int> initialIteration)
        {
            return IngressShard<R, T>.NewStage(stream, internalContext, initialIteration);
        }

        public Stream<R, T> ExitLoop<R>(Stream<R, IterationIn<T>> stream, int iterationNumber)
        {
            return EgressShard<R, T>.NewStage(stream, externalContext, iterationNumber);
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

