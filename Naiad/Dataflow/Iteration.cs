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
using Microsoft.Research.Naiad.Dataflow.StandardVertices;

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
            var stage = new Stage<IngressVertex<R, T>, IterationIn<T>>(new TimeContext<IterationIn<T>>(internalContext), Stage.OperatorType.IterationIngress, (i, v) => new IngressVertex<R, T>(i, v, initialIteration), "FixedPoint.Ingress");

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

    /// <summary>
    /// Represents a feedback edge in a Naiad computation
    /// </summary>
    /// <typeparam name="TRecord">record type</typeparam>
    /// <typeparam name="TTime">time type</typeparam>
    public class Feedback<TRecord, TTime>
        where TTime : Time<TTime>
    {
        /// <summary>
        /// Output of the feedback edge
        /// </summary>
        public Stream<TRecord, IterationIn<TTime>> Output { get { return this.output; } }

        /// <summary>
        /// Input to the feedback edge
        /// </summary>
        public Stream<TRecord, IterationIn<TTime>> Input { set { this.AttachInput(value); } }

        private StageInput<TRecord, IterationIn<TTime>> input;
        private Stream<TRecord, IterationIn<TTime>> output;

        private readonly Expression<Func<TRecord, int>> PartitionedBy;
        private readonly int MaxIterations;

        private readonly Stage<AdvanceVertex<TRecord, TTime>, IterationIn<TTime>> stage;

        private void AttachInput(Stream<TRecord, IterationIn<TTime>> stream)
        {
            if (this.PartitionedBy != null)
            {
                var compiled = this.PartitionedBy.Compile();
                Action<TRecord[], int[], int> vectoredPartitioning = (data, dsts, len) => { for (int i = 0; i < len; i++) dsts[i] = compiled(data[i]); };

                stage.InternalComputation.Connect(stream.StageOutput, this.input, vectoredPartitioning, Channel.Flags.None);
            }
            else
                stage.InternalComputation.Connect(stream.StageOutput, this.input, null, Channel.Flags.None);
        }

        internal Feedback(ITimeContext<IterationIn<TTime>> context,
            Expression<Func<TRecord, int>> partitionedBy, int maxIterations)
        {
            this.stage = new Stage<AdvanceVertex<TRecord, TTime>, IterationIn<TTime>>(new TimeContext<IterationIn<TTime>>(context), Stage.OperatorType.IterationAdvance, (i, v) => new AdvanceVertex<TRecord, TTime>(i, v, maxIterations),  "Iterate.Advance");

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
            if (message.time.iteration < this.MaxIterations)
            {
                var output = this.Output.GetBufferForTime(new IterationIn<T>(message.time.outerTime, message.time.iteration + 1));
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

        public void OnReceive(Message<R, IterationIn<T>> message)
        {
            if (message.time.iteration >= releaseAfter)
            {
                var output = this.outputs.GetBufferForTime(message.time.outerTime);
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
            var stage = new Stage<EgressVertex<R, T>, T>(new TimeContext<T>(externalContext), Stage.OperatorType.IterationEgress, (i, v) => new EgressVertex<R, T>(i, v, iterationNumber), "FixedPoint.Egress");

            stage.NewSurprisingTimeTypeInput(input, vertex => new ActionReceiver<R, IterationIn<T>>(vertex, m => vertex.OnReceive(m)), input.PartitionedBy);

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
        public void OnReceive(Message<string, IterationIn<T>> message, ReturnAddress sender)
        {
            var stripped = new Pair<string, T>[message.length];

            for (int i = 0; i < message.length; i++)
            {
                stripped[i].First = message.payload[i];
                stripped[i].Second = message.time.outerTime;
            }

            Context.Reporting.ForwardLog(stripped);
        }

        public void ForwardIntAggregate(Message<Pair<string, ReportingRecord<Int64>>, IterationIn<T>> message, ReturnAddress sender)
        {
            for (int i = 0; i < message.length; ++i)
            {
                ReportingRecord<Int64> r = message.payload[i].Second;
                Context.Reporting.LogAggregate(message.payload[i].First, r.type, r.payload, r.count, message.time.outerTime);
            }
        }

        public void ForwardDoubleAggregate(Message<Pair<string, ReportingRecord<double>>, IterationIn<T>> message, ReturnAddress sender)
        {
            for (int i = 0; i < message.length; ++i)
            {
                ReportingRecord<double> r = message.payload[i].Second;
                Context.Reporting.LogAggregate(message.payload[i].First, r.type, r.payload, r.count, message.time.outerTime);
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

        public readonly List<StageInput<string, IterationIn<T>>> receivers;
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
            receivers.Add(stage.NewSurprisingTimeTypeInput(sender, vertex => new ActionReceiver<string, IterationIn<T>>(vertex, (m,p) => vertex.OnReceive(m, p)), null));
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
            receivers = new List<StageInput<string, IterationIn<T>>>();
            intAggregator = null;
            doubleAggregator = null;

            this.stage = new Stage<ReportingEgressVertex<T>, T>(new TimeContext<T>(externalContext), Stage.OperatorType.IterationEgress, (i, v) => new ReportingEgressVertex<T>(i, v), "FixedPoint.ReportingEgress");
        }
    }

    /// <summary>
    /// Represents a Naiad loop context
    /// </summary>
    /// <typeparam name="TTime">time type</typeparam>
    public class LoopContext<TTime>
        where TTime : Time<TTime>
    {
        private readonly ITimeContext<TTime> externalContext;
        private readonly ITimeContext<IterationIn<TTime>> internalContext;

        private bool inputsSealed;

        /// <summary>
        /// Introduces a stream into the loop context from outside
        /// </summary>
        /// <typeparam name="TRecord">record type</typeparam>
        /// <param name="stream">stream</param>
        /// <returns>the same stream with an addition time coordinate</returns>
        public Stream<TRecord, IterationIn<TTime>> EnterLoop<TRecord>(Stream<TRecord, TTime> stream)
        {
            if (this.inputsSealed)
                throw new Exception("EnterLoop is not valid following the use of ExitLoop");

            return IngressVertex<TRecord, TTime>.NewStage(stream, internalContext);
        }

        /// <summary>
        /// Introduces a stream into the loop context from outside
        /// </summary>
        /// <typeparam name="TRecord">record type</typeparam>
        /// <param name="stream">stream</param>
        /// <param name="initialIteration">initial iteration selector</param>
        /// <returns>the same stream with an addition time coordinate</returns>
        public Stream<TRecord, IterationIn<TTime>> EnterLoop<TRecord>(Stream<TRecord, TTime> stream, Func<TRecord, int> initialIteration)
        {
            if (this.inputsSealed)
                throw new Exception("EnterLoop is not valid following the use of ExitLoop");

            return IngressVertex<TRecord, TTime>.NewStage(stream, internalContext, initialIteration);
        }

        /// <summary>
        /// Extracts a stream from a loop context
        /// </summary>
        /// <typeparam name="TRecord">record type</typeparam>
        /// <param name="stream">the stream</param>
        /// <param name="iterationNumber">the iteration to extract</param>
        /// <returns>A stream containing records in the corresponding iteration</returns>
        public Stream<TRecord, TTime> ExitLoop<TRecord>(Stream<TRecord, IterationIn<TTime>> stream, int iterationNumber)
        {
            this.inputsSealed = true;

            return EgressVertex<TRecord, TTime>.NewStage(stream, externalContext, iterationNumber);
        }

        /// <summary>
        /// Extracts a stream from a loop context
        /// </summary>
        /// <typeparam name="TRecord">record type</typeparam>
        /// <param name="stream">the stream</param>
        /// <returns>A stream containing all records</returns>
        public Stream<TRecord, TTime> ExitLoop<TRecord>(Stream<TRecord, IterationIn<TTime>> stream)
        {
            this.inputsSealed = true;

            return this.ExitLoop(stream, 0);
        }

        /// <summary>
        /// Constructs a new feedback edge
        /// </summary>
        /// <typeparam name="TRecord">record type</typeparam>
        /// <returns>A new feedback edge</returns>
        public Feedback<TRecord, TTime> Delay<TRecord>()
        {
            return Delay<TRecord>(null, Int32.MaxValue);
        }

        /// <summary>
        /// Constructs a new feedback edge with a maximum number of iterations
        /// </summary>
        /// <typeparam name="TRecord">record type</typeparam>
        /// <param name="maxIters">maximum number of iterations</param>
        /// <returns>A new feedback edge</returns>
        public Feedback<TRecord, TTime> Delay<TRecord>(int maxIters)
        {
            return Delay<TRecord>(null, maxIters);
        }

        /// <summary>
        /// Constructs a new feedback edge with an enforced partitioning
        /// </summary>
        /// <typeparam name="TRecord">record type</typeparam>
        /// <param name="partitionedBy">partitioning function</param>
        /// <returns>A new feedback edge</returns>
        public Feedback<TRecord, TTime> Delay<TRecord>(Expression<Func<TRecord, int>> partitionedBy)
        {
            return new Feedback<TRecord, TTime>(internalContext, partitionedBy, Int32.MaxValue);
        }

        /// <summary>
        /// Constructs a new feedback edge with an enforced partitioning and a maximum number of iterations
        /// </summary>
        /// <typeparam name="TRecord">record type</typeparam>
        /// <param name="partitionedBy">partitioning function</param>
        /// <param name="maxIters">maximum number of iterations</param>
        /// <returns>A new feedback edge</returns>
        public Feedback<TRecord, TTime> Delay<TRecord>(Expression<Func<TRecord, int>> partitionedBy, int maxIters)
        {
            return new Feedback<TRecord, TTime>(internalContext, partitionedBy, maxIters);
        }

        /// <summary>
        /// Constructs a new LoopContext from a containing TimeContext
        /// </summary>
        /// <param name="outerContext">outer time context</param>
        /// <param name="name">a descriptive name</param>
        public LoopContext(TimeContext<TTime> outerContext, string name)
        {
            externalContext = outerContext.Context;
            if (this.externalContext.HasReporting && this.externalContext.Manager.RootStatistics.HasInline)
            {
                internalContext = externalContext.Manager.MakeContextForScope<IterationIn<TTime>>(
                    externalContext.Scope + "." + name, new ReportingEgressStage<TTime>(externalContext));
            }
            else
            {
                internalContext = externalContext.Manager.MakeContextForScope<IterationIn<TTime>>(
                    externalContext.Scope + "." + name, null);
            }
        }
    }
}

