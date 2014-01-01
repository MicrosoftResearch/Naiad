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
using System.IO;
using Naiad.Dataflow.Channels;
using Naiad.CodeGeneration;
using Naiad.Runtime.Controlling;
using Naiad.DataStructures;
using Naiad.FaultTolerance;
using Naiad.Scheduling;
//using Naiad.Frameworks.Lindi;
using Naiad.Frameworks.Reduction;
using Naiad.Frameworks;

namespace Naiad.Dataflow
{
    namespace Reporting
    {
        /// <summary>
        /// The type of aggregation that is performed on a set of integers or doubles during reporting
        /// </summary>
        public enum AggregateType
        {
            Sum, Min, Max, Average
        }

        /// <summary>
        /// Interface used by vertex code to log messages and counters that are aggregated 
        /// and written out by centralized logging code when reporting is enabled.
        /// </summary>
        public interface IReporting<T> where T : Time<T>
        {
            /// <summary>
            /// Sends a log message to be written immediately to the 'out-of-band' logging subsystem. If
            /// Configuration.DomainReporting is true, this will be written to a file called rtdomain.txt
            /// at the root shard's computer, otherwise it will be written to the console at the vertex's
            /// local computer.
            /// </summary>
            void Log(string entry);

            /// <summary>
            /// Sends a log message to be written immediately to the inline logging subsystem that uses the graph's
            /// time domain. If Configuration.InlineReporting is true, this will be written to a file called rtinline.txt
            /// at the root shard's computer, otherwise it will be written to the console at the vertex's
            /// local computer. The log message is written out in the form "<time>.entry"
            /// </summary>
            void Log(string entry, T time);

            /// <summary>
            /// Incorporates value into the logical-time-based aggregation called name. If Configuration.InlineReporting and
            /// Configuration.AggregateReporting are both true, then the final aggregate of all values with the same time will
            /// be written to rtinline.txt at the root shard's computer once all computation with that time has drained from
            /// the system. This call is identical to logging an integer aggregate with a count of 1.
            /// </summary>
            void LogAggregate(string name, Reporting.AggregateType type, Int64 value, T time);

            /// <summary>
            /// Incorporates (value,count) into the logical-time-based aggregation called name. If Configuration.InlineReporting and
            /// Configuration.AggregateReporting are both true, then the final aggregate of all values with the same time will
            /// be written to rtinline.txt at the root shard's computer once all computation with that time has drained from
            /// the system. count is used only for aggregates of type Reporting.AggregateType.Average, for which the final
            /// aggregate is Sum(values)/Sum(counts).
            /// </summary>
            void LogAggregate(string name, Reporting.AggregateType type, Int64 value, Int64 count, T time);

            /// <summary>
            /// Incorporates value into the logical-time-based aggregation called name. If Configuration.InlineReporting and
            /// Configuration.AggregateReporting are both true, then the final aggregate of all values with the same time will
            /// be written to rtinline.txt at the root shard's computer once all computation with that time has drained from
            /// the system. This call is identical to logging a double aggregate with a count of 1.0.
            /// </summary>
            void LogAggregate(string name, Reporting.AggregateType type, double value, T time);

            /// <summary>
            /// Incorporates (value,count) into the logical-time-based aggregation called name. If Configuration.InlineReporting and
            /// Configuration.AggregateReporting are both true, then the final aggregate of all values with the same time will
            /// be written to rtinline.txt at the root shard's computer once all computation with that time has drained from
            /// the system. count is used only for aggregates of type Reporting.AggregateType.Average, for which the final
            /// aggregate is Sum(values)/Sum(counts).
            /// </summary>
            void LogAggregate(string name, Reporting.AggregateType type, double value, Int64 count, T time);

            /// <summary>
            /// Writes an array of log messages. This is equivalent to calling Log(entry, time) for every element in message,
            /// but has better performance.
            /// </summary>
            void ForwardLog(Pair<string, T>[] message);
        }

        internal interface IReportingConnector<T> where T : Time<T>
        {
            void ConnectInline(Stream<string, T> sender);
            void ConnectIntAggregator(Stream<Pair<string, ReportingRecord<Int64>>, T> sender);
            void ConnectDoubleAggregator(Stream<Pair<string, ReportingRecord<double>>, T> sender);
        }

        internal interface IRootReporting : IReportingConnector<Epoch>
        {
            bool HasDomain { get; }
            bool HasInline { get; }
            bool HasAggregate { get; }
        }

        internal struct ReportingRecord<R>
        {
            public R payload;
            public Int64 count;
            public Reporting.AggregateType type;

            public ReportingRecord(R r, Int64 c, Reporting.AggregateType t)
            {
                payload = r;
                count = c;
                type = t;
            }
        }

        internal class IntReportingReducer : Naiad.Frameworks.Reduction.IReducer<ReportingRecord<Int64>, ReportingRecord<Int64>, ReportingRecord<Int64>>
        {
            ReportingRecord<Int64> value;

            public void InitialAdd(ReportingRecord<Int64> r)
            {
                value = r;
            }

            public void Add(ReportingRecord<Int64> r)
            {
                switch (r.type)
                {
                    case Reporting.AggregateType.Sum:
                    case Reporting.AggregateType.Average:
                        value.payload += r.payload;
                        value.count += r.count;
                        break;

                    case Reporting.AggregateType.Min:
                        value.payload = Math.Min(value.payload, r.payload);
                        break;

                    case Reporting.AggregateType.Max:
                        value.payload = Math.Max(value.payload, r.payload);
                        break;
                }
            }

            public void InitialCombine(ReportingRecord<Int64> r)
            {
                InitialAdd(r);
            }

            public void Combine(ReportingRecord<Int64> r)
            {
                Add(r);
            }

            public ReportingRecord<Int64> State()
            {
                return value;
            }

            public ReportingRecord<Int64> Value()
            {
                if (value.type == Reporting.AggregateType.Average)
                {
                    value.payload /= value.count;
                    value.count = 1;
                }
                return value;
            }
        }

        internal class DoubleReportingReducer : Naiad.Frameworks.Reduction.IReducer<ReportingRecord<double>, ReportingRecord<double>, ReportingRecord<double>>
        {
            ReportingRecord<double> value;

            public void InitialAdd(ReportingRecord<double> r)
            {
                value = r;
            }

            public void Add(ReportingRecord<double> r)
            {
                switch (r.type)
                {
                    case Reporting.AggregateType.Sum:
                    case Reporting.AggregateType.Average:
                        value.payload += r.payload;
                        value.count += r.count;
                        break;

                    case Reporting.AggregateType.Min:
                        value.payload = Math.Min(value.payload, r.payload);
                        break;

                    case Reporting.AggregateType.Max:
                        value.payload = Math.Max(value.payload, r.payload);
                        break;
                }
            }

            public void InitialCombine(ReportingRecord<double> r)
            {
                InitialAdd(r);
            }

            public void Combine(ReportingRecord<double> r)
            {
                Combine(r);
            }

            public ReportingRecord<double> State()
            {
                return value;
            }

            public ReportingRecord<double> Value()
            {
                if (value.type == Reporting.AggregateType.Average)
                {
                    value.payload /= value.count;
                    value.count = 1;
                }
                return value;
            }
        }

        internal class Reporting<T> : IReporting<T> where T : Time<T>
        {
            private readonly ShardContext<T> context;
            private readonly InputVertex<string> rootShard;
            private readonly Naiad.Frameworks.VertexOutputBuffer<string, T> inlineStatistics;
            private readonly Naiad.Frameworks.VertexOutputBuffer<Pair<string, ReportingRecord<double>>, T> aggregateDouble;
            private readonly Naiad.Frameworks.VertexOutputBuffer<Pair<string, ReportingRecord<Int64>>, T> aggregateInt;

            public void Log(string s)
            {
                if (rootShard == null)
                {
                    Console.WriteLine(s);
                }
                else
                {
                    rootShard.OnNext(new[] { s });
                }
            }

            public void Log(string s, T t)
            {
                string decorated = context.Shard.ToString() + "<" + t.ToString() + ">" + ":" + s;
                if (inlineStatistics == null)
                {
                    Console.WriteLine(decorated);
                }
                else
                {
                    inlineStatistics.Send(decorated, t);
                }
            }

            public void ForwardLog(Pair<string, T>[] message)
            {
                for (int i = 0; i < message.Length; i++)
                {
                    this.inlineStatistics.Buffer.payload[this.inlineStatistics.Buffer.length++] = message[i];
                    if (this.inlineStatistics.Buffer.length == this.inlineStatistics.Buffer.payload.Length)
                        this.inlineStatistics.SendBuffer();
                }
                this.inlineStatistics.Flush();
            }

            public void LogAggregate(string name, Reporting.AggregateType type, Int64 value, Int64 count, T time)
            {
                if (this.aggregateInt != null)
                {
                    this.aggregateInt.Send(new Pair<string, ReportingRecord<Int64>>(name, new ReportingRecord<Int64>(value, count, type)), time);
                }
            }

            public void LogAggregate(string name, Reporting.AggregateType type, Int64 value, T time)
            {
                this.LogAggregate(name, type, value, 1, time);
            }

            public void LogAggregate(string name, Reporting.AggregateType type, double value, Int64 count, T time)
            {
                if (this.aggregateDouble != null)
                {
                    this.aggregateDouble.Send(new Pair<string, ReportingRecord<double>>(name, new ReportingRecord<double>(value, count, type)), time);
                }
            }

            public void LogAggregate(string name, Reporting.AggregateType type, double value, T time)
            {
                this.LogAggregate(name, type, value, 1, time);
            }

            public Reporting(
                ShardContext<T> c, Stream<string, T> inlineStats,
                Stream<Pair<string, ReportingRecord<Int64>>, T> aggInt, Stream<Pair<string, ReportingRecord<double>>, T> aggDouble)
            {
                context = c;

                if (c.parent.parent.manager.Reporting.HasDomain)
                {
                    rootShard = c.parent.parent.manager.Reporting.RootDomainShard(c.Shard.VertexId);
                }
                else
                {
                    rootShard = null;
                }

                if (inlineStats == null)
                {
                    inlineStatistics = null;
                }
                else
                {
                    inlineStatistics = new Frameworks.VertexOutputBuffer<string, T>(c.Shard);
                    inlineStats.StageOutput.Register(inlineStatistics);
                }

                if (aggInt == null)
                {
                    aggregateInt = null;
                    aggregateDouble = null;
                }
                else
                {
                    aggregateInt = new Frameworks.VertexOutputBuffer<Pair<string, ReportingRecord<long>>, T>(c.Shard);
                    aggInt.StageOutput.Register(aggregateInt);
                    aggregateDouble = new Frameworks.VertexOutputBuffer<Pair<string, ReportingRecord<double>>, T>(c.Shard);
                    aggDouble.StageOutput.Register(aggregateDouble);
                }
            }
        }

        internal class RootReporting : IRootReporting
        {
            private readonly bool doingAggregate;
            private readonly RootStatisticsStage domainReporter;
            private readonly RootStatisticsStage inlineReporter;
            internal readonly InputStage<string> domainReportingIngress;

            public bool HasDomain
            {
                get { return domainReporter != null; }
            }

            public bool HasInline
            {
                get { return inlineReporter != null; }
            }

            public bool HasAggregate
            {
                get { return HasInline && doingAggregate; }
            }

            public void ConnectInline(Stream<string, Epoch> sender)
            {
                inlineReporter.ConnectInline(sender);
            }

            public void ConnectIntAggregator(Stream<Pair<string, ReportingRecord<Int64>>, Epoch> sender)
            {
                // do nothing since this is the head of the chain
            }
            public void ConnectDoubleAggregator(Stream<Pair<string, ReportingRecord<double>>, Epoch> sender)
            {
                // do nothing since this is the head of the chain
            }

            public InputVertex<string> RootDomainShard(int index)
            {
                return domainReportingIngress.GetInputShard(index);
            }

            public void ShutDown()
            {
                if (domainReporter != null)
                {
                    if (!domainReportingIngress.IsCompleted)
                    {
                        Logging.Error("Statistics shutting down before domain input is completed");
                    }

                    domainReporter.ShutDown();
                }

                if (inlineReporter != null)
                {
                    inlineReporter.ShutDown();
                }
            }

            public RootReporting(TimeContextManager manager, bool makeDomain, bool makeInline, bool doAggregate)
            {
                if (makeDomain)
                {
                    domainReportingIngress = manager.GraphManager.NewInput<string>();
                    domainReporter = new RootStatisticsStage(
                        manager.MakeRawContextForScope<Epoch>("Domain Root"), "Domain Root Statistics", "rtdomain.txt");
                    domainReporter.ConnectInline(domainReportingIngress.Stream);
                }
                else
                {
                    domainReportingIngress = null;
                    domainReporter = null;
                }

                if (makeInline)
                {
                    inlineReporter = new RootStatisticsStage(
                        manager.MakeRawContextForScope<Epoch>("Inline Root"), "Inline Root Statistics", "rtinline.txt");

                    doingAggregate = doAggregate;
                }
                else
                {
                    inlineReporter = null;
                    doingAggregate = false;
                }
            }
        }

        internal class RootStatisticsShard : Naiad.Dataflow.Vertex<Epoch>
        {
            private readonly StreamWriter output;
            private readonly System.Diagnostics.Stopwatch timer;

            public void OnRecv(Message<Pair<string, Epoch>> message)
            {
                for (int i = 0; i < message.length; i++)
                {
                    output.WriteLine("{0:D8}: {1}", timer.ElapsedMilliseconds, message.payload[i].v1);
                }
            }

            public override void OnDone(Epoch time)
            {
                // do nothing since epochs mean nothing in this context
            }

            public void FinalizeReporting()
            {
                output.Close();
            }

            public RootStatisticsShard(int index, Stage<Epoch> parent, string outputFileName)
                : base(index, parent)
            {

                output = new StreamWriter(outputFileName);
                timer = new System.Diagnostics.Stopwatch();
                timer.Start();
            }
        }

        internal class RootStatisticsStage : IReportingConnector<Epoch>
        {
            private Stage<RootStatisticsShard, Epoch> stage;

            private readonly NaiadList<StageInput<string, Epoch>> receivers;
            public IEnumerable<StageInput<string, Epoch>> Receivers
            {
                get { return receivers; }
            }

            public void ShutDown()
            {
                foreach (var s in stage.Shards)
                {
                    stage.GetShard(s.VertexId).FinalizeReporting();
                }
            }

            public void ConnectInline(Stream<string, Epoch> sender)
            {
                this.stage.NewInput(sender, (message, shard) => shard.OnRecv(message), null);
            }

            public void ConnectIntAggregator(Stream<Pair<string, ReportingRecord<Int64>>, Epoch> sender)
            {
                // do nothing since this is the head of the chain
            }
            public void ConnectDoubleAggregator(Stream<Pair<string, ReportingRecord<double>>, Epoch> sender)
            {
                // do nothing since this is the head of the chain
            }

            internal RootStatisticsStage(ITimeContext<Epoch> context, string name, string outputFile)
            {
                this.stage = new Stage<RootStatisticsShard, Epoch>(new SingleVertexPlacement(0, 0), new OpaqueTimeContext<Epoch>(context), Stage.OperatorType.Default, (i, v) => new RootStatisticsShard(i, v, outputFile), name);

                receivers = new NaiadList<StageInput<string, Epoch>>();
            }
        }

        internal class AggregateStatisticsShard<R, T> : Naiad.Dataflow.Vertex<T>
            where T : Time<T>
        {
            internal readonly Naiad.Frameworks.VertexOutputBuffer<Pair<string, ReportingRecord<R>>, T> output;

            public void OnRecv(Message<Pair<Pair<string, ReportingRecord<R>>, T>> message)
            {
                for (int i = 0; i < message.length; i++)
                {
                    string name = message.payload[i].v1.v1;
                    ReportingRecord<R> r = message.payload[i].v1.v2;
                    if (r.type == Naiad.Dataflow.Reporting.AggregateType.Average)
                    {
                        Context.Reporting.Log(name + ": " + r.payload + "," + r.count, message.payload[i].v2);
                    }
                    else
                    {
                        Context.Reporting.Log(name + ": " + r.payload, message.payload[i].v2);
                    }
                }

                output.Send(message);
            }

            public override void OnDone(T time)
            {
                // do nothing since epochs mean nothing in this context
            }

            public AggregateStatisticsShard(int index, Stage<T> parent)
                : base(index, parent)
            {
                output = new Frameworks.VertexOutputBuffer<Pair<string, ReportingRecord<R>>, T>(this);
            }
        }

        internal class AggregateStatisticsStage<R, T>
            where T : Time<T>
        {
            private readonly Stage<AggregateStatisticsShard<R, T>, T> stage;

            private readonly NaiadList<StageInput<Pair<string, ReportingRecord<R>>, T>> inputs;
            public IEnumerable<StageInput<Pair<string, ReportingRecord<R>>, T>> Inputs
            {
                get { return inputs; }
            }

            public Stream<Pair<string, ReportingRecord<R>>, T> Output;

            public void ConnectTo(Stream<Pair<string, ReportingRecord<R>>, T> i)
            {
                //inputs.Add(this.NewInput(i));
                inputs.Add(stage.NewInput(i, (message, shard) => shard.OnRecv(message), null));
            }

            internal AggregateStatisticsStage(ITimeContext<T> context, string name)
            {
                this.stage = new Stage<AggregateStatisticsShard<R, T>, T>(new OpaqueTimeContext<T>(context), (i, v) => new AggregateStatisticsShard<R, T>(i, v), name);

                Output = this.stage.NewOutputWithoutSealing(shard => shard.output, null);

                inputs = new NaiadList<StageInput<Pair<string, ReportingRecord<R>>, T>>();
            }
        }

    }

    internal interface ITimeContextManager
    {
        Runtime.InternalGraphManager GraphManager { get; }
        ITimeContext<T> MakeContextForScope<T>(string name, Reporting.IReportingConnector<T> downstreamConnector) where T : Time<T>;
        ITimeContext<T> MakeRawContextForScope<T>(string name) where T : Time<T>;
        ITimeContext<Epoch> RootContext { get; }
        Reporting.IRootReporting RootStatistics { get; }
        void ShutDown();
    }

    internal interface ITimeContext<T> where T : Time<T>
    {
        bool HasReporting { get; }
        bool HasAggregate { get; }
        IStageContext<T> MakeStageContext(string name);
        IStageContext<T> MakeStageContext(string name, Stream<string, T> inlineStatsSender);
        IStageContext<T> MakeStageContext(
            string name, Stream<string, T> inlineStatsSender,
            Stream<Pair<string, Reporting.ReportingRecord<Int64>>, T> aggInt, Stream<Pair<string, Reporting.ReportingRecord<double>>, T> aggDouble);
        string Scope { get; }
        ITimeContextManager Manager { get; }
    }

    public struct OpaqueTimeContext<T> where T : Time<T>
    {
        internal ITimeContext<T> Context;

        internal OpaqueTimeContext(ITimeContext<T> context) { this.Context = context; }
    }

    internal interface IStageContext<T> where T : Time<T>
    {
        IShardContext<T> MakeShardContext(Vertex shard);
        ITimeContext<T> Parent { get; }
    }

    internal interface IShardContext<T> where T : Time<T>
    {
        IStageContext<T> Parent { get; }
        Reporting.IReporting<T> Reporting { get; }
    }

    internal class TimeContextManager : ITimeContextManager
    {
        private readonly Runtime.InternalGraphManager graphManager;
        public Runtime.InternalGraphManager GraphManager { get { return this.graphManager; } }

        private ITimeContext<Epoch> rootContext;
        public ITimeContext<Epoch> RootContext { get { return rootContext; } }

        private Reporting.RootReporting reporting;
        public Reporting.IRootReporting RootStatistics { get { return reporting; } }
        internal Reporting.RootReporting Reporting { get { return reporting; } }

        internal void InitializeReporting(bool makeDomain, bool makeInline, bool doAggregate)
        {
            this.reporting = new Reporting.RootReporting(this, makeDomain, makeInline, doAggregate);
            this.rootContext = MakeContextForScope<Epoch>("Root", this.reporting);
        }

        public void ShutDown()
        {
            if (this.reporting != null)
            {
                this.reporting.ShutDown();
            }
        }

        public ITimeContext<T> MakeRawContextForScope<T>(string name) where T : Time<T>
        {
            return new TimeContext<T>(this, name, null, false);
        }

        public ITimeContext<T> MakeContextForScope<T>(string name, Reporting.IReportingConnector<T> downstreamConnector) where T : Time<T>
        {
            bool hasAggregate = this.reporting != null && this.reporting.HasAggregate;
            return new TimeContext<T>(this, name, downstreamConnector, hasAggregate);
        }

        internal TimeContextManager(Runtime.InternalGraphManager g)
        {
            this.graphManager = g;
            this.rootContext = null;
            this.reporting = null;
        }
    }

    internal class TimeContext<T> : ITimeContext<T>
        where T : Time<T>
    {
        private Reporting.IReportingConnector<T> downstreamConnector;
        internal Reporting.AggregateStatisticsStage<Int64, T> intAggregator;
        internal Reporting.AggregateStatisticsStage<double, T> doubleAggregator;

        private readonly string scope;
        internal readonly TimeContextManager manager;
        public ITimeContextManager Manager { get { return manager; } }

        public bool HasAggregate
        {
            get { return intAggregator != null; }
        }

        public bool HasReporting
        {
            get { return downstreamConnector != null; }
        }

        public string Scope
        {
            get { return scope; }
        }

        public IStageContext<T> MakeStageContext(string name)
        {
            return new StageContext<T>(name, this, null, null, null);
        }

        public IStageContext<T> MakeStageContext(string name, Stream<string, T> inlineStats)
        {
            downstreamConnector.ConnectInline(inlineStats);
            return new StageContext<T>(name, this, inlineStats, null, null);
        }

        public IStageContext<T> MakeStageContext(
            string name, Stream<string, T> inlineStats,
            Stream<Pair<string, Reporting.ReportingRecord<Int64>>, T> aggInt, Stream<Pair<string, Reporting.ReportingRecord<double>>, T> aggDouble)
        {
            downstreamConnector.ConnectInline(inlineStats);
            return new StageContext<T>(name, this, inlineStats, aggInt, aggDouble);
        }

        public TimeContext<T> CloneWithoutAggregate()
        {
            return new TimeContext<T>(this.manager, this.scope, this.downstreamConnector, false);
        }

        void MakeAggregates()
        {
            var safeContext = this.CloneWithoutAggregate();

            intAggregator = new Reporting.AggregateStatisticsStage<Int64, T>(safeContext, scope + ".IRI");
            //intAggregator.Factory = (i => new Reporting.AggregateStatisticsShard<Int64, T>(i, intAggregator));
            downstreamConnector.ConnectIntAggregator(intAggregator.Output);

            doubleAggregator = new Reporting.AggregateStatisticsStage<double, T>(safeContext, scope + ".IRD");
            //doubleAggregator.Factory = (i => new Reporting.AggregateStatisticsShard<double, T>(i, doubleAggregator));
            downstreamConnector.ConnectDoubleAggregator(doubleAggregator.Output);
        }

        public TimeContext(TimeContextManager m, string s, Reporting.IReportingConnector<T> downstreamConn, bool hasAgg)
        {
            scope = s;
            manager = m;
            downstreamConnector = downstreamConn;
            if (hasAgg)
            {
                MakeAggregates();
            }
            else
            {
                intAggregator = null;
                doubleAggregator = null;
            }
        }
    }

    internal class StageContext<T> : IStageContext<T> where T : Time<T>
    {
        internal readonly TimeContext<T> parent;
        internal readonly Stream<string, T> inlineStatistics;
        internal readonly Stream<Pair<string, Reporting.ReportingRecord<Int64>>, T> aggregateInt;
        internal readonly Stream<Pair<string, Reporting.ReportingRecord<double>>, T> aggregateDouble;

        public ITimeContext<T> Parent
        {
            get { return parent; }
        }

        public IShardContext<T> MakeShardContext(Vertex parentShard)
        {
            return new ShardContext<T>(this, parentShard);
        }

        private void MakeAggregates(string name)
        {
            var safeContext = parent.CloneWithoutAggregate();

            var intReduced = aggregateInt
                .LocalTimeReduce<
                Reporting.IntReportingReducer, Reporting.ReportingRecord<Int64>, Reporting.ReportingRecord<Int64>, Reporting.ReportingRecord<Int64>,
                string, Pair<string, Reporting.ReportingRecord<Int64>>, T>(x => x.v1, x => x.v2, () => new Reporting.IntReportingReducer(),
                name + ".IRILR", null, null)
                .LocalTimeCombine<
                Reporting.IntReportingReducer, Reporting.ReportingRecord<Int64>, Reporting.ReportingRecord<Int64>, Reporting.ReportingRecord<Int64>,
                string, T>(() => new Reporting.IntReportingReducer(), name + ".IRILC", x => x.v1.GetHashCode());

            var intReporter = new Reporting.AggregateStatisticsStage<Int64, T>(safeContext, name + ".IRI");
            //intReporter.Factory = (i => new Reporting.AggregateStatisticsShard<Int64, T>(i, intReporter));
            intReporter.ConnectTo(intReduced);
            parent.intAggregator.ConnectTo(intReduced);

            var doubleReduced = aggregateDouble
                .LocalTimeReduce<
                Reporting.DoubleReportingReducer, Reporting.ReportingRecord<double>, Reporting.ReportingRecord<double>, Reporting.ReportingRecord<double>,
                string, Pair<string, Reporting.ReportingRecord<double>>, T>(x => x.v1, x => x.v2, () => new Reporting.DoubleReportingReducer(),
                name + ".IRDLR", null, null)
                .LocalTimeCombine<
                Reporting.DoubleReportingReducer, Reporting.ReportingRecord<double>, Reporting.ReportingRecord<double>, Reporting.ReportingRecord<double>,
                string, T>(() => new Reporting.DoubleReportingReducer(), name + ".IRDLC", x => x.v1.GetHashCode());

            var doubleReporter = new Reporting.AggregateStatisticsStage<double, T>(safeContext, name + ".IRD");
            //doubleReporter.Factory = (i => new Reporting.AggregateStatisticsShard<double, T>(i, doubleReporter));
            doubleReporter.ConnectTo(doubleReduced);
            parent.doubleAggregator.ConnectTo(doubleReduced);
        }

        public StageContext(
            string name, TimeContext<T> p, Stream<string, T> inlineStats,
            Stream<Pair<string, Reporting.ReportingRecord<Int64>>, T> aggInt, Stream<Pair<string, Reporting.ReportingRecord<double>>, T> aggDouble)
        {
            parent = p;
            inlineStatistics = inlineStats;
            aggregateInt = aggInt;
            aggregateDouble = aggDouble;
            if (aggregateInt != null)
            {
                MakeAggregates(name);
            }
        }
    }

    internal class ShardContext<T> : IShardContext<T> where T : Time<T>
    {
        private readonly Vertex shard;
        internal readonly StageContext<T> parent;
        private readonly Reporting.Reporting<T> reporting;

        public Vertex Shard
        {
            get { return shard; }
        }

        public IStageContext<T> Parent
        {
            get { return parent; }
        }

        public Reporting.IReporting<T> Reporting
        {
            get { return reporting; }
        }

        public ShardContext(StageContext<T> p, Vertex s)
        {
            parent = p;
            shard = s;
            if (parent.parent.manager.Reporting == null)
            {
                reporting = null;
            }
            else
            {
                reporting = new Reporting.Reporting<T>(this, parent.inlineStatistics, parent.aggregateInt, parent.aggregateDouble);
            }
        }
    }

}

