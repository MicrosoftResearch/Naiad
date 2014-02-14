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
using Naiad.Dataflow.Channels;
using Naiad.CodeGeneration;
using Naiad.Runtime.Controlling;
using Naiad.DataStructures;
using Naiad.FaultTolerance;
using Naiad.Frameworks;
using Naiad.Scheduling;
using System.IO;
using Naiad.Dataflow.Reporting;

namespace Naiad.Dataflow
{
    /// <summary>
    /// Base class for stages without committing to a time type. Supports input/output creation, tracks topology, etc.
    /// It is often important to have lists of stages with varying time types, and these support common functionality.
    /// </summary>
    public abstract class Stage
    {
        private readonly InternalGraphManager graphManager;
        internal InternalGraphManager InternalGraphManager { get { return this.graphManager; } }
        public readonly int StageId;
        internal abstract Pointstamp DefaultVersion { get; }

        private List<Edge> targets;
        internal List<Edge> Targets { get { return targets; } }

        public readonly Placement Placement;

        internal enum OperatorType { Unknown, Default, IterationAdvance, IterationIngress, IterationEgress };
        private readonly OperatorType collectionType;

        internal bool IsIterationAdvance { get { return collectionType == OperatorType.IterationAdvance; } }
        internal bool IsIterationIngress { get { return collectionType == OperatorType.IterationIngress; } }
        internal bool IsIterationEgress { get { return collectionType == OperatorType.IterationEgress; } }

        internal abstract IEnumerable<Vertex> Shards { get; }

        public virtual void Restore(NaiadReader reader)
        {
            throw new NotImplementedException();
        }

        internal abstract void Materialize();

        #region Input/Output creation

        bool inputsSealed = false;

        // creates a new receive port for the stage to read records from.
        public StageInput<R, T> NewInput<R, T>(Stream<R, T> stream, Expression<Func<R, int>> partitioning)
            where T : Time<T>
        {
            if (inputsSealed)
                throw new Exception("Inputs for a stage may not be added after outputs");

            if (stream == null)
                throw new ArgumentNullException("stream");

            var result = new StageInput<R, T>(this, partitioning);

            this.InternalGraphManager.Connect(stream.StageOutput, result, partitioning, Channel.Flags.None);

            return result;
        }

        internal StageInput<R, T> NewUnconnectedInput<R, T>(Expression<Func<R, int>> partitioning)
            where T : Time<T>
        {
            if (inputsSealed)
                throw new Exception("Inputs for a stage may not be added after outputs");

            var result = new StageInput<R, T>(this, partitioning);

            return result;
        }

        public StageInput<R, T> NewInput<R, T>(Stream<R, T> stream)
            where T : Time<T>
        {
            return this.NewInput(stream, null);
        }

        internal StageOutput<R, T> NewOutput<R, T>(ITimeContext<T> context, Expression<Func<R,int>> partitionedBy)
            where T : Time<T>
        {
            inputsSealed = true;
            
            var result = new StageOutput<R, T>(this, context, partitionedBy);

            return result;
        }
        
        // creates a new send socket for the stage to send records into.
        internal StageOutput<R, T> NewOutput<R, T>(ITimeContext<T> context)
            where T : Time<T>
        {
            return this.NewOutput<R, T>(context, null);
        }
        #endregion

        #region Constructor

        private readonly string MyName;
        /// <summary>
        /// Returns the stage name decorated by stage id
        /// </summary>
        public override string ToString()
        {
            return MyName;
        }

        private readonly string name;
        /// <summary>
        /// Returns the stage name undecorated by stage id
        /// </summary>
        public string Name { get { return name; } }

        internal Stage(Placement placement, InternalGraphManager graphManager, OperatorType operatorType, string name)
        {
            this.graphManager = graphManager;
            this.targets = new List<Edge>();
            this.StageId = this.InternalGraphManager.Register(this);
            this.collectionType = operatorType;
            this.Placement = placement;
            this.name = name;

            MyName = string.Format("{0}[{1}]", name, this.StageId);
        }

        #endregion Constructor
    }

    /// <summary>
    /// Stage with a committed time type, supporting a Context for the stage to use. This is less specific than a stage 
    /// with a time and an OP, which makes many generics and type inference problems easier. It is possible that this role 
    /// could be played by Stage above.
    /// </summary>
    /// <typeparam name="TTime">Time type</typeparam>
    public abstract class Stage<TTime> : Stage
        where TTime : Time<TTime>
    {
        private readonly ITimeContext<TTime> context;
        public OpaqueTimeContext<TTime> Context { get { return new OpaqueTimeContext<TTime>(context); } }

        private readonly IStageContext<TTime> localContext;
        internal IStageContext<TTime> LocalContext { get { return localContext; } }

        internal override Pointstamp DefaultVersion
        {
            get { return default(TTime).ToPointstamp(this.StageId); }
        }

        internal Stage(Placement placement, OpaqueTimeContext<TTime> c, OperatorType opType, string name)
            : base(placement, c.Context.Manager.GraphManager, opType, name)
        {
            context = c.Context;

            if (this.context.HasReporting && this.context.Manager.RootStatistics.HasInline)
            {
                // we are cheating and adding this without calling NewOutput to avoid sealing the inputs
                var inlineStats = new Stream<string, TTime>(new StageOutput<string, TTime>(this, context));

                if (this.context.HasAggregate)
                {
                    var rawContext = context.Manager.MakeRawContextForScope<TTime>(name + ".IR");

                    var aggInt = new Stream<Pair<string, Reporting.ReportingRecord<Int64>>, TTime>(new StageOutput<Pair<string, Reporting.ReportingRecord<Int64>>, TTime>(this, rawContext));
                    var aggDouble = new Stream<Pair<string, Reporting.ReportingRecord<double>>, TTime>(new StageOutput<Pair<string, Reporting.ReportingRecord<double>>, TTime>(this, rawContext));

                    localContext = context.MakeStageContext(name, inlineStats, aggInt, aggDouble);
                }
                else
                {
                    localContext = context.MakeStageContext(name, inlineStats);
                }
            }
            else
            {
                localContext = context.MakeStageContext(name);
            }
        }
    }

    /// <summary>
    /// Represents a stage of shards all of type OP. 
    /// This information allows users to access OP-specific fields and methods, 
    /// useful in connecting each instance of OP to stage inputs and outputs.
    /// </summary>
    /// <typeparam name="TVertex"></typeparam>
    /// <typeparam name="TTime"></typeparam>
    public class Stage<TVertex, TTime> : Stage<TTime>
        where TVertex : Vertex<TTime>
        where TTime : Time<TTime>
    {
        private readonly Dictionary<int, TVertex> shards;

        internal override IEnumerable<Vertex> Shards { get { return this.shards.Values; } }

        internal TVertex GetShard(int shardIndex)
        {
            return shards[shardIndex];
        }

        protected void AddShard(TVertex shard) { shards.Add(shard.VertexId, shard); }

        internal Stage(Placement placement, OpaqueTimeContext<TTime> context, OperatorType optype, Func<int, Stage<TTime>, TVertex> factory, string name)
            : base(placement, context, optype, name)
        {
            shards = new Dictionary<int, TVertex>();
            this.factory = factory;

            if (factory == null)
                throw new ArgumentNullException("factory");
        }

        internal Stage(OpaqueTimeContext<TTime> context, OperatorType optype, Func<int, Stage<TTime>, TVertex> factory, string name)
            : this(context.Context.Manager.GraphManager.DefaultPlacement, context, optype, factory, name)
        {
        }

        public Stage(OpaqueTimeContext<TTime> context, Func<int, Stage<TTime>, TVertex> factory, string name)
            : this(context, OperatorType.Default, factory, name)
        {
        }

        private readonly List<Action<TVertex>> materializeActions = new List<Action<TVertex>>();

        #region Input creation

        /// <summary>
        /// Creates a new input from a stream, a ShardInput selector, and partitioning information.
        /// </summary>
        /// <typeparam name="TRecord">Record type</typeparam>
        /// <param name="stream">source stream</param>
        /// <param name="shardInput">ShardInput selector</param>
        /// <param name="partitionedBy">partitioning expression, or null</param>
        /// <returns>StageInput</returns>
        public StageInput<TRecord, TTime> NewInput<TRecord>(Stream<TRecord, TTime> stream, Func<TVertex, VertexInput<TRecord, TTime>> shardInput, Expression<Func<TRecord, int>> partitionedBy)
        {
            var ret = this.NewInput(stream, partitionedBy);

            this.materializeActions.Add(shard => { ret.Register(shardInput(shard)); });

            return ret;
        }

        /// <summary>
        /// Creates a new input from a stream, a message callback, and partitioning information.
        /// </summary>
        /// <typeparam name="TRecord">Record type</typeparam>
        /// <param name="stream">source stream</param>
        /// <param name="onRecv">message callback</param>
        /// <param name="partitionedBy">partitioning expression, or null</param>
        /// <returns>StageInput</returns>
        public StageInput<TRecord, TTime> NewInput<TRecord>(Stream<TRecord, TTime> stream, Action<Message<Pair<TRecord, TTime>>, TVertex> onRecv, Expression<Func<TRecord, int>> partitionedBy)
        {
            return this.NewInput<TRecord>(stream, s => new ActionReceiver<TRecord, TTime>(s, m => onRecv(m, s)), partitionedBy);
        }

        /// <summary>
        /// Ok, listen. This is used in very few places when we need to violate the "inputs before outputs" rule. We create the input anyhow, and assign the stream later on
        /// </summary>
        internal StageInput<S, TTime> NewUnconnectedInput<S>(Func<TVertex, VertexInput<S, TTime>> shardInput, Expression<Func<S, int>> partitionedBy)
        {
            var ret = this.NewUnconnectedInput<S,TTime>(partitionedBy);

            this.materializeActions.Add(shard => { ret.Register(shardInput(shard)); });

            return ret;
        }

        internal StageInput<S, TTime> NewUnconnectedInput<S>(Action<Message<Pair<S, TTime>>, TVertex> onRecv, Expression<Func<S, int>> partitionedBy)
        {
            return this.NewUnconnectedInput<S>(shard => new ActionReceiver<S, TTime>(shard, message => onRecv(message, shard)), partitionedBy);
        }

        internal StageInput<S, T2> NewSurprisingTimeTypeInput<S, T2>(Stream<S, T2> stream, Func<TVertex, VertexInput<S, T2>> shardInput, Expression<Func<S, int>> partitionedBy)
            where T2 : Time<T2>
        {
            var ret = this.NewInput<S, T2>(stream, partitionedBy);

            this.materializeActions.Add(shard => { ret.Register(shardInput(shard)); });

            return ret;
        }


        #endregion

        #region Output creation

        public Stream<R, TTime> NewOutput<R>(Func<TVertex, VertexOutput<R, TTime>> shardOutput) 
        { 
            return this.NewOutput(shardOutput, null); 
        }

        public Stream<R, TTime> NewOutput<R>(Func<TVertex, VertexOutput<R, TTime>> shardOutput, Expression<Func<R, int>> partitionedBy) 
        { 
            var ret = this.NewOutput<R, TTime>(this.Context.Context, partitionedBy);

            this.materializeActions.Add(shard => { ret.Register(shardOutput(shard)); });

            return new Stream<R, TTime>(ret);
        }

        // experimental; the callback doesn't do flush suppression like a VertexOutput, but perhaps the SendWire should instead...
        internal Stream<R, TTime> NewOutput<R>(Action<SendWire<R, TTime>, TVertex> newListener, Expression<Func<R, int>> partitionedBy)
        {
            return this.NewOutput<R>(vertex => new ActionSubscriber<R, TTime>(vertex, listener => { newListener(listener, vertex); vertex.AddOnFlushAction(() => listener.Flush()); }), partitionedBy);
        }

        internal Stream<R, TTime> NewOutputWithoutSealing<R>(Func<TVertex, VertexOutput<R, TTime>> shardOutput, Expression<Func<R, int>> partitionedBy)
        {
            var result = new StageOutput<R, TTime>(this, this.Context.Context, partitionedBy);

            this.materializeActions.Add(shard => { result.Register(shardOutput(shard)); });

            return new Stream<R, TTime>(result);
        }

        #endregion

        private bool materialized = false;
        internal override void Materialize()
        {
            if (materialized)
                return; // progress stages get re-materialized because it is a pain not to.

            materialized = true;

            foreach (var loc in Placement)
            {
                if (loc.ProcessId == this.InternalGraphManager.Controller.Configuration.ProcessID)
                {
                    var shard = this.factory(loc.VertexId, this);
                    AddShard(shard);
                    shard.Context = this.LocalContext.MakeShardContext(shard);

                    foreach (var action in this.materializeActions)
                        action(shard);
                }
            }
        }

        private readonly Func<int, Stage<TTime>, TVertex> factory;
    }

}
