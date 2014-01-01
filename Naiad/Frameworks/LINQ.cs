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

#if false
using System;
using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;
using System.Text;
using Naiad.Dataflow.Channels;
using Naiad.Dataflow;
using Naiad.Frameworks;
using Naiad.Dataflow.Iteration;
using Naiad;

namespace Naiad.Frameworks
{
    public static class ExtensionMethods
    {
        public static StageOutput<R, T> UnaryExpression<S, R, T>(this StageOutput<S, T> stream, Expression<Func<S, int>> keyFunction, Expression<Func<IEnumerable<S>, IEnumerable<R>>> transformation, string name)
            where T : Time<T>
        {
            return UnaryBufferingShard<S, R, T>.MakeStage(stream, (i, v) => new Naiad.Frameworks.UnaryBufferingShard<S, R, T>(i, v, transformation), keyFunction, null, name);
        }

        public static StageOutput<R, T> BinaryExpression<S1, S2, R, T>(this StageOutput<S1, T> stream1, StageOutput<S2, T> stream2,
                                                              Expression<Func<S1, int>> keyFunction1, Expression<Func<S2, int>> keyFunction2,
                                                              Expression<Func<IEnumerable<S1>, IEnumerable<S2>, IEnumerable<R>>> transformation, string name)
            where T : Time<T>
        {
            return BinaryBufferingShard<S1, S2, R, T>.MakeStage(stream1, stream2, (i, v) => new Naiad.Frameworks.BinaryBufferingShard<S1, S2, R, T>(i, v, transformation), keyFunction1, keyFunction2, null, name);
        }
    }

}

namespace Naiad.Frameworks.Linq
{
    public static class ExtensionMethods
    {
        public static StageOutput<R, T> Select<S, R, T>(this StageOutput<S, T> stream, Func<S, R> function)
            where T : Time<T>
        {
            return UnaryStreamingShard<S, R, T>.MakeStage(stream, (i, v) => new Select<S, R, T>(i, v, function), null, null, "Select");
        }

        public static StageOutput<S, T> Where<S, T>(this StageOutput<S, T> stream, Func<S, bool> predicate)
            where T : Time<T>
        {
            return UnaryStreamingShard<S, S, T>.MakeStage(stream, (i, v) => new Where<S, T>(i, v, predicate), stream.PartitionedBy, stream.PartitionedBy, "Where");
        }

        public static StageOutput<R, T> SelectMany<S, R, T>(this StageOutput<S, T> stream, Func<S, IEnumerable<R>> function)
            where T : Time<T>
        {
            return UnaryStreamingShard<S, R, T>.MakeStage(stream, (i, v) => new SelectManyShard<S, R, T>(i, v, function), null, null, "SelectMany");
        }

        public static StageOutput<S, T> Concat<S, T>(this StageOutput<S, T> stream1, StageOutput<S, T> stream2)
            where T : Time<T>
        {
            // test to see if they are partitioned properly, and if so maintain the information in output partitionedby information.
            var partitionedBy = Naiad.CodeGeneration.ExpressionComparer.Instance.Equals(stream1.PartitionedBy, stream2.PartitionedBy) ? stream1.PartitionedBy : null;
            return BinaryStreamingShard<S, S, S, T>.MakeStage(stream1, stream2, (i, v) => new Concat<S, T>(i, v), partitionedBy, partitionedBy, partitionedBy, "Concat");
        }

        public static StageOutput<R, T> GroupBy<S, K, R, T>(this StageOutput<S, T> stream, Func<S, K> key, Func<K, IEnumerable<S>, IEnumerable<R>> selector)
            where T : Time<T>
        {
            return stream.UnaryExpression(x => key(x).GetHashCode(), x => x.GroupBy(key, selector).SelectMany(y => y), "GroupBy");
        }

        public static StageOutput<R, T> Join<S1, S2, K, R, T>(this StageOutput<S1, T> stream1, StageOutput<S2, T> stream2, Func<S1, K> key1, Func<S2, K> key2, Func<S1, S2, R> reducer)
            where T : Time<T>
        {
            return stream1.BinaryExpression(stream2, x => key1(x).GetHashCode(), x => key2(x).GetHashCode(), (x1, x2) => x1.Join(x2, key1, key2, reducer), "Join");
        }

        public static StageOutput<S, T> Distinct<S, T>(this StageOutput<S, T> stream)
            where T : Time<T>
        {
            return stream.NewStage((i, v) => new DistinctShard<S, T>(i, v), x => x.GetHashCode(), x => x.GetHashCode(), "Distinct");
        }
    }

    internal class DistinctShard<S, T> : UnaryStreamingShard<S, S, T>
        where T : Time<T>
    {
        private T lastTime;
        private HashSet<S> lastSet;

        private readonly Dictionary<T, HashSet<S>> values = new Dictionary<T,HashSet<S>>();

        internal DistinctShard(int i, Stage<T> stage)
            : base(i, stage)
        {
        }

        public override void OnRecv(Pair<S, T> record)
        {
            if (lastSet == null || !this.lastTime.Equals(record.t))
            {
                if (!this.values.TryGetValue(record.t, out this.lastSet))
                {
                    this.lastSet = new HashSet<S>();
                    this.values[record.t] = this.lastSet;
                    this.ScheduleAt(record.t);
                }
                this.lastTime = record.t;
            }

            if (this.lastSet.Add(record.s))
            {
                this.Output.Send(record.s, record.t);
            }
        }

        public override void OnDone(T time)
        {
            this.values.Remove(time);
            if (this.lastTime.Equals(time) && this.lastSet != null)
                this.lastSet = null;
        }
    }
 
}

namespace Naiad.Frameworks.BroadcastReduction
{
    public static class BroadcastReductionExtensionMethods
    {
        public static StageOutput<X, T> LocalReduce<A, X, R, S, T>(this StageOutput<R, T> port, Func<A> factory, string name)
            where A : IReducer<X, R, S>
            where T : Time<T>
        {
            return UnaryStreamingShard<R, X, T>.MakeStage(port, (i, v) => new LocalReduceShard<A, X, R, S, T>(i, v, factory), null, null, name);
        }

        public static StageOutput<S, T> LocalCombine<A, X, R, S, T>(this StageOutput<X, T> port, Func<A> factory, string name)
            where A : IReducer<X, R, S>
            where T : Time<T>
        {
            return UnaryStreamingShard<X, S, T>.MakeStage(port, (i, v) => new LocalCombineShard<A, X, R, S, T>(i, v, factory), null, null, name);
        }

        public static StageOutput<Pair<K, X>, T> LocalReduce<A, X, R, S, K, I, T>(
            this StageOutput<I, T> port, Func<I, K> key, Func<I, R> val, Func<A> factory, string name,
            Expression<Func<I,int>> inPlacement, Expression<Func<Pair<K,X>,int>> outPlacement)
            where A : IReducer<X, R, S>
            where T : Time<T>
        {
            return UnaryStreamingShard<I, Pair<K, X>, T>.MakeStage(port, (i, v) => new LocalKeyedReduceShard<A, X, R, S, K, I, T>(i, v, key, val, factory), inPlacement, outPlacement, name);
        }

        public static StageOutput<Pair<K, X>, T> LocalTimeReduce<A, X, R, S, K, I, T>(
            this StageOutput<I, T> port, Func<I, K> key, Func<I, R> val, Func<A> factory, string name,
            Expression<Func<I, int>> inPlacement, Expression<Func<Pair<K, X>, int>> outPlacement)
            where A : IReducer<X, R, S>
            where T : Time<T>
        {
            return UnaryStreamingShard<I, Pair<K, X>, T>.MakeStage(port, (i, v) => new LocalTimeKeyedReduceShard<A, X, R, S, K, I, T>(i, v, key, val, factory), inPlacement, outPlacement, name);
        }

        public static StageOutput<Pair<K, X>, T> LocalReduce<A, X, R, S, K, I, T>(
            this StageOutput<I, T> port, Func<I, K> key, Func<I, R> val, Func<A> factory, string name)
            where A : IReducer<X, R, S>
            where T : Time<T>
        {
            return LocalReduce<A, X, R, S, K, I, T>(port, key, val, factory, name, null, null);
        }

        public static StageOutput<Pair<K, S>, T> LocalCombine<A, X, R, S, K, T>(
            this StageOutput<Pair<K, X>, T> port, Func<A> factory, string name,
            Expression<Func<Pair<K, S>, int>> outPlacement)
            where A : IReducer<X, R, S>
            where T : Time<T>
        {
            
            Expression<Func<Pair<K,X>, int>> inPlacement = null;
            if (outPlacement != null)
            {
                inPlacement = x => x.s.GetHashCode();
            }

            return UnaryStreamingShard<Pair<K,X>,Pair<K,S>,T>.MakeStage(port, (i,v) => new LocalKeyedCombineShard<A, X, R, S, K, T>(i, v, factory), inPlacement, outPlacement, name);
        }

        public static StageOutput<Pair<K, S>, T> LocalTimeCombine<A, X, R, S, K, T>(
            this StageOutput<Pair<K, X>, T> port, Func<A> factory, string name,
            Expression<Func<Pair<K, S>, int>> outPlacement)
            where A : IReducer<X, R, S>
            where T : Time<T>
        {
            Expression<Func<Pair<K, X>, int>> inPlacement = null;
            if (outPlacement != null)
            {
                inPlacement = x => x.s.GetHashCode();
            }

            return UnaryStreamingShard<Pair<K, X>, Pair<K, S>, T>.MakeStage(port, (i, v) => new LocalTimeKeyedCombineShard<A, X, R, S, K, T>(i, v, factory), inPlacement, outPlacement, name);
        }

        public static StageOutput<Pair<K, S>, T> LocalCombine<A, X, R, S, K, T>(
            this StageOutput<Pair<K, X>, T> port, Func<A> factory, string name)
            where A : IReducer<X, R, S>
            where T : Time<T>
        {
            return port.LocalCombine<A, X, R, S, K, T>(factory, name, null);
        }

        public static StageOutput<Pair<K, S>, T> Reduce<A, X, R, S, K, I, T>(
            this StageOutput<I, T> port, Func<I, K> key, Func<I, R> val, Func<A> factory, string name)
            where A : IReducer<X, R, S>
            where T : Time<T>
        {
            return port.
                LocalReduce<A, X, R, S, K, I, T>(key, val, factory, name + "Reduce", null, null).
                LocalCombine<A, X, R, S, K, T>(factory, name + "Combine", x => x.s.GetHashCode());
        }

        public static StageOutput<R, T> Broadcast<R, T>(this StageOutput<R, T> port)
            where R : Cloneable<R>
            where T : Time<T>
        {
            var controller = port.Context.Manager.Controller;

            int threadCount = controller.DefaultPlacement.Count / controller.Configuration.Processes;
            if (threadCount * controller.Configuration.Processes != controller.DefaultPlacement.Count)
            {
                throw new Exception("Uneven thread count?");
            }

            var processDests = controller.DefaultPlacement.Where(x => x.ThreadId == 0).Select(x => x.ShardId).ToArray();

            var boutput = UnaryStreamingShard<R, Pair<int, R>, T>.MakeStage(port, (i, v) => new BroadcastSendShard<R, T>(i, v, processDests), null, null, "BroadcastProcessSend");

            var collectable = boutput;
            if (controller.DefaultPlacement.Where(x => x.ProcessId == controller.Configuration.ProcessID).Count() > 1)
            {
                var threadDests = controller.DefaultPlacement
                                            .Where(x => x.ProcessId == controller.Configuration.ProcessID)
                                            .Select(x => x.ShardId)
                                            .ToArray();

                collectable = UnaryStreamingShard<Pair<int, R>, Pair<int, R>, T>.MakeStage(boutput, (i, v) => new BroadcastForwardShard<R, T>(i, v, threadDests), x => x.s, null, "BroadcastShardSend");
            }

            return Naiad.Frameworks.Linq.ExtensionMethods.Select(collectable, x => x.t);
        }

        public static StageOutput<S, T> BroadcastReduce<A, X, R, S, T>(this StageOutput<R, T> port, Func<A> factory, string name)
            where A : IReducer<X, R, S>
            where X : Cloneable<X>
            where T : Time<T>
        {
            return port.LocalReduce<A, X, R, S, T>(factory, name + "Reduce").Broadcast().LocalCombine<A, X, R, S, T>(factory, name + "Combine");
        }

        public static StageOutput<X, T> BroadcastReduce<A, X, T>(this StageOutput<X, T> port, Func<A> factory, string name)
            where A : IReducer<X, X, X>
            where X : Cloneable<X>
            where T : Time<T>
        {
            return port.LocalReduce<A, X, X, X, T>(factory, name + "Reduce").Broadcast().LocalCombine<A, X, X, X, T>(factory, name + "Combine");
        }
    }
}

namespace Naiad.Frameworks
{
    public static class NonStandardExtensionMethods
    {
        public static StageOutput<R, T> ShardSelect<S, R, T>(this StageOutput<S, T> stream, Func<int, S, R> function)
            where T : Time<T>
        {
            return UnaryStreamingShard<S, R, T>.MakeStage(stream, (i, v) => new ShardSelect<S, R, T>(i, v, function), null, null, "ShardSelect");
        }

        public static StageOutput<R, T> SelectManyArraySegment<S, R, T>(this StageOutput<S, T> stream, Func<S, IEnumerable<ArraySegment<R>>> function)
            where T : Time<T>
        {
            return UnaryStreamingShard<S, R, T>.MakeStage(stream, (i, v) => new SelectManyArraySegment<S, R, T>(i, v, function), null, null, "SelectManyArraySegment");
        }

        public static StageOutput<Pair<S, Int64>, T> Count<S, T>(this StageOutput<S, T> stream)
            where T : Time<T>
        {
            return Naiad.Frameworks.BroadcastReduction.BroadcastReductionExtensionMethods.Reduce<CountReducer<S>, Int64, S, Int64, S, S, T>(stream, x => x, x => x, () => new CountReducer<S>(), "Count");
        }


        public static StageOutput<V, T> Delay<V, T>(this StageOutput<V, T> values)
            where T : Time<T>
        {
            return values.UnaryExpression(values.PartitionedBy, x => x, "Delay");
        }

        public static StageOutput<S, T> Iterate<S, T>(this StageOutput<S, T> input, Func<Naiad.Dataflow.Iteration.Context<T>, StageOutput<S, IterationIn<T>>, StageOutput<S, IterationIn<T>>> function, Expression<Func<S, int>> partitionedBy, int iterations, string name)
            where T : Time<T>
        {
            var helper = new Naiad.Dataflow.Iteration.Context<T>(input.Context, name);

            var delayed = helper.Delay<S>(partitionedBy, iterations);

            var ingress = Naiad.Dataflow.PartitionBy.ExtensionMethods.PartitionBy(helper.EnterLoop(input), partitionedBy);

            var loopHead = Naiad.Frameworks.Linq.ExtensionMethods.Concat(ingress, delayed.Output);

            var loopTail = function(helper, loopHead);

            delayed.Input = loopTail;

            return helper.ExitLoop(loopTail);
            //return helper.ExitLoop(loopTail, iterations);
        }


        public static StageOutput<S, T> Iterate<S, T>(this StageOutput<S, T> input, Func<Naiad.Dataflow.Iteration.Context<T>, StageOutput<S, IterationIn<T>>, StageOutput<S, IterationIn<T>>> function, Expression<Func<S, int>> initialIteration, Expression<Func<S, int>> partitionedBy, int iterations, string name)
            where T : Time<T>
        {
            var helper = new Naiad.Dataflow.Iteration.Context<T>(input.Context, name);

            var delayed = helper.Delay<S>(partitionedBy, iterations);

            var ingress = Naiad.Dataflow.PartitionBy.ExtensionMethods.PartitionBy(helper.EnterLoop(input, initialIteration.Compile()), partitionedBy);

            var loopHead = Naiad.Frameworks.Linq.ExtensionMethods.Concat(ingress, delayed.Output);

            var loopTail = function(helper, loopHead);

            delayed.Input = loopTail;

            return helper.ExitLoop(loopTail);
            //return helper.ExitLoop(loopTail, iterations);
        }

        /// <summary>
        /// Used to write records to files.
        /// </summary>
        /// <typeparam name="S">Record type</typeparam>
        /// <param name="input">Source of records</param>
        /// <param name="format">Format string for filename; {0} replaced with shard id</param>
        /// <param name="action">Operation to apply to each record and the output stream. Often (r,s) => s.Write(r);</param>
        public static void WriteToFiles<S>(this StageOutput<S, Epoch> input, string format, Action<S, System.IO.BinaryWriter> action)
        {
            Foundry.NewStage(input, (i, v) => new Writer<S>(i, v, action, format), null, "Writer");
        }


    }

    public class Writer<S> : Naiad.Frameworks.SinkStreamingShard<S, Epoch>
    {
        System.IO.BinaryWriter writer;
        Action<S, System.IO.BinaryWriter> Action;

        public override void OnRecv(Message<Pair<S, Epoch>> message)
        {
            for (int i = 0; i < message.length; i++)
                Action(message.payload[i].s, writer);
        }

        public override void OnDone(Epoch time)
        {
            writer.Flush();
            writer.Close();
        }

        public Writer(int index, Naiad.Dataflow.Stage<Epoch> stage, Action<S, System.IO.BinaryWriter> action, string format)
            : base(index, stage)
        {
            var filename = String.Format(format, index);

            if (System.IO.File.Exists(filename))
                System.IO.File.Delete(filename);

            this.writer = new System.IO.BinaryWriter(System.IO.File.OpenWrite(String.Format(format, index)));
            this.Action = action;
        }
    }
    
    public class SelectManyArraySegment<S, R, T> : UnaryStreamingShard<S, R, T>
        where T : Time<T>
    {
        Func<S, IEnumerable<ArraySegment<R>>> Function;

        public override void OnRecv(Pair<S, T> record)
        {
            throw new NotImplementedException();
        }
        
        public override void MessageReceived(Dataflow.Message<Pair<S, T>> message)
        {
            for (int ii = 0; ii < message.length; ii++)
            {
                var record = message.payload[ii];
                var time = record.t;
                foreach (var result in Function(record.s))
                {
                    for (int i = result.Offset; i < result.Offset + result.Count; i++)
                    {
                        this.Output.Buffer.payload[this.Output.Buffer.length++] = new Pair<R, T>(result.Array[i], time);
                        if (this.Output.Buffer.length == this.Output.Buffer.payload.Length)
                            this.Output.SendBuffer();
                    }
                }
            }
        }

        public override void OnDone(T time) { this.Output.Flush(); }

        public SelectManyArraySegment(int index, Stage<T> stage, Func<S, IEnumerable<ArraySegment<R>>> function)
            : base(index, stage)
        {
            this.Function = function;
        }
    }


    public class Select<S, R, T> : UnaryStreamingShard<S, R, T>
        where T : Time<T>
    {
        Func<S, R> Function;

        public override void OnRecv(Pair<S, T> record)
        {
            this.Output.Send(Function(record.s), record.t);
        }
        
        public override void OnDone(T time) { }

        public override void MessageReceived(Dataflow.Message<Pair<S, T>> message)
        {
            for (int i = 0; i < message.length; i++)
            {
                this.Output.Buffer.payload[this.Output.Buffer.length++] = new Pair<R, T>(Function(message.payload[i].s), message.payload[i].t);
                if (this.Output.Buffer.length == this.Output.Buffer.payload.Length)
                    this.Output.SendBuffer();
            }
        }

        public Select(int index, Stage<T> stage, Func<S, R> function)
            : base(index, stage)
        {
            this.Function = function;
        }
    }


    public class ShardSelect<S, R, T> : UnaryStreamingShard<S, R, T>
        where T : Time<T>
    {
        Func<int, S, R> Function;

        public override void OnRecv(Pair<S, T> record)
        {
            throw new NotImplementedException();
            this.Output.Send(Function(this.ShardId, record.s), record.t);
        }

        public override void OnDone(T time) { }

        public override void MessageReceived(Dataflow.Message<Pair<S, T>> message)
        {
            for (int i = 0; i < message.length; i++)
            {
                this.Output.Buffer.payload[this.Output.Buffer.length++] = new Pair<R, T>(Function(this.ShardId, message.payload[i].s), message.payload[i].t);
                if (this.Output.Buffer.length == this.Output.Buffer.payload.Length)
                    this.Output.SendBuffer();
            }
        }

        public ShardSelect(int index, Stage<T> stage, Func<int, S, R> function)
            : base(index, stage)
        {
            this.Function = function;
        }
    }

    public class Where<S, T> : UnaryStreamingShard<S, S, T>
        where T : Time<T>
    {
        Func<S, bool> Function;

        public override void OnRecv(Pair<S, T> record)
        {
            if (this.Function(record.s))
                this.Output.Send(record.s, record.t);
        }
        public override void OnDone(T time) { this.Output.Flush(); }


        public override void MessageReceived(Dataflow.Message<Pair<S, T>> message)
        {
            for (int i = 0; i < message.length; i++)
            {
                if (Function(message.payload[i].s))
                {
                    this.Output.Buffer.payload[this.Output.Buffer.length++] = message.payload[i];
                    if (this.Output.Buffer.length == this.Output.Buffer.payload.Length)
                        this.Output.SendBuffer();
                }
            }
        }

        public Where(int index, Stage<T> stage, Func<S, bool> function)
            : base(index, stage)
        {
            this.Function = function;
        }
    }

    public class Concat<S, T> : Naiad.Frameworks.BinaryStreamingShard<S, S, S, T>
        where T : Time<T>
    {
        public override void OnRecv1(Pair<S, T> record) { this.Output.Send(record.s, record.t); }
        public override void OnRecv2(Pair<S, T> record) { this.Output.Send(record.s, record.t); }

        public override void MessageReceived1(Dataflow.Message<Pair<S, T>> message) { this.Output.Send(message); }
        public override void MessageReceived2(Dataflow.Message<Pair<S, T>> message) { this.Output.Send(message); }

        public override void OnDone(T time) { }

        public Concat(int index, Naiad.Dataflow.Stage<T> stage)
            : base(index, stage) { }
    }


    public class SelectManyShard<S, R, T> : UnaryStreamingShard<S, R, T>
        where T : Time<T>
    {
        Func<S, IEnumerable<R>> Function;

        public override void OnRecv(Pair<S, T> record)
        {
            foreach (var result in this.Function(record.s))
                this.Output.Send(result, record.t);
        }
        public override void OnDone(T time) { }

        public SelectManyShard(int index, Stage<T> stage, Func<S, IEnumerable<R>> function)
            : base(index, stage)
        {
            this.Function = function;
        }
    }
}
#endif