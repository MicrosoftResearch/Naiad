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
using Microsoft.Research.Naiad.Serialization;
using Microsoft.Research.Naiad.Runtime.Controlling;
using Microsoft.Research.Naiad.DataStructures;
using Microsoft.Research.Naiad.Dataflow.StandardVertices;

using Microsoft.Research.Naiad.Dataflow;

namespace Microsoft.Research.Naiad.Frameworks.Reduction
{
    internal static class ExtensionMethods
    {
        

        internal static Stream<TState, TTime> LocalReduce<TReducer, TState, TInput, TOutput, TTime>(this Stream<TInput, TTime> stream, Func<TReducer> factory, string name)
            where TReducer : IReducer<TState, TInput, TOutput>
            where TTime : Time<TTime>
        {
            return Foundry.NewUnaryStage(stream, (i, v) => new LocalReduceVertex<TReducer, TState, TInput, TOutput, TTime>(i, v, factory), null, null, name);
        }

        internal static Stream<TOutput, TTime> LocalCombine<TReducer, TState, TInput, TOutput, TTime>(this Stream<TState, TTime> stream, Func<TReducer> factory, string name)
            where TReducer : IReducer<TState, TInput, TOutput>
            where TTime : Time<TTime>
        {
            return Foundry.NewUnaryStage(stream, (i, v) => new LocalCombineVertex<TReducer, TState, TInput, TOutput, TTime>(i, v, factory), null, null, name);
        }

        internal static Stream<Pair<TKey, TState>, TTime> LocalReduce<TReducer, TState, TValue, TOutput, TKey, TInput, TTime>(
            this Stream<TInput, TTime> stream, Func<TInput, TKey> key, Func<TInput, TValue> val, Func<TReducer> factory, string name,
            Expression<Func<TInput, int>> inPlacement, Expression<Func<Pair<TKey, TState>, int>> outPlacement)
            where TReducer : IReducer<TState, TValue, TOutput>
            where TTime : Time<TTime>
        {
            return Foundry.NewUnaryStage(stream, (i, v) => new LocalKeyedReduceVertex<TReducer, TState, TValue, TOutput, TKey, TInput, TTime>(i, v, key, val, factory), inPlacement, outPlacement, name);
        }

        internal static Stream<Pair<K, X>, T> LocalTimeReduce<A, X, R, S, K, I, T>(
            this Stream<I, T> stream, Func<I, K> key, Func<I, R> val, Func<A> factory, string name,
            Expression<Func<I, int>> inPlacement, Expression<Func<Pair<K, X>, int>> outPlacement)
            where A : IReducer<X, R, S>
            where T : Time<T>
        {
            return Foundry.NewUnaryStage(stream, (i, v) => new LocalTimeKeyedReduceVertex<A, X, R, S, K, I, T>(i, v, key, val, factory), inPlacement, outPlacement, name);
        }

        internal static Stream<Pair<K, X>, T> LocalReduce<A, X, R, S, K, I, T>(
            this Stream<I, T> stream, Func<I, K> key, Func<I, R> val, Func<A> factory, string name)
            where A : IReducer<X, R, S>
            where T : Time<T>
        {
            return LocalReduce<A, X, R, S, K, I, T>(stream, key, val, factory, name, null, null);
        }

        internal static Stream<Pair<K, S>, T> LocalCombine<A, X, R, S, K, T>(
            this Stream<Pair<K, X>, T> stream, Func<A> factory, string name,
            Expression<Func<Pair<K, S>, int>> outPlacement)
            where A : IReducer<X, R, S>
            where T : Time<T>
        {

            Expression<Func<Pair<K, X>, int>> inPlacement = null;
            if (outPlacement != null)
            {
                inPlacement = x => x.First.GetHashCode();
            }

            return Foundry.NewUnaryStage(stream, (i, v) => new LocalKeyedCombineVertex<A, X, R, S, K, T>(i, v, factory), inPlacement, outPlacement, name);
        }

        internal static Stream<Pair<K, S>, T> LocalTimeCombine<A, X, R, S, K, T>(
            this Stream<Pair<K, X>, T> stream, Func<A> factory, string name,
            Expression<Func<Pair<K, S>, int>> outPlacement)
            where A : IReducer<X, R, S>
            where T : Time<T>
        {
            Expression<Func<Pair<K, X>, int>> inPlacement = null;
            if (outPlacement != null)
            {
                inPlacement = x => x.First.GetHashCode();
            }

            return Foundry.NewUnaryStage(stream, (i, v) => new LocalTimeKeyedCombineVertex<A, X, R, S, K, T>(i, v, factory), inPlacement, outPlacement, name);
        }

        internal static Stream<Pair<K, S>, T> LocalCombine<A, X, R, S, K, T>(
            this Stream<Pair<K, X>, T> stream, Func<A> factory, string name)
            where A : IReducer<X, R, S>
            where T : Time<T>
        {
            return stream.LocalCombine<A, X, R, S, K, T>(factory, name, null);
        }

        internal static Stream<Pair<K, S>, T> Reduce<A, X, R, S, K, I, T>(
            this Stream<I, T> stream, Func<I, K> key, Func<I, R> val, Func<A> factory, string name)
            where A : IReducer<X, R, S>
            where T : Time<T>
        {
            return stream.
                LocalReduce<A, X, R, S, K, I, T>(key, val, factory, name + "Reduce", null, null).
                LocalCombine<A, X, R, S, K, T>(factory, name + "Combine", x => x.First.GetHashCode());
        }

        internal static Stream<R, T> Broadcast<R, T>(this Stream<R, T> stream)
            where R : Cloneable<R>
            where T : Time<T>
        {
            var controller = stream.ForStage.InternalComputation.Controller;

            int threadCount = stream.ForStage.InternalComputation.DefaultPlacement.Count / controller.Configuration.Processes;
            if (threadCount * controller.Configuration.Processes != stream.ForStage.InternalComputation.DefaultPlacement.Count)
            {
                throw new Exception("Uneven thread count?");
            }

            var processDests = stream.ForStage.InternalComputation.DefaultPlacement.Where(x => x.ThreadId == 0).Select(x => x.VertexId).ToArray();

            var boutput = Foundry.NewUnaryStage(stream, (i, v) => new BroadcastSendVertex<R, T>(i, v, processDests), null, null, "BroadcastProcessSend");

            var collectable = boutput;
            if (stream.ForStage.InternalComputation.DefaultPlacement.Where(x => x.ProcessId == controller.Configuration.ProcessID).Count() > 1)
            {
                var threadDests = stream.ForStage.InternalComputation.DefaultPlacement
                                            .Where(x => x.ProcessId == controller.Configuration.ProcessID)
                                            .Select(x => x.VertexId)
                                            .ToArray();

                collectable = Foundry.NewUnaryStage(boutput, (i, v) => new BroadcastForwardVertex<R, T>(i, v, threadDests), x => x.First, null, "BroadcastVertexSend");
            }

            // TODO : fix this to use a streaming expression
            return collectable.UnaryExpression(null, xs => xs.Select(x => x.Second), "Select");
        }

        internal static Stream<S, T> BroadcastReduce<A, X, R, S, T>(this Stream<R, T> stream, Func<A> factory, string name)
            where A : IReducer<X, R, S>
            where X : Cloneable<X>
            where T : Time<T>
        {
            return stream.LocalReduce<A, X, R, S, T>(factory, name + "Reduce").Broadcast().LocalCombine<A, X, R, S, T>(factory, name + "Combine");
        }

        internal static Stream<X, T> BroadcastReduce<A, X, T>(this Stream<X, T> stream, Func<A> factory, string name)
            where A : IReducer<X, X, X>
            where X : Cloneable<X>
            where T : Time<T>
        {
            return stream.LocalReduce<A, X, X, X, T>(factory, name + "Reduce").Broadcast().LocalCombine<A, X, X, X, T>(factory, name + "Combine");
        }
    }
}

namespace Microsoft.Research.Naiad.Frameworks.Reduction
{
    internal interface IReducer<TState, TInput, TOutput>
    {
        // Accumulate an object of type R
        void Add(TInput r);
        // This should be called if it is the first Add/Combine
        void InitialAdd(TInput r);

        // Accumulate another reducer
        void Combine(TState r);
        // This should be called if it is the first Add/Combine
        void InitialCombine(TState r);

        // Return the current accumulated state. Undefined if no
        // call to Initial{Add,Combine} has been made
        TState State();

        // Return final value after adding in all records. Undefined if no
        // call to Initial{Add,Combine} has been made
        TOutput Value();
    }

    // Placeholder type for generics that support reduction, used when
    // reduction is not needed
    internal struct DummyReducer<X> : IReducer<X, X, X>
    {
        public void Add(X x)
        {
            throw new NotImplementedException();
        }

        public void InitialAdd(X x)
        {
            throw new NotImplementedException();
        }

        public void Combine(X x)
        {
            throw new NotImplementedException();
        }

        public void InitialCombine(X x)
        {
            throw new NotImplementedException();
        }

        public X State()
        {
            throw new NotImplementedException();
        }

        public X Value()
        {
            throw new NotImplementedException();
        }
    }

    internal class Aggregation<X, R, S> : IReducer<X, R, S>
    {
        public void InitialAdd(R other)
        {
            value = initialAdd(other);
        }

        public void Add(R other)
        {
            value = add(value, other);
        }

        public void InitialCombine(X other)
        {
            value = initialCombine(other);
        }

        public void Combine(X other)
        {
            value = combine(value, other);
        }

        public X State()
        {
            return value;
        }

        public S Value()
        {
            return finalize(value);
        }

        public Aggregation(Func<X, R, X> a, Func<R, X> ia, Func<X, X, X> c, Func<X, X> ic, Func<X, S> f)
        {
            add = a;
            initialAdd = ia;
            combine = c;
            initialCombine = ic;
            finalize = f;
        }

        public Aggregation(Func<X, R, X> a, Func<R, X> ia, Func<X, X, X> c, Func<X, S> f)
            : this(a, ia, c, x => x, f)
        {
        }

        private readonly Func<X, R, X> add;
        private readonly Func<R, X> initialAdd;
        private readonly Func<X, X, X> combine;
        private readonly Func<X, X> initialCombine;
        private readonly Func<X, S> finalize;
        X value;
    }

    internal class Aggregation<T> : Aggregation<T, T, T>
    {
        public Aggregation(Func<T, T, T> c)
            : base(c, x => x, c, x => x)
        {
        }
    }

    internal struct CountReducer<S> : IReducer<Int64, S, Int64>
    {
        public void InitialAdd(S s) { value = 1; }
        public void Add(S t) { ++value; }
        public void InitialCombine(Int64 other) { value = other; }
        public void Combine(Int64 other) { value += other; }
        public Int64 State() { return value; }
        public Int64 Value() { return value; }
        private Int64 value;
    }

    internal struct IntSumReducer : IReducer<Int64, Int64, Int64>
    {
        public void InitialAdd(Int64 r) { value = r; }
        public void Add(Int64 r) { value += r; }
        public void InitialCombine(Int64 other) { value = other; }
        public void Combine(Int64 other) { value += other; }
        public Int64 State() { return value; }
        public Int64 Value() { return value; }
        private Int64 value;
    }

    internal struct FloatSumReducer : IReducer<float, float, float>
    {
        public void InitialAdd(float r) { value = r; }
        public void Add(float r) { value += r; }
        public void InitialCombine(float other) { value = other; }
        public void Combine(float other) { value += other; }
        public float State() { return value; }
        public float Value() { return value; }
        private float value;
    }

    internal struct DoubleSumReducer : IReducer<double, double, double>
    {
        public void InitialAdd(double r) { value = r; }
        public void Add(double r) { value += r; }
        public void InitialCombine(double other) { value = other; }
        public void Combine(double other) { value += other; }
        public double State() { return value; }
        public double Value() { return value; }
        private double value;
    }

    internal struct MinReducer<T> : IReducer<T, T, T> where T : IComparable<T>
    {
        public void InitialAdd(T r) { value = r; }
        public void Add(T r) { value = r.CompareTo(value) < 0 ? r : value; }
        public void InitialCombine(T other) { value = other; }
        public void Combine(T other) { value = other.CompareTo(value) < 0 ? other : value; }
        public T State() { return value; }
        public T Value() { return value; }
        private T value;
    }

    internal struct MaxReducer<T> : IReducer<T, T, T> where T : IComparable<T>
    {
        public void InitialAdd(T r) { value = r; }
        public void Add(T r) { value = r.CompareTo(value) > 0 ? r : value; }
        public void InitialCombine(T other) { value = other; }
        public void Combine(T other) { value = other.CompareTo(value) > 0 ? other : value; }
        public T State() { return value; }
        public T Value() { return value; }
        private T value;
    }

    internal class LocalReduceVertex<A, X, R, S, T> : UnaryVertex<R, X, T>
        where A : IReducer<X, R, S>
        where T : Time<T>
    {
        A reducer;
        T lastTime;
        bool valid;

        public override void OnReceive(Message<R, T> message)
        {
            for (int i = 0; i < message.length; i++)
            {
                //this.OnRecv(message.payload[i].PairWith(message.time));
                var record = message.payload[i];

                if (!valid)
                {
                    reducer.InitialAdd(record);
                    lastTime = message.time;
                    valid = true;
                    NotifyAt(message.time);
                }
                else
                {
                    if (!message.time.Equals(lastTime))
                    {
                        throw new Exception("One time at a time please!");
                    }

                    reducer.Add(record);
                }
            }
        }

        public override void OnNotify(T time)
        {
            var output = this.Output.GetBufferForTime(time);

            output.Send(reducer.State());
            valid = false;
        }

        public LocalReduceVertex(int index, Stage<T> stage, Func<A> factory)
            : base(index, stage)
        {
            valid = false;
            reducer = factory();
        }
    }

    internal class LocalCombineVertex<A, X, R, S, T> : UnaryVertex<X, S, T>
        where A : IReducer<X, R, S>
        where T : Time<T>
    {
        A reducer;
        T lastTime;
        bool valid;

        public override void OnReceive(Message<X, T> message)
        {
            for (int i = 0; i < message.length; i++)
            {
                //this.OnRecv(message.payload[i].PairWith(message.time));

                var record = message.payload[i];

                if (!valid)
                {
                    reducer.InitialCombine(record);
                    lastTime = message.time;
                    valid = true;
                    NotifyAt(message.time);
                }
                else
                {
                    if (!message.time.Equals(lastTime))
                    {
                        throw new Exception("One time at a time please!");
                    }

                    reducer.Combine(record);
                }

            }
        }

        public override void OnNotify(T time)
        {
            this.Output.GetBufferForTime(time).Send(reducer.Value());
            valid = false;
        }

        public LocalCombineVertex(int index, Stage<T> stage, Func<A> factory)
            : base(index, stage)
        {
            valid = false;
            reducer = factory();
        }
    }

    internal class LocalKeyedReduceVertex<A, X, R, S, K, I, T> : UnaryVertex<I, Pair<K, X>, T>
        where A : IReducer<X, R, S>
        where T : Time<T>
    {
        private readonly Func<I, K> key;
        private readonly Func<I, R> val;
        private readonly Func<A> factory;
        private Int64 recordsIn;
        ///private A[] reducers;
        private SpinedList<A> reducers;
        private int nextReducer;
        private Dictionary<K, int> index;
        private T lastTime;

        public override void OnReceive(Message<I, T> message)
        {
            if (reducers == null)
            {
                //reducers = new A[4];
                // Console.Error.WriteLine("Making a SpinedList in reducer");
                reducers = new SpinedList<A>();
                nextReducer = 0;
                index = new Dictionary<K, int>(2000000);
                lastTime = message.time;
                recordsIn = 0;
                NotifyAt(message.time);
            }
            else if (!message.time.Equals(lastTime))
            {
                throw new Exception("One time at a time please!");
            }

            for (int ii = 0; ii < message.length; ii++)
            {
                var record = message.payload[ii];

                K k = key(record);
                int i;
                if (index.TryGetValue(k, out i))
                {
                    A reducer = reducers[i];
                    reducer.Add(val(record));
                    reducers[i] = reducer;
                }
                else
                {
                    var reducer = factory();
                    reducer.InitialAdd(val(record));
                    index.Add(k, reducers.Count);
                    reducers.Add(reducer);
                    ++nextReducer;
                }

                ++recordsIn;
            }
        }

        public override void OnNotify(T time)
        {
            if (reducers != null)
            {
                // Console.Error.WriteLine("{0} OnNotify Reducers.count={1}", this.ToString(), reducers.Count);
                if (!time.Equals(lastTime))
                {
                    throw new Exception("One time at a time please!");
                }

                Context.Reporting.LogAggregate("RecordsIn", Dataflow.Reporting.AggregateType.Sum, recordsIn, time);
                Context.Reporting.LogAggregate("RecordsOut", Dataflow.Reporting.AggregateType.Sum, index.Count, time);

                var output = this.Output.GetBufferForTime(time);

                foreach (var r in index)
                    output.Send(new Pair<K, X>(r.Key, reducers[r.Value].State()));

                reducers = null;
                index = null;
                nextReducer = -1;
            }
        }

        public LocalKeyedReduceVertex(
            int i, Stage<T> stage, Func<I, K> k, Func<I, R> v, Func<A> f)
            : base(i, stage)
        {
            factory = f;
            key = k;
            val = v;
            lastTime = default(T);
            reducers = null;
            index = null;
            nextReducer = -1;
        }
    }

    internal class LocalTimeKeyedReduceVertex<A, X, R, S, K, I, T> : UnaryVertex<I, Pair<K, X>, T>
        where A : IReducer<X, R, S>
        where T : Time<T>
    {
        private readonly Func<I, K> key;
        private readonly Func<I, R> val;
        private readonly Func<A> factory;
        private readonly Dictionary<T, Time> reducers;

        class Time
        {
            public A[] reducers;
            public int nextReducer;
            public Dictionary<K, int> index;

            public Time(int c)
            {
                reducers = new A[c];
                nextReducer = 0;
                index = new Dictionary<K, int>();
            }
        }

        public override void OnReceive(Message<I, T> message)
        {
            Time t;
            if (!reducers.TryGetValue(message.time, out t))
            {
                t = new Time(4);
                reducers.Add(message.time, t);
                NotifyAt(message.time);
            }


            for (int ii = 0; ii < message.length; ii++)
            {
                //this.OnRecv(message.payload[i].PairWith(message.time));

                var record = message.payload[ii];

                K k = key(record);
                int i;
                if (t.index.TryGetValue(k, out i))
                {
                    t.reducers[i].Add(val(record));
                }
                else
                {
                    if (t.nextReducer == t.reducers.Length)
                    {
                        A[] n = new A[t.nextReducer * 2];
                        Array.Copy(t.reducers, n, t.nextReducer);
                        t.reducers = n;
                    }
                    t.reducers[t.nextReducer] = factory();
                    t.reducers[t.nextReducer].InitialAdd(val(record));
                    t.index.Add(k, t.nextReducer);
                    ++t.nextReducer;
                }
            }
        }

        public override void OnNotify(T time)
        {
            Time rt = reducers[time];

            var output = this.Output.GetBufferForTime(time);

            foreach (var r in rt.index)
                output.Send(new Pair<K, X>(r.Key, rt.reducers[r.Value].State()));

            reducers.Remove(time);
        }

        public LocalTimeKeyedReduceVertex(
            int i, Stage<T> stage, Func<I, K> k, Func<I, R> v, Func<A> f)
            : base(i, stage)
        {
            factory = f;
            key = k;
            val = v;
            reducers = new Dictionary<T, Time>();
        }
    }


    internal class LocalKeyedCombineVertex<A, X, R, S, K, T> : UnaryVertex<Pair<K, X>, Pair<K, S>, T>
        where A : IReducer<X, R, S>
        where T : Time<T>
    {
        private readonly Func<A> factory;
        private A[] reducers;
        private int nextReducer;
        private Dictionary<K, int> index;
        private T lastTime;
        private Int64 recordsIn;

        public override void OnReceive(Message<Pair<K, X>, T> message)
        {

            if (reducers == null)
            {
                reducers = new A[4];
                nextReducer = 0;
                index = new Dictionary<K, int>();
                lastTime = message.time;
                recordsIn = 0;
                NotifyAt(message.time);
            }
            else if (!message.time.Equals(lastTime))
            {
                throw new Exception("One time at a time please!");
            }

            for (int ii = 0; ii < message.length; ii++)
            {
                //this.OnRecv(message.payload[i].PairWith(message.time));

                var record = message.payload[ii];

                K k = record.First;
                int i;
                if (index.TryGetValue(k, out i))
                {
                    reducers[i].Combine(record.Second);
                }
                else
                {
                    if (nextReducer == reducers.Length)
                    {
                        A[] n = new A[nextReducer * 2];
                        Array.Copy(reducers, n, nextReducer);
                        reducers = n;
                    }
                    reducers[nextReducer] = factory();
                    reducers[nextReducer].InitialCombine(record.Second);
                    index.Add(k, nextReducer);
                    ++nextReducer;
                }

                ++recordsIn;

            }
        }



        public override void OnNotify(T time)
        {
            if (reducers != null)
            {
                if (!time.Equals(lastTime))
                {
                    throw new Exception("One time at a time please!");
                }

                Context.Reporting.LogAggregate("RecordsIn", Dataflow.Reporting.AggregateType.Sum, recordsIn, time);
                Context.Reporting.LogAggregate("RecordsOut", Dataflow.Reporting.AggregateType.Sum, index.Count, time);

                var output = this.Output.GetBufferForTime(time);
                foreach (var r in index)
                    output.Send(new Pair<K, S>(r.Key, reducers[r.Value].Value()));

                reducers = null;
                index = null;
                nextReducer = -1;
            }
        }

        public LocalKeyedCombineVertex(
            int i, Stage<T> stage, Func<A> f)
            : base(i, stage)
        {
            factory = f;
            lastTime = default(T);
            reducers = null;
            index = null;
            nextReducer = -1;
        }
    }

    internal class LocalTimeKeyedCombineVertex<A, X, R, S, K, T> : UnaryVertex<Pair<K, X>, Pair<K, S>, T>
        where A : IReducer<X, R, S>
        where T : Time<T>
    {
        private readonly Func<A> factory;
        private readonly Dictionary<T, Time> reducers;

        class Time
        {
            public A[] reducers;
            public int nextReducer;
            public Dictionary<K, int> index;

            public Time(int c)
            {
                reducers = new A[c];
                nextReducer = 0;
                index = new Dictionary<K, int>();
            }
        }

        public override void OnReceive(Message<Pair<K, X>, T> message)
        {
            Time r;
            if (!reducers.TryGetValue(message.time, out r))
            {
                r = new Time(4);
                reducers.Add(message.time, r);
                NotifyAt(message.time);
            }


            for (int ii = 0; ii < message.length; ii++)
            {
                //this.OnRecv(message.payload[i].PairWith(message.time));
                var record = message.payload[ii];

                K k = record.First;
                int i;
                if (r.index.TryGetValue(k, out i))
                {
                    r.reducers[i].Combine(record.Second);
                }
                else
                {
                    if (r.nextReducer == r.reducers.Length)
                    {
                        A[] n = new A[r.nextReducer * 2];
                        Array.Copy(r.reducers, n, r.nextReducer);
                        r.reducers = n;
                    }
                    r.reducers[r.nextReducer] = factory();
                    r.reducers[r.nextReducer].InitialCombine(record.Second);
                    r.index.Add(k, r.nextReducer);
                    ++r.nextReducer;
                }

            }
        }

        public override void OnNotify(T time)
        {
            Time rt = reducers[time];

            var output = this.Output.GetBufferForTime(time);
            foreach (var r in rt.index)
                output.Send(new Pair<K, S>(r.Key, rt.reducers[r.Value].Value()));

            reducers.Remove(time);
        }

        public LocalTimeKeyedCombineVertex(
            int i, Stage<T> stage, Func<A> f)
            : base(i, stage)
        {
            factory = f;
            reducers = new Dictionary<T, Time>();
        }
    }

    internal interface Cloneable<T>
    {
        // Return a copy suitable for broadcasting. Can be a shallow copy if the object
        // is known to be immutable
        T MakeCopy();
    }

    internal class BroadcastSendVertex<R, T> : UnaryVertex<R, Pair<int, R>, T>
        where R : Cloneable<R>
        where T : Time<T>
    {
        private readonly int[] destinations;

        public override void OnReceive(Message<R, T> message)
        {
            var output = this.Output.GetBufferForTime(message.time);
            for (int i = 0; i < message.length; i++)
            {
                //this.OnRecv(message.payload[i].PairWith(message.time));
                var record = message.payload[i];
                for (int j = 0; j < destinations.Length; ++i)
                {
                    if (j < destinations.Length - 1)
                    {
                        output.Send(new Pair<int, R>(destinations[j], record.MakeCopy()));
                    }
                    else
                    {
                        output.Send(new Pair<int, R>(destinations[j], record));
                    }
                }

            }
        }

        public override void OnNotify(T time) { }

        public BroadcastSendVertex(int index, Stage<T> stage, int[] dest)
            : base(index, stage)
        {
            destinations = dest;
        }
    }

    internal class BroadcastForwardVertex<R, T> : UnaryVertex<Pair<int, R>, Pair<int, R>, T>
        where R : Cloneable<R>
        where T : Time<T>
    {
        private readonly int[] destinations;

        public override void OnReceive(Message<Pair<int, R>, T> message)
        {
            var output = this.Output.GetBufferForTime(message.time);
            for (int i = 0; i < message.length; i++)
            {
                var record = message.payload[i];
                for (int j = 0; j < destinations.Length; ++j)
                {
                    if (j < destinations.Length - 1)
                    {
                        output.Send(new Pair<int, R>(destinations[j], record.Second.MakeCopy()));
                    }
                    else
                    {
                        output.Send(new Pair<int, R>(destinations[j], record.Second));
                    }
                }

            }
        }

        public override void OnNotify(T time) { }

        public BroadcastForwardVertex(int index, Stage<T> stage, int[] dest)
            : base(index, stage)
        {
            destinations = dest;
        }
    }
}
