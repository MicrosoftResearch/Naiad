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

using System.Collections.Concurrent;
using System.Linq.Expressions;
using Naiad.DataStructures;
using Naiad.FaultTolerance;
using Naiad.Dataflow.Channels;
using Naiad.CodeGeneration;
using Naiad;
using Naiad.Dataflow;
using Naiad.Frameworks;

namespace Naiad.Frameworks.DifferentialDataflow.Operators
{
    internal class Join<K, V1, V2, S1, S2, T, R> : OperatorImplementations.BinaryStatefulOperator<K, V1, V2, S1, S2, T, R>
        where K : IEquatable<K>
        where V1 : IEquatable<V1>
        where V2 : IEquatable<V2>
        where S1 : IEquatable<S1>
        where S2 : IEquatable<S2>
        where T : Time<T>
        where R : IEquatable<R>
    {
        Func<K, V1, V2, R> resultSelector;

        // performs the same role as keyIndices, just with less memory.
        Dictionary<K, JoinKeyIndices> JoinKeys;

        NaiadList<int> times = new NaiadList<int>(1);
        NaiadList<Weighted<V1>> differences1 = new NaiadList<Weighted<V1>>(1);
        NaiadList<Weighted<V2>> differences2 = new NaiadList<Weighted<V2>>(1);

        public override void OnInput1(Weighted<S1> entry, T time)
        {
            //Console.WriteLine("Join Recv1");

            var k = key1(entry.record);
            var v = value1(entry.record);

            var state = new JoinKeyIndices();
            if (!JoinKeys.TryGetValue(k, out state))
                state = new JoinKeyIndices();

            if (!inputShutdown2)//!this.inputImmutable2)
            {
                inputTrace1.EnsureStateIsCurrentWRTAdvancedTimes(ref state.processed1);
                inputTrace1.Introduce(ref state.processed1, v, entry.weight, internTable.Intern(time));
            } 

            if (inputShutdown1)
                Console.Error.WriteLine("Error in Join input shutdown1");

            if (state.processed2 != 0)
            {
                inputTrace2.EnsureStateIsCurrentWRTAdvancedTimes(ref state.processed2);

                times.Clear();
                inputTrace2.EnumerateTimes(state.processed2, times);

                for (int i = 0; i < times.Count; i++)
                {
                    differences2.Clear();
                    inputTrace2.EnumerateDifferenceAt(state.processed2, times.Array[i], differences2);
                    var newTime = time.Join(internTable.times[times.Array[i]]);

                    for (int j = 0; j < differences2.Count; j++)
                        if (differences2.Array[j].weight != 0)
                        {
                            //Send(resultSelector(k, v, differences2.Array[j].record).ToNaiadRecord(entry.weight * differences2.Array[j].weight, newTime));
                            var result = resultSelector(k, v, differences2.Array[j].record);
                            var weight = entry.weight * differences2.Array[j].weight;
                            this.Output.Buffer.payload[this.Output.Buffer.length++] = new Pair<Weighted<R>, T>(new Weighted<R>(result, weight), newTime);

                            if (this.Output.Buffer.length == this.Output.Buffer.payload.Length)
                            {
                                this.Output.Send(this.Output.Buffer);
                                this.Output.Buffer.length = 0;
                            }
                        }
                }
            }

            if (state.IsEmpty)
                JoinKeys.Remove(k);
            else
                JoinKeys[k] = state;
        }

        public override void OnInput2(Weighted<S2> entry, T time)
        {
            var k = key2(entry.record);
            var v = value2(entry.record);

            var state = new JoinKeyIndices();
            if (!JoinKeys.TryGetValue(k, out state))
                state = new JoinKeyIndices();

            if (!inputShutdown1)//!this.inputImmutable1)
            {
                inputTrace2.EnsureStateIsCurrentWRTAdvancedTimes(ref state.processed2);
                inputTrace2.Introduce(ref state.processed2, v, entry.weight, internTable.Intern(time));
            }

            if (inputShutdown2)
                Console.Error.WriteLine("Error in Join input shutdown2");

            if (state.processed1 != 0)
            {
                inputTrace1.EnsureStateIsCurrentWRTAdvancedTimes(ref state.processed1);

                times.Clear();
                inputTrace1.EnumerateTimes(state.processed1, times);

                for (int i = 0; i < times.Count; i++)
                {
                    differences1.Clear();
                    inputTrace1.EnumerateDifferenceAt(state.processed1, times.Array[i], differences1);
                    var newTime = time.Join(internTable.times[times.Array[i]]);

                    for (int j = 0; j < differences1.Count; j++)
                        if (differences1.Array[j].weight != 0)
                        {
                            //Send(resultSelector(k, differences1.Array[j].record, v).ToNaiadRecord(entry.weight * differences1.Array[j].weight, newTime));
                            var result = resultSelector(k, differences1.Array[j].record, v);
                            var weight = entry.weight * differences1.Array[j].weight;
                            this.Output.Buffer.payload[this.Output.Buffer.length++] = new Pair<Weighted<R>, T>(new Weighted<R>(result, weight), newTime);

                            if (this.Output.Buffer.length == this.Output.Buffer.payload.Length)
                            {
                                this.Output.Send(this.Output.Buffer);
                                this.Output.Buffer.length = 0;
                            }
                        }
                }
            }

            if (state.IsEmpty)
                JoinKeys.Remove(k);
            else
                JoinKeys[k] = state;
        }

        protected override void OnShutdown()
        {
            base.OnShutdown();

            //Console.Error.WriteLine("Shutting down Join: {0}", this);
            JoinKeys = null;
            times = null;
            difference1 = null;
            difference2 = null;
        }
        
        bool inputShutdown1 = false;    // set once an input is drained (typically: immutable, read once)
        bool inputShutdown2 = false;    // set once an input is drained (typically: immutable, read once)

        public override void OnDone(T workTime)
        {
            // if input is immutable, we can shut down the other trace
            if (this.inputImmutable1 && !inputShutdown1)
            {
                Logging.Info("{0}: Shutting down input1; nulling input2", this);
                inputTrace2 = null;
                inputShutdown1 = true;
            }
            // if input is immutable, we can shut down the other trace
            if (this.inputImmutable2 && !inputShutdown2)
            {
                Logging.Info("{0}: Shutting down input2; nulling input1", this);
                inputTrace1 = null;
                inputShutdown2 = true;
            }

            base.OnDone(workTime);
        }

        #region Checkpointing

        /* Checkpoint format:
         * (base)
         * if !terminated
         *     Dictionary<K,JoinKeyIndices> JoinKeys
         */

        public override void Checkpoint(NaiadWriter writer)
        {
            base.Checkpoint(writer);
            if (!this.isShutdown)
            {
                this.JoinKeys.Checkpoint(writer, keySerializer, JoinKeyIndices.Serializer);
            }
        }

        public override void Restore(NaiadReader reader)
        {
            base.Restore(reader);
            if (!this.isShutdown)
            {
                this.JoinKeys.Restore(reader, keySerializer, JoinKeyIndices.Serializer);
            }
        }

        #endregion

        public override void MessageReceived1(Message<Pair<Weighted<S1>, T>> message)
        {
            for (int i = 0; i < message.length; i++)
            {
                this.OnInput1(message.payload[i].v1, message.payload[i].v2);
                this.NotifyAt(message.payload[i].v2);
            }
        }

        public override void MessageReceived2(Message<Pair<Weighted<S2>, T>> message)
        {
            for (int i = 0; i < message.length; i++)
            {
                this.OnInput2(message.payload[i].v1, message.payload[i].v2);
                this.NotifyAt(message.payload[i].v2);
            }            
        }

        public Join(int index, Stage<T> collection, bool input1Immutable, bool input2Immutable, Expression<Func<S1, K>> k1, Expression<Func<S2, K>> k2, Expression<Func<S1, V1>> v1, Expression<Func<S2, V2>> v2, Expression<Func<K, V1, V2, R>> r)
            : base(index, collection, input1Immutable, input2Immutable, k1, k2, v1, v2)
        {
            resultSelector = r.Compile();
            keyIndices = new Dictionary<K,BinaryKeyIndices>();
            JoinKeys = new Dictionary<K, JoinKeyIndices>();

            //collection.LeftInput.Register(new ActionReceiver<Weighted<S1>, T>(this, x => { OnInput1(x.s, x.t); this.ScheduleAt(x.t); }));
            //collection.RightInput.Register(new ActionReceiver<Weighted<S2>, T>(this, x => { OnInput2(x.s, x.t); this.ScheduleAt(x.t); }));

            //this.input1 = new ActionReceiver<Weighted<S1>, T>(this, x => { this.OnInput1(x.s, x.t); this.ScheduleAt(x.t); });
            //this.input2 = new ActionReceiver<Weighted<S2>, T>(this, x => { this.OnInput2(x.s, x.t); this.ScheduleAt(x.t); });
        }
    }

    internal class JoinIntKeyed<V1, V2, S1, S2, T, R> : OperatorImplementations.BinaryStatefulIntKeyedOperator<V1, V2, S1, S2, T, R>
        where V1 : IEquatable<V1>
        where V2 : IEquatable<V2>
        where S1 : IEquatable<S1>
        where S2 : IEquatable<S2>
        where T : Time<T>
        where R : IEquatable<R>
    {
        Func<Int32, V1, V2, R> resultSelector;

        // performs the same role as keyIndices, just with less memory.
        JoinIntKeyIndices[][] JoinKeys;

        NaiadList<int> times = new NaiadList<int>(1);
        NaiadList<Weighted<V1>> differences1 = new NaiadList<Weighted<V1>>(1);
        NaiadList<Weighted<V2>> differences2 = new NaiadList<Weighted<V2>>(1);

        private int parts;

        public override void OnInput1(Weighted<S1> entry, T time)
        {
            var k = key1(entry.record);
            var v = value1(entry.record);

            var index = k / parts;

            if (JoinKeys[index / 65536] == null)
                JoinKeys[index / 65536] = new JoinIntKeyIndices[65536];

            var state = JoinKeys[index / 65536][index % 65536];

            if (!inputShutdown2)
            {
                inputTrace1.EnsureStateIsCurrentWRTAdvancedTimes(ref state.processed1);
                inputTrace1.Introduce(ref state.processed1, v, entry.weight, internTable.Intern(time));
            }

            if (inputShutdown1)
                Console.Error.WriteLine("Error in Join; input1 shutdown but recv'd input2");

            if (state.processed2 != 0)
            {
                inputTrace2.EnsureStateIsCurrentWRTAdvancedTimes(ref state.processed2);

                times.Clear();
                inputTrace2.EnumerateTimes(state.processed2, times);

                for (int i = 0; i < times.Count; i++)
                {
                    differences2.Clear();
                    inputTrace2.EnumerateDifferenceAt(state.processed2, times.Array[i], differences2);
                    var newTime = time.Join(internTable.times[times.Array[i]]);

                    for (int j = 0; j < differences2.Count; j++)
                        if (differences2.Array[j].weight != 0)
                            this.Output.Send(resultSelector(k, v, differences2.Array[j].record).ToWeighted(entry.weight * differences2.Array[j].weight), newTime);
                }
            }

            JoinKeys[index / 65536][index % 65536] = state;
        }

        public override void OnInput2(Weighted<S2> entry, T time)
        {
            var k = key2(entry.record);
            var v = value2(entry.record);

            var index = k / parts;
             
            if (JoinKeys[index / 65536] == null)
                JoinKeys[index / 65536] = new JoinIntKeyIndices[65536];

            var state = JoinKeys[index / 65536][index % 65536];

            if (!inputShutdown1)
            {
                inputTrace2.EnsureStateIsCurrentWRTAdvancedTimes(ref state.processed2);
                inputTrace2.Introduce(ref state.processed2, v, entry.weight, internTable.Intern(time));
            }

            if (inputShutdown2)
                Console.Error.WriteLine("Error in Join; input2 shutdown but recv'd input1");

            if (state.processed1 != 0)
            {
                inputTrace1.EnsureStateIsCurrentWRTAdvancedTimes(ref state.processed1);

                times.Clear();
                inputTrace1.EnumerateTimes(state.processed1, times);

                for (int i = 0; i < times.Count; i++)
                {
                    differences1.Clear();
                    inputTrace1.EnumerateDifferenceAt(state.processed1, times.Array[i], differences1);
                    var newTime = time.Join(internTable.times[times.Array[i]]);

                    for (int j = 0; j < differences1.Count; j++)
                        if (differences1.Array[j].weight != 0)
                            this.Output.Send(resultSelector(k, differences1.Array[j].record, v).ToWeighted(entry.weight * differences1.Array[j].weight), newTime);
                }
            }

            JoinKeys[index / 65536][index % 65536] = state;
        }

#if false
        public override void DoWork(Naiad.Scheduling.VertexVersion version)
        {
            base.DoWork(version);

            if (!inputShutdown1)
            {
                var immutable1 = inputTrace1 as CollectionTrace.CollectionTraceImmutable<V1>;
                if (immutable1 != null)
                {
                    //Console.Error.WriteLine("Join: Shutting down input1");
                    inputShutdown1 = true;
                    inputTrace2 = null;
                }
            }

            if (!inputShutdown2)
            {
                var immutable2 = inputTrace2 as CollectionTrace.CollectionTraceImmutable<V2>;
                if (immutable2 != null)
                {
                    //Console.Error.WriteLine("Join: Shutting down input2");
                    inputShutdown2 = true;
                    inputTrace1 = null;
                }
            }
        }
#endif
        protected override void OnShutdown()
        {
            base.OnShutdown();

            //Console.Error.WriteLine("Shutting down Join: {0}", this);
            JoinKeys = null;
            times = null;
            differences1 = null;
            differences2 = null;
        }

        bool inputShutdown1 = false;    // set once an input is drained (typically: immutable, read once)
        bool inputShutdown2 = false;    // set once an input is drained (typically: immutable, read once)

        public override void OnDone(T workTime)
        {
            if (this.inputImmutable1 && this.inputTrace1 != null)
                this.inputTrace1.Compact();

            if (this.inputImmutable2 && this.inputTrace2 != null)
                this.inputTrace2.Compact();

            // if input is immutable, we can shut down the other trace
            if (this.inputImmutable1 && !inputShutdown1)
            {
                //Console.Error.WriteLine("{0}: Shutting down input1; nulling input2", this);
                Logging.Info("{0}: Shutting down input1; nulling input2", this);
                inputTrace2 = null;
                inputShutdown1 = true;
            }
            // if input is immutable, we can shut down the other trace
            if (this.inputImmutable2 && !inputShutdown2)
            {
                //Console.Error.WriteLine("{0}: Shutting down input2; nulling input1", this);
                Logging.Info("{0}: Shutting down input2; nulling input1", this);
                inputTrace1 = null;
                inputShutdown2 = true;
            }

            base.OnDone(workTime);
        }

        #region Checkpointing

        /* Checkpoint format:
         * (base)
         * if !terminated
         *     int                                              keyIndicesLength
         *     (int n,n*BinaryKeyIndices|-1)*keyIndicesLength   keyIndices
         */

        public override void Checkpoint(NaiadWriter writer)
        {
            base.Checkpoint(writer);
            if (!this.isShutdown)
            {
                for (int i = 0; i < this.JoinKeys.Length; ++i)
                {
                    if (this.JoinKeys[i] == null)
                        writer.Write(-1, PrimitiveSerializers.Int32);
                    else
                    {
                        writer.Write(this.JoinKeys[i].Length, PrimitiveSerializers.Int32);
                        for (int j = 0; j < this.JoinKeys[i].Length; ++j)
                            writer.Write(this.JoinKeys[i][j], JoinIntKeyIndices.Serializer);
                    }
                }
            }
        }

        public override void Restore(NaiadReader reader)
        {
            base.Restore(reader);
            if (!this.isShutdown)
            {
                for (int i = 0; i < this.JoinKeys.Length; ++i)
                {
                    int count = reader.Read<int>(PrimitiveSerializers.Int32);
                    if (count >= 0)
                    {
                        this.JoinKeys[i] = new JoinIntKeyIndices[count];
                        for (int j = 0; j < this.JoinKeys[i].Length; ++j)
                            this.JoinKeys[i][j] = reader.Read<JoinIntKeyIndices>(JoinIntKeyIndices.Serializer);
                    }
                    else
                        this.JoinKeys[i] = null;
                }
            }
        }

        #endregion

        public override void MessageReceived1(Message<Pair<Weighted<S1>, T>> message)
        {
            for (int i = 0; i < message.length; i++)
            {
                this.OnInput1(message.payload[i].v1, message.payload[i].v2);
                this.NotifyAt(message.payload[i].v2);
            }
        }

        public override void MessageReceived2(Message<Pair<Weighted<S2>, T>> message)
        {
            for (int i = 0; i < message.length; i++)
            {
                this.OnInput2(message.payload[i].v1, message.payload[i].v2);
                this.NotifyAt(message.payload[i].v2);
            }
        }

        public JoinIntKeyed(int index, Stage<T> collection, bool input1Immutable, bool input2Immutable, Expression<Func<S1, Int32>> k1, Expression<Func<S2, Int32>> k2, Expression<Func<S1, V1>> v1, Expression<Func<S2, V2>> v2, Expression<Func<Int32, V1, V2, R>> r)
            : base(index, collection, input1Immutable, input2Immutable, k1, k2, v1, v2)
        {
            resultSelector = r.Compile();
            
            // Inhibits verbose serialization of the parent's keyIndices.
            keyIndices = new BinaryKeyIndices[0][];

            JoinKeys = new JoinIntKeyIndices[65536][];
            this.parts = collection.Placement.Count;
        }
    }
}
