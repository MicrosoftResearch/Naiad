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
using System.IO;

using Naiad.DataStructures;
using Naiad.Dataflow.Channels;
using Naiad.FaultTolerance;
using Naiad.CodeGeneration;
using Naiad;
using Naiad.Dataflow;

namespace Naiad.Frameworks.DifferentialDataflow.Operators
{
    internal class Consolidate<S, T> : UnaryVertex<Weighted<S>, Weighted<S>, T>
        //Naiad.Frameworks.DifferentialDataflow.OperatorImplementations.UnaryOperator<S, S, T> //NaiadLINQ.OperatorImplementations.UnaryStatefulOperator<S, S, S, T, S>
        where S : IEquatable<S>
        where T : Naiad.Time<T>
    {
        //protected ActionReceiver<Weighted<S>, T> input;
        //internal override ShardInput<Weighted<S>, T> Input { get { return this.input; } }

        Dictionary<T, Dictionary<S, Int64>> accumulations = new Dictionary<T, Dictionary<S, Int64>>();

        T recentTime;
        Dictionary<S, Int64> recentAccum;
        bool recentTimeValid = false;

        public void OnRecv(Message<Pair<Weighted<S>, T>> message)
        {
            for (int i = 0; i < message.length; i++)
                this.OnRecv(message.payload[i].v1, message.payload[i].v2);
        }

        private void OnRecv(Weighted<S> record, T time)
        {
            Dictionary<S, Int64> accum;

            if (recentTimeValid && recentTime.Equals(time))
            {
                accum = recentAccum;
            }
            else
            {
                if (!accumulations.TryGetValue(time, out accum))
                {
                    accum = new Dictionary<S, Int64>();
                    accumulations.Add(time, accum);

                    this.NotifyAt(time);
                }

                recentAccum = accum;
                recentTime = time;
                recentTimeValid = true;
            }

            // update dictionary, adding/removing if needed
            Int64 count;
            accum.TryGetValue(record.record, out count);
            if (count + record.weight == 0)
                accum.Remove(record.record);
            else
                accum[record.record] = count + record.weight;
        }

        public override void MessageReceived(Message<Pair<Weighted<S>, T>> message)
        {
            for (int i = 0; i < message.length; i++)
                this.OnRecv(message.payload[i].v1, message.payload[i].v2);
        }

        //public override void DoWork(Naiad.Scheduling.VertexVersion version)
        public override void OnDone(T time)
        {
            if (accumulations.Count > 0)
            {
                if (accumulations.ContainsKey(time))
                {
                    // send the accumulations at workTime
                    foreach (var pair in accumulations[time])
                        if (pair.Value != 0)
                            this.Output.Send(new Weighted<S>(pair.Key, pair.Value), time);

                    accumulations.Remove(time);

                    if (recentTime.Equals(time))
                    {
                        recentTimeValid = false;
                        recentAccum = null;
                    }

                    Flush();
                }
            }
        }

        /* Checkpoint format:
         * (base)
         * if !terminated
         *     int                                       accumulationsCount
         *     (T, Dictionary<S,int>)*accumulationsCount accumulations
         */
        private NaiadSerialization<S> sSerializer = null;

        public override void Checkpoint(NaiadWriter writer)
        {
            base.Checkpoint(writer);
            //if (!this.terminated)
            {
                var timeSerializer = AutoSerialization.GetSerializer<T>();

                if (sSerializer == null)
                    sSerializer = AutoSerialization.GetSerializer<S>();

                writer.Write(this.accumulations.Count, PrimitiveSerializers.Int32);
                foreach (var kvp in this.accumulations)
                {
                    writer.Write(kvp.Key, timeSerializer);
                    kvp.Value.Checkpoint(writer, sSerializer, PrimitiveSerializers.Int64);
                }
            }
        }

        public override void Restore(NaiadReader reader)
        {
            base.Restore(reader);
            //if (!this.terminated)
            {
                var timeSerializer = AutoSerialization.GetSerializer<T>();

                if (sSerializer == null)
                    sSerializer = AutoSerialization.GetSerializer<S>();

                this.accumulations.Clear();
                int accumulationsCount = reader.Read<int>(PrimitiveSerializers.Int32);
                for (int i = 0; i < accumulationsCount; ++i)
                {
                    T key = reader.Read<T>(timeSerializer);
                    Dictionary<S, Int64> value = new Dictionary<S, Int64>();
                    value.Restore(reader, sSerializer, PrimitiveSerializers.Int64);
                    this.accumulations[key] = value;
                }
            }
        }

        public Consolidate(int index, Stage<T> collection)
            : base(index, collection)
        {
            //this.input = new Naiad.Dataflow.ActionReceiver<Weighted<S>, T>(this, x => this.OnRecv(x));
        }
    }
#if false
    internal class ConsolidateInt64s<T> : Naiad.Frameworks.DifferentialDataflow.OperatorImplementations.UnaryOperator<Int64, Int64, T> //NaiadLINQ.OperatorImplementations.UnaryStatefulOperator<S, S, S, T, S>
        where T : Naiad.Lattice<T>
    {
        Dictionary<T, Int64[][]> accumulations = new Dictionary<T, Int64[][]>();

        T recentTime;
        Int64[][] recentAccum;
        bool recentTimeValid = false;

        private readonly int parts;

        public override void OnRecv(Weighted<Int64> record, T time)
        {
            Int64[][] accum;

            if (recentTimeValid && recentTime.Equals(time))
            {
                accum = recentAccum;
            }
            else
            {
                if (!accumulations.TryGetValue(time, out accum))
                {
                    accum = new Int64[65536][];

                    accumulations.Add(time, accum);

                    this.ScheduleAt(time);
                }

                recentAccum = accum;
                recentTime = time;
                recentTimeValid = true;
            }

            var index = record.record / parts;
            if (accum[index / 65536] == null)
                accum[index / 65536] = new Int64[65536];

            accum[index / 65536][index % 65536] += record.weight;
        }

        //public override void DoWork(Naiad.Scheduling.VertexVersion version)
        public override void ResumingAt(T time)
        {
            if (accumulations.Count > 0)
            {
                if (accumulations.ContainsKey(time))
                {
                    var accum = accumulations[time];
                    for (int i = 0; i < accum.Length; i++)
                    {
                        if (accum[i] != null)
                        {
                            var weights = accum[i];
                            for (int j = 0; j < accum[i].Length; j++)
                            {
                                if (weights[j] != 0)
                                {
                                    this.Output.Buffer.payload[this.Output.Buffer.length++] = new Pair<Weighted<Int64>, T>(new Weighted<Int64>((i * 65536 + j) * parts + this.ShardId, weights[j]), time);

                                    if (this.Output.Buffer.length == this.Output.Buffer.payload.Length)
                                    {
                                        this.Output.Send(this.Output.Buffer);
                                        this.Output.Buffer.length = 0;
                                    }
                                }
                            }
                        }

                        accum[i] = null; // return to a pool?
                    }

                    accumulations.Remove(time);

                    if (recentTime.Equals(time))
                    {
                        recentTimeValid = false;
                        recentAccum = null;
                    }

                    Flush();
                }
            }
        }

        /* Checkpoint format:
         * (base)
         * if !terminated
         *     int                                       accumulationsCount
         *     (T, Dictionary<S,int>)*accumulationsCount accumulations
         */
        private NaiadSerialization<Int64> sSerializer = null;

        public override void Checkpoint(NaiadWriter writer)
        {
            base.Checkpoint(writer);
            throw new NotImplementedException();

#if false
            //if (!this.terminated)
            {
                var timeSerializer = AutoSerialization.GetSerializer<T>();

                if (sSerializer == null)
                    sSerializer = AutoSerialization.GetSerializer<S>();

                writer.Write(this.accumulations.Count, PrimitiveSerializers.Int32);
                foreach (var kvp in this.accumulations)
                {
                    writer.Write(kvp.Key, timeSerializer);
                    kvp.Value.Checkpoint(writer, sSerializer, PrimitiveSerializers.Int64);
                }
            }
#endif
        }

        public override void Restore(NaiadReader reader)
        {
            base.Restore(reader);
            throw new NotImplementedException();

#if false
            //if (!this.terminated)
            {
                var timeSerializer = AutoSerialization.GetSerializer<T>();

                if (sSerializer == null)
                    sSerializer = AutoSerialization.GetSerializer<S>();

                this.accumulations.Clear();
                int accumulationsCount = reader.Read<int>(PrimitiveSerializers.Int32);
                for (int i = 0; i < accumulationsCount; ++i)
                {
                    T key = reader.Read<T>(timeSerializer);
                    Dictionary<S, Int64> value = new Dictionary<S, Int64>();
                    value.Restore(reader, sSerializer, PrimitiveSerializers.Int64);
                    this.accumulations[key] = value;
                }
            }
#endif
        }

        public ConsolidateInt64s(int index, UnaryCollectionVertex<Int64, Int64, T> collection)
            : base(index, collection)
        {
            this.parts = collection.Placement.Count;
        }
    }
#endif
}
