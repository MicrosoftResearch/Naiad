/*
 * Naiad ver. 0.4
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

using Microsoft.Research.Naiad.Serialization;
using Microsoft.Research.Naiad.DataStructures;
using Microsoft.Research.Naiad.Dataflow.Channels;
using Microsoft.Research.Naiad.Scheduling;
using Microsoft.Research.Naiad.Frameworks.DifferentialDataflow.CollectionTrace;

using System.Linq.Expressions;
using System.Diagnostics;
using Microsoft.Research.Naiad.Runtime.Progress;
using Microsoft.Research.Naiad;
using Microsoft.Research.Naiad.Dataflow;
using Microsoft.Research.Naiad.Dataflow.StandardVertices;

namespace Microsoft.Research.Naiad.Frameworks.DifferentialDataflow.OperatorImplementations
{
    internal class UnaryStatefulIntKeyedOperator<V, S, T, R> : UnaryBufferingVertex<Weighted<S>, Weighted<R>, T>
        where V : IEquatable<V>
        where S : IEquatable<S>
        where T : Time<T>
        where R : IEquatable<R>
    {
        bool inputImmutable = false;

        public override void OnReceive(Message<Weighted<S>, T> message)
        {
            if (this.inputImmutable)
            {
                this.NotifyAt(message.time);
                for (int i = 0; i < message.length; i++)
                    this.OnInput(message.payload[i], message.time);
            }
            else
                base.OnReceive(message);
        }

        protected CollectionTraceWithHeap<R> createOutputTrace()
        {
            return new CollectionTraceWithHeap<R>((x, y) => internTable.LessThan(x, y), x => internTable.UpdateTime(x), this.Stage.Placement.Count);
        }

        protected virtual CollectionTraceCheckpointable<V> createInputTrace()
        {

            if (Microsoft.Research.Naiad.Utilities.ExpressionComparer.Instance.Equals(keyExpression, valueExpression))
            {
                if (this.inputImmutable)
                    return new CollectionTraceImmutableNoHeap<V>();
                else
                    return new CollectionTraceWithoutHeap<V>((x, y) => internTable.LessThan(x, y),
                                                                   x => internTable.UpdateTime(x));
            }
            else
            {
                if (this.inputImmutable)
                    return new CollectionTraceImmutable<V>();
                else
                    return new CollectionTraceWithHeap<V>((x, y) => internTable.LessThan(x, y),
                                                                 x => internTable.UpdateTime(x), this.Stage.Placement.Count);
            }
        }

        protected override void OnShutdown()
        {
            base.OnShutdown();

            if (inputTrace != null)
                inputTrace.Release();
            inputTrace = null;

            if (outputTrace != null)
                outputTrace.Release();
            outputTrace = null;

            internTable = null;
            keyIndices = null;
            keysToProcess = null;
        }

        protected override void UpdateReachability(List<Pointstamp> causalTimes)
        {
            base.UpdateReachability(causalTimes);

            if (causalTimes != null && internTable != null)
                internTable.UpdateReachability(causalTimes);
        }

        public readonly Func<S, int> key;      // extracts the key from the input record
        public readonly Func<S, V> value;    // reduces input record to relevant value

        public readonly Expression<Func<S, int>> keyExpression;
        public readonly Expression<Func<S, V>> valueExpression;

        readonly bool MaintainOutputTrace;

        public override void OnNotify(T workTime)
        {
            if (!this.inputImmutable)
                foreach (var item in this.Input.GetRecordsAt(workTime))
                    OnInput(item, workTime);

            Compute();

            Flush();

            //if (this.inputImmutable)
            //    this.ShutDown();
        }

        protected LatticeInternTable<T> internTable;
        protected CollectionTraceCheckpointable<V> inputTrace;          // collects all differences that have processed.
        protected CollectionTraceCheckpointable<R> outputTrace;             // collects outputs

        protected UnaryKeyIndices[][] keyIndices;

        protected NaiadList<int> keysToProcess = new NaiadList<int>(1);

        public virtual void OnInput(Weighted<S> entry, T time)
        {
            var k = key(entry.record);

            var index = (int)(k / this.Stage.Placement.Count);

            if (keyIndices[index / 65536] == null)
                keyIndices[index / 65536] = new UnaryKeyIndices[65536];

            keysToProcess.Add(index);

            inputTrace.Introduce(ref keyIndices[index / 65536][index % 65536].unprocessed, value(entry.record), entry.weight, internTable.Intern(time));
        }

        public virtual void Compute()
        {
            for (int i = 0; i < keysToProcess.Count; i++)
                Update(keysToProcess.Array[i]);

            inputTrace.Compact();
            outputTrace.Compact();

            keysToProcess.Clear();
        }

        protected NaiadList<Weighted<V>> collection = new NaiadList<Weighted<V>>(1);
        protected NaiadList<Weighted<V>> difference = new NaiadList<Weighted<V>>(1);

        // Moves from unprocessed[key] to processed[key], updating output[key] and Send()ing.
        protected int outputWorkspace;
        protected virtual void Update(int index)
        {
            var traceIndices = keyIndices[index / 65536][index % 65536];

            if (traceIndices.unprocessed != 0)
            {
                inputTrace.EnsureStateIsCurrentWRTAdvancedTimes(ref traceIndices.processed);
                //inputTrace.EnsureStateIsCurrentWRTAdvancedTimes(ref traceIndices.unprocessed);

                if (MaintainOutputTrace)
                    outputTrace.EnsureStateIsCurrentWRTAdvancedTimes(ref traceIndices.output);

                // iterate through the times that may require updates.
                var interestingTimes = InterestingTimes(traceIndices);

                // incorporate the updates, so we can compare old and new outputs.
                inputTrace.IntroduceFrom(ref traceIndices.processed, ref traceIndices.unprocessed, false);

                for (int i = 0; i < interestingTimes.Count; i++)
                    UpdateTime(index, traceIndices, interestingTimes.Array[i]);

                // clean out the state we just processed
                inputTrace.ZeroState(ref traceIndices.unprocessed);

                // move the differences we produced from local to persistent storage.
                if (MaintainOutputTrace)
                    outputTrace.IntroduceFrom(ref traceIndices.output, ref outputWorkspace);
                else
                    outputTrace.ZeroState(ref outputWorkspace);

                keyIndices[index / 65536][index % 65536] = traceIndices;
            }
        }

        protected NaiadList<int> timeList = new NaiadList<int>(1);
        protected NaiadList<int> truthList = new NaiadList<int>(1);
        protected NaiadList<int> deltaList = new NaiadList<int>(1);

        protected virtual NaiadList<int> InterestingTimes(UnaryKeyIndices keyIndices)
        {
            deltaList.Clear();
            inputTrace.EnumerateTimes(keyIndices.unprocessed, deltaList);

            truthList.Clear();
            inputTrace.EnumerateTimes(keyIndices.processed, truthList);

            timeList.Clear();
            this.internTable.InterestingTimes(timeList, truthList, deltaList);

            return timeList;
        }

        protected virtual void UpdateTime(int index, UnaryKeyIndices keyIndices, int timeIndex)
        {
            // subtract out prior output updates before adding new ones
            outputTrace.SubtractStrictlyPriorDifferences(ref outputWorkspace, timeIndex);

            NewOutputMinusOldOutput(index, keyIndices, timeIndex);

            var outputTime = this.internTable.times[timeIndex];

            toSend.Clear();
            outputTrace.EnumerateDifferenceAt(outputWorkspace, timeIndex, toSend);

            var output = this.Output.GetBufferForTime(outputTime);

            for (int i = 0; i < toSend.Count; i++)
                output.Send(toSend.Array[i]);
        }

        protected NaiadList<Weighted<R>> toSend = new NaiadList<Weighted<R>>(1);
        protected virtual void NewOutputMinusOldOutput(int index, UnaryKeyIndices keyIndices, int timeIndex)
        {
            if (!MaintainOutputTrace)
                throw new Exception("Override NewOutputMinusOldOutput or set MaintainOutputTrace");

            if (keyIndices.processed != 0)
                Reduce(index, keyIndices, timeIndex);

            toSend.Clear();
            outputTrace.EnumerateCollectionAt(keyIndices.output, timeIndex, toSend);
            for (int i = 0; i < toSend.Count; i++)
                outputTrace.Introduce(ref outputWorkspace, toSend.Array[i].record, -toSend.Array[i].weight, timeIndex);
        }

        // expected to populate resultList to match reduction(collection.source)
        protected virtual void Reduce(int index, UnaryKeyIndices keyIndices, int time) 
        {
            var key = index * this.Stage.Placement.Count + this.VertexId;
        }

        /* Checkpoint format:
         * bool terminated
         * if !terminated:
         *     LatticeInternTable<T>                            internTable
         *     CollectionTrace<>                                inputTrace
         *     CollectionTrace<>                                outputTrace
         *     int                                              keyIndicesCount
         *     (K,KeyIndices)*keyIndicesCount                   keyIndices
         *     int                                              recordsToProcessCount
         *     (T,NaiadList<Weighted<S>>)*recordsToProcessCount recordsToProcess
         */

        // keyIndices is spined list

        protected override void Checkpoint(NaiadWriter writer)
        {
            base.Checkpoint(writer);
            writer.Write(this.isShutdown);
            if (!this.isShutdown)
            {
                this.internTable.Checkpoint(writer);
                this.inputTrace.Checkpoint(writer);
                this.outputTrace.Checkpoint(writer);

                for (int i = 0; i < this.keyIndices.Length; ++i)
                {
                    if (this.keyIndices[i] == null)
                        writer.Write(0);
                    else
                    {
                        writer.Write(this.keyIndices[i].Length);
                        for (int j = 0; j < this.keyIndices[i].Length; ++j)
                            writer.Write(this.keyIndices[i][j]);
                    }
                }

                this.keysToProcess.Checkpoint(writer);

                this.Input.Checkpoint(writer);

            }
        }

        protected override void Restore(NaiadReader reader)
        {
            base.Restore(reader);
            this.isShutdown = reader.Read<bool>();

            if (!this.isShutdown)
            {
                this.internTable.Restore(reader);
                this.inputTrace.Restore(reader);
                this.outputTrace.Restore(reader);

                for (int i = 0; i < this.keyIndices.Length; ++i)
                {
                    int length = reader.Read<int>();
                    if (length == 0)
                        this.keyIndices[i] = null;
                    else
                    {
                        Debug.Assert(length == 65536);
                        this.keyIndices[i] = new UnaryKeyIndices[length];
                        for (int j = 0; j < this.keyIndices[i].Length; ++j)
                            this.keyIndices[i][j] = reader.Read<UnaryKeyIndices>();
                    }
                }

                this.keysToProcess.Restore(reader);

                this.Input.Restore(reader);
            }
        }

        public UnaryStatefulIntKeyedOperator(int index, Stage<T> collection, bool immutableInput, Expression<Func<S, int>> k, Expression<Func<S, V>> v, bool maintainOutputTrace = true)
            : base(index, collection, null)
        {
            key = k.Compile();
            value = v.Compile();

            keyExpression = k;
            valueExpression = v;

            MaintainOutputTrace = maintainOutputTrace;

            if (immutableInput)
                this.inputImmutable = true;

            internTable = new LatticeInternTable<T>();
            keyIndices = new UnaryKeyIndices[65536][];
            inputTrace = createInputTrace();
            outputTrace = createOutputTrace();

            //this.Input = new Naiad.Frameworks.RecvFiberBank<Weighted<S>, T>(this, collection.Input);

            //if (inputImmutable)
            //    collection.Input.Register(new ActionReceiver<Weighted<S>, T>(this, x => { this.OnInput(x.s, x.t); this.ScheduleAt(x.t); }));

#if false
            if (this.inputImmutable)
                this.input = new ActionReceiver<Weighted<S>, T>(this, x => { this.OnInput(x.s, x.t); this.ScheduleAt(x.t); });
            else
            {
                this.RecvInput = new Naiad.Frameworks.RecvFiberBank<Weighted<S>, T>(this);
                this.input = this.RecvInput;
            }
#endif
        }
    }
}
