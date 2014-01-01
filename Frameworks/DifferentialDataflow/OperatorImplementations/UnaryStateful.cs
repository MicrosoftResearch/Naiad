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

using Naiad.DataStructures;
using Naiad.Dataflow.Channels;
using Naiad.Scheduling;
using Naiad.Frameworks.DifferentialDataflow.CollectionTrace;

using System.Linq.Expressions;
using System.Diagnostics;
using Naiad.FaultTolerance;
using Naiad.CodeGeneration;
using Naiad;
using Naiad.Dataflow;

namespace Naiad.Frameworks.DifferentialDataflow.OperatorImplementations
{
    internal class UnaryStatefulOperator<K, V, S, T, R> : UnaryBufferingVertex<Weighted<S>, Weighted<R>, T>
        where K : IEquatable<K>
        where V : IEquatable<V>
        where S : IEquatable<S>
        where T : Time<T>
        where R : IEquatable<R>
    {
        bool inputImmutable = false;

        public override void MessageReceived(Message<Pair<Weighted<S>, T>> message)
        {
            if (this.inputImmutable)
            {
                for (int i = 0; i < message.length; i++)
                {
                    this.OnInput(message.payload[i].v1, message.payload[i].v2);
                    this.NotifyAt(message.payload[i].v2);
                }
            }
            else
                base.MessageReceived(message);
        }

        protected CollectionTraceWithHeap<R> createOutputTrace()
        {
            return new CollectionTraceWithHeap<R>((x, y) => internTable.LessThan(x, y), x => internTable.UpdateTime(x), this.Stage.Placement.Count);
        }

        protected virtual CollectionTraceCheckpointable<V> createInputTrace()
        {

            if (Naiad.CodeGeneration.ExpressionComparer.Instance.Equals(keyExpression, valueExpression))
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

        public override void UpdateReachability(NaiadList<Pointstamp> causalTimes)
        {
            base.UpdateReachability(causalTimes);

            if (causalTimes != null && internTable != null)
                internTable.UpdateReachability(causalTimes);
        }

        public readonly Func<S, K> key;      // extracts the key from the input record
        public readonly Func<S, V> value;    // reduces input record to relevant value

        public readonly Expression<Func<S, K>> keyExpression;
        public readonly Expression<Func<S, V>> valueExpression;

        readonly bool MaintainOutputTrace;

        public override void OnDone(T workTime)
        {
            if (!this.inputImmutable)
                foreach (var record in this.Input.GetRecordsAt(workTime))
                    OnInput(record, workTime);

            Compute();

            Flush();

            if (inputTrace != null)
                inputTrace.Compact();

            //if (this.inputImmutable)
            //    this.ShutDown();
        }

        protected LatticeInternTable<T> internTable;
        protected CollectionTraceCheckpointable<V> inputTrace;          // collects all differences that have processed.
        protected CollectionTraceCheckpointable<R> outputTrace;             // collects outputs

        protected Dictionary<K, UnaryKeyIndices> keyIndices;
        protected NaiadList<K> keysToProcess = new NaiadList<K>(1);

        public virtual void OnInput(Weighted<S> entry, T time)
        {
            var k = key(entry.record);

            UnaryKeyIndices state;
            if (!keyIndices.TryGetValue(k, out state))
                state = new UnaryKeyIndices();

            if (state.unprocessed == 0)
                keysToProcess.Add(k);

            inputTrace.Introduce(ref state.unprocessed, value(entry.record), entry.weight, internTable.Intern(time));

            keyIndices[k] = state;
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
        protected virtual void Update(K key)
        {
            var traceIndices = new UnaryKeyIndices();

            if (keyIndices.TryGetValue(key, out traceIndices) && traceIndices.unprocessed != 0)
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
                    UpdateTime(key, traceIndices, interestingTimes.Array[i]);

                // clean out the state we just processed
                inputTrace.ZeroState(ref traceIndices.unprocessed);

                // move the differences we produced from local to persistent storage.
                if (MaintainOutputTrace)
                    outputTrace.IntroduceFrom(ref traceIndices.output, ref outputWorkspace);
                else
                    outputTrace.ZeroState(ref outputWorkspace);

                if (traceIndices.IsEmpty)
                    keyIndices.Remove(key);
                else
                    keyIndices[key] = traceIndices;
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

        protected virtual void UpdateTime(K key, UnaryKeyIndices keyIndices, int timeIndex)
        {
            // subtract out prior output updates before adding new ones
            outputTrace.SubtractStrictlyPriorDifferences(ref outputWorkspace, timeIndex);

            NewOutputMinusOldOutput(key, keyIndices, timeIndex);

            var outputTime = this.internTable.times[timeIndex];

            toSend.Clear();
            outputTrace.EnumerateDifferenceAt(outputWorkspace, timeIndex, toSend);
            for (int i = 0; i < toSend.Count; i++)
                this.Output.Send(toSend.Array[i], outputTime);
        }

        protected NaiadList<Weighted<R>> toSend = new NaiadList<Weighted<R>>(1);
        protected virtual void NewOutputMinusOldOutput(K key, UnaryKeyIndices keyIndices, int timeIndex)
        {
            if (!MaintainOutputTrace)
                throw new Exception("Override NewOutputMinusOldOutput or set MaintainOutputTrace");

            if (keyIndices.processed != 0)
                Reduce(key, keyIndices, timeIndex);

            toSend.Clear();
            outputTrace.EnumerateCollectionAt(keyIndices.output, timeIndex, toSend);
            for (int i = 0; i < toSend.Count; i++)
                outputTrace.Introduce(ref outputWorkspace, toSend.Array[i].record, -toSend.Array[i].weight, timeIndex);
        }

        // expected to populate resultList to match reduction(collection.source)
        protected virtual void Reduce(K key, UnaryKeyIndices keyIndices, int time) { }

        #region Checkpoint/Restore

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

        protected static NaiadSerialization<T> timeSerializer = null;
        protected static NaiadSerialization<K> keySerializer = null;
        protected static NaiadSerialization<Weighted<S>> weightedSSerializer = null;

        public override void Checkpoint(NaiadWriter writer)
        {
            base.Checkpoint(writer);
            writer.Write(this.isShutdown, PrimitiveSerializers.Bool);
            if (!this.isShutdown)
            {
                if (timeSerializer == null)
                    timeSerializer = AutoSerialization.GetSerializer<T>();
                if (keySerializer == null)
                    keySerializer = AutoSerialization.GetSerializer<K>();
                if (weightedSSerializer == null)
                    weightedSSerializer = AutoSerialization.GetSerializer<Weighted<S>>();

                this.internTable.Checkpoint(writer, timeSerializer);
                this.inputTrace.Checkpoint(writer);
                this.outputTrace.Checkpoint(writer);

                this.keyIndices.Checkpoint(writer, keySerializer, UnaryKeyIndices.Serializer);
                this.keysToProcess.Checkpoint(writer, keySerializer);

                this.Input.Checkpoint(writer);

                /*
                writer.Write(this.recordsToProcess.Count, PrimitiveSerializers.Int32);
                foreach (KeyValuePair<T, NaiadList<Weighted<S>>> kvp in this.recordsToProcess)
                {
                    writer.Write(kvp.Key, timeSerializer);
                    kvp.Value.Checkpoint(writer, weightedSSerializer);
                }
                 */
            }
        }

        public override void Restore(NaiadReader reader)
        {
            base.Restore(reader);
            this.isShutdown = reader.Read<bool>(PrimitiveSerializers.Bool);

            if (!this.isShutdown)
            {
                if (timeSerializer == null)
                    timeSerializer = AutoSerialization.GetSerializer<T>();
                if (keySerializer == null)
                    keySerializer = AutoSerialization.GetSerializer<K>();
                if (weightedSSerializer == null)
                    weightedSSerializer = AutoSerialization.GetSerializer<Weighted<S>>();

                this.internTable.Restore(reader, timeSerializer);
                this.inputTrace.Restore(reader);
                this.outputTrace.Restore(reader);

                this.keyIndices.Restore(reader, keySerializer, UnaryKeyIndices.Serializer);
                this.keysToProcess.Restore(reader, keySerializer);

                this.Input.Restore(reader);

                /*
                int recordsToProcessCount = reader.Read<int>(PrimitiveSerializers.Int32);

                foreach (NaiadList<Weighted<S>> recordList in this.recordsToProcess.Values)
                    recordList.Free();
                this.recordsToProcess.Clear();

                for (int i = 0; i < recordsToProcessCount; ++i)
                {
                    T key = reader.Read<T>(timeSerializer);
                    NaiadList<Weighted<S>> value = new NaiadList<Weighted<S>>();
                    value.Restore(reader, weightedSSerializer);
                    this.recordsToProcess[key] = value;
                }
                */
            }
        }

        #endregion

        public UnaryStatefulOperator(int index, Stage<T> collection, bool immutableInput, Expression<Func<S, K>> k, Expression<Func<S, V>> v, bool maintainOutputTrace = true)
            : base(index, collection, null)
        {
            key = k.Compile();
            value = v.Compile();

            keyExpression = k;
            valueExpression = v;

            MaintainOutputTrace = maintainOutputTrace;

            this.inputImmutable = immutableInput;

            internTable = new LatticeInternTable<T>();
            keyIndices = new Dictionary<K, UnaryKeyIndices>();
            inputTrace = createInputTrace();
            outputTrace = createOutputTrace();
        }
    }

    internal abstract class UnaryStatefulOperator<S, TS> : OperatorImplementations.UnaryStatefulOperator<S, S, S, TS, S>
        where S : IEquatable<S>
        where TS : Time<TS>
    {
        protected abstract Int64 WeightFunction(Int64 weight);

        protected override void NewOutputMinusOldOutput(S key, UnaryKeyIndices keyIndices, int timeIndex)
        {
            collection.Clear();
            inputTrace.EnumerateCollectionAt(keyIndices.processed, timeIndex, collection);
            var newSum = 0L;
            for (int i = 0; i < collection.Count; i++)
                newSum += collection.Array[i].weight;

            var oldSum = newSum;
            difference.Clear();
            inputTrace.EnumerateCollectionAt(keyIndices.unprocessed, timeIndex, difference);
            for (int i = 0; i < difference.Count; i++)
                oldSum -= difference.Array[i].weight;

            var oldOut = WeightFunction(oldSum);
            var newOut = WeightFunction(newSum);

            if (oldOut != newOut)
                outputTrace.Introduce(ref outputWorkspace, key, (newOut - oldOut), timeIndex);
        }

        public UnaryStatefulOperator(int index, Stage<TS> collection, bool inputImmutable)
            : base(index, collection, inputImmutable, x => x, x => x, false)
        { }
    }
}
