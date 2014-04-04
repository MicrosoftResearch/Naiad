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
using Microsoft.Research.Naiad.DataStructures;
using Microsoft.Research.Naiad.Dataflow.Channels;
using Microsoft.Research.Naiad.Scheduling;
using Microsoft.Research.Naiad.Frameworks.DifferentialDataflow.CollectionTrace;
using Microsoft.Research.Naiad.FaultTolerance;

using System.Linq.Expressions;
using System.Diagnostics;
using Microsoft.Research.Naiad;
using Microsoft.Research.Naiad.CodeGeneration;
using Microsoft.Research.Naiad.Dataflow;

namespace Microsoft.Research.Naiad.Frameworks.DifferentialDataflow.OperatorImplementations
{
    internal class BinaryStatefulIntKeyedOperator<V1, V2, S1, S2, T, R> : BinaryBufferingVertex<Weighted<S1>, Weighted<S2>, Weighted<R>, T>
        //: BinaryOperator<S1, S2, R, T>
        where V1 : IEquatable<V1>
        where V2 : IEquatable<V2>
        where S1 : IEquatable<S1>
        where S2 : IEquatable<S2>
        where T : Time<T>
        where R : IEquatable<R>
    {
        protected override void OnShutdown()
        {
            base.OnShutdown();

            if (inputTrace1 != null)    // the input may have already been nulled by the operator
            {
                inputTrace1.Release();
                inputTrace1 = null;
            }

            if (inputTrace2 != null)
            {
                inputTrace2.Release();
                inputTrace2 = null;
            }

            if (outputTrace != null)
            {
                outputTrace.Release();
                outputTrace = null;
            }

            keysToProcess = null;
            keyIndices = null;
        }
        
        public override void UpdateReachability(NaiadList<Pointstamp> versions)
        {
            base.UpdateReachability(versions);

            if (versions != null && internTable != null)
                internTable.UpdateReachability(versions);
        }

        //public readonly Naiad.Frameworks.RecvFiberBank<Weighted<S1>, T> LeftInput;
        //public readonly Naiad.Frameworks.RecvFiberBank<Weighted<S2>, T> RightInput;

        public Func<S1, Int32> key1;      // extracts the key from the input record
        public Func<S1, V1> value1;    // reduces input record to relevant value

        public Func<S2, Int32> key2;      // extracts the key from the input record
        public Func<S2, V2> value2;    // reduces input record to relevant value

        public Expression<Func<S1, Int32>> keyExpression1;      // extracts the key from the input record
        public Expression<Func<S1, V1>> valueExpression1;    // reduces input record to relevant value

        public Expression<Func<S2, Int32>> keyExpression2;      // extracts the key from the input record
        public Expression<Func<S2, V2>> valueExpression2;    // reduces input record to relevant value

        readonly bool MaintainOutputTrace;

        protected CollectionTraceWithHeap<R> createOutputTrace()
        {
            return new CollectionTraceWithHeap<R>((x, y) => internTable.LessThan(x, y), x => internTable.UpdateTime(x), this.Stage.Placement.Count);
        }

        protected CollectionTraceCheckpointable<V2> createInputTrace2()
        {
            if (Microsoft.Research.Naiad.CodeGeneration.ExpressionComparer.Instance.Equals(keyExpression2, valueExpression2))
            {
                if (this.inputImmutable2)
                    return new CollectionTraceImmutableNoHeap<V2>();
                else
                    return new CollectionTraceWithoutHeap<V2>((x, y) => internTable.LessThan(x, y),
                                                                   x => internTable.UpdateTime(x));
            }
            else
            {
                if (this.inputImmutable2)
                    return new CollectionTraceImmutable<V2>();
                else
                    return new CollectionTraceWithHeap<V2>((x, y) => internTable.LessThan(x, y),
                                                                 x => internTable.UpdateTime(x), this.Stage.Placement.Count);
            }
        }

        protected CollectionTraceCheckpointable<V1> createInputTrace1()
        {
            if (Microsoft.Research.Naiad.CodeGeneration.ExpressionComparer.Instance.Equals(keyExpression1, valueExpression1))
            {
                if (this.inputImmutable1)
                    return new CollectionTraceImmutableNoHeap<V1>();
                else
                    return new CollectionTraceWithoutHeap<V1>((x, y) => internTable.LessThan(x, y),
                                                                    x => internTable.UpdateTime(x));
            }
            else
            {
                if (this.inputImmutable1)
                    return new CollectionTraceImmutable<V1>();
                else
                    return new CollectionTraceWithHeap<V1>((x, y) => internTable.LessThan(x, y),
                                                                 x => internTable.UpdateTime(x), this.Stage.Placement.Count);
            }
        }

        public override void OnReceive1(Message<Weighted<S1>, T> message)
        {
            if (this.inputImmutable1)
            {
                this.NotifyAt(message.time);
                for (int i = 0; i < message.length; i++)
                    this.OnInput1(message.payload[i], message.time);
            }
            else
                base.OnReceive1(message);
        }


        public override void OnReceive2(Message<Weighted<S2>, T> message)
        {
            if (this.inputImmutable2)
            {
                this.NotifyAt(message.time);
                for (int i = 0; i < message.length; i++)
                {
                    this.OnInput2(message.payload[i], message.time);
                }
            }
            else
                base.OnReceive2(message);
        }

        public override void OnNotify(T workTime)
        {
            if (!this.inputImmutable1)
            {
                foreach (var record in this.Input1.GetRecordsAt(workTime))
                    OnInput1(record, workTime);
            }

            if (!this.inputImmutable2)
            {
                foreach (var record in this.Input2.GetRecordsAt(workTime))
                    OnInput2(record, workTime);
            }

            Compute();

            if (inputTrace1 != null) inputTrace1.Compact();
            if (inputTrace2 != null) inputTrace2.Compact();

            Flush();
        }

        protected BinaryKeyIndices[][] keyIndices;

        protected CollectionTraceCheckpointable<V1> inputTrace1;          // collects all differences that have processed.
        protected CollectionTraceCheckpointable<V2> inputTrace2;          // collects all differences that have processed.
        protected CollectionTraceCheckpointable<R> outputTrace;           // collects outputs

        protected int outputWorkspace = 0;

        protected LatticeInternTable<T> internTable = new LatticeInternTable<T>();

        protected NaiadList<int> keysToProcess = new NaiadList<int>(1);

        public virtual void OnInput1(Weighted<S1> entry, T time)
        {
            var k = key1(entry.record);

            var index = (int)(k / this.Stage.Placement.Count);

            if (keyIndices[index / 65536] == null)
                keyIndices[index / 65536] = new BinaryKeyIndices[65536];


            keysToProcess.Add(index);

            inputTrace1.Introduce(ref keyIndices[index / 65536][index % 65536].unprocessed1, value1(entry.record), entry.weight, internTable.Intern(time));
        }

        public virtual void OnInput2(Weighted<S2> entry, T time)
        {
            var k = key2(entry.record);

            var index = (int)(k / this.Stage.Placement.Count);

            if (keyIndices[index / 65536] == null)
                keyIndices[index / 65536] = new BinaryKeyIndices[65536];

            keysToProcess.Add(index);

            inputTrace2.Introduce(ref keyIndices[index / 65536][index % 65536].unprocessed2, value2(entry.record), entry.weight, internTable.Intern(time));

        }

        public virtual void Compute()
        {
            for (int i = 0; i < keysToProcess.Count; i++)
                Update(keysToProcess.Array[i]);

            //inputTrace1.Compact();
            //inputTrace2.Compact();
            //outputTrace.Compact();

            keysToProcess.Clear();
        }

#if false // These don't seem to be used anywhere.
        protected NaiadList<Weighted<V1>> collection1 = new NaiadList<Weighted<V1>>(1);
        protected NaiadList<Weighted<V1>> difference1 = new NaiadList<Weighted<V1>>(1);

        protected NaiadList<Weighted<V2>> collection2 = new NaiadList<Weighted<V2>>(1);
        protected NaiadList<Weighted<V2>> difference2 = new NaiadList<Weighted<V2>>(1);
#endif

        // Moves from unprocessed[key] to processed[key], updating output[key] and Send()ing.
        protected virtual void Update(int index)
        {
            var traceIndices = keyIndices[index / 65536][index % 65536];

            if (traceIndices.unprocessed1 != 0 || traceIndices.unprocessed2 != 0)
            {
                // iterate through the times that may require updates.
                var interestingTimes = InterestingTimes(traceIndices);

                // incorporate the updates, so we can compare old and new outputs.
                inputTrace1.IntroduceFrom(ref traceIndices.processed1, ref traceIndices.unprocessed1, false);
                inputTrace2.IntroduceFrom(ref traceIndices.processed2, ref traceIndices.unprocessed2, false);

                for (int i = 0; i < interestingTimes.Count; i++)
                    UpdateTime(index, traceIndices, interestingTimes.Array[i]);

                // clean out the state we just processed
                inputTrace1.ZeroState(ref traceIndices.unprocessed1);
                inputTrace2.ZeroState(ref traceIndices.unprocessed2);

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

        protected virtual NaiadList<int> InterestingTimes(BinaryKeyIndices keyIndex)
        {
            deltaList.Clear();
            inputTrace1.EnumerateTimes(keyIndex.unprocessed1, deltaList);
            inputTrace2.EnumerateTimes(keyIndex.unprocessed2, deltaList);

            truthList.Clear();
            inputTrace1.EnumerateTimes(keyIndex.processed1, truthList);
            inputTrace2.EnumerateTimes(keyIndex.processed2, truthList);

            timeList.Clear();
            this.internTable.InterestingTimes(timeList, truthList, deltaList);

            return timeList;
        }

        protected NaiadList<Weighted<R>> outputCollection = new NaiadList<Weighted<R>>(1);

        protected virtual void UpdateTime(int index, BinaryKeyIndices keyIndex, int timeIndex)
        {
            // subtract out prior records before adding new ones
            outputTrace.SubtractStrictlyPriorDifferences(ref outputWorkspace, timeIndex);

            NewOutputMinusOldOutput(index, keyIndex, timeIndex);

            var outputTime = this.internTable.times[timeIndex];

            outputCollection.Clear();
            outputTrace.EnumerateDifferenceAt(outputWorkspace, timeIndex, outputCollection);

            var output = this.Output.GetBufferForTime(outputTime);

            for (int i = 0; i < outputCollection.Count; i++)
                output.Send(outputCollection.Array[i]);
        }
        
        protected virtual void NewOutputMinusOldOutput(int index, BinaryKeyIndices keyIndex, int timeIndex)
        {
            Reduce(index, keyIndex, timeIndex);

            // this suggests we want to init updateToOutput with -output
            outputCollection.Clear();
            outputTrace.EnumerateCollectionAt(keyIndex.output, timeIndex, outputCollection);
            for (int i = 0; i < outputCollection.Count; i++)
                outputTrace.Introduce(ref outputWorkspace, outputCollection.Array[i].record, -outputCollection.Array[i].weight, timeIndex);
        }

        // expected to populate resultList to match reduction(collection.source)
        protected virtual void Reduce(int index, BinaryKeyIndices keyIndex, int time) 
        {
            var key = index * this.Stage.Placement.Count + this.VertexId;
        }

        #region Checkpointing

        /* Checkpoint format:
         * bool terminated
         * if !terminated:
         *     LatticeInternTable<T>                            internTable
         *     CollectionTrace<>                                inputTrace1
         *     CollectionTrace<>                                inputTrace2
         *     CollectionTrace<>                                outputTrace
         *     int                                              keyIndicesLength
         *     (int n,n*BinaryKeyIndices|-1)*keyIndicesLength         keyIndices
         *     int                                              recordsToProcessCount1
         *     (T,NaiadList<Weighted<S>>)*recordsToProcessCount recordsToProcess1
         *     int                                              recordsToProcessCount2
         *     (T,NaiadList<Weighted<S>>)*recordsToProcessCount recordsToProcess2
         */

        //private static NaiadSerialization<T> timeSerializer = null;
        //private static NaiadSerialization<Weighted<S1>> weightedS1Serializer = null;
        //private static NaiadSerialization<Weighted<S2>> weightedS2Serializer = null;

        public override void Checkpoint(NaiadWriter writer)
        {
            base.Checkpoint(writer);
            writer.Write(this.isShutdown);
            if (!this.isShutdown)
            {
                this.internTable.Checkpoint(writer);
                this.inputTrace1.Checkpoint(writer);
                this.inputTrace2.Checkpoint(writer);
                this.outputTrace.Checkpoint(writer);

                for (int i = 0; i < this.keyIndices.Length; ++i)
                {
                    if (this.keyIndices[i] == null)
                        writer.Write(-1);
                    else
                    {
                        writer.Write(this.keyIndices[i].Length);
                        for (int j = 0; j < this.keyIndices[i].Length; ++j)
                            writer.Write(this.keyIndices[i][j]);
                    }
                }

                this.Input1.Checkpoint(writer);
                this.Input2.Checkpoint(writer);
            }
        }

        protected readonly bool inputImmutable1 = false;
        protected readonly bool inputImmutable2 = false;

        public override void Restore(NaiadReader reader)
        {
            base.Restore(reader);
            
            if (!this.isShutdown)
            {
                this.internTable.Restore(reader);
                this.inputTrace1.Restore(reader);
                this.inputTrace2.Restore(reader);
                this.outputTrace.Restore(reader);

                for (int i = 0; i < this.keyIndices.Length; ++i)
                {
                    int length = reader.Read<int>();
                    if (length >= 0)
                    {
                        this.keyIndices[i] = new BinaryKeyIndices[length];
                        for (int j = 0; j < this.keyIndices[i].Length; ++j)
                            this.keyIndices[i][j] = reader.Read<BinaryKeyIndices>();
                    }
                }

                this.Input1.Restore(reader);
                this.Input2.Restore(reader);
            }
        }

        #endregion
        
        public BinaryStatefulIntKeyedOperator(int index, Stage<T> stage, bool input1Immutable, bool input2Immutable, Expression<Func<S1, Int32>> k1, Expression<Func<S2, Int32>> k2, Expression<Func<S1, V1>> v1, Expression<Func<S2, V2>> v2, bool maintainOC = true)
            : base(index, stage, null)
        {
            key1 = k1.Compile();
            value1 = v1.Compile();

            key2 = k2.Compile();
            value2 = v2.Compile();

            keyExpression1 = k1;
            keyExpression2 = k2;
            valueExpression1 = v1;
            valueExpression2 = v2;

            MaintainOutputTrace = maintainOC;

            this.keyIndices = new BinaryKeyIndices[0][];

            this.inputImmutable1 = input1Immutable;
            this.inputImmutable2 = input2Immutable;

            keyIndices = new BinaryKeyIndices[65536][];
            inputTrace1 = createInputTrace1();
            inputTrace2 = createInputTrace2();
            outputTrace = createOutputTrace();

            outputWorkspace = 0;
        }
    }

}
