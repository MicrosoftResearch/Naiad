/*
 * Naiad ver. 0.6
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
using Microsoft.Research.Naiad.DataStructures;
using Microsoft.Research.Naiad.Serialization;
using Microsoft.Research.Naiad.Dataflow.Channels;
using Microsoft.Research.Naiad;
using Microsoft.Research.Naiad.Dataflow;
using Microsoft.Research.Naiad.Frameworks;
using Microsoft.Research.Naiad.Runtime.Progress;
using Microsoft.Research.Naiad.Runtime.FaultTolerance;
using Microsoft.Research.Naiad.Frameworks.DifferentialDataflow.CollectionTrace;

using Microsoft.Research.Naiad.Diagnostics;

namespace Microsoft.Research.Naiad.Frameworks.DifferentialDataflow.Operators
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

                    var output = this.Output.GetBufferForTime(newTime);

                    for (int j = 0; j < differences2.Count; j++)
                        if (differences2.Array[j].weight != 0)
                            output.Send(resultSelector(k, v, differences2.Array[j].record).ToWeighted(entry.weight * differences2.Array[j].weight));
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

            if (!inputShutdown1)
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

                    var output = this.Output.GetBufferForTime(newTime);

                    for (int j = 0; j < differences1.Count; j++)
                        if (differences1.Array[j].weight != 0)
                            output.Send(resultSelector(k, differences1.Array[j].record, v).ToWeighted(entry.weight * differences1.Array[j].weight));
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

        public override void OnNotify(T workTime)
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

            base.OnNotify(workTime);
        }

        #region Checkpointing

        /* Checkpoint format:
         * (base)
         * if !terminated
         *     Dictionary<K,JoinKeyIndices> JoinKeys
         */

        protected override void Checkpoint(NaiadWriter writer)
        {
            base.Checkpoint(writer);
            if (!this.isShutdown)
            {
                this.JoinKeys.Checkpoint(writer);
            }
        }

        protected override void Restore(NaiadReader reader)
        {
            base.Restore(reader);
            if (!this.isShutdown)
            {
                this.JoinKeys.Restore(reader);
            }
        }

        private bool HasStateInCheckpoint(JoinKeyIndices indices, ICheckpoint<T> checkpoint)
        {
            // we don't care about unprocessed times since they will be replayed when the checkpoint is restored

            NaiadList<int> timeList = new NaiadList<int>(16);
            if (this.inputTrace1 != null)
            {
                this.inputTrace1.EnumerateTimes(indices.processed1, timeList);
                for (int i = 0; i < timeList.Count; ++i)
                {
                    if (checkpoint.ContainsTime(this.internTable.times[timeList.Array[i]]))
                    {
                        return true;
                    }
                }
            }

            if (this.inputTrace2 != null)
            {
                timeList.Clear();
                this.inputTrace2.EnumerateTimes(indices.processed2, timeList);
                for (int i = 0; i < timeList.Count; ++i)
                {
                    if (checkpoint.ContainsTime(this.internTable.times[timeList.Array[i]]))
                    {
                        return true;
                    }
                }
            }

            return false;
        }

        private Pair<int, int> CountTimesInCheckpoint(JoinKeyIndices indices, ICheckpoint<T> checkpoint)
        {
            // we don't care about unprocessed times since they will be replayed when the checkpoint is restored

            NaiadList<int> timeList = new NaiadList<int>(16);
            int input1Count = 0;
            if (this.inputTrace1 != null)
            {
                this.inputTrace1.EnumerateTimes(indices.processed1, timeList);
                for (int i = 0; i < timeList.Count; ++i)
                {
                    if (checkpoint.ContainsTime(this.internTable.times[timeList.Array[i]]))
                    {
                        ++input1Count;
                    }
                }
            }

            timeList.Clear();
            int input2Count = 0;
            this.inputTrace2.EnumerateTimes(indices.processed2, timeList);
            if (this.inputTrace2 != null)
            {
                for (int i = 0; i < timeList.Count; ++i)
                {
                    if (checkpoint.ContainsTime(this.internTable.times[timeList.Array[i]]))
                    {
                        ++input2Count;
                    }
                }
            }

            return input1Count.PairWith(input2Count);
        }

        protected override long CountEntries(ICheckpoint<T> checkpoint)
        {
            long checkpointEntries = 0;

            if (!this.isShutdown)
            {
                foreach (var indices in this.JoinKeys.Values)
                {
                    checkpointEntries +=
                        this.inputTrace1.CountEntries(indices.processed1, checkpoint, this.internTable.times, true, false).First;
                    checkpointEntries +=
                        this.inputTrace2.CountEntries(indices.processed2, checkpoint, this.internTable.times, true, false).First;
                }
            }

            return checkpointEntries;
        }

        protected override void Checkpoint(NaiadWriter writer, ICheckpoint<T> checkpoint)
        {
            long checkpointEntries = 0;

            writer.Write(this.isShutdown);
            if (!this.isShutdown)
            {
                if (checkpoint.IsFullCheckpoint)
                {
                    this.CompactInternTable();
                }

                int compactedCount = 0;
                foreach (var indices in this.JoinKeys.Values)
                {
                    if (this.HasStateInCheckpoint(indices, checkpoint))
                    {
                        ++compactedCount;
                    }
                }

                writer.Write(compactedCount);
                foreach (var key in this.JoinKeys)
                {
                    JoinKeyIndices indices = key.Value;
                    Pair<int, int> timeCounts = this.CountTimesInCheckpoint(indices, checkpoint);

                    if (timeCounts.First > 0 || timeCounts.Second > 0)
                    {
                        writer.Write(key.Key);

                        writer.Write(timeCounts.First);
                        checkpointEntries +=
                            this.inputTrace1.CheckpointKey(indices.processed1, checkpoint, this.internTable.times, writer);

                        writer.Write(timeCounts.Second);
                        checkpointEntries +=
                            this.inputTrace2.CheckpointKey(indices.processed2, checkpoint, this.internTable.times, writer);
                    }
                }
            }

            this.checkpointManager.RegisterCheckpoint(checkpointEntries, checkpoint);
        }

        public override void RollBackPreservingState(Pointstamp[] frontier, ICheckpoint<T> lastFullCheckpoint, ICheckpoint<T> lastIncrementalCheckpoint)
        {
            base.RollBackBasePreservingState(frontier, lastFullCheckpoint, lastIncrementalCheckpoint);

            if (!this.isShutdown)
            {
                bool countFull = !this.checkpointManager.CachedFullCheckpoint.Equals(lastFullCheckpoint);
                bool countIncremental = countFull || !this.checkpointManager.CachedIncrementalCheckpoint.Equals(lastIncrementalCheckpoint);

                Pair<long, long> counts = 0L.PairWith(0L);

                if (frontier.Length == 0)
                {
                    this.InitializeRestoration(frontier);
                }
                else
                {
                    ICheckpoint<T> timeRange = FrontierCheckpointTester<T>.CreateDownwardClosed(frontier);

                    foreach (var key in this.JoinKeys.Keys.ToArray())
                    {
                        JoinKeyIndices indices = this.JoinKeys[key];

                        this.inputTrace1.RemoveStateInTimes(ref indices.processed1, t => timeRange.ContainsTime(this.internTable.times[t]));
                        this.inputTrace2.RemoveStateInTimes(ref indices.processed2, t => timeRange.ContainsTime(this.internTable.times[t]));

                        if (countFull || countIncremental)
                        {
                            Pair<long, long> thisCounts;
                            thisCounts = this.inputTrace1.CountEntries(indices.processed1, lastFullCheckpoint, this.internTable.times, countFull, countIncremental);
                            counts.First += thisCounts.First;
                            counts.Second += thisCounts.Second;
                            thisCounts = this.inputTrace2.CountEntries(indices.processed2, lastFullCheckpoint, this.internTable.times, countFull, countIncremental);
                            counts.First += thisCounts.First;
                            counts.Second += thisCounts.Second;
                        }

                        this.JoinKeys[key] = indices;
                    }

                    if (!countFull)
                    {
                        counts.First = this.checkpointManager.CachedFullCheckpointEntries;
                    }

                    if (!countIncremental)
                    {
                        counts.Second = this.checkpointManager.CachedIncrementalCheckpointEntries;
                    }

                    this.checkpointManager.RegisterCheckpoint(counts.First, lastFullCheckpoint, counts.Second, lastIncrementalCheckpoint);
                }
            }
        }

        internal override void CompactInternTable()
        {
            if (!this.isShutdown)
            {
                LatticeInternTable<T> newInternTable = new LatticeInternTable<T>();

                foreach (var key in this.JoinKeys.Keys.ToArray())
                {
                    JoinKeyIndices indices = this.JoinKeys[key];
                    this.inputTrace1.EnsureStateIsCurrentWRTAdvancedTimes(ref indices.processed1);
                    this.inputTrace2.EnsureStateIsCurrentWRTAdvancedTimes(ref indices.processed2);
                    this.JoinKeys[key] = indices;

                    this.inputTrace1.TransferTimesToNewInternTable(indices.processed1, t => newInternTable.Intern(this.internTable.times[t]));
                    this.inputTrace2.TransferTimesToNewInternTable(indices.processed2, t => newInternTable.Intern(this.internTable.times[t]));
                }

                this.inputTrace1.InstallNewUpdateFunction((t1, t2) => newInternTable.LessThan(t1, t2), t => newInternTable.UpdateTime(t));
                this.inputTrace2.InstallNewUpdateFunction((t1, t2) => newInternTable.LessThan(t1, t2), t => newInternTable.UpdateTime(t));
                this.internTable = newInternTable;
            }
        }

        protected override void InitializeRestoration(Pointstamp[] frontier)
        {
            // empty all the state
            this.JoinKeys = new Dictionary<K, JoinKeyIndices>();
            this.internTable = new LatticeInternTable<T>();
            this.inputTrace1 = createInputTrace1();
            this.inputTrace2 = createInputTrace2();
            this.checkpointManager.ForceFullCheckpoint = true;
        }

        protected override void RestorePartialCheckpoint(NaiadReader reader, ICheckpoint<T> checkpoint)
        {
            long checkpointEntries = 0;

            this.isShutdown = reader.Read<bool>();

            if (!this.isShutdown)
            {
                int numberOfKeys = reader.Read<int>();
                for (int i = 0; i < numberOfKeys; ++i)
                {
                    K key = reader.Read<K>();

                    JoinKeyIndices indices;
                    if (!this.JoinKeys.TryGetValue(key, out indices))
                    {
                        indices = new JoinKeyIndices();
                    }

                    checkpointEntries += this.inputTrace1.RestoreKey(ref indices.processed1, this.internTable, reader);
                    checkpointEntries += this.inputTrace2.RestoreKey(ref indices.processed2, this.internTable, reader);

                    this.JoinKeys[key] = indices;
                }
            }

            this.checkpointManager.RegisterCheckpoint(checkpointEntries, checkpoint);
        }

        #endregion

        public override void OnReceive1(Message<Weighted<S1>, T> message)
        {
            this.NotifyAt(message.time);
            for (int i = 0; i < message.length; i++)
                this.OnInput1(message.payload[i], message.time);
        }

        public override void OnReceive2(Message<Weighted<S2>, T> message)
        {
            this.NotifyAt(message.time);
            for (int i = 0; i < message.length; i++)
                this.OnInput2(message.payload[i], message.time);
        }

        public Join(int index, Stage<T> collection, bool input1Immutable, bool input2Immutable, Expression<Func<S1, K>> k1, Expression<Func<S2, K>> k2, Expression<Func<S1, V1>> v1, Expression<Func<S2, V2>> v2, Expression<Func<K, V1, V2, R>> r)
            : base(index, collection, input1Immutable, input2Immutable, k1, k2, v1, v2)
        {
            this.outputTrace = null;
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

                    var output = this.Output.GetBufferForTime(newTime);

                    for (int j = 0; j < differences2.Count; j++)
                        if (differences2.Array[j].weight != 0)
                            output.Send(resultSelector(k, v, differences2.Array[j].record).ToWeighted(entry.weight * differences2.Array[j].weight));
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

                    var output = this.Output.GetBufferForTime(newTime);

                    for (int j = 0; j < differences1.Count; j++)
                        if (differences1.Array[j].weight != 0)
                            output.Send(resultSelector(k, differences1.Array[j].record, v).ToWeighted(entry.weight * differences1.Array[j].weight));
                }
            }

            JoinKeys[index / 65536][index % 65536] = state;
        }

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

        public override void OnNotify(T workTime)
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

            base.OnNotify(workTime);
        }

        #region Checkpointing

        /* Checkpoint format:
         * (base)
         * if !terminated
         *     int                                              keyIndicesLength
         *     (int n,n*BinaryKeyIndices|-1)*keyIndicesLength   keyIndices
         */

        protected override void Checkpoint(NaiadWriter writer)
        {
            base.Checkpoint(writer);
            if (!this.isShutdown)
            {
                for (int i = 0; i < this.JoinKeys.Length; ++i)
                {
                    if (this.JoinKeys[i] == null)
                        writer.Write(-1);
                    else
                    {
                        writer.Write(this.JoinKeys[i].Length);
                        for (int j = 0; j < this.JoinKeys[i].Length; ++j)
                            writer.Write(this.JoinKeys[i][j]);
                    }
                }
            }
        }

        protected override void Restore(NaiadReader reader)
        {
            base.Restore(reader);
            if (!this.isShutdown)
            {
                for (int i = 0; i < this.JoinKeys.Length; ++i)
                {
                    int count = reader.Read<int>();
                    if (count >= 0)
                    {
                        this.JoinKeys[i] = new JoinIntKeyIndices[count];
                        for (int j = 0; j < this.JoinKeys[i].Length; ++j)
                            this.JoinKeys[i][j] = reader.Read<JoinIntKeyIndices>();
                    }
                    else
                        this.JoinKeys[i] = null;
                }
            }
        }

        private bool HasStateInCheckpoint(JoinIntKeyIndices indices, ICheckpoint<T> checkpoint)
        {
            // we don't care about unprocessed times since they will be replayed when the checkpoint is restored

            NaiadList<int> timeList = new NaiadList<int>(16);
            if (this.inputTrace1 != null)
            {
                this.inputTrace1.EnumerateTimes(indices.processed1, timeList);
                for (int i = 0; i < timeList.Count; ++i)
                {
                    if (checkpoint.ContainsTime(this.internTable.times[timeList.Array[i]]))
                    {
                        return true;
                    }
                }
            }

            if (this.inputTrace2 != null)
            {
                timeList.Clear();
                this.inputTrace2.EnumerateTimes(indices.processed2, timeList);
                for (int i = 0; i < timeList.Count; ++i)
                {
                    if (checkpoint.ContainsTime(this.internTable.times[timeList.Array[i]]))
                    {
                        return true;
                    }
                }
            }

            return false;
        }

        private Pair<int, int> CountTimesInCheckpoint(JoinIntKeyIndices indices, ICheckpoint<T> checkpoint)
        {
            // we don't care about unprocessed times since they will be replayed when the checkpoint is restored

            NaiadList<int> timeList = new NaiadList<int>(16);
            int input1Count = 0;
            if (this.inputTrace1 != null)
            {
                this.inputTrace1.EnumerateTimes(indices.processed1, timeList);
                for (int i = 0; i < timeList.Count; ++i)
                {
                    if (checkpoint.ContainsTime(this.internTable.times[timeList.Array[i]]))
                    {
                        ++input1Count;
                    }
                }
            }

            timeList.Clear();
            int input2Count = 0;
            this.inputTrace2.EnumerateTimes(indices.processed2, timeList);
            if (this.inputTrace2 != null)
            {
                for (int i = 0; i < timeList.Count; ++i)
                {
                    if (checkpoint.ContainsTime(this.internTable.times[timeList.Array[i]]))
                    {
                        ++input2Count;
                    }
                }
            }

            return input1Count.PairWith(input2Count);
        }

        protected override long CountEntries(ICheckpoint<T> checkpoint)
        {
            long checkpointEntries = 0;

            if (!this.isShutdown)
            {
                for (int outerKeys = 0; outerKeys < this.JoinKeys.Length; ++outerKeys)
                {
                    if (this.JoinKeys[outerKeys] != null)
                    {
                        for (int innerKeys = 0; innerKeys < this.JoinKeys[outerKeys].Length; ++innerKeys)
                        {
                            int index = (outerKeys * 65536) + innerKeys;
                            JoinIntKeyIndices indices = this.JoinKeys[outerKeys][innerKeys];

                            checkpointEntries +=
                                this.inputTrace1.CountEntries(indices.processed1, checkpoint, this.internTable.times, true, false).First;
                            checkpointEntries +=
                                this.inputTrace2.CountEntries(indices.processed2, checkpoint, this.internTable.times, true, false).First;
                        }
                    }
                }
            }

            return checkpointEntries;
        }

        protected override void Checkpoint(NaiadWriter writer, ICheckpoint<T> checkpoint)
        {
            long checkpointEntries = 0;

            writer.Write(this.isShutdown);
            if (!this.isShutdown)
            {
                if (checkpoint.IsFullCheckpoint)
                {
                    this.CompactInternTable();
                }

                int compactedCount = 0;
                for (int outerKeys = 0; outerKeys < this.JoinKeys.Length; ++outerKeys)
                {
                    if (this.JoinKeys[outerKeys] != null)
                    {
                        for (int innerKeys = 0; innerKeys < this.JoinKeys[outerKeys].Length; ++innerKeys)
                        {
                            if (this.HasStateInCheckpoint(this.JoinKeys[outerKeys][innerKeys], checkpoint))
                            {
                                ++compactedCount;
                            }
                        }
                    }
                }

                writer.Write(compactedCount);
                for (int outerKeys = 0; outerKeys < this.JoinKeys.Length; ++outerKeys)
                {
                    if (this.JoinKeys[outerKeys] != null)
                    {
                        for (int innerKeys = 0; innerKeys < this.JoinKeys[outerKeys].Length; ++innerKeys)
                        {
                            int index = (outerKeys * 65536) + innerKeys;
                            JoinIntKeyIndices indices = this.JoinKeys[outerKeys][innerKeys];

                            Pair<int, int> timeCounts = this.CountTimesInCheckpoint(indices, checkpoint);

                            if (timeCounts.First > 0 || timeCounts.Second > 0)
                            {
                                writer.Write(index);

                                writer.Write(timeCounts.First);
                                checkpointEntries +=
                                    this.inputTrace1.CheckpointKey(indices.processed1, checkpoint, this.internTable.times, writer);

                                writer.Write(timeCounts.Second);
                                checkpointEntries +=
                                    this.inputTrace2.CheckpointKey(indices.processed2, checkpoint, this.internTable.times, writer);
                            }
                        }
                    }
                }
            }

            this.checkpointManager.RegisterCheckpoint(checkpointEntries, checkpoint);
        }

        public override void RollBackPreservingState(Pointstamp[] frontier, ICheckpoint<T> lastFullCheckpoint, ICheckpoint<T> lastIncrementalCheckpoint)
        {
            base.RollBackBasePreservingState(frontier, lastFullCheckpoint, lastIncrementalCheckpoint);

            if (!this.isShutdown)
            {
                bool countFull = !this.checkpointManager.CachedFullCheckpoint.Equals(lastFullCheckpoint);
                bool countIncremental = countFull || !this.checkpointManager.CachedIncrementalCheckpoint.Equals(lastIncrementalCheckpoint);

                Pair<long, long> counts = 0L.PairWith(0L);

                if (frontier.Length == 0)
                {
                    this.InitializeRestoration(frontier);
                }
                else
                {
                    ICheckpoint<T> timeRange = FrontierCheckpointTester<T>.CreateDownwardClosed(frontier);

                    for (int outerKeys = 0; outerKeys < this.JoinKeys.Length; ++outerKeys)
                    {
                        if (this.JoinKeys[outerKeys] != null)
                        {
                            for (int innerKeys = 0; innerKeys < this.JoinKeys[outerKeys].Length; ++innerKeys)
                            {
                                int index = (outerKeys * 65536) + innerKeys;
                                JoinIntKeyIndices indices = this.JoinKeys[outerKeys][innerKeys];

                                this.inputTrace1.RemoveStateInTimes(ref indices.processed1, t => timeRange.ContainsTime(this.internTable.times[t]));
                                this.inputTrace2.RemoveStateInTimes(ref indices.processed2, t => timeRange.ContainsTime(this.internTable.times[t]));

                                if (countFull || countIncremental)
                                {
                                    Pair<long, long> thisCounts;
                                    thisCounts = this.inputTrace1.CountEntries(indices.processed1, lastFullCheckpoint, this.internTable.times, countFull, countIncremental);
                                    counts.First += thisCounts.First;
                                    counts.Second += thisCounts.Second;
                                    thisCounts = this.inputTrace2.CountEntries(indices.processed2, lastFullCheckpoint, this.internTable.times, countFull, countIncremental);
                                    counts.First += thisCounts.First;
                                    counts.Second += thisCounts.Second;
                                }

                                this.JoinKeys[outerKeys][innerKeys] = indices;
                            }
                        }
                    }

                    if (!countFull)
                    {
                        counts.First = this.checkpointManager.CachedFullCheckpointEntries;
                    }

                    if (!countIncremental)
                    {
                        counts.Second = this.checkpointManager.CachedIncrementalCheckpointEntries;
                    }

                    this.checkpointManager.RegisterCheckpoint(counts.First, lastFullCheckpoint, counts.Second, lastIncrementalCheckpoint);
                }
            }
        }

        internal override void CompactInternTable()
        {
            if (!this.isShutdown)
            {
                LatticeInternTable<T> newInternTable = new LatticeInternTable<T>();

                for (int outerKeys = 0; outerKeys < this.JoinKeys.Length; ++outerKeys)
                {
                    if (this.JoinKeys[outerKeys] != null)
                    {
                        for (int innerKeys = 0; innerKeys < this.JoinKeys[outerKeys].Length; ++innerKeys)
                        {
                            int index = (outerKeys * 65536) + innerKeys;
                            JoinIntKeyIndices indices = this.JoinKeys[outerKeys][innerKeys];
                            this.inputTrace1.EnsureStateIsCurrentWRTAdvancedTimes(ref indices.processed1);
                            this.inputTrace2.EnsureStateIsCurrentWRTAdvancedTimes(ref indices.processed2);
                            this.JoinKeys[outerKeys][innerKeys] = indices;

                            this.inputTrace1.TransferTimesToNewInternTable(indices.processed1, t => newInternTable.Intern(this.internTable.times[t]));
                            this.inputTrace2.TransferTimesToNewInternTable(indices.processed2, t => newInternTable.Intern(this.internTable.times[t]));
                        }
                    }
                }

                this.inputTrace1.InstallNewUpdateFunction((t1, t2) => newInternTable.LessThan(t1, t2), t => newInternTable.UpdateTime(t));
                this.inputTrace2.InstallNewUpdateFunction((t1, t2) => newInternTable.LessThan(t1, t2), t => newInternTable.UpdateTime(t));
                this.internTable = newInternTable;
            }
        }

        protected override void InitializeRestoration(Pointstamp[] frontier)
        {
            // empty all the state
            this.JoinKeys = new JoinIntKeyIndices[65536][];
            this.internTable = new LatticeInternTable<T>();
            this.inputTrace1 = createInputTrace1();
            this.inputTrace2 = createInputTrace2();
            this.checkpointManager.ForceFullCheckpoint = true;
        }

        protected override void RestorePartialCheckpoint(NaiadReader reader, ICheckpoint<T> checkpoint)
        {
            long checkpointEntries = 0;

            this.isShutdown = reader.Read<bool>();

            if (!this.isShutdown)
            {
                int numberOfKeys = reader.Read<int>();
                for (int i = 0; i < numberOfKeys; ++i)
                {
                    int index = reader.Read<int>();

                    if (JoinKeys[index / 65536] == null)
                        JoinKeys[index / 65536] = new JoinIntKeyIndices[65536];

                    JoinIntKeyIndices indices = this.JoinKeys[index / 65536][index % 65536];

                    checkpointEntries += this.inputTrace1.RestoreKey(ref indices.processed1, this.internTable, reader);
                    checkpointEntries += this.inputTrace2.RestoreKey(ref indices.processed2, this.internTable, reader);

                    this.JoinKeys[index / 65536][index % 65536] = indices;
                }
            }

            this.checkpointManager.RegisterCheckpoint(checkpointEntries, checkpoint);
        }

        #endregion

        public override void OnReceive1(Message<Weighted<S1>, T> message)
        {
            this.NotifyAt(message.time);
            for (int i = 0; i < message.length; i++)
                this.OnInput1(message.payload[i], message.time);
        }

        public override void OnReceive2(Message<Weighted<S2>, T> message)
        {
            this.NotifyAt(message.time);
            for (int i = 0; i < message.length; i++)
                this.OnInput2(message.payload[i], message.time);
        }

        public JoinIntKeyed(int index, Stage<T> collection, bool input1Immutable, bool input2Immutable, Expression<Func<S1, Int32>> k1, Expression<Func<S2, Int32>> k2, Expression<Func<S1, V1>> v1, Expression<Func<S2, V2>> v2, Expression<Func<Int32, V1, V2, R>> r)
            : base(index, collection, input1Immutable, input2Immutable, k1, k2, v1, v2)
        {
            this.outputTrace = null;
            resultSelector = r.Compile();
            
            // Inhibits verbose serialization of the parent's keyIndices.
            keyIndices = new BinaryKeyIndices[0][];

            JoinKeys = new JoinIntKeyIndices[65536][];
            this.parts = collection.Placement.Count;
        }
    }
}
