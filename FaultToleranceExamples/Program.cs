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
using System.IO;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

using Microsoft.Research.Naiad;
using Microsoft.Research.Naiad.Dataflow;
using Microsoft.Research.Naiad.Dataflow.Iteration;
using Microsoft.Research.Naiad.Dataflow.PartitionBy;
using Microsoft.Research.Naiad.Diagnostics;
using Microsoft.Research.Naiad.Input;
using Microsoft.Research.Naiad.Frameworks.Lindi;
using Microsoft.Research.Naiad.Frameworks.DifferentialDataflow;
using Microsoft.Research.Naiad.Dataflow.StandardVertices;
using Microsoft.Research.Naiad.Runtime.Progress;
using Microsoft.Research.Naiad.Runtime.FaultTolerance;
using Microsoft.Research.Naiad.FaultToleranceManager;

namespace FaultToleranceExamples
{
    public static class ExtensionMethods
    {
        public static Stream<S, T> Compose<R, S, T>(this Stream<R, T> input,
            Computation computation, Placement placement,
            Func<Stream<R,T>, Stream<S,T>> function)
            where T : Time<T>
        {
            Stream<S, T> output = null;
            using (var ov = computation.Placement(placement))
            {
                output = function(input);
            }
            return output;
        }

        public static Collection<S, T> Compose<R, S, T>(this Collection<R, T> input,
            Computation computation, Placement placement,
            Func<Collection<R, T>, Collection<S, T>> function)
            where T : Time<T>
            where R : IEquatable<R>
            where S : IEquatable<S>
        {
            Collection<S, T> output = null;
            using (var ov = computation.Placement(placement))
            {
                output = function(input);
            }
            return output;
        }

        public static Pair<Stream<Program.FastPipeline.Record, TInner>, HashSet<Program.FastPipeline.StaggeredJoinVertex<TInput1, TInput2, TKey, TInner, TOuter>>>
            StaggeredJoin<TInput1, TInput2, TKey, TInner, TOuter>(
            this Stream<TInput1, TInner> stream1,
            Stream<TInput2, TInner> stream2,
            Func<TInput1, TKey> keySelector1, Func<TInput2, TKey> keySelector2,
            Func<TInput1, TInput2, Program.FastPipeline.Record> resultSelector,
            Func<TInner, TOuter> timeSelector, Func<TOuter, TInner> notifySelector)
            where TInput2 : Program.IRecord
            where TInner : Time<TInner>
            where TOuter : Time<TOuter>
        {
            HashSet<Program.FastPipeline.StaggeredJoinVertex<TInput1, TInput2, TKey, TInner, TOuter>> vertices =
                new HashSet<Program.FastPipeline.StaggeredJoinVertex<TInput1, TInput2, TKey, TInner, TOuter>>();

            return stream1.NewBinaryStage(stream2.Prepend(), (i, s) =>
                {
                    var vertex = new Program.FastPipeline.StaggeredJoinVertex<TInput1, TInput2, TKey, TInner, TOuter>(
                        i, s, keySelector1, keySelector2, resultSelector, timeSelector, notifySelector);
                    vertices.Add(vertex);
                    return vertex;
                },
                    x => keySelector1(x).GetHashCode(), x => keySelector2(x).GetHashCode(), null, "StaggeredJoin")
                    .SetCheckpointType(CheckpointType.StatelessLogEphemeral).SetCheckpointPolicy(s => new CheckpointWithoutPersistence())
                    .PairWith(vertices);
        }

        public static Stream<R, T> Prepend<R, T>(this Stream<R, T> stream)
            where R : Program.IRecord
            where T : Time<T>
        {
            return stream.NewUnaryStage((i, s) =>
                new Program.FastPipeline.PrependVertex<R, T>(i, s), null, null, "Prepend")
                    .SetCheckpointType(CheckpointType.StatelessLogEphemeral).SetCheckpointPolicy(s => new CheckpointWithoutPersistence());
        }

        public static void HelperStage<R>(this Stream<Pair<int,R>, Epoch> stream, Action<R> action)
        {
            Program.HelperVertex<R>.HelperStage(stream, action);
        }
    }

    public class Program
    {
        private string accountName = "";
        private string accountKey = "";
        private string containerName = "checkpoint";

        public interface IRecord
        {
            long EntryTicks { get; set; }
        }

        public class SlowPipeline
        {
            public int baseProc;
            public int range;
            public SubBatchDataSource<HTRecord, Epoch> source;

            public struct Record : IRecord, IEquatable<Record>
            {
                public int key;
                public long count;
                public long entryTicks;
                public long EntryTicks { get { return this.entryTicks; } set { this.entryTicks = value; } }

                public Record(HTRecord large)
                {
                    this.key = large.key;
                    this.count = -1;
                    this.entryTicks = large.entryTicks;
                }

                public bool Equals(Record other)
                {
                    return key == other.key && EntryTicks == other.EntryTicks && count == other.count;
                }

                public override string ToString()
                {
                    return key + " " + count + " " + entryTicks;
                }
            }

            public Stream<Record, IterationIn<Epoch>> Reduce(Stream<HTRecord, IterationIn<Epoch>> input)
            {
                var smaller = input.Select(r => new Record(r)).SetCheckpointPolicy(i => new CheckpointEagerly());
                var count = smaller.Select(r => r.key).Count().SetCheckpointPolicy(i => new CheckpointEagerly());
                var consumable = smaller
                    .Select(r => r.key.PairWith(r)).SetCheckpointPolicy(i => new CheckpointEagerly())
                    .Join(count, r => r.First, c => c.First, (r, c) => { r.Second.count = c.Second; return r.Second; }).SetCheckpointPolicy(i => new CheckpointEagerly());
                var reduced = consumable.SetCheckpointType(CheckpointType.StatelessLogAll).SetCheckpointPolicy(i => new CheckpointEagerly());
                this.reduceStage = reduced.ForStage.StageId;
                return reduced;
            }

            public Stream<Record, Epoch> Compute(Stream<Record, Epoch> input)
            {
                return input;
            }

            public int reduceStage;
            public IEnumerable<int> ToMonitor
            {
                get { return new int[] { reduceStage }; }
            }

            public Collection<Record, Epoch> Make(Computation computation)
            {
                this.source = new SubBatchDataSource<HTRecord, Epoch>();

                Placement placement =
                    new Placement.ProcessRange(Enumerable.Range(this.baseProc, this.range),
                        Enumerable.Range(0, computation.Controller.Configuration.WorkerCount));

                Collection<Record, Epoch> output;

                using (var p = computation.Placement(placement))
                {
                    var reduced = computation.BatchedEntry<Record, Epoch>(c =>
                            {
                                var input = computation.NewInput(this.source)
                                    .SetCheckpointType(CheckpointType.CachingInput)
                                    .SetCheckpointPolicy(s => new CheckpointEagerly());
                                return this.Reduce(input);
                            });

                    var computed = this.Compute(reduced);

                    output = computed
                        .Select(r =>
                        {
                            if (r.EntryTicks < 0)
                            {
                                r.EntryTicks = -r.EntryTicks;
                                return new Weighted<Record>(r, -1);
                            }
                            else
                            {
                                return new Weighted<Record>(r, 1);
                            }
                        }).AsCollection(false);
                }
                return output;
            }

            public SlowPipeline(int baseProc, int range)
            {
                this.baseProc = baseProc;
                this.range = range;
            }
        }

        public class FastPipeline
        {
            private FileStream checkpointLogFile = null;
            private StreamWriter checkpointLog = null;
            internal StreamWriter CheckpointLog
            {
                get
                {
                    if (checkpointLog == null)
                    {
                        string fileName = String.Format("fastPipe.{0:D3}.log", processId);
                        this.checkpointLogFile = new FileStream(fileName, FileMode.Create, FileAccess.Write, FileShare.ReadWrite);
                        this.checkpointLog = new StreamWriter(this.checkpointLogFile);
                        var flush = new System.Threading.Thread(new System.Threading.ThreadStart(() => FlushThread()));
                        flush.Start();
                    }
                    return checkpointLog;
                }
            }

            private void FlushThread()
            {
                while (true)
                {
                    Thread.Sleep(1000);
                    lock (this)
                    {
                        if (this.checkpointLog != null)
                        {
                            this.checkpointLog.Flush();
                            this.checkpointLogFile.Flush(true);
                        }
                    }
                }
            }

            public void WriteLog(string entry, params object[] args)
            {
                lock (this)
                {
                    this.CheckpointLog.WriteLine(entry, args);
                }
            }

            public int processId;
            public int baseProc;
            public int range;

            public class PrependVertex<R, T> : UnaryVertex<R, R, T> where T : Time<T> where R : IRecord
            {
                private HashSet<T> seenAny = new HashSet<T>();

                public override void OnReceive(Message<R, T> message)
                {
                    var output = this.Output.GetBufferForTime(message.time);
                    if (!seenAny.Contains(message.time))
                    {
                        this.NotifyAt(message.time);
                        this.seenAny.Add(message.time);

                        R r = default(R);
                        r.EntryTicks = -1;
                        output.Send(r);
                    }

                    for (int i = 0; i < message.length; i++)
                    {
                        output.Send(message.payload[i]);
                    }
                }

                public override void OnNotify(T time)
                {
                    this.seenAny.Remove(time);
                }

                public PrependVertex(int index, Stage<T> stage) : base(index, stage)
                {
                }
            }

            public class StaggeredJoinVertex<TInput1, TInput2, TKey, TInner, TOuter> : BinaryVertex<TInput1, TInput2, Record, TInner>
                where TInput2 : IRecord
                where TInner : Time<TInner>
                where TOuter : Time<TOuter>
            {
                private readonly Dictionary<TOuter, Dictionary<TKey, List<TInput2>>> values = new Dictionary<TOuter,Dictionary<TKey,List<TInput2>>>();

                private readonly Func<TInput1, TKey> keySelector1;
                private readonly Func<TInput2, TKey> keySelector2;
                private readonly Func<TInput1, TInput2, Record> resultSelector;
                private readonly Func<TInner, TOuter> timeSelector;
                private readonly Func<TOuter, TInner> notifySelector;

                protected override bool CanRollBackPreservingState(Pointstamp[] frontier)
                {
                    return true;
                }

                public override void RollBackPreservingState(Pointstamp[] frontier, ICheckpoint<TInner> lastFullCheckpoint, ICheckpoint<TInner> lastIncrementalCheckpoint)
                {
                    base.RollBackPreservingState(frontier, lastFullCheckpoint, lastIncrementalCheckpoint);
                }

                public override void OnReceive1(Message<TInput1, TInner> message)
                {
                    TOuter outer = timeSelector(message.time);

                    bool present;

                    lock (this)
                    {
                        present = this.values.ContainsKey(outer);
                    }

                    if (!present)
                    {
                        Console.WriteLine("Missing time " + outer + " for " + message.time);
                        // we are running ahead a bit. just drop the record on the floor instead of implementing
                        // it properly.
                        var brokenOutput = this.Output.GetBufferForTime(message.time);

                        for (int i = 0; i < message.length; i++)
                        {
                            Record r = resultSelector(message.payload[i], default(TInput2));
                            // at least we'll make it clear the latency is not correct...
                            r.startMs = -1;
                            brokenOutput.Send(r);
                        }
                        return;
                    }

                    Dictionary<TKey, List<TInput2>> currentValues;

                    lock (this)
                    {
                        currentValues = this.values[outer];
                    }

                    var output = this.Output.GetBufferForTime(message.time);

                    for (int i = 0; i < message.length; i++)
                    {
                        var key = keySelector1(message.payload[i]);

                        List<TInput2> currentEntry;
                        if (currentValues.TryGetValue(key, out currentEntry))
                        {
                            foreach (var match in currentEntry)
                                output.Send(resultSelector(message.payload[i], match));
                        }
                    }
                }

                public override void OnReceive2(Message<TInput2, TInner> message)
                {
                    Console.WriteLine("Receiving " + message.time);
                    TOuter outerTime = timeSelector(message.time);

                    int baseRecord = 0;

                    bool present;

                    if (message.payload[0].EntryTicks < 0)
                    {
                        ++baseRecord;
                        lock (this)
                        {
                            present = this.values.ContainsKey(outerTime);
                            if (present)
                            {
                                Console.WriteLine("Discarding " + outerTime);
                                this.values.Remove(outerTime);
                            }
                        }
                    }

                    Dictionary<TKey, List<TInput2>> currentValues;

                    lock (this)
                    {
                        if (!this.values.ContainsKey(outerTime))
                        {
                            this.values.Add(outerTime, new Dictionary<TKey, List<TInput2>>());
                        }
                        
                        currentValues = this.values[outerTime];
                    }

                    for (int i = baseRecord; i < message.length; i++)
                    {
                        var key = keySelector2(message.payload[i]);

                        List<TInput2> currentEntry;
                        if (!currentValues.TryGetValue(key, out currentEntry))
                        {
                            currentEntry = new List<TInput2>();
                            currentValues[key] = currentEntry;
                        }

                        currentEntry.Add(message.payload[i]);
                    }
                }

                public void RemoveTimesBelow(TOuter outerTime)
                {
                    Pointstamp outerStamp = outerTime.ToPointstamp(0);
                    lock (this)
                    {
                        foreach (var time in this.values.Keys.ToArray())
                        {
                            Pointstamp stamp = time.ToPointstamp(0);

                            if (!stamp.Equals(outerStamp) && FTFrontier.IsLessThanOrEqualTo(stamp, outerStamp))
                            {
                                Console.WriteLine("Removing " + time);
                                this.values.Remove(time);
                            }
                        }
                    }
                }

                public StaggeredJoinVertex(int index, Stage<TInner> stage, Func<TInput1, TKey> key1, Func<TInput2, TKey> key2, Func<TInput1, TInput2, Record> result,
                    Func<TInner, TOuter> timeSelector, Func<TOuter, TInner> notifySelector)
                    : base(index, stage)
                {
                    this.values = new Dictionary<TOuter, Dictionary<TKey, List<TInput2>>>();
                    this.keySelector1 = key1;
                    this.keySelector2 = key2;
                    this.resultSelector = result;
                    this.timeSelector = timeSelector;
                    this.notifySelector = notifySelector;
                }
            }

            public class ExitVertex : UnaryVertex<Record, Record, IterationIn<IterationIn<Epoch>>>
            {
                private readonly FastPipeline parent;

                public override void OnReceive(Message<Record, IterationIn<IterationIn<Epoch>>> message)
                {
                    parent.HoldOutputs(message);
                    this.NotifyAt(message.time);
                }

                public override void OnNotify(IterationIn<IterationIn<Epoch>> time)
                {
                    foreach (var vertex in this.parent.slowVertices)
                    {
                        vertex.RemoveTimesBelow(time.outerTime.outerTime);
                    }
                    foreach (var vertex in this.parent.ccVertices)
                    {
                        vertex.RemoveTimesBelow(time.outerTime);
                    }
                }

                private ExitVertex(int index, Stage<IterationIn<IterationIn<Epoch>>> stage, FastPipeline parent)
                    : base(index, stage)
                {
                    this.parent = parent;
                }

                public static Stream<Record, IterationIn<IterationIn<Epoch>>> ExitStage(
                    Stream<Record, IterationIn<IterationIn<Epoch>>> stream,
                    FastPipeline parent)
                {
                    return stream.NewUnaryStage<Record, Record, IterationIn<IterationIn<Epoch>>>(
                        (i,s) => new ExitVertex(i, s, parent), null, null, "ExitFastPipeline")
                        .SetCheckpointType(CheckpointType.Stateless);
                }
            }

            public struct Record : IEquatable<Record>
            {
                public int homeProcess;
                public long startMs;
                public int slowJoinKey;
                public int ccJoinKey;

                public bool Equals(Record other)
                {
                    return homeProcess == other.homeProcess &&
                        startMs == other.startMs &&
                        slowJoinKey == other.slowJoinKey;
                }
            }

            private Epoch slowTime;
            private bool gotSlowTime = false;
            private bool gotCCTime = false;

            public void AcceptSlowTime(Pointstamp stamp)
            {
                Epoch slowTime = new Epoch(stamp.Timestamp.a);
                lock (this)
                {
                    Console.WriteLine("Fast got slow time " + slowTime);
                    this.slowTime = slowTime;
                    this.gotSlowTime = true;
                }
            }

            public void AcceptCCTime(Pointstamp stamp)
            {
                bool start = false;

                IterationIn<Epoch> ccTime = new IterationIn<Epoch>(new Epoch(stamp.Timestamp.a), stamp.Timestamp.b);

                if (ccTime.iteration == int.MaxValue)
                {
                    return;
                }

                lock (this)
                {
                    //Console.WriteLine("Fast got CC time " + ccTime);
                    if (this.gotSlowTime && ccTime.outerTime.epoch >= this.slowTime.epoch && ccTime.iteration > 1)
                    {
                        if (ccTime.outerTime.epoch > this.slowTime.epoch)
                        {
                            ccTime = new IterationIn<Epoch>(this.slowTime, int.MaxValue);
                        }

                        Console.WriteLine("Setting new cc time " + ccTime);
                        this.dataSource.StartOuterBatch(ccTime);

                        if (!this.gotCCTime)
                        {
                            start = true;
                            this.gotCCTime = true;
                        }
                    }
                }

                if (start)
                {
                    var thread = new System.Threading.Thread(new System.Threading.ThreadStart(() => FeedThread()));
                    thread.Start();
                }
            }

            private Random random = new Random();

            private IEnumerable<Record> MakeBatch(int count)
            {
                long ticks = program.computation.TicksSinceStartup / TimeSpan.TicksPerMillisecond;
                for (int i=0; i<count; ++i)
                {
                    yield return new Record
                    {
                        homeProcess = this.processId,
                        startMs = ticks,
                        slowJoinKey = random.Next(Program.numberOfKeys),
                        ccJoinKey = random.Next(Program.numberOfKeys),
                    };
                }

                for (int i=0; i<range; ++i)
                {
                    yield return new Record
                    {
                        homeProcess = this.processId,
                        startMs = ticks,
                        slowJoinKey = i,
                        ccJoinKey = i,
                    };
                }
            }

            private void FeedThread()
            {
                while (true)
                {
                    lock (this)
                    {
                        this.dataSource.OnNext(this.MakeBatch(Program.fastBatchSize));
                    }
                    Thread.Sleep(Program.fastSleepTime);
                }
            }

            private Dictionary<IterationIn<IterationIn<Epoch>>, List<Record>>
                bufferedOutputs = new Dictionary<IterationIn<IterationIn<Epoch>>, List<Record>>();

            private Pointstamp holdTime = new Pointstamp();

            private Pointstamp ToPointstamp(IterationIn<IterationIn<Epoch>> time)
            {
                Pointstamp stamp = new Pointstamp();
                stamp.Location = this.resultStage;
                stamp.Timestamp.Length = 3;
                stamp.Timestamp.a = time.outerTime.outerTime.epoch;
                stamp.Timestamp.b = time.outerTime.iteration;
                stamp.Timestamp.c = time.iteration;
                return stamp;
            }

            private void HoldOutputs(Message<Record, IterationIn<IterationIn<Epoch>>> message)
            {
                lock (this)
                {
                    Pointstamp time = this.ToPointstamp(message.time);
                    if (holdTime.Location != 0 && FTFrontier.IsLessThanOrEqualTo(time, holdTime))
                    {
                        throw new ApplicationException("Behind the times");
                    }

                    List<Record> buffer;
                    if (!this.bufferedOutputs.TryGetValue(message.time, out buffer))
                    {
                        buffer = new List<Record>();
                        this.bufferedOutputs.Add(message.time, buffer);
                    }
                    for (int i = 0; i < message.length; ++i)
                    {
                        buffer.Add(message.payload[i]);
                    }
                }
            }

            private void ReleaseOutputs(Pointstamp time)
            {
                long doneMs = program.computation.TicksSinceStartup / TimeSpan.TicksPerMillisecond;

                List<Pair<IterationIn<IterationIn<Epoch>>, Record>> released = new List<Pair<IterationIn<IterationIn<Epoch>>, Record>>();

                lock (this)
                {
                    if (this.holdTime.Location == 0 || !FTFrontier.IsLessThanOrEqualTo(time, this.holdTime))
                    {
                        this.holdTime = time;
                        var readyTimes = this.bufferedOutputs.Keys
                            .Where(t => FTFrontier.IsLessThanOrEqualTo(this.ToPointstamp(t), this.holdTime))
                            .ToArray();
                        foreach (var ready in readyTimes)
                        {
                            Console.WriteLine("READY " + ready);
                            foreach (var record in this.bufferedOutputs[ready])
                            {
                                released.Add(ready.PairWith(record));
                            }
                            this.bufferedOutputs.Remove(ready);
                        }
                    }
                }

                foreach (var record in released.Take(1))
                {
                    if (record.Second.startMs == -1)
                    {
                        this.WriteLog("-1 -1 -1");
                        Console.WriteLine("-1");
                    }
                    else
                    {
                        long slowBatchMs = -1;
                        lock (program.slowBatchTimes)
                        {
                            if (!program.slowBatchTimes.TryGetValue(record.First.outerTime.outerTime, out slowBatchMs))
                            {
                                slowBatchMs = -1;
                            }
                        }
                        long ccBatchMs = -1;
                        lock (program.ccBatchTimes)
                        {
                            if (!program.ccBatchTimes.TryGetValue(record.First.outerTime, out ccBatchMs))
                            {
                                ccBatchMs = -1;
                            }
                        }

                        long latency = doneMs - record.Second.startMs;
                        long slowStaleness = (slowBatchMs < 0) ? -2 : doneMs - slowBatchMs;
                        long ccStaleness = (ccBatchMs < 0) ? -2 : doneMs - ccBatchMs;
                        this.WriteLog("{0:D11} {1:D11} {2:D11}", latency, slowStaleness, ccStaleness);
                        Console.WriteLine("{0:D11} {1:D11} {2:D11}", latency, slowStaleness, ccStaleness);
                    }
                }
            }

            private void ReactToStable(object o, StageStableEventArgs args)
            {
                if (args.stageId == this.slowStage)
                {
                    this.AcceptSlowTime(args.frontier[0]);
                }
                else if (args.stageId == this.ccStage)
                {
                    this.AcceptCCTime(args.frontier[0]);
                }
                else if (args.stageId == resultStage)
                {
                    this.ReleaseOutputs(args.frontier[0]);
                }
            }

            private SubBatchDataSource<Record, IterationIn<Epoch>> dataSource;
            public int slowStage;
            private int ccStage;
            private int resultStage;

            public IEnumerable<int> ToMonitor
            {
                get { return new int[] { this.slowStage, this.ccStage, this.resultStage }; }
            }

            private Stream<R, IterationIn<Epoch>> PrepareForFast<R>(Collection<R, IterationIn<Epoch>> input, Func<R, int> partitioning)
                where R : IEquatable<R>
            {
                return input
                    .ForcePartitionBy(r => partitioning(r))
                    .ToStateless().SetCheckpointType(CheckpointType.StatefulLogEphemeral).SetCheckpointPolicy(i => new CheckpointEagerly()).Output
                    .SelectMany(r => Enumerable.Repeat(r.record, (int)Math.Max(0, r.weight))).SetCheckpointType(CheckpointType.StatelessLogEphemeral).SetCheckpointPolicy(i => new CheckpointWithoutPersistence());
            }

            private Stream<Record, IterationIn<IterationIn<Epoch>>> Exit(Stream<Record, IterationIn<IterationIn<Epoch>>> results,
                int placementCount)
            {
                var output = results.PartitionBy(r => r.homeProcess).SetCheckpointPolicy(i => new CheckpointWithoutPersistence());
                var exit = ExitVertex.ExitStage(output, this).SetCheckpointPolicy(i => new CheckpointWithoutPersistence());

                this.resultStage = exit.ForStage.StageId;

                return output.SelectMany(r => Enumerable.Range(0, placementCount).Select(i =>
                    {
                        r.homeProcess = i;
                        return r;
                    })).SetCheckpointPolicy(i => new CheckpointWithoutPersistence())
                    .PartitionBy(r => r.homeProcess).SetCheckpointPolicy(i => new CheckpointWithoutPersistence());
            }

            HashSet<Program.FastPipeline.StaggeredJoinVertex<Record, SlowPipeline.Record, int, IterationIn<IterationIn<Epoch>>, Epoch>> slowVertices;
            HashSet<Program.FastPipeline.StaggeredJoinVertex<Record, CCPipeline.Record, int, IterationIn<IterationIn<Epoch>>, IterationIn<Epoch>>> ccVertices;

            public void Make(Computation computation,
                Collection<SlowPipeline.Record, IterationIn<Epoch>> slowOutput,
                Collection<CCPipeline.Record, IterationIn<Epoch>> ccOutput)
            {
                this.dataSource = new SubBatchDataSource<Record, IterationIn<Epoch>>();

                Placement placement = new Placement.ProcessRange(Enumerable.Range(this.baseProc, this.range), Enumerable.Range(0, computation.Controller.Configuration.WorkerCount));
                Placement senderPlacement = new Placement.ProcessRange(Enumerable.Range(this.baseProc, 1), Enumerable.Range(0, 1));

                using (var procs = computation.Placement(placement))
                {
                    Stream<Record, IterationIn<IterationIn<Epoch>>> input;
                    using (var sender = computation.Placement(senderPlacement))
                    {
                        input = computation.NewInput(dataSource).SetCheckpointType(CheckpointType.CachingInput);
                    }

                    var slow = this.PrepareForFast(slowOutput, r => r.key);
                    this.slowStage = slow.ForStage.StageId;
                    var cc = this.PrepareForFast(ccOutput, r => r.key);
                    this.ccStage = cc.ForStage.StageId;

                    var output = computation.BatchedEntry<Record, IterationIn<Epoch>>(ic =>
                        {
                            var firstJoin = input.SetCheckpointPolicy(i => new CheckpointWithoutPersistence())
                                .StaggeredJoin(ic.EnterLoop(slow).SetCheckpointPolicy(i => new CheckpointWithoutPersistence()),
                                    i => i.slowJoinKey, s => s.key, (i, s) => i,
                                    t => t.outerTime.outerTime, t => new IterationIn<IterationIn<Epoch>>(new IterationIn<Epoch>(t, int.MaxValue - 1), int.MaxValue - 1));

                            this.slowVertices = firstJoin.Second;

                            var secondJoin = firstJoin.First
                                .StaggeredJoin(ic.EnterLoop(cc).SetCheckpointPolicy(s => new CheckpointWithoutPersistence()),
                                    i => i.ccJoinKey, c => c.key, (i, c) => i,
                                    t => t.outerTime, t => new IterationIn<IterationIn<Epoch>>(t, int.MaxValue - 1));

                            this.ccVertices = secondJoin.Second;

                            return secondJoin.First.Compose(computation, senderPlacement, i => this.Exit(i, placement.Count));
                        }).SetCheckpointPolicy(s => new CheckpointWithoutPersistence());
                }

                if (Enumerable.Range(this.baseProc, this.range).Contains(computation.Controller.Configuration.ProcessID))
                {
                    if (computation.Controller.Configuration.ProcessID == this.baseProc)
                    {
                        computation.OnStageStable += this.ReactToStable;
                    }
                    else
                    {
                        this.dataSource.OnCompleted();
                    }
                }
            }

            public FastPipeline(int baseProc, int range)
            {
                this.baseProc = baseProc;
                this.range = range;
            }
        }

        public class CCPipeline
        {
            public int baseProc;
            public int range;
            public SubBatchDataSource<HTRecord, IterationIn<Epoch>> source;

            public struct Record : IRecord, IEquatable<Record>
            {
                public int key;
                public int otherKey;
                public long entryTicks;
                public long EntryTicks { get { return this.entryTicks; } set { this.entryTicks = value; } }

                public Record(HTRecord large)
                {
                    this.key = large.key;
                    this.otherKey = large.otherKey;
                    this.entryTicks = large.entryTicks;
                }

                public bool Equals(Record other)
                {
                    return key == other.key && EntryTicks == other.EntryTicks && otherKey == other.otherKey;
                }

                public override string ToString()
                {
                    return key + " " + otherKey + " " + entryTicks;
                }
            }

            private Stream<Record, IterationIn<IterationIn<Epoch>>> Reduce(Stream<HTRecord, IterationIn<IterationIn<Epoch>>> input)
            {
                var smaller = input.Select(r => new Record(r)).SetCheckpointPolicy(i => new CheckpointEagerly());
                var consumable = smaller.PartitionBy(r => r.key).SetCheckpointPolicy(i => new CheckpointEagerly());
                var reduced = consumable.SetCheckpointType(CheckpointType.StatelessLogAll).SetCheckpointPolicy(i => new CheckpointEagerly());
                this.reduceStage = reduced.ForStage.StageId;
                return reduced;
            }

            private Collection<Record, IterationIn<Epoch>> Compute(Collection<Record, IterationIn<Epoch>> input)
            {
                return input;
            }

            public Stream<R, IterationIn<T>> MakeInput<R, T>(
                Computation computation, Placement inputPlacement, SubBatchDataSource<R, T> source)
                where T : Time<T>
            {
                Stream<R, IterationIn<T>> input;

                using (var placement = computation.Placement(inputPlacement))
                {
                    input = computation.NewInput(source).SetCheckpointPolicy(s => new CheckpointEagerly());
                }

                return input;
            }

            public int reduceStage;
            public IEnumerable<int> ToMonitor
            {
                get { return new int[] { reduceStage }; }
            }

            public void Make(Computation computation, Placement inputPlacement, SlowPipeline slow, FastPipeline buggy, FastPipeline perfect)
            {
                this.source = new SubBatchDataSource<HTRecord, IterationIn<Epoch>>();

                Placement ccPlacement =
                    new Placement.ProcessRange(Enumerable.Range(this.baseProc, this.range),
                        Enumerable.Range(0, computation.Controller.Configuration.WorkerCount));

                var slowOutput = slow.Make(computation);

                var forCC = computation.BatchedEntry<Record, Epoch>(c =>
                    {
                        Collection<Record, IterationIn<Epoch>> cc;

                        using (var p = computation.Placement(ccPlacement))
                        {
                            var reduced = computation
                                .BatchedEntry<Record, IterationIn<Epoch>>(ic =>
                                    {
                                        var input = this.MakeInput(computation, inputPlacement, this.source);
                                        return this.Reduce(input);
                                    });

                            var asCollection = reduced.Select(r =>
                                {
                                    if (r.EntryTicks < 0)
                                    {
                                        r.EntryTicks = -r.EntryTicks;
                                        return new Weighted<Record>(r, -1);
                                    }
                                    else
                                    {
                                        return new Weighted<Record>(r, 1);
                                    }
                                }).AsCollection(false);
                            cc = this.Compute(asCollection);
                        }

                        //buggy.Make(computation, c.EnterLoop(slowOutput.Output).AsCollection(false), cc);
                        perfect.Make(computation, c.EnterLoop(slowOutput.Output).AsCollection(false), cc);

                        return cc;
                    });
            }

            public CCPipeline(int baseProc, int range)
            {
                this.baseProc = baseProc;
                this.range = range;
            }
        }

        public class HelperVertex<R> : SinkVertex<Pair<int, R>, Epoch>
        {
            private readonly Action<R> action;

            public override void OnReceive(Message<Pair<int, R>, Epoch> message)
            {
                for (int i = 0; i < message.length; ++i)
                {
                    action(message.payload[i].Second);
                }
            }

            private HelperVertex(int index, Stage<Epoch> stage, Action<R> action)
                : base(index, stage)
            {
                this.action = action;
            }

            public static void HelperStage(Stream<Pair<int, R>, Epoch> stream, Action<R> action)
            {
                stream.NewSinkStage<Pair<int, R>, Epoch>((i, s) => new HelperVertex<R>(i, s, action), null, "Helper")
                    .SetCheckpointType(CheckpointType.None);
            }
        }

        public struct HTRecord : IEquatable<HTRecord>
        {
            public int key;
            public int otherKey;
            public long entryTicks;

            public bool Equals(HTRecord other)
            {
                return key == other.key && otherKey == other.otherKey && entryTicks == other.entryTicks;
            }
        }

        private int processId;
        private FileStream checkpointLogFile = null;
        private StreamWriter checkpointLog = null;
        internal StreamWriter CheckpointLog
        {
            get
            {
                if (checkpointLog == null)
                {
                    string fileName = String.Format("fastPipe.{0:D3}.log", this.processId);
                    this.checkpointLogFile = new FileStream(fileName, FileMode.Create, FileAccess.Write, FileShare.ReadWrite);
                    this.checkpointLog = new StreamWriter(this.checkpointLogFile);
                    var flush = new System.Threading.Thread(new System.Threading.ThreadStart(() => FlushThread()));
                    flush.Start();
                }
                return checkpointLog;
            }
        }

        private void FlushThread()
        {
            while (true)
            {
                Thread.Sleep(1000);
                lock (this)
                {
                    if (this.checkpointLog != null)
                    {
                        this.checkpointLog.Flush();
                        this.checkpointLogFile.Flush(true);
                    }
                }
            }
        }

        public void WriteLog(string entry)
        {
            lock (this)
            {
                this.CheckpointLog.WriteLine(entry);
            }
        }

        static IEnumerable<HTRecord> MakeStartingBatch(Random random, int batchBase, int stride, int batchSize)
        {
            for (int i = 0; i < batchSize; ++i)
            {
                yield return new HTRecord
                {
                    key = batchBase + i*stride,
                    otherKey = random.Next(numberOfKeys),
                    entryTicks = 1
                };
            }
        }

        static IEnumerable<HTRecord> MakeBatch(Random random, int batchSize, int sign)
        {
            for (int i = 0; i < batchSize; ++i)
            {
                HTRecord record = new HTRecord
                {
                    key = random.Next(numberOfKeys),
                    otherKey = random.Next(numberOfKeys),
                    entryTicks = sign
                };
                yield return record;
            }
        }

        private int randomSeed;
        private Random random;

        void InitialBatch(
            int processId,
            Placement inputPlacement,
            SubBatchDataSource<HTRecord, Epoch> slowSource,
            SubBatchDataSource<HTRecord, IterationIn<Epoch>> ccSource)
        {
            if (inputPlacement.Select(p => p.ProcessId).Contains(processId))
            {
                this.random = new Random();

                int processes = inputPlacement.Count();

                // the processes are a contiguous range but don't start from zero. each process should put in
                // different keys...
                for (int i = (processId % processes); i < numberOfKeys; i += htBatchSize*processes)
                {
                    var batch = MakeStartingBatch(random, i, processes, htBatchSize);
                    slowSource.OnNext(batch);
                    slowSource.CompleteInnerBatch();
                    ccSource.OnNext(batch);
                    ccSource.CompleteInnerBatch();
                }

                // now start records that we will want to revoke, so use a seed
                this.randomSeed = random.Next();
                this.random = new Random(this.randomSeed);

                for (int i = 0; i < Program.htInitialBatches; ++i)
                {
                    var batch = MakeBatch(random, htBatchSize, 1).ToArray();
                    slowSource.OnNext(batch);
                    slowSource.CompleteInnerBatch();
                    ccSource.OnNext(batch);
                    ccSource.CompleteInnerBatch();
                }

                slowSource.CompleteOuterBatch();
                ccSource.CompleteOuterBatch();
            }
        }

        private Dictionary<IterationIn<Epoch>, long> slowBatches = new Dictionary<IterationIn<Epoch>, long>();
        private Dictionary<IterationIn<IterationIn<Epoch>>, long> ccBatches = new Dictionary<IterationIn<IterationIn<Epoch>>, long>();

        private long RoundBatch(long batchTime)
        {
            long millisecondsSinceStartup = this.computation.TicksSinceStartup / TimeSpan.TicksPerMillisecond;
            return millisecondsSinceStartup - (millisecondsSinceStartup % batchTime) + batchTime;
        }

        void FeedThread(Pair<long,long> firstBatchTime)
        {
            long nextSlowBatch = firstBatchTime.First;
            long nextCCBatch = firstBatchTime.Second;

            Random oldRandom = new Random(this.randomSeed);
            while (true)
            {
                Thread.Sleep(Program.htSleepTime);
                //var minusBatch = MakeBatch(oldRandom, Program.htBatchSize, -1).ToArray();
                //var plusBatch = MakeBatch(this.random, Program.htBatchSize, 1).ToArray();
                var minusBatch = MakeBatch(oldRandom, 1, -1).ToArray();
                var plusBatch = MakeBatch(this.random, 1, 1).ToArray();

                long ticks = DateTime.Now.Ticks;
                IterationIn<Epoch> slowBatchTime;
                IterationIn<IterationIn<Epoch>> ccBatchTime;
                Epoch? slowBatch = null;
                IterationIn<Epoch>? ccBatch = null;
                lock (this)
                {
                    if (this.slowBatches.Count < 20 && this.ccBatches.Count < 20)
                    {
                        if (ticks / TimeSpan.TicksPerMillisecond > nextSlowBatch)
                        {
                            slowBatch = this.slow.source.CompleteOuterBatch();
                            nextSlowBatch += Program.slowBatchTime;
                        }

                        this.slow.source.OnNext(minusBatch);
                        this.slow.source.OnNext(plusBatch);
                        slowBatchTime = this.slow.source.CompleteInnerBatch();

                        if (ticks / TimeSpan.TicksPerMillisecond > nextCCBatch)
                        {
                            ccBatch = this.cc.source.CompleteOuterBatch();
                            nextCCBatch += Program.ccBatchTime;
                        }

                        this.cc.source.OnNext(minusBatch);
                        this.cc.source.OnNext(plusBatch);
                        ccBatchTime = this.cc.source.CompleteInnerBatch();

                        this.slowBatches.Add(slowBatchTime, ticks);
                        this.ccBatches.Add(ccBatchTime, ticks);
                    }
                }

                if (this.processId == slowBase)
                {
                    if (slowBatch.HasValue)
                    {
                        this.SendSlowBatchTimes(slowBatch.Value, nextSlowBatch - Program.slowBatchTime);
                    }
                    if (ccBatch.HasValue)
                    {
                        Console.WriteLine("Sending batch time " + ccBatch.Value);
                        this.SendCCBatchTimes(ccBatch.Value, nextCCBatch - Program.slowBatchTime);
                    }
                }
            }
        }

        private void AcceptCCTime(Pointstamp stamp)
        {
            IterationIn<IterationIn<Epoch>> ccTime = new IterationIn<IterationIn<Epoch>>(
                new IterationIn<Epoch>(new Epoch(stamp.Timestamp.a), stamp.Timestamp.b), stamp.Timestamp.c);

            lock (this)
            {
                long ticks = DateTime.Now.Ticks;
                foreach (var time in this.ccBatches.Where(t => t.Key.LessThan(ccTime)).ToArray())
                {
                    long latency = (ticks - time.Value) / TimeSpan.TicksPerMillisecond;
                    this.WriteLog(String.Format("CC {0:D11} {1}", ticks, time.Key));
                    Console.WriteLine("CC " + time.Key);
                    this.ccBatches.Remove(time.Key);
                }
            }
        }

        private void AcceptSlowBatchTime(Pointstamp stamp)
        {
            IterationIn<Epoch> slowTime = new IterationIn<Epoch>(new Epoch(stamp.Timestamp.a), stamp.Timestamp.b);

            lock (this)
            {
                long ticks = DateTime.Now.Ticks;
                foreach (var time in this.slowBatches.Where(t => t.Key.LessThan(slowTime)).ToArray())
                {
                    long latency = (ticks - time.Value) / TimeSpan.TicksPerMillisecond;
                    this.WriteLog(String.Format("CC {0:D11} {1}", ticks, time.Key));
                    Console.WriteLine("SLOW " + time.Key);
                    this.slowBatches.Remove(time.Key);
                }
            }
        }

        private int lastEpoch = -1;

        private void AcceptSlowStableTime(Pointstamp stamp)
        {
            IterationIn<Epoch> slowTime = new IterationIn<Epoch>(new Epoch(stamp.Timestamp.a), stamp.Timestamp.b);
            bool start = false;
            lock (this)
            {
                if (slowTime.outerTime.epoch != this.lastEpoch)
                {
                    this.lastEpoch = slowTime.outerTime.epoch;
                    if (slowTime.outerTime.epoch == 0)
                    {
                        start = this.processId == slowBase;
                    }
                    else
                    {
                        this.cc.source.CompleteOuterBatch(new IterationIn<Epoch>(new Epoch(slowTime.outerTime.epoch - 1), int.MaxValue));
                    }
                }
            }
            if (start)
            {
                long firstSlowBatchTime = this.RoundBatch(slowBatchTime);
                long firstCCBatchTime = this.RoundBatch(ccBatchTime);
                this.batchCoordinator.OnNext(Enumerable.Range(0, slowRange).Select(i => i.PairWith(firstSlowBatchTime.PairWith(firstCCBatchTime))));
            }
        }

        private void StartBatches(Pair<long, long> firstBatchTime)
        {
            var thread = new System.Threading.Thread(new System.Threading.ThreadStart(() => FeedThread(firstBatchTime)));
            thread.Start();
        }

        private void ReactToStable(object o, StageStableEventArgs args)
        {
            if (args.stageId == this.slow.reduceStage)
            {
                this.AcceptSlowBatchTime(args.frontier[0]);
            }
            else if (args.stageId == this.cc.reduceStage)
            {
                this.AcceptCCTime(args.frontier[0]);
            }
            else if (args.stageId == this.perfect.slowStage)
            {
                this.AcceptSlowStableTime(args.frontier[0]);
            }
        }

        private Dictionary<Epoch, long> slowBatchTimes = new Dictionary<Epoch,long>();
        private Dictionary<IterationIn<Epoch>, long> ccBatchTimes = new Dictionary<IterationIn<Epoch>,long>();

        private void SlowBatchTimes(Pair<Epoch,long> time)
        {
            lock (slowBatchTimes)
            {
                if (time.Second < 0)
                {
                    if (this.slowBatchTimes.ContainsKey(time.First))
                    {
                        this.slowBatchTimes.Remove(time.First);
                    }
                }
                else
                {
                    this.slowBatchTimes.Add(time.First, time.Second);
                }
            }
        }

        private void CCBatchTimes(Pair<IterationIn<Epoch>, long> time)
        {
            Console.WriteLine("Got cc time " + time.First);
            lock (ccBatchTimes)
            {
                if (time.Second < 0)
                {
                    if (this.ccBatchTimes.ContainsKey(time.First))
                    {
                        this.ccBatchTimes.Remove(time.First);
                    }
                }
                else
                {
                    this.ccBatchTimes.Add(time.First, time.Second);
                }
            }
        }

        private void SendSlowBatchTimes(Epoch batch, long ticks)
        {
            this.slowBatchSource.OnNext(Enumerable.Range(0, processes).Select(i =>
                i.PairWith(batch.PairWith(ticks))));
        }

        private void SendCCBatchTimes(IterationIn<Epoch> batch, long ticks)
        {
            this.ccBatchSource.OnNext(Enumerable.Range(0, processes).Select(i =>
                i.PairWith(batch.PairWith(ticks))));
        }

#if false
        static private int slowBase = 1;
        static private int slowRange = 10;
        static private int ccBase = 11;
        static private int ccRange = 20;
        static private int fbBase = 31;
        static private int fbRange = 5;
        static private int fpBase = 36;
        static private int fpRange = 5;
        static private int numberOfKeys = 10000;
        static private int fastBatchSize = 1;
        static private int fastSleepTime = 100;
        static private int ccBatchTime = 1000;
        static private int slowBatchTime = 60000;
        static private int htBatchSize = 100;
        static private int htInitialBatches = 100;
        static private int htSleepTime = 1000;
#else
#if false
        static private int slowBase = 0;
        static private int slowRange = 1;
        static private int ccBase = 1;
        static private int ccRange = 1;
        static private int fbBase = 1;
        static private int fbRange = 1;
        static private int fpBase = 2;
        static private int fpRange = 1;
        static private int numberOfKeys = 100;
        static private int fastBatchSize = 1;
        static private int fastSleepTime = 100;
        static private int ccBatchTime = 1000;
        static private int slowBatchTime = 60000;
        static private int htBatchSize = 10;
        static private int htInitialBatches = 100;
#else
        static private int slowBase = 1;
        static private int slowRange = 1;
        static private int ccBase = 1;
        static private int ccRange = 1;
        static private int fbBase = 1;
        static private int fbRange = 1;
        static private int fpBase = 2;
        static private int fpRange = 1;
        static private int numberOfKeys = 10;
        static private int fastBatchSize = 1;
        static private int fastSleepTime = 1000;
        static private int ccBatchTime = 5000;
        static private int slowBatchTime = 20000;
        static private int htBatchSize = 100;
        static private int htSleepTime = 2000;
        static private int htInitialBatches = 10;
#endif
#endif

        static private Program program;
        private int processes;
        private Computation computation;
        private SlowPipeline slow;
        private CCPipeline cc;
        private FastPipeline buggy;
        private FastPipeline perfect;
        private BatchedDataSource<Pair<int,Pair<long,long>>> batchCoordinator;
        private BatchedDataSource<Pair<int, Pair<Epoch,long>>> slowBatchSource;
        private BatchedDataSource<Pair<int, Pair<IterationIn<Epoch>,long>>> ccBatchSource;

        public void Execute(string[] args)
        {
            FTManager manager = new FTManager();

            Configuration conf = Configuration.FromArgs(ref args);
            this.processId = conf.ProcessID;
            this.processes = conf.Processes;

            if (args.Length > 0 && args[0].ToLower() == "-azure")
            {
                conf.CheckpointingFactory = s => new AzureStreamSequence(accountName, accountKey, containerName, s);
            }
            else
            {
                System.IO.Directory.CreateDirectory("checkpoint");
                conf.CheckpointingFactory = s => new FileStreamSequence("checkpoint", s);
            }

            conf.DefaultCheckpointInterval = 5000;

            using (var computation = NewComputation.FromConfig(conf))
            {
                this.computation = computation;
                this.slow = new SlowPipeline(slowBase, slowRange);
                this.cc = new CCPipeline(ccBase, ccRange);
                this.buggy = new FastPipeline(fbBase, fbRange);
                this.perfect = new FastPipeline(fpBase, fpRange);

                Placement inputPlacement = new Placement.ProcessRange(Enumerable.Range(slowBase, slowRange), Enumerable.Range(0, 1));

                this.batchCoordinator = new BatchedDataSource<Pair<int, Pair<long, long>>>();
                using (var p = computation.Placement(inputPlacement))
                {
                    computation.NewInput(this.batchCoordinator).SetCheckpointType(CheckpointType.None)
                        .PartitionBy(x => x.First).SetCheckpointType(CheckpointType.None)
                        .HelperStage(r => this.StartBatches(r));
                }

                Placement processPlacement = new Placement.ProcessRange(Enumerable.Range(0, conf.Processes), Enumerable.Range(0, 1));

                using (var p = computation.Placement(processPlacement))
                {
                    this.slowBatchSource = new BatchedDataSource<Pair<int, Pair<Epoch, long>>>();
                    computation.NewInput(this.slowBatchSource).SetCheckpointType(CheckpointType.None)
                        .PartitionBy(x => x.First).SetCheckpointType(CheckpointType.None)
                        .HelperStage(r => this.SlowBatchTimes(r));

                    this.ccBatchSource = new BatchedDataSource<Pair<int, Pair<IterationIn<Epoch>, long>>>();
                    computation.NewInput(this.ccBatchSource).SetCheckpointType(CheckpointType.None)
                        .PartitionBy(x => x.First).SetCheckpointType(CheckpointType.None)
                        .HelperStage(r => this.CCBatchTimes(r));
                }

                this.cc.Make(computation, inputPlacement, this.slow, this.buggy, this.perfect);

                if (conf.ProcessID == 0)
                {
                    manager.Initialize(computation, slow.ToMonitor.Concat(cc.ToMonitor.Concat(perfect.ToMonitor.Concat(buggy.ToMonitor))).Distinct());
                }

                //computation.OnStageStable += (x, y) => { Console.WriteLine(y.stageId + " " + y.frontier[0]); };

                var stopwatch = System.Diagnostics.Stopwatch.StartNew();
                //computation.OnFrontierChange += (x, y) => { Console.WriteLine(stopwatch.Elapsed + "\t" + string.Join(", ", y.NewFrontier)); Console.Out.Flush(); };

                if (Enumerable.Range(slowBase, slowRange).Contains(conf.ProcessID))
                {
                    computation.OnStageStable += this.ReactToStable;
                    if (conf.ProcessID != slowBase)
                    {
                        this.batchCoordinator.OnCompleted();
                    }
                }

                if (conf.ProcessID != slowBase)
                {
                    this.slowBatchSource.OnCompleted();
                    this.ccBatchSource.OnCompleted();
                }

                computation.Activate();

                InitialBatch(conf.ProcessID, inputPlacement, slow.source, cc.source);

                if (conf.ProcessID == 0)
                {
                    IEnumerable<int> failSlow = Enumerable.Range(slowBase, slowRange);
                    IEnumerable<int> failMedium =
                        Enumerable.Range(ccBase, ccRange).Concat(Enumerable.Range(fbBase, fbRange)).Distinct()
                        .Except(failSlow);
                    IEnumerable<int> failFast = Enumerable.Range(fpBase, fpRange)
                        .Except(failSlow.Concat(failMedium));

                    while (true)
                    {
                        //System.Threading.Thread.Sleep(Timeout.Infinite);
                        System.Threading.Thread.Sleep(60000);
                        if (conf.Processes > 2)
                        {
                            manager.FailProcess(1);
                        }

                        manager.PerformRollback(failSlow, failMedium, failFast);
                    }
                }

                Thread.Sleep(Timeout.Infinite);

                computation.Join();
            }
        }

        static void Main(string[] args)
        {
            program = new Program();
            program.Execute(args);
        }
    }
}
