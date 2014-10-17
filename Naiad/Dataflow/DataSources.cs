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

using Microsoft.Research.Naiad.Runtime;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

using Microsoft.Research.Naiad.Diagnostics;
using Microsoft.Research.Naiad.Dataflow;

namespace Microsoft.Research.Naiad.Input
{
    /// <summary>
    /// Represents an untyped external input to a Naiad <see cref="Computation"/>.
    /// </summary>
    public interface DataSource
    {
        /// <summary>
        /// Called by the Computation before it completes its own Activate().
        /// </summary>
        void Activate();
        /// <summary>
        /// Called by the Computation before it completes its own Join().
        /// </summary>
        void Join();
    }

    /// <summary>
    /// Represents a typed external input of <typeparamref name="TRecord"/> records.
    /// </summary>
    /// <typeparam name="TRecord">The type of records that this data source provides.</typeparam>
    public interface DataSource<TRecord> : DataSource
    {
        /// <summary>
        /// Called with a sequence of streaming inputs to attach to the data source.
        /// </summary>
        /// <param name="inputs">A sequence of streaming inputs, one per local worker.</param>
        void RegisterInputs(IEnumerable<StreamingInput<TRecord>> inputs);
    }

    /// <summary>
    /// Extension methods
    /// </summary>
    public static class DataSourceExtensionMethods
    {
        /// <summary>
        /// Converts an <see cref="IEnumerable{TRecord}"/> into a constant Naiad <see cref="Stream{TRecord,Epoch}"/>, using the supplied <paramref name="computation"/>.
        /// </summary>
        /// <typeparam name="TRecord">record type</typeparam>
        /// <param name="source">input records</param>
        /// <param name="computation">graph manager</param>
        /// <returns>single epoch stream containing source records</returns>
        public static Stream<TRecord, Epoch> AsNaiadStream<TRecord>(this IEnumerable<TRecord> source, Computation computation)
        {
            return computation.NewInput(new ConstantDataSource<TRecord>(source));
        }

        /// <summary>
        /// Converts an <see cref="IObservable{TRecordEnumerable}"/> into a Naiad <see cref="Stream{TRecord,Epoch}"/>, using the supplied <paramref name="computation"/>. 
        /// </summary>
        /// <typeparam name="TRecord">record type</typeparam>
        /// <param name="source">input sequence of records</param>
        /// <param name="computation">graph manager</param>
        /// <returns>stream of records, each epoch defined by consecutive OnNext calls from the observable</returns>
        public static Stream<TRecord, Epoch> AsNaiadStream<TRecord>(this IObservable<IEnumerable<TRecord>> source, Computation computation)
        {
            var batched = new BatchedDataSource<TRecord>();
            var result = computation.NewInput(batched);

            source.Subscribe(batched);

            return result;
        }
    }

    /// <summary>
    /// A default DataSource implementation, recording per-vertex inputs.
    /// </summary>
    /// <typeparam name="TRecord">record type</typeparam>
    public class BaseDataSource<TRecord> : DataSource<TRecord>
    {
        /// <summary>
        /// StreamingInputs corresponding to local workers.
        /// </summary>
        protected StreamingInput<TRecord>[] inputsByWorker;

        /// <summary>
        /// Registers StreamingInputs with the DataSource.
        /// </summary>
        /// <param name="inputs">streaming inputs</param>
        public void RegisterInputs(IEnumerable<StreamingInput<TRecord>> inputs)
        {
            var inputsAsArray = inputs.ToArray();
            this.inputsByWorker = new StreamingInput<TRecord>[inputsAsArray.Length];
            for (int i = 0; i < inputsAsArray.Length; ++i)
                this.inputsByWorker[inputsAsArray[i].WorkerId] = inputsAsArray[i];
        }

        /// <summary>
        /// Called by the Computation before it completes its own Activate().
        /// </summary>
        public virtual void Activate() { }

        /// <summary>
        /// Called by the Computation before it completes its own Join().
        /// </summary>
        public virtual void Join() { }
    }

    /// <summary>
    /// A DataSource with fixed contents, produced in the first epoch.
    /// </summary>
    /// <typeparam name="TRecord">record type</typeparam>
    public class ConstantDataSource<TRecord> : BaseDataSource<TRecord>
    {
        private IEnumerable<TRecord> contents;

        /// <summary>
        /// Constructs a DataSource with constant contents.
        /// </summary>
        /// <param name="contents">source records</param>
        public ConstantDataSource(IEnumerable<TRecord> contents) { this.contents = contents; }

        /// <summary>
        /// Constructs a DataSource with a single element.
        /// </summary>
        /// <param name="element">source record</param>
        public ConstantDataSource(TRecord element) { this.contents = new TRecord[] { element }; }

        /// <summary>
        /// Constructs a DataSource with no elements.
        /// </summary>
        public ConstantDataSource() : this(Enumerable.Empty<TRecord>()) { }

        /// <summary>
        /// Supplies contents to the StreamingInputs.
        /// </summary>
        public override void Activate()
        {
            var currentBuffer = new TRecord[1024];
            var currentCursor = 0;
            var currentWorker = 0;

            lock (this)
            {
                foreach (var element in this.contents)
                {
                    currentBuffer[currentCursor++] = element;

                    if (currentCursor == currentBuffer.Length)
                    {
                        this.inputsByWorker[currentWorker].OnStreamingRecv(currentBuffer, 0);

                        currentBuffer = new TRecord[currentBuffer.Length];
                        currentCursor = 0;
                        currentWorker = (currentWorker + 1) % this.inputsByWorker.Length;
                    }
                }

                if (currentCursor > 0)
                    this.inputsByWorker[currentWorker].OnStreamingRecv(currentBuffer.Take(currentCursor).ToArray(), 0);

                for (int i = 0; i < this.inputsByWorker.Length; i++)
                    this.inputsByWorker[i].OnCompleted();
            }
        }
    }

    /// <summary>
    /// DataSource for reading from the Console.
    /// </summary>
    internal class ConsoleDataSource : BaseDataSource<string>
    {
        /// <summary>
        /// Consumes lines from the console
        /// </summary>
        public void ConsumeLines()
        {
            if (this.inputsByWorker == null)
                throw new InvalidOperationException("Cannot ingest data before the source has been connected.");

            while (true)
            {
                string[] parts = Console.ReadLine().Split(new char[] { ' ' }, StringSplitOptions.RemoveEmptyEntries);

                if (parts.Length == 0)
                {
                    for (int j = 0; j < this.inputsByWorker.Length; ++j)
                        this.inputsByWorker[j].OnCompleted();
                    return;
                }
                else if (parts[0].StartsWith("!"))
                {
                    int notifyEpoch = int.Parse(parts[0].Substring(1));
                    for (int j = 0; j < this.inputsByWorker.Length; ++j)
                        this.inputsByWorker[j].OnStreamingNotify(notifyEpoch);
                }
                else
                {
                    int recvEpoch = int.Parse(parts[0]);
                    Random rand = new Random();
                    this.inputsByWorker[rand.Next(this.inputsByWorker.Length)].OnStreamingRecv(new string[] { parts[1] }, recvEpoch);
                }
            }
        }
    }
    
    #region Inter-graph data source

    /// <summary>
    /// Data source from another graph manager.
    /// </summary>
    /// <typeparam name="TRecord">record type</typeparam>
    public class InterGraphDataSource<TRecord> : BaseDataSource<TRecord>
    {
        private readonly InterGraphDataSink<TRecord> Sink;

        /// <summary>
        /// Registers itself with the InterGraphDataSink.
        /// </summary>
        public override void Activate()
        {
            this.Sink.Register(this);
        }
        
        /// <summary>
        /// Forwards a message to the appropriate worker
        /// </summary>
        /// <param name="message">message</param>
        /// <param name="epoch">epoch</param>
        /// <param name="fromWorker">worker</param>
        public void OnRecv(TRecord[] message, int epoch, int fromWorker)
        {
            this.inputsByWorker[fromWorker].OnStreamingRecv(message, epoch);
        }

        /// <summary>
        /// Forwards a notification to the appropriate worker
        /// </summary>
        /// <param name="epoch">epoch</param>
        /// <param name="fromWorker">worker</param>
        public void OnNotify(int epoch, int fromWorker)
        {
            this.inputsByWorker[fromWorker].OnStreamingNotify(epoch);
        }

        /// <summary>
        /// Forwards a completion to the appropriate worker
        /// </summary>
        /// <param name="fromWorker">worker</param>
        public void OnCompleted(int fromWorker)
        {
            this.inputsByWorker[fromWorker].OnCompleted();
        }

        /// <summary>
        /// Constructs a new InterGraphDataSource from the given InterGraphDataSink.
        /// </summary>
        /// <param name="sink">The sink from which this source will consume records.</param>
        public InterGraphDataSource(InterGraphDataSink<TRecord> sink)
        {
            this.Sink = sink;
        }
    }

    /// <summary>
    /// Data sink for use by other graph managers.
    /// </summary>
    /// <typeparam name="TRecord">The type of records produced by this sink.</typeparam>
    public class InterGraphDataSink<TRecord>
    {
        private readonly List<InterGraphDataSource<TRecord>> TargetSources;
        private readonly Dictionary<int, List<TRecord>>[] StatesByWorker;
        private readonly List<Pair<int, TRecord[]>>[] FrozenStates;
        private readonly bool[] Completed;

        private int OutstandingRegistrations;
        private bool Sealed;

        private void OnRecv(Message<TRecord, Epoch> message, int fromWorker)
        {
            lock (this)
            {
                var dictionary = this.StatesByWorker[fromWorker];
                if (!dictionary.ContainsKey(message.time.epoch))
                    dictionary.Add(message.time.epoch, new List<TRecord>());

                var dictionaryTime = dictionary[message.time.epoch];

                for (int i = 0; i < message.length; i++)
                    dictionaryTime.Add(message.payload[i]);
            }
        }

        private void OnNotify(int epoch, int fromWorker)
        {
            lock (this)
            {
                var array = new TRecord[] { };

                if (this.StatesByWorker[fromWorker].ContainsKey(epoch))
                {
                    array = this.StatesByWorker[fromWorker][epoch].ToArray();
                    this.StatesByWorker[fromWorker].Remove(epoch);
                }

                foreach (var source in this.TargetSources)
                {
                    source.OnRecv(array, epoch, fromWorker);
                    source.OnNotify(epoch, fromWorker);
                }

                // stash the data if the sink is not yet sealed, as we might need to replay it for a new source.
                if (this.FrozenStates[fromWorker] != null)
                    this.FrozenStates[fromWorker].Add(new Pair<int, TRecord[]>(epoch, array));
            }
        }

        private void OnCompleted(int fromWorker)
        {
            lock (this)
            {
                foreach (var source in this.TargetSources)
                    source.OnCompleted(fromWorker);

                this.Completed[fromWorker] = true;
            }
        }

        /// <summary>
        /// Returns a new InterGraphDataSource for use by other graph managers.
        /// </summary>
        /// <returns>data source</returns>
        public InterGraphDataSource<TRecord> NewDataSource()
        {
            lock (this)
            {
                if (this.Sealed)
                    throw new Exception("Cannot create a new data source from a sealed data sink");
                else
                {
                    this.OutstandingRegistrations++;
                    return new InterGraphDataSource<TRecord>(this);
                }
            }
        }

        /// <summary>
        /// Mark the collection as not accepting any more data sources
        /// </summary>
        public void Seal()
        {
            lock (this)
            {
                if (!this.Sealed )
                {
                    this.Sealed = true;
                    this.TestForCleanup();
                }
            }
        }

        private void TestForCleanup()
        {
            lock (this)
            {
                if (this.Sealed && this.OutstandingRegistrations == 0)
                {
                    for (int i = 0; i < this.FrozenStates.Length; i++)
                        this.FrozenStates[i] = null;                    
                }
            }
        }

        internal void Register(InterGraphDataSource<TRecord> source)
        {
            lock (this)
            {
                this.OutstandingRegistrations--;
                this.TargetSources.Add(source);

                for (int i = 0; i < this.FrozenStates.Length; i++)
                {
                    foreach (var frozen in this.FrozenStates[i])
                    {
                        source.OnRecv(frozen.Second, frozen.First, i);
                        source.OnNotify(frozen.First, i);
                    }

                    if (this.Completed[i])
                        source.OnCompleted(i);
                }

                this.TestForCleanup();
            }
        }

        /// <summary>
        /// Creates a new InterGraphDataSink from a stream.
        /// </summary>
        /// <param name="stream">source stream</param>
        public InterGraphDataSink(Stream<TRecord, Epoch> stream)
        {
            this.TargetSources = new List<InterGraphDataSource<TRecord>>();

            var workers = stream.Context.Context.Manager.InternalComputation.Controller.Workers.Count;

            this.StatesByWorker = new Dictionary<int,List<TRecord>>[workers];
            for (int i = 0; i < this.StatesByWorker.Length; i++)
                this.StatesByWorker[i] = new Dictionary<int, List<TRecord>>();

            this.FrozenStates = new List<Pair<int,TRecord[]>>[workers];
            for (int i = 0; i < this.FrozenStates.Length; i++)
                this.FrozenStates[i] = new List<Pair<int, TRecord[]>>();

            this.Completed = new bool[workers];
            for (int i = 0; i < this.Completed.Length; i++)
                this.Completed[i] = false;

            var placement = stream.StageOutput.ForStage.Placement;

            stream.Subscribe((message, workerId) => { this.OnRecv(message, workerId); }, 
                             (epoch, workerId) => { this.OnNotify(epoch.epoch, workerId); }, 
                             workerId => this.OnCompleted(workerId));
        }
    }

    #endregion


    /// <summary>
    /// DataSource supporting manual epoch-at-a-time data introduction.
    /// </summary>
    /// <typeparam name="TRecord">record type</typeparam>
    public class BatchedDataSource<TRecord> : BaseDataSource<TRecord>, IObserver<IEnumerable<TRecord>>, IObserver<TRecord>
    {
        private int currentEpoch = 0;
        private bool completed = false;
        /// <summary>
        /// Introduces a batch of data for the next epoch.
        /// </summary>
        /// <param name="batch">records</param>
        public void OnNext(IEnumerable<TRecord> batch)
        {
            if (this.inputsByWorker == null)
                throw new InvalidOperationException("Cannot ingest data before the source has been connected.");

            var array = batch == null ? new TRecord[] { } : batch.ToArray();
            lock (this)
            {
                var arrayCursor = 0;
                for (int i = 0; i < this.inputsByWorker.Length; i++)
                {
                    var toEat = (array.Length / this.inputsByWorker.Length) + ((i < (array.Length % this.inputsByWorker.Length)) ? 1 : 0);
                    var chunk = new TRecord[toEat];

                    Array.Copy(array, arrayCursor, chunk, 0, toEat);
                    arrayCursor += toEat;

                    this.inputsByWorker[i].OnStreamingRecv(chunk, this.currentEpoch);
                    this.inputsByWorker[i].OnStreamingNotify(this.currentEpoch);
                }
                ++this.currentEpoch;
            }
        }

        /// <summary>
        /// Introduces a single record for the next epoch.
        /// </summary>
        /// <param name="record">record</param>
        public void OnNext(TRecord record)
        {
            this.OnNext(new TRecord[] { record });
        }

        /// <summary>
        /// Introduces no data for the next epcoh.
        /// </summary>
        public void OnNext()
        {
            this.OnNext(null);
        }

        /// <summary>
        /// Introduces a batch of data for the final epoch.
        /// </summary>
        /// <param name="batch">records</param>
        public void OnCompleted(IEnumerable<TRecord> batch)
        {
            if (this.inputsByWorker == null)
                throw new InvalidOperationException("Cannot ingest data before the source has been connected.");

            var array = batch == null ? new TRecord[] { } : batch.ToArray();
            lock (this)
            {
                var arrayCursor = 0;
                for (int i = 0; i < this.inputsByWorker.Length; i++)
                {
                    var toEat = (array.Length / this.inputsByWorker.Length) + ((i < (array.Length % this.inputsByWorker.Length)) ? 1 : 0);
                    var chunk = new TRecord[toEat];

                    Array.Copy(array, arrayCursor, chunk, 0, toEat);
                    arrayCursor += toEat;

                    this.inputsByWorker[i].OnStreamingRecv(chunk, this.currentEpoch);
                    this.inputsByWorker[i].OnCompleted();
                }
                ++this.currentEpoch;
            }

            this.completed = true;
        }

        /// <summary>
        /// Introduces a single record for the final epoch.
        /// </summary>
        /// <param name="record">record</param>
        public void OnCompleted(TRecord record)
        {
            this.OnCompleted(new TRecord[] { record });
        }

        /// <summary>
        /// Introduces no data for the final epoch.
        /// </summary>
        public void OnCompleted()
        {
            lock (this)
            {
                for (int i = 0; i < this.inputsByWorker.Length; i++)
                    this.inputsByWorker[i].OnCompleted();

                this.completed = true;
            }
        }

        /// <summary>
        /// Does nothing except test if OnCompleted has been called.
        /// </summary>
        public override void Join()
        {
            if (!this.completed)
                Logging.Error("BatchedDataSource.Join() called before BatchedDataSource.OnCompleted()");
        }

        /// <summary>
        /// Re-throws the exception.
        /// </summary>
        /// <param name="exception">exception</param>
        public void OnError(System.Exception exception) { throw exception; }
    }
}
