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

using Naiad.Runtime;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Naiad.Dataflow
{
    /// <summary>
    /// Can be activated by a graph manager, and joined to block until complete.
    /// </summary>
    public interface DataSource
    {
        /// <summary>
        /// Called by the GraphManager before it completes its own Activate().
        /// </summary>
        void Activate();
        /// <summary>
        /// Called by the GraphManager before it completes its own Join().
        /// </summary>
        void Join();
    }

    /// <summary>
    /// Can be supplied with per-vertex inputs.
    /// </summary>
    /// <typeparam name="TRecord"></typeparam>
    public interface DataSource<TRecord> : DataSource
    {
        // indicates a StreamingInput to be used for each local worker.
        void RegisterInputs(IEnumerable<StreamingInput<TRecord>> inputs);
    }

    public static class DataSourceExtensionMethods
    {
        /// <summary>
        /// Converts an IEnumerable into a constant Naiad stream, using the supplied graph manager.
        /// </summary>
        /// <typeparam name="TRecord">record type</typeparam>
        /// <param name="source">input records</param>
        /// <param name="manager">graph manager</param>
        /// <returns>single epoch stream containing source records</returns>
        public static Stream<TRecord, Epoch> AsNaiadStream<TRecord>(this IEnumerable<TRecord> source, GraphManager manager)
        {
            return manager.NewInput(new ConstantDataSource<TRecord>(source));
        }

        /// <summary>
        /// Converts an IObservable of IEnumerables into a Naiad stream, using the supplied graph manager. 
        /// </summary>
        /// <typeparam name="TRecord">record type</typeparam>
        /// <param name="source">input sequence of records</param>
        /// <param name="manager">graph manager</param>
        /// <returns>stream of records, each epoch defined by consecutive OnNext calls from the observable</returns>
        public static Stream<TRecord, Epoch> AsNaiadStream<TRecord>(this IObservable<IEnumerable<TRecord>> source, GraphManager manager)
        {
            var batched = new BatchedDataSource<TRecord>();
            var result = manager.NewInput(batched);

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
        /// Called by the GraphManager before it completes its own Activate().
        /// </summary>
        public virtual void Activate() { }

        /// <summary>
        /// Called by the GraphManager before it completes its own Join().
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
            var array = this.contents == null ? new TRecord[] { } : this.contents.ToArray();
            lock (this)
            {
                var arrayCursor = 0;
                for (int i = 0; i < this.inputsByWorker.Length; i++)
                {
                    var toEat = (array.Length / this.inputsByWorker.Length) + ((i < (array.Length % this.inputsByWorker.Length)) ? 1 : 0);
                    var chunk = new TRecord[toEat];

                    Array.Copy(array, arrayCursor, chunk, 0, toEat);
                    arrayCursor += toEat;

                    this.inputsByWorker[i].OnStreamingRecv(chunk, 0);
                    this.inputsByWorker[i].OnCompleted();
                }
            }
        }
    }

    /// <summary>
    /// DataSource for reading from the Console.
    /// </summary>
    public class ConsoleDataSource : BaseDataSource<string>
    {
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

        public override void Activate()
        {
            this.Sink.Register(this);
        }
        
        public void OnRecv(TRecord[] message, int epoch, int fromWorker)
        {
            this.inputsByWorker[fromWorker].OnStreamingRecv(message, epoch);
        }

        public void OnNotify(int epoch, int fromWorker)
        {
            this.inputsByWorker[fromWorker].OnStreamingNotify(epoch);
        }

        public void OnCompleted(int fromWorker)
        {
            this.inputsByWorker[fromWorker].OnCompleted();
        }

        /// <summary>
        /// Constructs a new InterGraphDataSource from an InterGraphDataSink.
        /// </summary>
        /// <param name="sink"></param>
        public InterGraphDataSource(InterGraphDataSink<TRecord> sink)
        {
            this.Sink = sink;
        }
    }

    /// <summary>
    /// Data sink for use by other graph managers.
    /// </summary>
    /// <typeparam name="TRecord"></typeparam>
    public class InterGraphDataSink<TRecord>
    {
        private readonly List<InterGraphDataSource<TRecord>> TargetSources;
        private readonly Dictionary<int, List<TRecord>>[] StatesByWorker;
        private readonly List<Pair<int, TRecord[]>>[] FrozenStates;
        private readonly bool[] Completed;

        private void OnRecv(Message<Pair<TRecord, Epoch>> message, int fromWorker)
        {
            lock (this)
            {
                for (int i = 0; i < message.length; i++)
                {
                    var dictionary = this.StatesByWorker[fromWorker];
                    if (!dictionary.ContainsKey(message.payload[i].v2.t))
                        dictionary.Add(message.payload[i].v2.t, new List<TRecord>());

                    dictionary[message.payload[i].v2.t].Add(message.payload[i].v1);
                }
            }
        }

        private void OnNotify(int epoch, int fromWorker)
        {
            lock (this)
            {
                if (this.StatesByWorker[fromWorker].ContainsKey(epoch))
                {
                    var array = this.StatesByWorker[fromWorker][epoch].ToArray();

                    foreach (var source in this.TargetSources)
                    {
                        source.OnRecv(array, epoch, fromWorker);
                        source.OnNotify(epoch, fromWorker);
                    }

                    this.FrozenStates[fromWorker].Add(new Pair<int, TRecord[]>(epoch, array));
                    this.StatesByWorker[fromWorker].Remove(epoch);
                }
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
            return new InterGraphDataSource<TRecord>(this);
        }

        internal void Register(InterGraphDataSource<TRecord> source)
        {
            lock (this)
            {
                this.TargetSources.Add(source);

                for (int i = 0; i < this.FrozenStates.Length; i++)
                {
                    foreach (var frozen in this.FrozenStates[i])
                    {
                        source.OnRecv(frozen.v2, frozen.v1, i);
                        source.OnNotify(frozen.v1, i);
                    }

                    if (this.Completed[i])
                        source.OnCompleted(i);
                }
            }
        }

        /// <summary>
        /// Creates a new InterGraphDataSink from a stream.
        /// </summary>
        /// <param name="stream">source stream</param>
        public InterGraphDataSink(Stream<TRecord, Epoch> stream)
        {
            this.TargetSources = new List<InterGraphDataSource<TRecord>>();

            var workers = stream.Context.Context.Manager.GraphManager.Controller.Workers.Count;

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

            stream.Subscribe((msg, i) => { this.OnRecv(msg, placement[i].ThreadId); }, (epoch, i) => { this.OnNotify(epoch.t, placement[i].ThreadId); }, i => this.OnCompleted(placement[i].ThreadId));
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

        public override void Join()
        {
            if (!this.completed)
                Logging.Error("BatchedDataSource.Join() called before BatchedDataSource.OnCompleted()");
        }

        public void OnError(System.Exception exception) { throw exception; }
    }
}
