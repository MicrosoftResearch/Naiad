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

﻿using Naiad.Runtime;
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
        void Activate();
        void Join();
    }

    /// <summary>
    /// Can be supplied with per-vertex inputs.
    /// </summary>
    /// <typeparam name="TRecord"></typeparam>
    public interface DataSource<TRecord> : DataSource
    {
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

    /// <summary>
    /// DataSource for adapting a stream from another graph manager.
    /// </summary>
    /// <typeparam name="TRecord">record type</typeparam>
    public class ExternalGraphDataSource<TRecord> : BaseDataSource<TRecord>
    {
        private void OnRecv(Message<Pair<TRecord, Epoch>> message, int fromWorker)
        {
            Logging.Info("In EGSI.OnRecv");
            for (int i = 0; i < message.length; ++i)
            {
                this.inputsByWorker[fromWorker].OnStreamingRecv(new TRecord[] { message.payload[i].v1 }, message.payload[i].v2.t);
            }
        }

        private void OnNotify(int epoch, int fromWorker)
        {
            this.inputsByWorker[fromWorker].OnStreamingNotify(epoch);
        }

        private void OnCompleted(int fromWorker)
        {
            this.inputsByWorker[fromWorker].OnCompleted();
        }

        public ExternalGraphDataSource(Stream<TRecord, Epoch> externalGraphStream)
        {
            externalGraphStream.Subscribe((msg, i) => { this.OnRecv(msg, i); }, (epoch, i) => { this.OnNotify(epoch.t, i); }, this.OnCompleted);
        }
    }

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
                throw new Exception("BatchedDataSource.Join() called before BatchedDataSource.OnCompleted()");
        }

        public void OnError(System.Exception exception) { throw exception; }
    }
}
