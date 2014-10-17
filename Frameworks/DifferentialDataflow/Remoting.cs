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

using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Collections.Specialized;
using System.ComponentModel;
using System.Diagnostics;
using System.Linq;
using System.Net;
using System.Net.Sockets;
using System.Text;
using System.Threading;

using Microsoft.Research.Naiad.Dataflow;
using Microsoft.Research.Naiad;

using Microsoft.Research.Naiad.Diagnostics;

namespace Microsoft.Research.Naiad.Frameworks.DifferentialDataflow
{
    internal static class NaiadDataStreamProtocolExtensionMethods
    {
        public static void ToReceiver<R>(this Collection<R, Epoch> collection, NaiadDataStreamProtocol<Weighted<R>> receiver)
            where R : IEquatable<R>
        {
            collection.Subscribe(xs => { receiver.StartEpoch(); foreach (Weighted<R> x in xs) receiver.Send(x); receiver.EndEpoch(); });
        }
    }

    /// <summary>
    /// A set of callback methods that should be implemented to receive a stream that follows the
    /// Naiad remote data protocol.
    /// 
    /// The sequence of calls will be as follows:
    /// 
    /// ((StartEpoch (Send*) EndEpoch)* Close)* (StartEpoch (Send*) EndEpoch)? Shutdown
    /// </summary>
    /// <typeparam name="T">The type of records in this data stream.</typeparam>
    internal interface NaiadDataStreamProtocol<T>
    {
        /// <summary>
        /// Called before sending a batch of records in the same epoch.
        /// </summary>
        void StartEpoch();

        /// <summary>
        /// Called for each record in an epoch.
        /// </summary>
        /// <param name="record">The record being sent.</param>
        void Send(T record);

        /// <summary>
        /// Called after sending a batch of records in the same epoch.
        /// </summary>
        void EndEpoch();

        /// <summary>
        /// Called at the end of a remote Naiad session.
        /// </summary>
        void Close();

        /// <summary>
        /// Called at the end of a remote Naiad session, and the final message sent to this object.
        /// </summary>
        void Shutdown();
    }

    /// <summary>
    /// A data stream protocol handler that will broadcast data of a particular type to a group of zero or more Sockets or other protocol handlers,
    /// and aggregate the entire multiset of records seen so that they may be sent to new participants.
    /// </summary>
    /// <typeparam name="R">The type of records in this data stream.</typeparam>
    internal class BufferingDataStreamBroadcaster<R> : NaiadDataStreamProtocol<Weighted<R>>
        where R : IEquatable<R>
    {
        private readonly string name;

        private readonly Dictionary<R, long> totalAccumulation;
        private readonly Dictionary<R, long> lastEpochAccumulation;

        private readonly ConcurrentBag<NaiadDataStreamProtocol<Weighted<R>>> senders;
        private List<NaiadDataStreamProtocol<Weighted<R>>> currentSnapshot;

        public BufferingDataStreamBroadcaster(string name)
        {
            this.name = name;
            this.totalAccumulation = new Dictionary<R, long>();
            this.lastEpochAccumulation = new Dictionary<R, long>();
            this.senders = new ConcurrentBag<NaiadDataStreamProtocol<Weighted<R>>>();
            this.currentSnapshot = new List<NaiadDataStreamProtocol<Weighted<R>>>(1);
        }


        public void Add(NaiadDataStreamProtocol<Weighted<R>> sender)
        {
            lock (this) // Mutually exclusive with the execution of an epoch.
            {
                sender.StartEpoch();
                foreach (KeyValuePair<R, long> kvp in this.totalAccumulation)
                    sender.Send(new Weighted<R>(kvp.Key, kvp.Value));
                sender.EndEpoch();
                this.senders.Add(sender);
            }
        }

        public void Add(Socket s)
        {
            //this.Add(new DataStreamSocketSender<Weighted<R>>(s));
        }

        public void StartEpoch()
        {
            Monitor.Enter(this); // Exited at the end of EndEpoch().
            List<NaiadDataStreamProtocol<Weighted<R>>> newSenders = null;
            NaiadDataStreamProtocol<Weighted<R>> newSender;
            while (this.senders.TryTake(out newSender))
            {
                this.currentSnapshot.Add(newSender);
                if (newSenders == null)
                    newSenders = new List<NaiadDataStreamProtocol<Weighted<R>>>(1);
                newSenders.Add(newSender);
            }

            if (newSenders != null)
            {
                foreach (var sender in newSenders)
                    sender.StartEpoch();
                foreach (KeyValuePair<R, long> kvp in this.totalAccumulation)
                {
                    foreach (var sender in newSenders)
                        sender.Send(new Weighted<R>(kvp.Key, kvp.Value));
                }
                foreach (var sender in newSenders)
                    sender.EndEpoch();
            }

            foreach (var sender in this.currentSnapshot)
                sender.StartEpoch();

        }

        public void Send(Weighted<R> record)
        {
            Logging.Info("[BMRS {1}] Sending record: {0}", record, this.name);
            long value = 0;
            bool exists = this.lastEpochAccumulation.TryGetValue(record.record, out value);
            if (exists && value + record.weight == 0)
                this.lastEpochAccumulation.Remove(record.record);
            else
                this.lastEpochAccumulation[record.record] = value + record.weight;
        }

        public void EndEpoch()
        {
            foreach (KeyValuePair<R, long> kvp in this.lastEpochAccumulation)
            {
                foreach (var sender in this.currentSnapshot)
                {
                    sender.Send(new Weighted<R>(kvp.Key, kvp.Value));
                }
                long value = 0;
                bool exists = this.totalAccumulation.TryGetValue(kvp.Key, out value);
                if (exists && value + kvp.Value == 0)
                    this.totalAccumulation.Remove(kvp.Key);
                else
                    this.totalAccumulation[kvp.Key] = value + kvp.Value;
            }
            this.lastEpochAccumulation.Clear();
            foreach (var sender in this.currentSnapshot)
                sender.EndEpoch();
            Monitor.Exit(this); // Entered at the start of StartEpoch().
        }


        public void Close()
        {
            foreach (var sender in this.currentSnapshot)
                sender.Close();
        }

        public void Shutdown()
        {
            foreach (var sender in this.currentSnapshot)
                sender.Shutdown();
        }


    }

    /// <summary>
    /// A Naiad data stream protocol handler that presents the current aggregated state of a computation output
    /// as a key-value dictionary.
    /// </summary>
    /// <typeparam name="R">The type of records in the data stream.</typeparam>
    /// <typeparam name="K">The key type for the dictionary.</typeparam>
    /// <typeparam name="V">The value type for the dictionary.</typeparam>
    internal class RemoteDictionary<R, K, V> : INotifyCollectionChanged, NaiadDataStreamProtocol<Weighted<R>>, IEnumerable<KeyValuePair<K, V>>
        where R : IEquatable<R>
    {
        private readonly Action<Action> dispatcher;
        private readonly Func<R, K> keySelector;
        private readonly Func<R, V> valueSelector;
        private readonly SortedList<K, R> collection;

        /// <summary>
        /// The number of unique keys in the dictionary.
        /// </summary>
        public int Count
        {
            get { return this.collection.Count; }
        }

        /// <summary>
        /// The first key in the dictionary, according to the natural ordering on keys.
        /// </summary>
        public K FirstKey
        {
            get { return this.collection.Keys.Count > 0 ? this.collection.Keys[0] : default(K); }
        }

        /// <summary>
        /// The last key in the dictionary, according to the natural ordering on keys.
        /// </summary>
        public K LastKey
        {
            get { return this.collection.Keys.Count > 0 ? this.collection.Keys[this.collection.Keys.Count - 1] : default(K); }
        }

        /// <summary>
        /// Looks up the value associated with the given key.
        /// </summary>
        /// <param name="key">The key to look up.</param>
        /// <returns>The value associated with the given key.</returns>
        public V this[K key]
        {
            get
            {
                return this.valueSelector(this.collection[key]);
            }
        }

        private readonly List<Weighted<R>> buffer;
        private readonly Func<IEnumerable<KeyValuePair<K, V>>, IEnumerable<KeyValuePair<K, V>>> observedCollectionConverter;

        /// <summary>
        /// Constructs a new dictionary that processes the incoming data stream using the given selectors.
        /// </summary>
        /// <param name="keySelector">A function that extracts a key from an incoming record.</param>
        /// <param name="valueSelector">A function that extracts a value from an incoming record.</param>
        /// <param name="dispatcher">An action that invokes the given action using the appropriate dispatcher (i.e. on the appropriate thread).</param>
        /// <param name="observedCollectionConverter">A function that transforms the collection, e.g. by reordering it.</param>
        public RemoteDictionary(Func<R, K> keySelector, Func<R, V> valueSelector, Action<Action> dispatcher, Func<IEnumerable<KeyValuePair<K, V>>, IEnumerable<KeyValuePair<K, V>>> observedCollectionConverter = null)
        {
            this.dispatcher = dispatcher;
            this.keySelector = keySelector;
            this.valueSelector = valueSelector;
            this.collection = new SortedList<K, R>();
            this.buffer = new List<Weighted<R>>();
            this.observedCollectionConverter = observedCollectionConverter;
        }

        public void StartEpoch()
        {

        }

        public void Send(Weighted<R> record)
        {
            Debug.Assert(record.weight == -1 || record.weight == 1);
            this.buffer.Add(record);
        }

        public void EndEpoch()
        {
            foreach (Weighted<R> deletion in this.buffer.Where(x => x.weight == -1))
                this.collection.Remove(this.keySelector(deletion.record));
            foreach (Weighted<R> addition in this.buffer.Where(x => x.weight == 1))
                this.collection[this.keySelector(addition.record)] = addition.record;
            this.buffer.Clear();

            this.dispatcher(() => this.DoEndEpochInUIThread());
        }

        public void CancelEpoch()
        {
            this.buffer.Clear();
        }

        private void DoEndEpochInUIThread()
        {
            if (this.CollectionChanged != null)
            {
                this.CollectionChanged(this, new NotifyCollectionChangedEventArgs(NotifyCollectionChangedAction.Reset));
            }
        }

        public void Close()
        {
            Logging.Info("Closing connection");
        }

        public void Shutdown()
        {
            Logging.Info("Shutting down computation");
        }

        System.Collections.IEnumerator System.Collections.IEnumerable.GetEnumerator()
        {
            if (this.observedCollectionConverter != null)
                return this.observedCollectionConverter(this.collection.Select(x => new KeyValuePair<K, V>(x.Key, this.valueSelector(x.Value)))).GetEnumerator();
            else
                return this.collection.Select(x => new KeyValuePair<K, V>(x.Key, this.valueSelector(x.Value))).GetEnumerator();
        }

        public IEnumerator<KeyValuePair<K, V>> GetEnumerator()
        {
            if (this.observedCollectionConverter != null)
                return this.observedCollectionConverter(this.collection.Select(x => new KeyValuePair<K, V>(x.Key, this.valueSelector(x.Value)))).GetEnumerator();
            else
                return this.collection.Select(x => new KeyValuePair<K, V>(x.Key, this.valueSelector(x.Value))).GetEnumerator();
        }

        /// <summary>
        /// This event fires at the end of an epoch where the collection has changed.
        /// </summary>
        public event NotifyCollectionChangedEventHandler CollectionChanged;
    }

    /// <summary>
    /// A Naiad data stream protocol handler that presents the current aggregated state of a computation output
    /// as a collection of records.
    /// </summary>
    /// <typeparam name="R">The type of records in the data stream.</typeparam>
    internal class RemoteEnumerable<R> : INotifyCollectionChanged, INotifyPropertyChanged, NaiadDataStreamProtocol<Weighted<R>>, IEnumerable<R>
        where R : IEquatable<R>
    {
        private readonly Action<Action> dispatcher;
        private readonly List<R> collection;

        private readonly List<Weighted<R>> buffer;

        private R first;


        /// <summary>
        /// Constructs a new collection that transforms the incoming data stream into an enumerable collection of records.
        /// </summary>
        /// <param name="dispatcher">An action that invokes the given action using the appropriate dispatcher (i.e. on the appropriate thread).</param>
        public RemoteEnumerable(Action<Action> dispatcher)
        {
            this.dispatcher = dispatcher;
            this.collection = new List<R>();
            this.buffer = new List<Weighted<R>>();
            this.first = default(R);
        }

        public void StartEpoch()
        {

        }

        public void Send(Weighted<R> record)
        {
            Console.Error.WriteLine("Got!: {0}", record);
            this.buffer.Add(record);
        }

        private void DeferredSend(Weighted<R> record)
        {
            if (record.weight < 0)
            {
                for (int i = 0; i < -record.weight; ++i)
                    this.collection.Remove(record.record);
            }
            else
            {
                for (int i = 0; i < record.weight; ++i)
                    this.collection.Add(record.record);
            }
        }

        public void EndEpoch()
        {
            foreach (Weighted<R> record in this.buffer)
                this.DeferredSend(record);
            this.buffer.Clear();
            this.dispatcher(() => this.DoEndEpochInUIThread());
        }

        public void CancelEpoch()
        {
            this.buffer.Clear();
        }

        private void DoEndEpochInUIThread()
        {
            if (this.CollectionChanged != null)
            {
                this.CollectionChanged(this, new NotifyCollectionChangedEventArgs(NotifyCollectionChangedAction.Reset));
            }

            if (this.first == null || !this.first.Equals(this.collection.FirstOrDefault()))
            {
                this.first = this.collection.FirstOrDefault();
                if (this.PropertyChanged != null)
                {
                    this.PropertyChanged(this, new PropertyChangedEventArgs("First"));
                }
            }

        }

        public void Close()
        {
            Logging.Info("Closing connection");
        }

        public void Shutdown()
        {
            Logging.Info("Shutting down computation");
        }

        System.Collections.IEnumerator System.Collections.IEnumerable.GetEnumerator()
        {
            return this.collection.GetEnumerator();
        }

        public IEnumerator<R> GetEnumerator()
        {
            return this.collection.GetEnumerator();
        }

        /// <summary>
        /// Returns the first item in this collection. Changes to this property will fire the
        /// PropertyChanged event.
        /// </summary>
        public R First
        {
            get
            {
                return this.first;
            }
        }

        /// <summary>
        /// Fired when the collection changes at the end of an epoch.
        /// </summary>
        public event NotifyCollectionChangedEventHandler CollectionChanged;

        /// <summary>
        /// Fired when the .First property changes at the end of an epoch.
        /// </summary>
        public event PropertyChangedEventHandler PropertyChanged;

    }
}
