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
using System.Threading.Tasks;

using Microsoft.WindowsAzure.Storage.Blob;
using Microsoft.WindowsAzure.Storage.Table;
using Microsoft.WindowsAzure.Storage;
using System.Reactive;

using Microsoft.Research.Naiad;
using Microsoft.Research.Naiad.Dataflow;
using Microsoft.Research.Naiad.Frameworks.Lindi;
using Microsoft.Research.Naiad.Dataflow.PartitionBy;
using Microsoft.Research.Naiad.Dataflow.Channels;
using Microsoft.Research.Naiad.Serialization;
using Microsoft.Research.Naiad.Input;

namespace Microsoft.Research.Naiad.Frameworks.Azure
{
    /// <summary>
    /// The Azure framework contains extension methods for <see cref="Computation"/> and <see cref="Stream{TRecord,TTime}"/>
    /// that facilitate the use of Microsoft Azure Storage.
    /// </summary>
    class NamespaceDoc
    { }

    /// <summary>
    /// Extension methods
    /// </summary>
    public static class ExtensionMethods
    {
        #region Default connection string.

        private const string DefaultConnectionString = "Microsoft.Research.Naiad.Cluster.Azure.DefaultConnectionString";

        /// <summary>
        /// Returns the default <see cref="CloudStorageAccount"/> for this computation.
        /// </summary>
        /// <param name="computation">The computation.</param>
        /// <returns>The default <see cref="CloudStorageAccount"/> for this computation.</returns>
        /// <exception cref="ArgumentOutOfRangeException">Thrown when the <c>Microsoft.Research.Naiad.Cluster.Azure.DefaultConnectionString</c> setting is not set or has more than one value in <see cref="Configuration.AdditionalSettings"/> 
        /// </exception>
        /// <exception cref="ArgumentOutOfRangeException">Thrown when the <c>Microsoft.Research.Naiad.Cluster.Azure.DefaultConnectionString</c> setting cannot be parsed.</exception>
        public static CloudStorageAccount DefaultAccount(this Computation computation)
        {
            string[] connectionStrings = computation.Controller.Configuration.AdditionalSettings.GetValues(DefaultConnectionString);

            if (connectionStrings == null)
                throw new ArgumentOutOfRangeException("DefaultConnectionString not set");

            if (connectionStrings.Length == 0)
                throw new ArgumentOutOfRangeException("DefaultConnectionString not set");

            if (connectionStrings.Length > 1)
                throw new ArgumentOutOfRangeException("Cannot set more than one DefaultConnectionString");

            CloudStorageAccount ret;
            bool success = CloudStorageAccount.TryParse(connectionStrings[0], out ret);

            if (!success)
                throw new ArgumentOutOfRangeException(string.Format("Invalid DefaultConnectionString: {0}", connectionStrings[0]));

            return ret;
        }

        /// <summary>
        /// Returns a reference to the <see cref="CloudBlobContainer"/> with the given <paramref name="containerName"/>
        /// in the default account for this computation. The container will be created if it does not exist.
        /// </summary>
        /// <param name="computation">The computation.</param>
        /// <param name="containerName">The container name.</param>
        /// <returns>A reference to the container.</returns>
        /// <exception cref="ArgumentOutOfRangeException">Thrown when the <c>Microsoft.Research.Naiad.Cluster.Azure.DefaultConnectionString</c> setting is not set or has more than one value in <see cref="Configuration.AdditionalSettings"/> 
        /// </exception>
        /// <exception cref="ArgumentOutOfRangeException">Thrown when the <c>Microsoft.Research.Naiad.Cluster.Azure.DefaultConnectionString</c> setting cannot be parsed.</exception>
        public static CloudBlobContainer DefaultBlobContainer(this Computation computation, string containerName)
        {
            CloudStorageAccount account = computation.DefaultAccount();
            CloudBlobClient blobClient = account.CreateCloudBlobClient();
            CloudBlobContainer container = blobClient.GetContainerReference(containerName);
            container.CreateIfNotExists();
            return container;
        }

        #endregion

        #region Azure file-reading extension methods

        /// <summary>
        /// Reads the contents of all files in an Azure directory into a Naiad stream.
        /// </summary>
        /// <typeparam name="TRecord">Type of records stored in the blobs</typeparam>
        /// <param name="manager">Graph manager</param>
        /// <param name="container">Azure container</param>
        /// <param name="prefix">Azure blob prefix</param>
        /// <param name="reader">Decoding function from a stream to sequence of records</param>
        /// <returns>Naiad stream containing the records extracted from files in the Azure directory</returns>
        public static Stream<TRecord, Epoch> ReadFromAzureBlobs<TRecord>(this Computation manager, CloudBlobContainer container, string prefix, Func<System.IO.Stream, IEnumerable<TRecord>> reader)
        {
            var directory = container.GetDirectoryReference(prefix);

            var blobs = directory.ListBlobs();

            return blobs.AsNaiadStream(manager)
                        .Select(blob => blob.Uri.AbsolutePath)
                        .Distinct()
                        .Select(path => path.Substring(path.LastIndexOf('/') + 1))
                        .Select(blobname => directory.GetBlockBlobReference(blobname).OpenRead())
                        .SelectMany(reader);
        }

        /// <summary>
        /// Reads the contents of all files in an Azure directory into a Naiad stream.
        /// 
        /// The serialization format is the same as WriteBinaryToAzureBlobs, and based on the Naiad message format.
        /// </summary>
        /// <typeparam name="TRecord">Type of records stored in the blobs</typeparam>
        /// <param name="manager">Graph manager</param>
        /// <param name="container">Azure container</param>
        /// <param name="prefix">Azure blob prefix</param>
        /// <returns>Naiad stream containing the records extracted from files in the Azure directory</returns>
        public static Stream<TRecord, Epoch> ReadBinaryFromAzureBlobs<TRecord>(this Computation manager, CloudBlobContainer container, string prefix)
        {
            return manager.ReadFromAzureBlobs<TRecord>(container, prefix, stream => GetNaiadReaderEnumerable<TRecord>(stream, manager.Controller.SerializationFormat));
        }

        /// <summary>
        /// Reads the contents of all binary files in an Azure directory into a Naiad stream.
        /// </summary>
        /// <typeparam name="R">Type of records stored in the blobs</typeparam>
        /// <param name="manager">Graph manager</param>
        /// <param name="container">Azure container</param>
        /// <param name="prefix">Azure blob prefix</param>
        /// <param name="reader">Decoding function from a binary reader to a sequence of records</param>
        /// <returns>Naiad stream containing the records extracted from files in the Azure directory</returns>
        public static Stream<R, Epoch> ReadCustomBinaryFromAzureBlobs<R>(this Computation manager, CloudBlobContainer container, string prefix, Func<System.IO.BinaryReader, IEnumerable<R>> reader)
        {
            return manager.ReadFromAzureBlobs<R>(container, prefix, s => reader(new System.IO.BinaryReader(s)));
        }

        /// <summary>
        /// Reads the contents of all text files in an Azure directory into a Naiad stream.
        /// </summary>
        /// <param name="manager">Graph manager</param>
        /// <param name="container">Azure container</param>
        /// <param name="prefix">Azure blob prefix</param>
        /// <returns>Naiad stream containing the lines extracted from files in the Azure directory</returns>
        public static Stream<string, Epoch> ReadTextFromAzureBlobs(this Computation manager, CloudBlobContainer container, string prefix)
        {
            return manager.ReadFromAzureBlobs<string>(container, prefix, s => s.ReadLines());
        }

        /// <summary>
        /// Reads the entities from the given Azure table, filtered by the given query.
        /// </summary>
        /// <typeparam name="TEntity">Type of entity returned by the query</typeparam>
        /// <param name="manager">Graph manager</param>
        /// <param name="table">Azure table</param>
        /// <param name="query">Query to be applied to the table</param>
        /// <returns>Naiad stream containing the entities extracted by the table query</returns>
        public static Stream<TEntity, Epoch> ReadFromAzureTable<TEntity>(this Computation manager, CloudTable table, TableQuery<TEntity> query)
            where TEntity : ITableEntity, new()
        {
            CloudTable[] tables = new CloudTable[] { table };

            return tables.AsNaiadStream(manager).SelectMany(x => x.ExecuteQuery(query));
        }

        /// <summary>
        /// Enumerates lines of text from a stream
        /// </summary>
        /// <param name="stream">source stream</param>
        /// <returns>Each line of text in the source stream</returns>
        internal static IEnumerable<string> ReadLines(this System.IO.Stream stream)
        {
            using (var reader = new System.IO.StreamReader(stream))
            {
                while (!reader.EndOfStream)
                    yield return reader.ReadLine();
            }
        }

        /// <summary>
        /// Enumerates records from a stream in the Naiad serialization format.
        /// </summary>
        /// <typeparam name="TRecord">Type of record in the stream</typeparam>
        /// <param name="stream">A stream containing records serialized in the Naiad messaging format</param>
        /// <param name="codeGenerator">code generator</param>
        /// <returns>An enumeration of records in the stream</returns>
        internal static IEnumerable<TRecord> GetNaiadReaderEnumerable<TRecord>(System.IO.Stream stream, SerializationFormat codeGenerator)
        {
            NaiadReader reader = new NaiadReader(stream, codeGenerator);
            NaiadSerialization<TRecord> deserializer = codeGenerator.GetSerializer<TRecord>();
            TRecord nextElement;
            while (reader.TryRead<TRecord>(deserializer, out nextElement))
                yield return nextElement;
        }


        #endregion

        #region Azure file-writing extension methods

        /// <summary>
        /// Writes records to files in an Azure directory.
        /// </summary>
        /// <typeparam name="TRecord">Type of records in the incoming stream</typeparam>
        /// <param name="source">Source stream</param>
        /// <param name="container">Azure container</param>
        /// <param name="format">Format string for filenames, where {0} is the process ID and {1} is the worker ID</param>
        /// <param name="writer">Function mapping I/O streams to record observers</param>
        /// <returns>Subscription corresponding to the Azure writer</returns>
        public static Subscription WriteToAzureBlobs<TRecord>(this Stream<TRecord, Epoch> source, CloudBlobContainer container, string format, Func<CloudBlobStream, IObserver<TRecord>> writer)
        {
            var writers = new Dictionary<int, IObserver<TRecord>>();

            return source.Subscribe((message, workerid) =>
            {
                IObserver<TRecord> observer;

                lock (writers)
                {
                    if (!writers.ContainsKey(workerid))
                        writers.Add(workerid, writer(container.GetBlockBlobReference(string.Format(format, workerid)).OpenWrite()));

                    observer = writers[workerid];
                }

                for (int i = 0; i < message.length; i++)
                    observer.OnNext(message.payload[i]);
            },
                                    (epoch, workerid) => { },
                                    workerid =>
                                    {
                                        lock (writers)
                                        {
                                            if (writers.ContainsKey(workerid))
                                                writers[workerid].OnCompleted();
                                        };
                                    });

        }

        /// <summary>
        /// Writes records to files in an Azure directory.
        /// 
        /// The serialization format is the same as ReadBinaryFromAzureBlobs, and based on the Naiad message format.
        /// </summary>
        /// <typeparam name="TRecord">Type of records in the incoming stream</typeparam>
        /// <param name="source">Source stream</param>
        /// <param name="container">Azure container</param>
        /// <param name="format">Format string for filenames, where {0} is the process ID and {1} is the worker ID</param>
        /// <returns>Subscription corresponding to the Azure writer</returns>
        public static Subscription WriteBinaryToAzureBlobs<TRecord>(this Stream<TRecord, Epoch> source, CloudBlobContainer container, string format)
        {
            return source.WriteToAzureBlobs(container, format, stream => GetNaiadWriterObserver<TRecord>(stream, source.ForStage.Computation.Controller.SerializationFormat));
        }

        /// <summary>
        /// Returns an record observer that writes records to the given stream in the Naiad message format.
        /// </summary>
        /// <typeparam name="TRecord">Type of records to be written</typeparam>
        /// <param name="stream">Target I/O stream</param>
        /// <param name="codeGenerator">code generator</param>
        /// <returns>A record observer that writes records to the given stream.</returns>
        internal static IObserver<TRecord> GetNaiadWriterObserver<TRecord>(System.IO.Stream stream, SerializationFormat codeGenerator)
        {
            NaiadWriter<TRecord> writer = new NaiadWriter<TRecord>(stream, codeGenerator);
            return Observer.Create<TRecord>(r => 
            {
                writer.Write(r);
            },
            () => writer.Dispose());
        }

        /// <summary>
        /// Writes records to binary files in an Azure directory.
        /// </summary>
        /// <typeparam name="TRecord">Type of records to be written</typeparam>
        /// <param name="source">Source stream</param>
        /// <param name="container">Azure container</param>
        /// <param name="format">Format string for filenames, where {0} is the process ID and {1} is the worker ID</param>
        /// <param name="writeAction">Action for writing a record to a binary writer</param>
        /// <returns>Subscription corresponding to the Azure writer</returns>
        public static Subscription WriteCustomBinaryToAzureBlobs<TRecord>(this Stream<TRecord, Epoch> source, CloudBlobContainer container, string format, Action<System.IO.BinaryWriter, TRecord> writeAction)
        {
            var writers = new Dictionary<int, System.IO.BinaryWriter>();

            return source.Subscribe(
                (message, workerid) =>
                {
                    System.IO.BinaryWriter writer;

                    lock (writers)
                    {
                        if (!writers.ContainsKey(workerid))
                            writers.Add(workerid, new System.IO.BinaryWriter(container.GetBlockBlobReference(string.Format(format, source.ForStage.Computation.Controller.Configuration.ProcessID, workerid)).OpenWrite()));

                        writer = writers[workerid];
                    }

                    for (int i = 0; i < message.length; i++)
                        writeAction(writer, message.payload[i]);
                },
                (epoch, workerid) => { },
                workerid =>
                {
                    lock (writers)
                    {
                        if (writers.ContainsKey(workerid))
                            writers[workerid].Dispose();
                    }
                });
        }

        /// <summary>
        /// Writes strings as lines to files in an Azure directory.
        /// </summary>
        /// <param name="source">Source stream</param>
        /// <param name="container">Azure container</param>
        /// <param name="format">Format string for filenames, where {0} is the process ID and {1} is the worker ID</param>
        /// <returns>Subscription corresponding to the Azure writer</returns>
        public static Subscription WriteTextToAzureBlobs(this Stream<string, Epoch> source, CloudBlobContainer container, string format)
        {
            var writers = new Dictionary<int, System.IO.StreamWriter>();

            return source.Subscribe((message, workerid) =>
            {
                System.IO.StreamWriter writer;

                lock (writers)
                {
                    if (!writers.ContainsKey(workerid))
                        writers.Add(workerid, new System.IO.StreamWriter(container.GetBlockBlobReference(string.Format(format, source.ForStage.Computation.Controller.Configuration.ProcessID, workerid)).OpenWrite()));

                    writer = writers[workerid];
                }

                for (int i = 0; i < message.length; i++)
                    writer.WriteLine(message.payload[i]);
            },
                                    (epoch, workerid) => { },
                                    workerid =>
                                    {
                                        lock (writers)
                                        {
                                            if (writers.ContainsKey(workerid))
                                                writers[workerid].Dispose();
                                        }
                                    });
        }

        /// <summary>
        /// Writes each record (using its default string representation) to files in an Azure directory.
        /// </summary>
        /// <typeparam name="TRecord">Type of records to be written</typeparam>
        /// <param name="source">Source stream</param>
        /// <param name="container">Azure container</param>
        /// <param name="format">Format string for filenames, where {0} is the process ID and {1} is the worker ID</param>
        /// <returns>Subscription corresponding to the Azure writer</returns>
        public static Subscription WriteTextToAzureBlobs<TRecord>(this Stream<TRecord, Epoch> source, CloudBlobContainer container, string format)
        {
            return source.Select(x => x.ToString()).WriteTextToAzureBlobs(container, format);
        }

        #if false /// ifdef'd out until we find a less Exception-y way to create and write things to tables

        /// <summary>
        /// Writes records to the given Azure table.
        /// </summary>
        /// <typeparam name="TRecord">Type of records to be written</typeparam>
        /// <param name="source">Source stream</param>
        /// <param name="table">Azure table</param>
        /// <param name="entitySelector">Maps records in the stream to Azure table entities</param>
        /// <returns>Subscription corresponding to the Azure writer</returns>
        public static Subscription WriteToAzureTable<TRecord>(this Stream<TRecord, Epoch> source, CloudTable table, Func<TRecord, ITableEntity> entitySelector)
        {
            return source.Select(x => entitySelector(x)).WriteToAzureTable(table);
        }

        /// <summary>
        /// Writes a stream of entities to the given Azure table.
        /// </summary>
        /// <typeparam name="TEntity">Type of entities to be written</typeparam>
        /// <param name="source">Source stream</param>
        /// <param name="table">Azure table</param>
        /// <returns>Subscription corresponding to the Azure writer</returns>
        public static Subscription WriteToAzureTable<TEntity>(this Stream<TEntity, Epoch> source, CloudTable table)
            where TEntity : ITableEntity
        {
            return source.PartitionBy(x => x.PartitionKey.GetHashCode()).Subscribe((message, workerid) =>
            {
                string lastPartitionKey = null;
                TableBatchOperation batchOperation = new TableBatchOperation();
                for (int i = 0; i < message.length; ++i)
                {
                    if (batchOperation.Count == 100 || (lastPartitionKey != null && !message.payload[i].PartitionKey.Equals(lastPartitionKey)))
                    {
                        table.ExecuteBatch(batchOperation);
                        batchOperation = new TableBatchOperation();
                    }

                    lastPartitionKey = message.payload[i].PartitionKey;

                    batchOperation.Insert(message.payload[i]);
                }

                if (batchOperation.Count > 0)
                {
                    table.ExecuteBatch(batchOperation);
                }
            }, (epoch, workerid) => { }, workerid => { });   
        }

        #endif

        #endregion

    }
}
