using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

using Microsoft.Research.Naiad;
using Microsoft.Research.Naiad.Dataflow;
using Microsoft.Research.Naiad.Frameworks.Lindi;
using Microsoft.Research.Naiad.Dataflow.PartitionBy;
using Microsoft.WindowsAzure.Storage.Blob;
using Microsoft.WindowsAzure.Storage.Table;
using Microsoft.WindowsAzure.Storage;
using System.Reactive;
using Microsoft.Research.Naiad.Dataflow.Channels;
using Microsoft.Research.Naiad.CodeGeneration;

namespace Microsoft.Research.Naiad.Frameworks.Azure
{
    public static class ExtensionMethods
    {
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
        public static Stream<TRecord, Epoch> ReadFromAzureBlobs<TRecord>(this GraphManager manager, CloudBlobContainer container, string prefix, Func<System.IO.Stream, IEnumerable<TRecord>> reader)
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
        public static Stream<TRecord, Epoch> ReadBinaryFromAzureBlobs<TRecord>(this GraphManager manager, CloudBlobContainer container, string prefix)
        {
            return manager.ReadFromAzureBlobs<TRecord>(container, prefix, stream => GetNaiadReaderEnumerable<TRecord>(stream, manager.Controller.CodeGenerator));
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
        public static Stream<R, Epoch> ReadCustomBinaryFromAzureBlobs<R>(this GraphManager manager, CloudBlobContainer container, string prefix, Func<System.IO.BinaryReader, IEnumerable<R>> reader)
        {
            return manager.ReadFromAzureBlobs<R>(container, prefix, s => reader(new System.IO.BinaryReader(s)));
        }

        /// <summary>
        /// Reads the contents of all text files in an Azure directory into a Naiad stream.
        /// </summary>
        /// <param name="manager">Graph manager</param>
        /// <param name="prefix">Azure blob prefix</param>
        /// <returns>Naiad stream containing the lines extracted from files in the Azure directory</returns>
        public static Stream<string, Epoch> ReadTextFromAzureBlobs(this GraphManager manager, CloudBlobContainer container, string prefix)
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
        public static Stream<TEntity, Epoch> ReadFromAzureTable<TEntity>(this GraphManager manager, CloudTable table, TableQuery<TEntity> query)
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
        /// <returns>An enumeration of records in the stream</returns>
        internal static IEnumerable<TRecord> GetNaiadReaderEnumerable<TRecord>(System.IO.Stream stream, SerializationCodeGenerator codeGenerator)
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
            return source.WriteToAzureBlobs(container, format, stream => GetNaiadWriterObserver<TRecord>(stream, source.ForStage.GraphManager.Controller.CodeGenerator));
        }

        /// <summary>
        /// Returns an record observer that writes records to the given stream in the Naiad message format.
        /// </summary>
        /// <typeparam name="TRecord">Type of records to be written</typeparam>
        /// <param name="stream">Target I/O stream</param>
        /// <returns>A record observer that writes records to the given stream.</returns>
        internal static IObserver<TRecord> GetNaiadWriterObserver<TRecord>(System.IO.Stream stream, SerializationCodeGenerator codeGenerator)
        {
            NaiadStreamWriter<TRecord> writer = new NaiadStreamWriter<TRecord>(stream, codeGenerator);
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
                            writers.Add(workerid, new System.IO.BinaryWriter(container.GetBlockBlobReference(string.Format(format, source.ForStage.GraphManager.Controller.Configuration.ProcessID, workerid)).OpenWrite()));

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
                        writers.Add(workerid, new System.IO.StreamWriter(container.GetBlockBlobReference(string.Format(format, source.ForStage.GraphManager.Controller.Configuration.ProcessID, workerid)).OpenWrite()));

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
