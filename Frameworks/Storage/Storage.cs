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
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Reactive;
using System.Text;
using System.Text.RegularExpressions;

using Microsoft.Research.Naiad.Dataflow;
using Microsoft.Research.Naiad.Dataflow.StandardVertices;
using Microsoft.Research.Naiad.Serialization;

namespace Microsoft.Research.Naiad.Frameworks.Storage
{
    /// <summary>
    /// The Storage framework includes base classes to support reading and writing a variety of files including Azure and Hdfs
    /// </summary>
    class NamespaceDoc
    {

    }

    /// <summary>
    /// Utility classes and methods for the Naiad Storage framework
    /// </summary>
    public static class Utils
    {
        /// <summary>
        /// Wrapper for StreamWriter than exposes the fact that it implements Flush via the IFlushable interface
        /// </summary>
        public class FStreamWriter : StreamWriter, IFlushable
        {
            /// <summary>
            /// Create a StreamWriter exposing the IFlushable interface
            /// </summary>
            /// <param name="stream">stream to write data to</param>
            /// <param name="encoding">character encoding</param>
            /// <param name="bufferSize">size of streamwriter buffer</param>
            public FStreamWriter(Stream stream, Encoding encoding, int bufferSize) : base(stream, encoding, bufferSize)
            {
            }
        }

        /// <summary>
        /// Enumerates lines of text from a stream
        /// </summary>
        /// <param name="stream">source stream</param>
        /// <returns>Each line of text in the source stream</returns>
        public static IEnumerable<string> ReadLines(this System.IO.Stream stream)
        {
            using (var reader = new System.IO.StreamReader(stream, Encoding.UTF8, true, 1024 * 1024))
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
        public static IEnumerable<TRecord> GetNaiadReaderEnumerable<TRecord>(System.IO.Stream stream)
        {
            using (NaiadReader<TRecord> reader = new NaiadReader<TRecord>(stream))
            {
                TRecord nextElement;
                while (reader.TryRead(out nextElement))
                {
                    yield return nextElement;
                }
            }
        }

        /// <summary>
        /// Enumerates batches of records from a stream in the Naiad serialization format.
        /// </summary>
        /// <typeparam name="TRecord">Type of record in the stream</typeparam>
        /// <param name="stream">A stream containing records serialized in the Naiad messaging format</param>
        /// <param name="batchSize">number of records per batch</param>
        /// <returns>An enumeration of records in the stream</returns>
        public static IEnumerable<ArraySegment<TRecord>> GetNaiadReaderBatchEnumerable<TRecord>(
            System.IO.Stream stream, int batchSize)
        {
            using (NaiadReader<TRecord> reader = new NaiadReader<TRecord>(stream))
            {
                int batchIndex;
                do
                {
                    TRecord[] batch = new TRecord[batchSize];
                    for (batchIndex = 0; batchIndex < batchSize; ++batchIndex)
                    {
                        if (!reader.TryRead(out batch[batchIndex]))
                        {
                            break;
                        }
                    }

                    if (batchIndex > 0)
                    {
                        yield return new ArraySegment<TRecord>(batch, 0, batchIndex);
                    }
                } while (batchIndex == batchSize);
            }
        }

        /// <summary>
        /// Returns an record observer that writes records to the given stream in the Naiad message format.
        /// </summary>
        /// <typeparam name="TRecord">Type of records to be written</typeparam>
        /// <param name="stream">Target I/O stream</param>
        /// <param name="codeGenerator">code generator</param>
        /// <returns>A record observer that writes records to the given stream.</returns>
        public static IObserver<TRecord> GetNaiadWriterObserver<TRecord>(System.IO.Stream stream, SerializationFormat codeGenerator)
        {
            NaiadWriter<TRecord> writer = new NaiadWriter<TRecord>(stream, codeGenerator);
            return Observer.Create<TRecord>(r =>
            {
                writer.Write(r);
            },
            () => writer.Dispose());
        }

        /// <summary>
        /// class to wrap a sequence of files for writing. This is used so that records are written as a sequence of
        /// relatively small files rather than one large file, which increases the available parallelism when they
        /// are read later
        /// </summary>
        /// <typeparam name="TWriter">the type of the serializer</typeparam>
        internal class WriterStreamSequence<TWriter>
            where TWriter : class, IDisposable, IFlushable
        {
            /// <summary>
            /// function to generate the name for a file from its sequence number
            /// </summary>
            private readonly Func<int, Uri> fileNameFunction;
            /// <summary>
            /// function to generate the stream for a file from its name
            /// </summary>
            private readonly Func<Uri, Stream> streamFunction;
            /// <summary>
            /// function to generate the writer for a file from its stream
            /// </summary>
            private readonly Func<Stream, TWriter> writerFunction;

            /// <summary>
            /// threshold number of bytes after which a file is closed and the next one in the sequence is opened
            /// </summary>
            private readonly long fileLengthThreshold;

            /// <summary>
            /// stopwatch used for timing writes
            /// </summary>
            private readonly Stopwatch stopwatch;

            /// <summary>
            /// stream corresponding to the current file being written
            /// </summary>
            private Stream segmentStream;
            /// <summary>
            /// serializer for the current file being written
            /// </summary>
            private TWriter segmentWriter;
            /// <summary>
            /// the index of the next file to write
            /// </summary>
            private int nextSegmentIndex;
            /// <summary>
            /// the total number of bytes written
            /// </summary>
            private long totalLength;

            /// <summary>
            /// flush and close the current serializer and file, and create the next file in the sequence
            /// along with its serializer 
            /// </summary>
            private void StartNextFile()
            {
                // close the current file if any
                CloseCurrentFile();

                // use the supplied functions to open the file and create its serializer
                Uri fileName = this.fileNameFunction(this.nextSegmentIndex);
                this.segmentStream = this.streamFunction(fileName);
                this.segmentWriter = this.writerFunction(this.segmentStream);

                // get ready for the next file
                ++this.nextSegmentIndex;
            }

            /// <summary>
            /// check to see if the current file is long enough that it's time to start a new one, and if
            /// so close the current writer and open the next one
            /// </summary>
            public void CheckForFileBoundary()
            {
                if (this.segmentStream.Position > this.fileLengthThreshold)
                {
                    StartNextFile();
                }
            }

            /// <summary>
            /// the serializer for the file that is currently being written
            /// </summary>
            public TWriter Writer { get { return this.segmentWriter; } }

            /// <summary>
            /// all of the filenames that have been written to by this instance
            /// </summary>
            public IEnumerable<Uri> Filenames
            {
                get
                {
                    return Enumerable.Range(0, this.nextSegmentIndex).Select(i => this.fileNameFunction(i));
                }
            }

            /// <summary>
            /// close the current serializer and file, if any
            /// </summary>
            public void CloseCurrentFile()
            {
                if (this.segmentStream != null)
                {
                    // flush the writer before determining the stream position
                    this.segmentWriter.Flush();
                    this.totalLength += this.segmentStream.Position;
                    // this should close the underlying stream
                    this.segmentWriter.Dispose();
                    // dispose the stream, just in case
                    this.segmentStream.Dispose();
                }

                this.segmentStream = null;
                this.segmentWriter = null;
            }

            /// <summary>
            /// close the current serializer and file, if any
            /// </summary>
            public void Close()
            {
                CloseCurrentFile();

                this.stopwatch.Stop();
#if false
                long elapsed = this.stopwatch.ElapsedMilliseconds;
                Console.WriteLine(String.Format(
                    "Wrote {0} bytes in {1}ms --- {2:f3}MB/s",
                    this.totalLength, elapsed,
                    (double)(((double)this.totalLength / (1024.0 * 1024.0)) / ((double)elapsed / 1000.0))));
#endif
            }

            /// <summary>
            /// Return the number of bytes written in total. This does not include unflushed writes
            /// </summary>
            public long Length
            {
                get
                {
                    // if there is a current segment stream, this will not include any unflushed data
                    return (this.segmentStream == null) ? this.totalLength : this.totalLength + this.segmentStream.Position;
                }
            }

            /// <summary>
            /// create a new wrapper for writing a sequence of serialized files
            /// </summary>
            /// <param name="fileNameFunction">function to generate a filename from a sequence number</param>
            /// <param name="streamFunction">function to open a stream from a filename</param>
            /// <param name="writerFunction">function to create a serializer from a stream</param>
            /// <param name="fileLengthThreshold">maximum length in bytes for a file before starting the next one</param>
            public WriterStreamSequence(
                Func<int, Uri> fileNameFunction,
                Func<Uri, Stream> streamFunction,
                Func<Stream, TWriter> writerFunction,
                long fileLengthThreshold)
            {
                this.fileNameFunction = fileNameFunction;
                this.streamFunction = streamFunction;
                this.writerFunction = writerFunction;
                this.fileLengthThreshold = fileLengthThreshold;

                this.stopwatch = Stopwatch.StartNew();

                // open the first file immediately
                this.nextSegmentIndex = 0;
                StartNextFile();
            }
        }

        /// <summary>
        /// regular expression to match non-alphanumeric characters
        /// </summary>
        private static Regex nonAlpha = new Regex(@"[^a-zA-Z0-9]+");

        /// <summary>
        /// default filename generator for a collection partitioned by key and time
        /// </summary>
        /// <typeparam name="TTime">the type of times</typeparam>
        /// <param name="prefix">the directory prefix, not including the trailing slash</param>
        /// <param name="processId">the process id of the file part</param>
        /// <param name="threadId">the thread id of the file part</param>
        /// <param name="time">the time of the file part</param>
        /// <param name="segment">the sequence number of the file part within the time/process/thread</param>
        /// <returns>a filename representing the given part</returns>
        public static Uri DefaultPartFormat<TTime>(Uri prefix, int processId, int threadId, TTime time, int segment)
            where TTime : Time<TTime>
        {
            // times are rendered with spaces and punctuation: get rid of that
            string timeString = nonAlpha.Replace(time.ToString(), "_").Trim('_');
            UriBuilder builder = new UriBuilder(prefix);
            builder.Path = String.Format("{0}/time_{1}_part_{2:D4}.{3:D4}.{4:D4}", prefix.AbsolutePath, timeString, processId, threadId, segment);
            return builder.Uri;
        }

        /// <summary>
        /// default filename generator for a collection partitioned by key, but concatenated across times
        /// </summary>
        /// <param name="prefix">the directory prefix, not including the trailing slash</param>
        /// <param name="processId">the process id of the file part</param>
        /// <param name="threadId">the thread id of the file part</param>
        /// <param name="segment">the sequence number of the file part within process/thread</param>
        /// <returns>a filename representing the given part</returns>
        public static Uri DefaultPartFormat(Uri prefix, int processId, int threadId, int segment)
        {
            UriBuilder builder = new UriBuilder(prefix);
            builder.Path = String.Format("{0}/part_{1:D4}.{2:D4}.{3:D4}", prefix.AbsolutePath, processId, threadId, segment);
            return builder.Uri;
        }

        /// <summary>
        /// serialize a sequence of records to a collection of files partitioned by process and thread. For each
        /// process/thread this writes a sequence of files; each time a file reaches a threshold number of bytes,
        /// it is closed and another is opened. This keeps individual files of bounded length, allowing for more
        /// parallelism when reading them later
        /// </summary>
        /// <typeparam name="TOutput">type of record to serialize</typeparam>
        /// <typeparam name="TWriter">type of the serializer</typeparam>
        /// <param name="source">stream of records to serialize</param>
        /// <param name="pathFunction">function from processId, threadId and sequence number to filename</param>
        /// <param name="streamFunction">function to create an output stream given a filename</param>
        /// <param name="writerFunction">function to create a serializer from a stream</param>
        /// <param name="serialize">action to serialize a batch of records</param>
        /// <param name="fileLengthThreshold">length in bytes of a file after which it is closed and a new one is opened</param>
        /// <returns>a handle that can be waited on for the computation to complete</returns>
        public static Subscription WriteBySubscription<TOutput, TWriter>(
            this Stream<TOutput, Epoch> source,
            Func<int, int, int, Uri> pathFunction,
            Func<Uri, Stream> streamFunction,
            Func<Stream, TWriter> writerFunction,
            Action<TWriter, ArraySegment<TOutput>> serialize,
            long fileLengthThreshold) where TWriter : class, IDisposable, IFlushable
        {
            // dictionary of sequence writers, indexed by worker id
            var writers = new Dictionary<int, WriterStreamSequence<TWriter>>();

            return source.Subscribe(
                // OnRecv callback
                (message, workerid) =>
                {
                    WriterStreamSequence<TWriter> writer;

                    lock (writers)
                    {
                        if (!writers.TryGetValue(workerid, out writer))
                        {
                            // make a filename generator for the specified worker and process
                            Func<int, Uri> format = segment =>
                                pathFunction(source.ForStage.Computation.Controller.Configuration.ProcessID,
                                             workerid,
                                             segment);
                            // make the sequence writer for the specified worker and process
                            writer = new WriterStreamSequence<TWriter>(
                                format, streamFunction, writerFunction, fileLengthThreshold);
                            writers.Add(workerid, writer);
                        }
                    }

                    // before serializing a batch of records, check to see if the current file has gone over
                    // its length threshold; if so the current file will be closed, and the next one will be
                    // opened
                    writer.CheckForFileBoundary();

                    // serialize the batch of records to the current file
                    serialize(writer.Writer, new ArraySegment<TOutput>(message.payload, 0, message.length));
                },
                // OnNotify callback
                (epoch, workerid) => { },
                // OnCompleted callback
                workerid =>
                {
                    lock (writers)
                    {
                        if (writers.ContainsKey(workerid))
                        {
                            writers[workerid].Close();
                            writers.Remove(workerid);
                        }
                    }
                });
        }

        /// <summary>
        /// serialize a sequence of records to a collection of files partitioned by process, thread and time. For each
        /// process/thread/time this writes a sequence of files; each time a file reaches a threshold number of bytes,
        /// it is closed and another is opened. This keeps individual files of bounded length, allowing for more
        /// parallelism when reading them later
        /// </summary>
        /// <typeparam name="TOutput">type of record to serialize</typeparam>
        /// <typeparam name="TClient">type of client that is passed as a context to the stream function</typeparam>
        /// <typeparam name="TWriter">type of the serializer</typeparam>
        /// <typeparam name="TTime">type of the time of records</typeparam>
        /// <param name="source">stream of records to serialize</param>
        /// <param name="pathFunction">function from processId, threadId, time and sequence number to filename</param>
        /// <param name="clientFunction">function to return a client to pass as context to the stream function</param>
        /// <param name="streamFunction">function to create an output stream given a client and filename</param>
        /// <param name="writerFunction">function to create a serializer from a stream</param>
        /// <param name="serialize">action to serialize a batch of records</param>
        /// <param name="fileLengthThreshold">length in bytes of a file after which it is closed and a new one is opened</param>
        /// <returns>stream of names of files written. The set of names written for a given time is released when the
        /// time completes</returns>
        public static Stream<Uri, TTime> WriteByTime<TOutput, TClient, TWriter, TTime>(
            this Stream<TOutput, TTime> source,
            Func<int, int, TTime, int, Uri> pathFunction,
            Func<TClient> clientFunction,
            Func<TClient, Uri, Stream> streamFunction,
            Func<Stream, TWriter> writerFunction,
            Action<TWriter, ArraySegment<TOutput>> serialize,
            long fileLengthThreshold)
            where TWriter : class, IDisposable, IFlushable
            where TTime : Time<TTime>
        {
            return source.NewUnaryStage(
                (i, v) => new WriteByTimeVertex<TOutput, TClient, TWriter, TTime>(
                    i, v, pathFunction, clientFunction(), streamFunction, writerFunction, serialize, fileLengthThreshold),
                null, null, "WriteByTime");
        }

        /// <summary>
        /// vertex to serialize a sequence of records to a collection of files partitioned by process, thread and time. For each
        /// process/thread/time this writes a sequence of files; each time a file reaches a threshold number of bytes,
        /// it is closed and another is opened. This keeps individual files of bounded length, allowing for more
        /// parallelism when reading them later
        /// </summary>
        /// <typeparam name="TOutput">type of record to serialize</typeparam>
        /// <typeparam name="TClient">type of client to use as context in the stream generator function</typeparam>
        /// <typeparam name="TWriter">type of the serializer</typeparam>
        /// <typeparam name="TTime">type of the time of records</typeparam>
        internal class WriteByTimeVertex<TOutput, TClient, TWriter, TTime> : UnaryVertex<TOutput, Uri, TTime>
            where TWriter : class, IDisposable, IFlushable
            where TTime : Time<TTime>
        {
            /// <summary>
            /// client to use as context for the stream function
            /// </summary>
            private readonly TClient client;
            /// <summary>
            /// dictionary of sequence writers, indexed by time
            /// </summary>
            private readonly Dictionary<TTime, WriterStreamSequence<TWriter>> writers;
            /// <summary>
            /// worker id of this vertex
            /// </summary>
            private readonly int workerId;
            /// <summary>
            /// function from processId, workerId, time and sequence number to filename
            /// </summary>
            private readonly Func<int, int, TTime, int, Uri> pathFunction;
            /// <summary>
            /// function from filename and client to output stream
            /// </summary>
            private readonly Func<TClient, Uri, Stream> streamFunction;
            /// <summary>
            /// function from output stream to serializer
            /// </summary>
            private readonly Func<Stream, TWriter> writerFunction;
            /// <summary>
            /// action to serialize a batch of records
            /// </summary>
            private readonly Action<TWriter, ArraySegment<TOutput>> serialize;
            /// <summary>
            /// length in bytes after which a file is closed and the next one is opened
            /// </summary>
            private readonly long fileLengthThreshold;

            public override void OnReceive(Message<TOutput, TTime> message)
            {
                WriterStreamSequence<TWriter> writer;
                if (!writers.TryGetValue(message.time, out writer))
                {
                    // make a filename generator for the specified process, worker and time
                    Func<int, Uri> format = segment =>
                        this.pathFunction(this.Stage.Computation.Controller.Configuration.ProcessID,
                                          this.workerId,
                                          message.time,
                                          segment);
                    // make a sequence writer for the specified process, worker and time
                    writer = new WriterStreamSequence<TWriter>(format, u => streamFunction(this.client, u), writerFunction, fileLengthThreshold);
                    writers.Add(message.time, writer);
                    // ensure that we are called later to close the sequence writer when the time completes
                    this.NotifyAt(message.time);
                }

                // before serializing a batch of records, check to see if the current file has gone over
                // its length threshold; if so the current file will be closed, and the next one will be
                // opened
                writer.CheckForFileBoundary();

                // serialize the batch of records to the current file
                this.serialize(writer.Writer, new ArraySegment<TOutput>(message.payload, 0, message.length));
            }

            public override void OnNotify(TTime time)
            {
                WriterStreamSequence<TWriter> writer = writers[time];
                writers.Remove(time);

                // close the sequence writer
                writer.Close();

                var output = this.Output.GetBufferForTime(time);
                foreach (Uri fileName in writer.Filenames)
                {
                    // emit the filename of each file written by this writer
                    output.Send(fileName);
                }
            }

            /// <summary>
            /// construct a new vertex to serialize records partitioned by processId, threadId and time
            /// </summary>
            /// <param name="workerId">threadId of this worker</param>
            /// <param name="stage">stage the vertex belongs to</param>
            /// <param name="pathFunction">function from processId, workerId, time and sequence number to filename</param>
            /// <param name="client">function to return the client used as context for the stream function</param>
            /// <param name="streamFunction">function from client and filename to output stream</param>
            /// <param name="writerFunction">function from stream to serializer</param>
            /// <param name="serialize">action to serialize a batch of records</param>
            /// <param name="fileLengthThreshold">length in bytes after which a file is closed and the next one is opened</param>
            public WriteByTimeVertex(
                int workerId,
                Stage<TTime> stage,
                Func<int, int, TTime, int, Uri> pathFunction,
                TClient client,
                Func<TClient, Uri, Stream> streamFunction,
                Func<Stream, TWriter> writerFunction,
                Action<TWriter, ArraySegment<TOutput>> serialize,
                long fileLengthThreshold)
                : base(workerId, stage)
            {
                this.workerId = workerId;
                this.client = client;
                this.pathFunction = pathFunction;
                this.streamFunction = streamFunction;
                this.writerFunction = writerFunction;
                this.serialize = serialize;
                this.fileLengthThreshold = fileLengthThreshold;

                this.writers = new Dictionary<TTime, WriterStreamSequence<TWriter>>();
            }
        }
    }
}
