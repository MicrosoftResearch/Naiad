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
using System.IO;
using System.Linq;
using System.Net;
using System.Text;

using Microsoft.Research.Peloponnese.Hdfs;

using Microsoft.Research.Naiad.Input;
using Microsoft.Research.Naiad.Dataflow;
using Microsoft.Research.Naiad.Frameworks.Lindi;
using Microsoft.Research.Naiad.Frameworks.Storage;
using Microsoft.Research.Naiad.Frameworks.Storage.Dfs;
using Microsoft.Research.Naiad.Frameworks.WorkGenerator;
using Microsoft.Research.Naiad.Serialization;

namespace Microsoft.Research.Naiad.Frameworks.Hdfs
{
    /// <summary>
    /// The Hdfs framework supports reading and writing Hdfs files using the Java-based native Hdfs protocol. It requires Java and
    /// the Hdfs jar files to be installed locally, and will throw an exception if they are not installed. The environment JAVA_HOME
    /// must be set to the location of the Java installation, and HADOOP_COMMON_HOME must be set to the location of the Hadoop jar files
    /// including those for Hdfs
    /// </summary>
    class NamespaceDoc
    {

    }

    #region extension methods
    /// <summary>
    /// extension methods for working with Java-protocol Hdfs files
    /// </summary>
    public static class ExtensionMethods
    {
        /// <summary>
        /// Read a stream of path names (file or directory names) each of which corresponds to a collection of HDFS files
        /// serialized as lines of text. Concatenate all the lines of the files to the output, in an unspecified order.
        /// </summary>
        /// <typeparam name="TTime">time of the input and output records</typeparam>
        /// <param name="input">stream of input paths</param>
        /// <returns>stream of text lines in the hdfs files</returns>
        public static Stream<string, TTime> FromHdfsText<TTime>(this Stream<Uri, TTime> input) where TTime : Time<TTime>
        {
            return input.GenerateWork(
                time => new DfsTextCoordinator(new HdfsClient()),
                (workerIndex, time) => new DfsTextWorker(new HdfsClient(), 256));
        }

        /// <summary>
        /// Read a collection of HDFS files serialized as lines of text. Concatenate all the lines of the files to the output,
        /// in an unspecified order.
        /// </summary>
        /// <param name="manager">Naiad computation</param>
        /// <param name="fileOrDirectoryPath">path of the file or directory to read</param>
        /// <returns>stream of text lines in the hdfs files</returns>
        public static Stream<string, Epoch> ReadHdfsTextCollection(
            this Computation manager, Uri fileOrDirectoryPath)
        {
            return new Uri[] { fileOrDirectoryPath }
                .AsNaiadStream(manager)
                // this distinct ensures that the same code can be run at every process without reading files multiple times
                .Distinct()
                .GenerateWork(
                    time => new DfsTextCoordinator(new HdfsClient()),
                    (workerIndex, time) => new DfsTextWorker(new HdfsClient(), 256));
        }

        /// <summary>
        /// Read a stream of path names (file or directory names) each of which corresponds to a collection of HDFS files
        /// serialized in a custom binary format. Concatenate all the records to the output, in an unspecified order.
        /// </summary>
        /// <typeparam name="TOutput">output record type</typeparam>
        /// <typeparam name="TTime">time of the input and output records</typeparam>
        /// <param name="input">stream of input paths</param>
        /// <param name="deserialize">custom deserializer function to convert a stream of bytes into a sequence of records</param>
        /// <returns>stream of records in the hdfs files</returns>
        public static Stream<TOutput, TTime> FromHdfs<TOutput, TTime>(
            this Stream<Uri, TTime> input,
            Func<Stream, IEnumerable<ArraySegment<TOutput>>> deserialize) where TTime : Time<TTime>
        {
            return input.GenerateWork(
                time => new DfsFileCoordinator(new HdfsClient()),
                (workerIndex, time) => new DfsFileWorker<TOutput>(new HdfsClient(), deserialize));
        }

        /// <summary>
        /// Read a collection of HDFS files serialized in a custom binary format. Concatenate all the records to the output,
        /// in an unspecified order.
        /// </summary>
        /// <typeparam name="TOutput">output record type</typeparam>
        /// <param name="manager">Naiad computation</param>
        /// <param name="fileOrDirectoryPath">path of the file or directory to read</param>
        /// <param name="deserialize">custom deserializer function to convert a stream of bytes into a sequence of records</param>
        /// <returns>stream of records in the hdfs files</returns>
        public static Stream<TOutput, Epoch> ReadHdfsCollection<TOutput>(
            this Computation manager, Uri fileOrDirectoryPath,
            Func<Stream, IEnumerable<ArraySegment<TOutput>>> deserialize)
        {
            return new Uri[] { fileOrDirectoryPath }
                .AsNaiadStream(manager)
                // this distinct ensures that the same code can be run at every process without reading files multiple times
                .Distinct()
                .GenerateWork(
                    time => new DfsFileCoordinator(new HdfsClient()),
                    (workerIndex, time) => new DfsFileWorker<TOutput>(new HdfsClient(), deserialize));
        }

        /// <summary>
        /// Read a stream of path names (file or directory names) each of which corresponds to a collection of HDFS files
        /// serialized in the default Naiad binary format. Concatenate all the records to the output, in an unspecified order.
        /// </summary>
        /// <typeparam name="TOutput">output record type</typeparam>
        /// <typeparam name="TTime">time of the input and output records</typeparam>
        /// <param name="input">stream of input paths</param>
        /// <returns>stream of records in the hdfs files</returns>
        public static Stream<TOutput, TTime> FromHdfsBinary<TOutput, TTime>(
            this Stream<Uri, TTime> input) where TTime : Time<TTime>
        {
            return input.GenerateWork(
                    time => new DfsFileCoordinator(new HdfsClient()),
                    (workerIndex, time) =>
                        new DfsFileWorker<TOutput>(
                            new HdfsClient(),
                            stream => Utils.GetNaiadReaderBatchEnumerable<TOutput>(stream, 256)));
        }

        /// <summary>
        /// Read a collection of HDFS files serialized in the default Naiad binary format. Concatenate all the records to the output,
        /// in an unspecified order.
        /// </summary>
        /// <typeparam name="TOutput">output record type</typeparam>
        /// <param name="manager">Naiad computation</param>
        /// <param name="fileOrDirectoryPath">path of the file or directory to read</param>
        /// <returns>stream of records in the hdfs files</returns>
        public static Stream<TOutput, Epoch> ReadHdfsBinaryCollection<TOutput>(
            this Computation manager, Uri fileOrDirectoryPath)
        {
            return new Uri[] { fileOrDirectoryPath }
                .AsNaiadStream(manager)
                // this distinct ensures that the same code can be run at every process without reading files multiple times
                .Distinct()
                .GenerateWork(
                    time => new DfsFileCoordinator(new HdfsClient()),
                    (workerIndex, time) =>
                        new DfsFileWorker<TOutput>(
                            new HdfsClient(),
                            stream => Utils.GetNaiadReaderBatchEnumerable<TOutput>(stream, 256)));
        }

        /// <summary>
        /// general method to write a stream of records to a collection of HDFS files. The collection is active
        /// throughout the computation and is closed when the computation terminates: it concatenates records from all
        /// epochs in an undefined order
        /// </summary>
        /// <typeparam name="TOutput">type of the records to write</typeparam>
        /// <typeparam name="TWriter">type of the serializer object</typeparam>
        /// <param name="source">stream of records to write</param>
        /// <param name="format">function to generate a filename given a processId, threadId and sequence number</param>
        /// <param name="writerFunction">function to generate a serializer given a Stream to write to</param>
        /// <param name="serialize">function to serialize a batch of records given a serializer</param>
        /// <param name="bufferSize">buffer size to use in the serializer</param>
        /// <param name="blockSize">hdfs block size to use, or -1 for the file system default value</param>
        /// <param name="segmentThreshold">file size to write before closing the file and opening another one</param>
        /// <returns>handle to wait on until the computation completes</returns>
        public static Subscription WriteHdfsBinary<TOutput, TWriter>(
            this Stream<TOutput, Epoch> source,
            Func<int, int, int, Uri> format,
            Func<Stream, TWriter> writerFunction,
            Action<TWriter, ArraySegment<TOutput>> serialize,
            int bufferSize,
            long blockSize,
            long segmentThreshold) where TWriter : class, IDisposable, IFlushable
        {
            return source.WriteBySubscription(
                format,
                fileName => new HdfsClient().GetDfsStreamWriter(fileName, bufferSize, blockSize),
                stream => writerFunction(stream),
                serialize,
                segmentThreshold);
        }

        /// <summary>
        /// method to write a stream of records to a collection of HDFS files using the default Naiad binary serializer.
        /// The collection is active throughout the computation and is closed when the computation terminates: it concatenates
        /// records from all epochs in an undefined order
        /// </summary>
        /// <typeparam name="TOutput">type of the records to write</typeparam>
        /// <param name="source">stream of records to write</param>
        /// <param name="prefix">webhdfs directory to write the partitioned data into</param>
        /// <param name="bufferSize">buffer size to use in the serializer</param>
        /// <param name="blockSize">hdfs block size to use, or -1 for the file system default value</param>
        /// <param name="segmentThreshold">file size to write before closing the file and opening another one</param>
        /// <returns>handle to wait on until the computation completes</returns>
        public static Subscription WriteHdfsBinary<TOutput>(
            this Stream<TOutput, Epoch> source,
            Uri prefix,
            int bufferSize = 1024*1024,
            long blockSize = -1,
            long segmentThreshold = 254 * 1024 * 1024)
        {
            // make sure we'll be able to write the partitioned data
            HdfsClient client = new HdfsClient();
            client.EnsureDirectory(prefix, false);

            return source.WriteHdfsBinary(
                (processId, threadId, segment) => Utils.DefaultPartFormat(prefix, processId, threadId, segment),
                stream => new NaiadWriter<TOutput>(stream, source.ForStage.Computation.Controller.SerializationFormat, bufferSize),
                (writer, arraySegment) =>
                {
                    for (int i = 0; i < arraySegment.Count; i++)
                    {
                        writer.Write(arraySegment.Array[i]);
                    }
                },
                bufferSize, blockSize, segmentThreshold);
        }

        /// <summary>
        /// general method to write a stream of records to a collection of HDFS files, partitioned by time as well as key.
        /// Within a given time and part, records are written in an undefined order
        /// </summary>
        /// <typeparam name="TOutput">type of the records to write</typeparam>
        /// <typeparam name="TWriter">type of the serializer object</typeparam>
        /// <typeparam name="TTime">type of the record time</typeparam>
        /// <param name="source">stream of records to write</param>
        /// <param name="format">function to generate a filename given a processId, threadId, time and sequence number</param>
        /// <param name="writerFunction">function to generate a serializer given a Stream to write to</param>
        /// <param name="serialize">function to serialize a batch of records given a serializer</param>
        /// <param name="bufferSize">buffer size to use for the serializer</param>
        /// <param name="blockSize">hdfs block size to use, or -1 for the file system default value</param>
        /// <param name="segmentThreshold">file size to write before closing the file and opening another one</param>
        /// <returns>stream of filenames written</returns>
        public static Stream<Uri, TTime> ToHdfsBinary<TOutput, TWriter, TTime>(
            this Stream<TOutput, TTime> source,
            Func<int, int, TTime, int, Uri> format,
            Func<Stream, TWriter> writerFunction,
            Action<TWriter, ArraySegment<TOutput>> serialize,
            int bufferSize,
            long blockSize,
            long segmentThreshold)
            where TWriter : class, IDisposable, IFlushable
            where TTime : Time<TTime>
        {
            return source.WriteByTime(
                format,
                () => new HdfsClient(),
                (client, fileName) => client.GetDfsStreamWriter(fileName, bufferSize, blockSize),
                stream => writerFunction(stream),
                serialize,
                segmentThreshold);
        }

        /// <summary>
        /// method to write a stream of records to a collection of HDFS files using the default Naiad binary serializer,
        /// partitioned by time as well as key. Within a given time and part, records are written in an undefined order
        /// </summary>
        /// <typeparam name="TOutput">type of the records to write</typeparam>
        /// <typeparam name="TTime">type of the record time</typeparam>
        /// <param name="source">stream of records to write</param>
        /// <param name="prefix">webhdfs directory to write the partitioned data into</param>
        /// <param name="bufferSize">buffer size to use for the serializer</param>
        /// <param name="blockSize">hdfs block size to use, or -1 for the file system default value</param>
        /// <param name="segmentThreshold">file size to write before closing the file and opening another one</param>
        /// <returns>stream of filenames written</returns>
        public static Stream<Uri, TTime> ToHdfsBinary<TOutput, TTime>(
            this Stream<TOutput, TTime> source,
            Uri prefix,
            int bufferSize = 1024 * 1024,
            long blockSize = -1,
            long segmentThreshold = 254 * 1024 * 1024) where TTime : Time<TTime>
        {
            // make sure we'll be able to write the partitioned data
            HdfsClient client = new HdfsClient();
            client.EnsureDirectory(prefix, false);

            return source.ToHdfsBinary(
                (processId, threadId, time, segment) => Utils.DefaultPartFormat(prefix, processId, threadId, time, segment),
                stream => new NaiadWriter<TOutput>(stream, source.ForStage.Computation.Controller.SerializationFormat, bufferSize),
                (writer, arraySegment) =>
                {
                    for (int i = 0; i < arraySegment.Count; i++)
                    {
                        writer.Write(arraySegment.Array[i]);
                    }
                },
                bufferSize, blockSize, segmentThreshold);
        }

        /// <summary>
        /// write a sequence of strings as hdfs text files. The collection is active throughout the computation and is 
        /// closed when the computation terminates: it concatenates records from all epochs in an undefined order
        /// </summary>
        /// <param name="source">stream of records to write</param>
        /// <param name="prefix">webhdfs directory to write the partitioned data into</param>
        /// <param name="bufferSize">buffer size to use for the text serializer</param>
        /// <param name="blockSize">hdfs block size to use, or -1 for the file system default value</param>
        /// <param name="segmentThreshold">file size to write before closing the file and opening another one</param>
        /// <returns>handle to wait on until the computation completes</returns>
        public static Subscription WriteHdfsText(
            this Stream<string, Epoch> source,
            Uri prefix,
            int bufferSize = 1024 * 1024,
            long blockSize = -1,
            long segmentThreshold = 254 * 1024 * 1024)
        {
            // make sure we'll be able to write the partitioned data
            HdfsClient client = new HdfsClient();
            client.EnsureDirectory(prefix, false);

            // don't write byte order marks at the start of the files
            Encoding utf8 = new UTF8Encoding(false, true);

            return source.WriteHdfsBinary(
                (processId, threadId, segment) => Utils.DefaultPartFormat(prefix, processId, threadId, segment),
                stream => new Utils.FStreamWriter(stream, utf8, 1024 * 1024),
                (writer, arraySegment) =>
                {
                    for (int i = 0; i < arraySegment.Count; i++)
                    {
                        writer.WriteLine(arraySegment.Array[i]);
                    }
                },
                bufferSize, blockSize, segmentThreshold);
        }

        /// <summary>
        /// write a sequence of strings as hdfs text files, partitioned by time as well as key.
        /// Within a given time and part, records are written in an undefined order
        /// </summary>
        /// <typeparam name="TTime">type of the record time</typeparam>
        /// <param name="source">stream of records to write</param>
        /// <param name="prefix">webhdfs directory to write the partitioned data into</param>
        /// <param name="bufferSize">buffer size to use for the text serializer</param>
        /// <param name="blockSize">hdfs block size to use, or -1 for the file system default value</param>
        /// <param name="segmentThreshold">file size to write before closing the file and opening another one</param>
        /// <returns>stream of filenames written</returns>
        public static Stream<Uri, TTime> ToHdfsText<TTime>(
            this Stream<string, TTime> source,
            Uri prefix,
            int bufferSize = 1024 * 1024,
            long blockSize = -1,
            long segmentThreshold = 254 * 1024 * 1024) where TTime : Time<TTime>
        {
            // make sure we'll be able to write the partitioned data
            HdfsClient client = new HdfsClient();
            client.EnsureDirectory(prefix, false);

            // don't write byte order marks at the start of the files
            Encoding utf8 = new UTF8Encoding(false, true);

            return source.ToHdfsBinary(
                (processId, threadId, time, segment) => Utils.DefaultPartFormat(prefix, processId, threadId, time, segment),
                stream => new Utils.FStreamWriter(stream, utf8, 1024 * 1024),
                (writer, arraySegment) =>
                {
                    for (int i = 0; i < arraySegment.Count; i++)
                    {
                        writer.WriteLine(arraySegment.Array[i]);
                    }
                },
                bufferSize, blockSize, segmentThreshold);
        }
    }
    #endregion
}
