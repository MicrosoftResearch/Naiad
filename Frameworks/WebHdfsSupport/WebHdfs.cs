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
using System.Net;
using System.IO;
using System.Text;
using System.Threading.Tasks;

using Microsoft.Research.Peloponnese.Shared;
using Microsoft.Research.Peloponnese.WebHdfs;

using Microsoft.Research.Naiad;
using Microsoft.Research.Naiad.Dataflow;
using Microsoft.Research.Naiad.Frameworks.Lindi;
using Microsoft.Research.Naiad.Frameworks.WorkGenerator;
using Microsoft.Research.Naiad.Dataflow.PartitionBy;
using Microsoft.Research.Naiad.Dataflow.Channels;
using Microsoft.Research.Naiad.Serialization;
using Microsoft.Research.Naiad.Input;
using Microsoft.Research.Naiad.Frameworks.Storage;
using Microsoft.Research.Naiad.Frameworks.Storage.Dfs;

using Newtonsoft.Json;
using Newtonsoft.Json.Linq;

namespace Microsoft.Research.Naiad.Frameworks.WebHdfs
{
    /// <summary>
    /// The WebHdfs framework supports reading and writing Hdfs files using the REST-based WebHdfs protocol. 
    /// </summary>
    class NamespaceDoc
    {

    }

    #region extension methods
    /// <summary>
    /// extension methods for working with WebHdfs files
    /// </summary>
    public static class ExtensionMethods
    {
        /// <summary>
        /// Read a stream of path names (file or directory names) each of which corresponds to a collection of HDFS files
        /// serialized as lines of text. Concatenate all the lines of the files to the output, in an unspecified order.
        /// </summary>
        /// <typeparam name="TTime">time of the input and output records</typeparam>
        /// <param name="input">stream of input paths</param>
        /// <param name="user">hdfs user</param>
        /// <param name="webPort">webhdfs protocol port</param>
        /// <returns>stream of text lines in the hdfs files</returns>
        public static Stream<string, TTime> FromWebHdfsText<TTime>(
            this Stream<Uri, TTime> input, string user, int webPort) where TTime : Time<TTime>
        {
            return input.GenerateWork(
                time => new DfsTextCoordinator(new WebHdfsClient(user, webPort)),
                (workerIndex, time) => new DfsTextWorker(new WebHdfsClient(user, webPort), 256));
        }

        /// <summary>
        /// Read a stream of path names (file or directory names) each of which corresponds to a collection of HDFS files
        /// serialized as lines of text. Concatenate all the lines of the files to the output, in an unspecified order.
        /// </summary>
        /// <typeparam name="TTime">time of the input and output records</typeparam>
        /// <param name="input">stream of input paths</param>
        /// <returns>stream of text lines in the hdfs files</returns>
        public static Stream<string, TTime> FromWebHdfsText<TTime>(
            this Stream<Uri, TTime> input) where TTime : Time<TTime>
        {
            return input.FromWebHdfsText<TTime>(Environment.UserName, 50070);
        }

        /// <summary>
        /// Read a collection of HDFS files serialized as lines of text. Concatenate all the lines of the files to the output,
        /// in an unspecified order.
        /// </summary>
        /// <param name="manager">Naiad computation</param>
        /// <param name="fileOrDirectoryPath">path of the file or directory to read</param>
        /// <param name="user">hdfs user</param>
        /// <param name="webPort">webhdfs protocol port</param>
        /// <returns>stream of text lines in the hdfs files</returns>
        public static Stream<string, Epoch> ReadWebHdfsTextCollection(
            this Computation manager, Uri fileOrDirectoryPath,
            string user, int webPort)
        {
            return new Uri[] { fileOrDirectoryPath }
                .AsNaiadStream(manager)
                // this distinct ensures that the same code can be run at every process without reading files multiple times
                .Distinct()
                .GenerateWork(
                    time => new DfsTextCoordinator(new WebHdfsClient(user, webPort)),
                    (workerIndex, time) => new DfsTextWorker(new WebHdfsClient(user, webPort), 256));
        }

        /// <summary>
        /// Read a collection of HDFS files serialized as lines of text. Concatenate all the lines of the files to the output,
        /// in an unspecified order.
        /// </summary>
        /// <param name="manager">Naiad computation</param>
        /// <param name="fileOrDirectoryPath">path of the file or directory to read</param>
        /// <returns>stream of text lines in the hdfs files</returns>
        public static Stream<string, Epoch> ReadWebHdfsTextCollection(
            this Computation manager, Uri fileOrDirectoryPath)
        {
            return manager.ReadWebHdfsTextCollection(fileOrDirectoryPath, Environment.UserName, 50070);
        }

        /// <summary>
        /// Read a stream of path names (file or directory names) each of which corresponds to a collection of HDFS files
        /// serialized in a custom binary format. Concatenate all the records to the output, in an unspecified order.
        /// </summary>
        /// <typeparam name="TOutput">output record type</typeparam>
        /// <typeparam name="TTime">time of the input and output records</typeparam>
        /// <param name="input">stream of input paths</param>
        /// <param name="deserialize">custom deserializer function to convert a stream of bytes into a sequence of records</param>
        /// <param name="user">hdfs user</param>
        /// <param name="webPort">webhdfs protocol port</param>
        /// <returns>stream of records in the hdfs files</returns>
        public static Stream<TOutput, TTime> FromWebHdfs<TOutput, TTime>(
            this Stream<Uri, TTime> input,
            Func<Stream, IEnumerable<ArraySegment<TOutput>>> deserialize,
            string user, int webPort) where TTime : Time<TTime>
        {
            return input.GenerateWork(
                time => new DfsFileCoordinator(new WebHdfsClient(user, webPort)),
                (workerIndex, time) => new DfsFileWorker<TOutput>(new WebHdfsClient(user, webPort), deserialize));
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
        public static Stream<TOutput, TTime> FromWebHdfs<TOutput, TTime>(
            this Stream<Uri, TTime> input,
            Func<Stream, IEnumerable<ArraySegment<TOutput>>> deserialize) where TTime : Time<TTime>
        {
            return input.FromWebHdfs<TOutput, TTime>(deserialize, Environment.UserName, 50070);
        }

        /// <summary>
        /// Read a collection of HDFS files serialized in a custom binary format. Concatenate all the records to the output,
        /// in an unspecified order.
        /// </summary>
        /// <typeparam name="TOutput">output record type</typeparam>
        /// <param name="manager">Naiad computation</param>
        /// <param name="fileOrDirectoryPath">path of the file or directory to read</param>
        /// <param name="deserialize">custom deserializer function to convert a stream of bytes into a sequence of records</param>
        /// <param name="user">hdfs user</param>
        /// <param name="webPort">webhdfs protocol port</param>
        /// <returns>stream of records in the hdfs files</returns>
        public static Stream<TOutput, Epoch> ReadWebHdfsCollection<TOutput>(
            this Computation manager, Uri fileOrDirectoryPath,
            Func<Stream, IEnumerable<ArraySegment<TOutput>>> deserialize,
            string user, int webPort)
        {
            return new Uri[] { fileOrDirectoryPath }
                .AsNaiadStream(manager)
                // this distinct ensures that the same code can be run at every process without reading files multiple times
                .Distinct()
                .GenerateWork(
                    time => new DfsFileCoordinator(new WebHdfsClient(user, webPort)),
                    (workerIndex, time) => new DfsFileWorker<TOutput>(new WebHdfsClient(user, webPort), deserialize));
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
        public static Stream<TOutput, Epoch> ReadWebHdfsCollection<TOutput>(
            this Computation manager, Uri fileOrDirectoryPath,
            Func<Stream, IEnumerable<ArraySegment<TOutput>>> deserialize)
        {
            return manager.ReadWebHdfsCollection<TOutput>(fileOrDirectoryPath, deserialize, Environment.UserName, 50070);
        }

        /// <summary>
        /// Read a stream of path names (file or directory names) each of which corresponds to a collection of HDFS files
        /// serialized in the default Naiad binary format. Concatenate all the records to the output, in an unspecified order.
        /// </summary>
        /// <typeparam name="TOutput">output record type</typeparam>
        /// <typeparam name="TTime">time of the input and output records</typeparam>
        /// <param name="input">stream of input paths</param>
        /// <param name="user">hdfs user</param>
        /// <param name="webPort">webhdfs protocol port</param>
        /// <returns>stream of records in the hdfs files</returns>
        public static Stream<TOutput, TTime> FromWebHdfsBinary<TOutput, TTime>(
            this Stream<Uri, TTime> input,
            string user, int webPort) where TTime : Time<TTime>
        {
            return input.GenerateWork(
                    time => new DfsFileCoordinator(new WebHdfsClient(user, webPort)),
                    (workerIndex, time) =>
                        new DfsFileWorker<TOutput>(
                            new WebHdfsClient(user, webPort), 
                            stream => Utils.GetNaiadReaderBatchEnumerable<TOutput>(stream, 256)));
        }

        /// <summary>
        /// Read a stream of path names (file or directory names) each of which corresponds to a collection of HDFS files
        /// serialized in the default Naiad binary format. Concatenate all the records to the output, in an unspecified order.
        /// </summary>
        /// <typeparam name="TOutput">output record type</typeparam>
        /// <typeparam name="TTime">time of the input and output records</typeparam>
        /// <param name="input">stream of input paths</param>
        /// <returns>stream of records in the hdfs files</returns>
        public static Stream<TOutput, TTime> FromWebHdfsBinary<TOutput, TTime>(
            this Stream<Uri, TTime> input) where TTime : Time<TTime>
        {
            return input.FromWebHdfsBinary<TOutput, TTime>(Environment.UserName, 50070);
        }

        /// <summary>
        /// Read a collection of HDFS files serialized in the default Naiad binary format. Concatenate all the records to the output,
        /// in an unspecified order.
        /// </summary>
        /// <typeparam name="TOutput">output record type</typeparam>
        /// <param name="manager">Naiad computation</param>
        /// <param name="fileOrDirectoryPath">path of the file or directory to read</param>
        /// <param name="user">hdfs user</param>
        /// <param name="webPort">webhdfs protocol port</param>
        /// <returns>stream of records in the hdfs files</returns>
        public static Stream<TOutput, Epoch> ReadWebHdfsBinaryCollection<TOutput>(
            this Computation manager, Uri fileOrDirectoryPath,
            string user, int webPort)
        {
            return new Uri[] { fileOrDirectoryPath }
                .AsNaiadStream(manager)
                // this distinct ensures that the same code can be run at every process without reading files multiple times
                .Distinct()
                .GenerateWork(
                    time => new DfsFileCoordinator(new WebHdfsClient(user, webPort)),
                    (workerIndex, time) =>
                        new DfsFileWorker<TOutput>(
                            new WebHdfsClient(user, webPort),
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
        public static Stream<TOutput, Epoch> ReadWebHdfsBinaryCollection<TOutput>(
            this Computation manager, Uri fileOrDirectoryPath)
        {
            return manager.ReadWebHdfsBinaryCollection<TOutput>(fileOrDirectoryPath, Environment.UserName, 50070);
        }

        /// <summary>
        /// general method to write a stream of records to a collection of HDFS files. The collection is active
        /// throughout the computation and is closed when the computation terminates: it concatenates records from all
        /// epochs in an undefined order
        /// </summary>
        /// <typeparam name="TOutput">type of the records to write</typeparam>
        /// <typeparam name="TWriter">type of the serializer object</typeparam>
        /// <param name="source">stream of records to write</param>
        /// <param name="user">hdfs user</param>
        /// <param name="webPort">webhdfs protocol port</param>
        /// <param name="format">function to generate a filename given a processId, threadId and sequence number</param>
        /// <param name="writerFunction">function to generate a serializer given a Stream to write to</param>
        /// <param name="serialize">function to serialize a batch of records given a serializer</param>
        /// <param name="blockSize">hdfs block size to use, or -1 for the file system default value</param>
        /// <param name="segmentThreshold">file size to write before closing the file and opening another one</param>
        /// <returns>handle to wait on until the computation completes</returns>
        public static Subscription WriteWebHdfsBinary<TOutput, TWriter>(
            this Stream<TOutput, Epoch> source,
            string user, int webPort,
            Func<int, int, int, Uri> format,
            Func<Stream, TWriter> writerFunction,
            Action<TWriter, ArraySegment<TOutput>> serialize,
            long blockSize,
            long segmentThreshold) where TWriter : class, IDisposable, IFlushable
        {
            return source.WriteBySubscription(
                format,
                fileName => new WebHdfsClient(user, webPort).GetDfsStreamWriter(fileName, blockSize),
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
        /// <param name="user">hdfs user</param>
        /// <param name="webPort">webhdfs protocol port</param>
        /// <param name="prefix">webhdfs directory to write the partitioned data into</param>
        /// <param name="bufferSize">buffer size to use in the serializer</param>
        /// <param name="blockSize">hdfs block size to use, or -1 for the file system default value</param>
        /// <param name="segmentThreshold">file size to write before closing the file and opening another one</param>
        /// <returns>handle to wait on until the computation completes</returns>
        public static Subscription WriteWebHdfsBinary<TOutput>(
            this Stream<TOutput, Epoch> source,
            string user, int webPort,
            Uri prefix,
            int bufferSize = 1024 * 1024,
            long blockSize = -1,
            long segmentThreshold = 254 * 1024 * 1024)
        {
            // make sure we'll be able to write the partitioned data
            WebHdfsClient client = new WebHdfsClient(user, webPort);
            client.EnsureDirectory(prefix, false);

            return source.WriteWebHdfsBinary(
                user, webPort,
                (processId, threadId, segment) => Utils.DefaultPartFormat(prefix, processId, threadId, segment), 
                stream => new NaiadWriter<TOutput>(stream, source.ForStage.Computation.Controller.SerializationFormat, true, bufferSize),
                (writer, arraySegment) =>
                {
                    for (int i = 0; i < arraySegment.Count; i++)
                    {
                        writer.Write(arraySegment.Array[i]);
                    }
                },
                blockSize,
                segmentThreshold);
        }

        /// <summary>
        /// general method to write a stream of records to a collection of HDFS files, partitioned by time as well as key.
        /// Within a given time and part, records are written in an undefined order
        /// </summary>
        /// <typeparam name="TOutput">type of the records to write</typeparam>
        /// <typeparam name="TWriter">type of the serializer object</typeparam>
        /// <typeparam name="TTime">type of the record time</typeparam>
        /// <param name="source">stream of records to write</param>
        /// <param name="user">hdfs user</param>
        /// <param name="webPort">webhdfs protocol port</param>
        /// <param name="format">function to generate a filename given a processId, threadId, time and sequence number</param>
        /// <param name="writerFunction">function to generate a serializer given a Stream to write to</param>
        /// <param name="serialize">function to serialize a batch of records given a serializer</param>
        /// <param name="blockSize">hdfs block size to use, or -1 for the file system default value</param>
        /// <param name="segmentThreshold">file size to write before closing the file and opening another one</param>
        /// <returns>stream of filenames written</returns>
        public static Stream<Uri, TTime> ToWebHdfsBinary<TOutput, TWriter, TTime>(
            this Stream<TOutput, TTime> source,
            string user, int webPort,
            Func<int, int, TTime, int, Uri> format,
            Func<Stream, TWriter> writerFunction,
            Action<TWriter, ArraySegment<TOutput>> serialize,
            long blockSize,
            long segmentThreshold)
            where TWriter : class, IDisposable, IFlushable
            where TTime : Time<TTime>
        {
            return source.WriteByTime(
                format,
                () => new WebHdfsClient(user, webPort),
                (client, fileName) => client.GetDfsStreamWriter(fileName, blockSize),
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
        /// <param name="user">hdfs user</param>
        /// <param name="webPort">webhdfs protocol port</param>
        /// <param name="prefix">webhdfs directory to write the partitioned data into</param>
        /// <param name="bufferSize">buffer size to use in the serializer</param>
        /// <param name="blockSize">hdfs block size to use, or -1 for the file system default value</param>
        /// <param name="segmentThreshold">file size to write before closing the file and opening another one</param>
        /// <returns>stream of filenames written</returns>
        public static Stream<Uri, TTime> ToWebHdfsBinary<TOutput, TTime>(
            this Stream<TOutput, TTime> source,
            string user, int webPort,
            Uri prefix,
            int bufferSize = 1024 * 1024,
            long blockSize = -1,
            long segmentThreshold = 254 * 1024 * 1024) where TTime : Time<TTime>
        {
            // make sure we'll be able to write the partitioned data
            WebHdfsClient client = new WebHdfsClient(user, webPort);
            client.EnsureDirectory(prefix, false);

            return source.ToWebHdfsBinary(
                user, webPort,
                (processId, threadId, time, segment) => Utils.DefaultPartFormat(prefix, processId, threadId, time, segment),
                stream => new NaiadWriter<TOutput>(stream, source.ForStage.Computation.Controller.SerializationFormat, true, bufferSize),
                (writer, arraySegment) =>
                {
                    for (int i = 0; i < arraySegment.Count; i++)
                    {
                        writer.Write(arraySegment.Array[i]);
                    }
                },
                blockSize,
                segmentThreshold);
        }

        /// <summary>
        /// write a sequence of strings as hdfs text files. The collection is active throughout the computation and is 
        /// closed when the computation terminates: it concatenates records from all epochs in an undefined order
        /// </summary>
        /// <param name="source">stream of records to write</param>
        /// <param name="user">hdfs user</param>
        /// <param name="webPort">webhdfs protocol port</param>
        /// <param name="prefix">webhdfs directory to write the partitioned data into</param>
        /// <param name="blockSize">hdfs block size to use, or -1 for the file system default value</param>
        /// <param name="segmentThreshold">file size to write before closing the file and opening another one</param>
        /// <returns>handle to wait on until the computation completes</returns>
        public static Subscription WriteWebHdfsText(
            this Stream<string, Epoch> source,
            string user, int webPort,
            Uri prefix,
            long blockSize = -1,
            long segmentThreshold = 254 * 1024 * 1024)
        {
            // make sure we'll be able to write the partitioned data
            WebHdfsClient client = new WebHdfsClient(user, webPort);
            client.EnsureDirectory(prefix, false);

            // don't write byte order marks at the start of the files
            Encoding utf8 = new UTF8Encoding(false, true);

            return source.WriteWebHdfsBinary(
                user, webPort,
                (processId, threadId, segment) => Utils.DefaultPartFormat(prefix, processId, threadId, segment),
                stream => new Utils.FStreamWriter(stream, utf8, 1024 * 1024),
                (writer, arraySegment) =>
                {
                    for (int i = 0; i < arraySegment.Count; i++)
                    {
                        writer.WriteLine(arraySegment.Array[i]);
                    }
                },
                blockSize,
                segmentThreshold);
        }

        /// <summary>
        /// write a sequence of strings as hdfs text files, partitioned by time as well as key.
        /// Within a given time and part, records are written in an undefined order
        /// </summary>
        /// <typeparam name="TTime">type of the record time</typeparam>
        /// <param name="source">stream of records to write</param>
        /// <param name="user">hdfs user</param>
        /// <param name="webPort">webhdfs protocol port</param>
        /// <param name="prefix">webhdfs directory to write the partitioned data into</param>
        /// <param name="blockSize">hdfs block size to use, or -1 for the file system default value</param>
        /// <param name="segmentThreshold">file size to write before closing the file and opening another one</param>
        /// <returns>stream of filenames written</returns>
        public static Stream<Uri, TTime> ToWebHdfsText<TTime>(
            this Stream<string, TTime> source,
            string user, int webPort,
            Uri prefix,
            long blockSize = -1,
            long segmentThreshold = 254 * 1024 * 1024) where TTime : Time<TTime>
        {
            // make sure we'll be able to write the partitioned data
            WebHdfsClient client = new WebHdfsClient(user, webPort);
            client.EnsureDirectory(prefix, false);

            // don't write byte order marks at the start of the files
            Encoding utf8 = new UTF8Encoding(false, true);

            return source.ToWebHdfsBinary(
                user, webPort,
                (processId, threadId, time, segment) => Utils.DefaultPartFormat(prefix, processId, threadId, time, segment),
                stream => new Utils.FStreamWriter(stream, utf8, 1024 * 1024),
                (writer, arraySegment) =>
                {
                    for (int i = 0; i < arraySegment.Count; i++)
                    {
                        writer.WriteLine(arraySegment.Array[i]);
                    }
                },
                blockSize, segmentThreshold);
        }
    }
    #endregion
}
