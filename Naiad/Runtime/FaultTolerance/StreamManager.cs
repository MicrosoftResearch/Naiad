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
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

using Microsoft.Research.Naiad.Dataflow;
using Microsoft.Research.Naiad.Runtime.Progress;
using Microsoft.Research.Naiad.Serialization;

namespace Microsoft.Research.Naiad.Runtime.FaultTolerance
{
    /// <summary>
    /// Position in a sequence of streams. This is public so it can be serialized
    /// </summary>
    public struct StreamSequenceIndex : IEquatable<StreamSequenceIndex>, IComparable<StreamSequenceIndex>
    {
        /// <summary>
        /// the stream in the sequence
        /// </summary>
        public int index;
        /// <summary>
        /// the position in the stream
        /// </summary>
        public long position;

        /// <summary>
        /// get the hash code for the index
        /// </summary>
        /// <returns>hash code</returns>
        public override int GetHashCode()
        {
            return this.index + 123412324 * this.position.GetHashCode();
        }

        /// <summary>
        /// compares equality of two indices
        /// </summary>
        /// <param name="other">the index to compare</param>
        /// <returns>true if the indices point to the same place in the sequence</returns>
        public bool Equals(StreamSequenceIndex other)
        {
            return index == other.index && position == other.position;
        }

        /// <summary>
        /// compares two indices
        /// </summary>
        /// <param name="other">the index to compare</param>
        /// <returns>-1 if this index is earlier than <paramref name="other"/>, 0 if they are equal, 1 if this index is later than <paramref name="other"/></returns>
        public int CompareTo(StreamSequenceIndex other)
        {
            if (this.index == other.index)
            {
                return this.position.CompareTo(other.position);
            }
            else
            {
                return this.index.CompareTo(other.index);
            }
        }
    }

    /// <summary>
    /// Interface defining a stream sequence used for logging
    /// </summary>
    public interface IStreamSequence
    {
        /// <summary>
        /// The base name of the stream
        /// </summary>
        string Name { get; }

        /// <summary>
        /// The current stream being written
        /// </summary>
        Stream CurrentStream { get; }

        /// <summary>
        /// Flush a stream
        /// </summary>
        /// <param name="stream">stream to flush</param>
        void Flush(Stream stream);

        /// <summary>
        /// Stop using the sequence
        /// </summary>
        void Shutdown();

        /// <summary>
        /// Close a stream
        /// </summary>
        /// <param name="stream">stream to close</param>
        void Close(Stream stream);

        /// <summary>
        /// Return the current stream and set the current stream to null
        /// </summary>
        /// <returns>the old current stream</returns>
        Stream CloseCurrentStream();

        /// <summary>
        /// Open a new stream to write and make it the current stream
        /// </summary>
        /// <param name="index">index of the stream in the sequence</param>
        void OpenWriteStream(int index);

        /// <summary>
        /// Open a new stream to read
        /// </summary>
        /// <param name="index">index of the stream in the sequence</param>
        /// <returns>a readable stream</returns>
        Stream OpenReadStream(int index);

        /// <summary>
        /// Delete the indicated stream
        /// </summary>
        /// <param name="streamIndex">index of the stream to delete</param>
        void GarbageCollectStream(int streamIndex);

        /// <summary>
        /// Find all the indices of streams that already exist
        /// </summary>
        /// <returns>the indices of existing streams</returns>
        IEnumerable<int> FindStreams();
    }

    internal struct GarbageCollectionWrapper
    {
        public StreamSequence stream;
        public int index;

        public void Collect()
        {
            this.stream.GarbageCollectStream(this.index);
        }

        public GarbageCollectionWrapper(StreamSequence stream, int index)
        {
            this.stream = stream;
            this.index = index;
        }
    }

    internal class StreamSequence
    {
        private class ReadStreamWrapper : Stream
        {
            private StreamSequence parent;
            private StreamSequenceIndex end;
            private Stream currentStream;
            private int currentIndex;

            public override bool CanRead
            {
                get { return true; }
            }

            public override bool CanSeek
            {
                get { return false; }
            }

            public override bool CanWrite
            {
                get { return false; }
            }

            public override void Flush()
            {
                throw new NotImplementedException();
            }

            public override long Length
            {
                get { throw new NotImplementedException(); }
            }

            public override long Position
            {
                get
                {
                    throw new NotImplementedException();
                }
                set
                {
                    throw new NotImplementedException();
                }
            }

            public override int Read(byte[] buffer, int offset, int count)
            {
                int toRead = count;

                while (true)
                {
                    if (this.currentIndex == this.end.index)
                    {
                        long remaining = this.end.position - this.currentStream.Position;
                        toRead = (int)Math.Min(toRead, remaining);
                    }

                    if (toRead == 0)
                    {
                        return 0;
                    }

                    int nRead = this.currentStream.Read(buffer, offset, toRead);
                    if (nRead > 0)
                    {
                        return nRead;
                    }

                    if (this.currentIndex == this.end.index)
                    {
                        throw new ApplicationException("Couldn't read enough data from stream");
                    }

                    ++this.currentIndex;
                    this.currentStream.Close();
                    this.currentStream.Dispose();
                    this.currentStream = this.parent.OpenReadStream(this.currentIndex);
                }
            }

            public override long Seek(long offset, SeekOrigin origin)
            {
                throw new NotImplementedException();
            }

            public override void SetLength(long value)
            {
                throw new NotImplementedException();
            }

            public override void Write(byte[] buffer, int offset, int count)
            {
                throw new NotImplementedException();
            }

            public override void Close()
            {
                this.currentStream.Close();
                this.currentStream.Dispose();
                this.currentStream = null;
                base.Close();
            }

            public ReadStreamWrapper(StreamSequence parent, StreamSequenceIndex start, StreamSequenceIndex end)
            {
                this.parent = parent;
                this.end = end;
                this.currentIndex = start.index;
                this.currentStream = this.parent.OpenReadStream(this.currentIndex);
                this.currentStream.Seek(start.position, SeekOrigin.Begin);
            }
        }

        private readonly IStreamSequence sequence;
        private readonly long streamThreshold;
        private long currentPosition = 0;

        public static int TryReadLength(byte[] buffer, Stream input, int length, NaiadSerialization<int> intSerializer)
        {
            int toRead = length;
            while (toRead > 0)
            {
                int nRead = input.Read(buffer, length - toRead, toRead);
                if (nRead == 0)
                {
                    if (toRead == sizeof(int))
                    {
                        return -1;
                    }
                    else
                    {
                        return -2;
                    }
                }
                toRead -= nRead;
            }

            RecvBuffer recv = new RecvBuffer(buffer, length - sizeof(int), length);

            int nextBlockLength;
            if (intSerializer.TryDeserialize(ref recv, out nextBlockLength))
            {
                return nextBlockLength;
            }
            else
            {
                return -2;
            }
        }

        public string BaseName { get { return this.sequence.Name; } }

        public int CurrentIndex { get; private set; }

        public void Shutdown()
        {
            this.sequence.Shutdown();
        }

        public void CloseCurrentStream()
        {
            this.FlushAsync();
            Stream stream = this.sequence.CloseCurrentStream();
            if (stream != null)
            {
                this.AddIO(t => this.sequence.Close(stream));
            }
        }

        public Stream OpenReadStream(int index)
        {
            return this.sequence.OpenReadStream(index);
        }

        public IEnumerable<int> FindStreams()
        {
            return this.sequence.FindStreams();
        }

        public void GarbageCollectStream(int streamIndex)
        {
            this.sequence.GarbageCollectStream(streamIndex);
        }

        public StreamSequenceIndex Index
        {
            get
            {
                if (this.NeedToBreak)
                {
                    return new StreamSequenceIndex { index = this.CurrentIndex + 1, position = this.nextOffsetToWrite };
                }
                else
                {
                    return new StreamSequenceIndex { index = this.CurrentIndex, position = this.currentPosition + this.nextOffsetToWrite };
                }
            }
        }

        public StreamSequenceIndex MaxIndex
        {
            get
            {
                foreach (int index in this.FindStreams().OrderByDescending(i => i))
                {
                    Stream s = this.OpenReadStream(index);
                    return new StreamSequenceIndex { index = index, position = s.Length };
                }

                return new StreamSequenceIndex { index = -1, position = 0L };
            }
        }

        public StreamSequenceIndex UnboundedIndex
        {
            get { return new StreamSequenceIndex { index = Int32.MaxValue, position = Int64.MaxValue }; }
        }

        public IEnumerable<int> ExtentRange(Pair<StreamSequenceIndex, StreamSequenceIndex> extent)
        {
            int max = Math.Min(this.CurrentIndex, extent.Second.index);
            return Enumerable.Range(extent.First.index, max + 1 - extent.First.index);
        }

        public Stream Read(StreamSequenceIndex start, StreamSequenceIndex end)
        {
            return new ReadStreamWrapper(this, start, end);
        }

        public bool NeedToBreak { get { return this.sequence.CurrentStream == null || this.currentPosition >= this.streamThreshold;  } }

        private readonly int bufferLength;

        private byte[] buffer;
        private int nextOffsetToWrite;
        private int inQueue;
        private TaskCompletionSource<bool> waitTask = null;
        private Task pendingIO = Task.FromResult(true);
        private void AddIO(Action<Task> io)
        {
            //io(Task.FromResult(true));
            if (this.pendingIO.IsCompleted)
            {
                this.pendingIO = Task.Run(() => io(Task.FromResult(true)));
            }
            else
            {
                this.pendingIO = this.pendingIO.ContinueWith(t => io(t));
            }
        }

        private void WriteCompleted(Task t, byte[] buffer)
        {
            if (t.IsFaulted)
            {
                throw t.Exception;
            }

            GlobalBufferPool<byte>.pool.CheckIn(buffer);

            lock (this)
            {
                if (inQueue == 0)
                {
                    throw new ApplicationException("bad queue logic");
                }

                --inQueue;

                if (this.waitTask != null)
                {
                    this.waitTask.SetResult(true);
                    this.waitTask = null;
                }
            }
        }

        private void EnsureBuffer(int length)
        {
            if (this.buffer != null && this.buffer.Length < length)
            {
                if (this.nextOffsetToWrite > 0)
                {
                    throw new ApplicationException("send buffer before requesting a larger one");
                }
                GlobalBufferPool<byte>.pool.CheckIn(this.buffer);
                this.buffer = null;
            }

            if (this.buffer == null)
            {
                this.buffer = GlobalBufferPool<byte>.pool.CheckOut(length);
            }
        }

        public Task FlushAsync()
        {
            if (buffer != null)
            {
                this.SendBuffer();
            }

            Stream toFlush = this.sequence.CurrentStream;
            if (toFlush != null)
            {
                this.AddIO(t => { this.sequence.Flush(toFlush); });
            }

            return this.pendingIO;
        }

        public long TotalWritten { get; set; }

        private void SendBuffer()
        {
            if (this.nextOffsetToWrite == 0)
            {
                return;
            }

            Task toWait = null;
            lock (this)
            {
                if (this.inQueue > 3)
                {
                    this.waitTask = new TaskCompletionSource<bool>();
                    toWait = this.waitTask.Task;
                }
                ++inQueue;
            }
            if (toWait != null)
            {
                toWait.Wait();
            }

            byte[] thisBuffer = this.buffer;
            int thisCount = this.nextOffsetToWrite;

            this.buffer = null;
            this.nextOffsetToWrite = 0;

            if (this.NeedToBreak)
            {
                ++this.CurrentIndex;
                this.CloseCurrentStream();
                this.sequence.OpenWriteStream(this.CurrentIndex);
                this.currentPosition = 0;
            }

            this.currentPosition += thisCount;
            this.TotalWritten += thisCount;

            Stream captured = this.sequence.CurrentStream;
            this.AddIO(t => { captured.Write(thisBuffer, 0, thisCount); this.WriteCompleted(t, thisBuffer); });
        }

        public void Write(byte[] data, int offset, int length)
        {
            this.EnsureBuffer(this.bufferLength);
            if (this.nextOffsetToWrite + length > buffer.Length)
            {
                this.SendBuffer();
                this.EnsureBuffer(Math.Max(length, this.bufferLength));
            }

            Buffer.BlockCopy(data, offset, this.buffer, this.nextOffsetToWrite, length);
            this.nextOffsetToWrite += length;

            if (this.nextOffsetToWrite == this.buffer.Length)
            {
                this.SendBuffer();
            }
        }

        public void Restart(int index)
        {
            if (this.sequence.CurrentStream != null)
            {
                throw new ApplicationException("Can't restart after writing");
            }

            this.CurrentIndex = index;
        }

        public StreamSequence(IStreamSequence sequence, int bufferLength, long threshold)
        {
            this.sequence = sequence;
            this.bufferLength = bufferLength;
            this.streamThreshold = threshold;

            this.CurrentIndex = -1;
            foreach (int index in this.FindStreams())
            {
                if (index > this.CurrentIndex)
                {
                    this.CurrentIndex = index;
                }
            }
        }
    }

    internal class BlockStreamSequence
    {
        private readonly StreamSequence streams;

        private NaiadSerialization<int> intSerializer = null;

        public bool NeedToBreak { get { return this.streams.NeedToBreak; } }

        public void StartNewStream()
        {
            this.streams.CloseCurrentStream();
        }

        public Task Write(byte[] buffer, int offset, int count)
        {
            this.streams.Write(buffer, offset, count);
            return this.streams.FlushAsync();
        }

        public IEnumerable<RecvBuffer> Reinitialize(SerializationFormat format)
        {
            if (this.intSerializer == null)
            {
                this.intSerializer = format.GetSerializer<int>();
            }

            byte[] buffer = new byte[sizeof(int)];

            int foundIndex = -1;

            foreach (int index in this.streams.FindStreams().OrderByDescending(i => i))
            {
                bool streamGood = false;

                if (foundIndex == -1)
                {
                    using (Stream s = this.streams.OpenReadStream(index))
                    {
                        int blockLength = StreamSequence.TryReadLength(buffer, s, sizeof(int), this.intSerializer);
                        while (blockLength >= 0)
                        {
                            if (buffer.Length < blockLength)
                            {
                                buffer = new byte[blockLength];
                            }

                            int newBlockLength = StreamSequence.TryReadLength(buffer, s, blockLength, this.intSerializer);

                            if (blockLength >= -1)
                            {
                                streamGood = true;
                                foundIndex = index;
                                yield return new RecvBuffer(buffer, 0, blockLength - sizeof(int));
                            }

                            blockLength = newBlockLength;
                        }
                    }
                }

                if (!streamGood)
                {
                    this.streams.GarbageCollectStream(index);
                }
            }

            this.streams.Restart(foundIndex);
        }

        public void Initialize()
        {
            foreach (int index in this.streams.FindStreams())
            {
                this.streams.GarbageCollectStream(index);
            }

            this.streams.Restart(-1);
        }

        public IEnumerable<GarbageCollectionWrapper> GarbageCollectNonCurrent()
        {
            return this.streams.FindStreams()
                .Where(i => i != this.streams.CurrentIndex)
                .Select(i => new GarbageCollectionWrapper(this.streams, i));
        }

        public void Shutdown()
        {
            this.streams.Shutdown();
        }

        public BlockStreamSequence(StreamSequence streams)
        {
            this.streams = streams;
        }
    }

    internal class StreamSequenceManager<T> where T : Time<T>
    {
        private readonly int vertexId;
        private readonly int stageId;
        private BlockStreamSequence metaData = null;
        private StreamSequence eventLog = null;
        private readonly Dictionary<int, StreamSequence> messageLog = new Dictionary<int, StreamSequence>();
        private StreamSequence checkpoint = null;

        private readonly Func<string, IStreamSequence> streamSequenceFactory;
        private StreamSequence GetStreamSequence(string name, int bufferLength, long threshold)
        {
            return new StreamSequence(this.streamSequenceFactory(name), bufferLength, threshold);
        }

        private string StreamName(int stageId, int vertexId, string extension)
        {
            return String.Format("{0:D4}-{1:D4}.{2}", stageId, vertexId, extension);
        }

        private string StreamName(int stageId, int channelId, int vertexId, string extension)
        {
            return String.Format("{0:D4}-{1:D4}-{2:D4}.{3}", stageId, channelId, vertexId, extension);
        }

        public BlockStreamSequence MetaData
        {
            get
            {
                if (this.metaData == null)
                {
                    this.metaData = new BlockStreamSequence(this.GetStreamSequence(StreamName(this.stageId, this.vertexId, "metadata"), 16384, Int64.MaxValue));
                }
                return this.metaData;
            }
        }

        public StreamSequence EventLog
        {
            get
            {
                if (this.eventLog == null)
                {
                    this.eventLog = this.GetStreamSequence(StreamName(this.stageId, this.vertexId, "eventlog"), 4 * 1024 * 1024, 256L * 1024L * 1024L);
                }
                return this.eventLog;
            }
        }

        public StreamSequence MessageLog(int stageOutputIndex)
        {
            StreamSequence sequence = null;
            if (!this.messageLog.TryGetValue(stageOutputIndex, out sequence))
            {
                sequence = this.GetStreamSequence(StreamName(this.stageId, stageOutputIndex, this.vertexId, "outputlog"), 4 * 1024 * 1024, 256L * 1024L * 1024L);
            }
            this.messageLog.Add(stageOutputIndex, sequence);

            return sequence;
        }

        public StreamSequence Checkpoint
        {
            get
            {
                if (this.checkpoint == null)
                {
                    this.checkpoint = this.GetStreamSequence(StreamName(this.stageId, this.vertexId, "checkpoint"), 4 * 1024 * 1024, Int64.MaxValue);
                }
                return this.checkpoint;
            }
        }

        public void Shutdown()
        {
            if (this.metaData != null)
            {
                this.metaData.Shutdown();
            }

            if (this.eventLog != null)
            {
                this.eventLog.Shutdown();
            }

            foreach (StreamSequence seq in this.messageLog.Values)
            {
                seq.Shutdown();
            }

            if (this.checkpoint != null)
            {
                this.checkpoint.Shutdown();
            }

            this.messageLog.Clear();
        }

        public StreamSequenceManager(Func<string, IStreamSequence> streamSequenceFactory, int vertexId, int stageId)
        {
            this.streamSequenceFactory = streamSequenceFactory;
            this.vertexId = vertexId;
            this.stageId = stageId;
        }
    }

    /// <summary>
    /// Class to manage checkpoint streams in an NTFS directory
    /// </summary>
    public class FileStreamSequence : IStreamSequence
    {
        private readonly string directory;
        private readonly string streamNameBase;
        private FileStream currentStream;

        /// <summary>
        /// The base name of the stream
        /// </summary>
        public string Name { get { return this.streamNameBase; } }

        /// <summary>
        /// The current stream being written
        /// </summary>
        public Stream CurrentStream
        {
            get { return this.currentStream; }
        }

        /// <summary>
        /// Flush a stream
        /// </summary>
        /// <param name="stream">stream to flush</param>
        public void Flush(Stream stream)
        {
            FileStream fStream = stream as FileStream;
            fStream.Flush(true);
        }

        /// <summary>
        /// Stop using the sequence
        /// </summary>
        public void Shutdown()
        {
            Stream stream = this.CloseCurrentStream();
            if (stream != null)
            {
                this.Close(stream);
            }
        }

        /// <summary>
        /// Flush and close a stream
        /// </summary>
        /// <param name="stream">stream to close</param>
        public void Close(Stream stream)
        {
            stream.Flush();
            stream.Close();
            stream.Dispose();
        }

        /// <summary>
        /// Remove the current stream
        /// </summary>
        /// <returns>removed stream</returns>
        public Stream CloseCurrentStream()
        {
            Stream stream = this.currentStream;
            this.currentStream = null;
            return stream;
        }

        private string StreamName(int index)
        {
            return Path.Combine(this.directory, String.Format("{0}.{1:D4}", this.streamNameBase, index));
        }


        /// <summary>
        /// Open a new stream to write and make it the current stream
        /// </summary>
        /// <param name="index">index of the stream in the sequence</param>
        public void OpenWriteStream(int index)
        {
            this.currentStream = new FileStream(this.StreamName(index), FileMode.Create, FileAccess.Write, FileShare.Read);
        }

        /// <summary>
        /// Open a new stream to read
        /// </summary>
        /// <param name="index">index of the stream in the sequence</param>
        /// <returns>a readable stream</returns>
        public Stream OpenReadStream(int index)
        {
            return new FileStream(this.StreamName(index), FileMode.Open, FileAccess.Read, FileShare.ReadWrite);
        }

        /// <summary>
        /// Delete the indicated stream
        /// </summary>
        /// <param name="streamIndex">index of the stream to delete</param>
        public void GarbageCollectStream(int streamIndex)
        {
            string streamName = this.StreamName(streamIndex);
            if (File.Exists(streamName))
            {
                try
                {
                    File.Delete(streamName);
                }
                catch (IOException)
                {
                    // maybe anti-virus is stopping us here
                }
            }
        }

        /// <summary>
        /// Find all the indices of streams that already exist
        /// </summary>
        /// <returns>indices of existing streams</returns>
        public IEnumerable<int> FindStreams()
        {
            if (Directory.Exists(this.directory))
            {
                foreach (string file in Directory.EnumerateFiles(this.directory, this.streamNameBase + ".*"))
                {
                    string suffix = file.Split('.').Last();
                    int index = Int32.Parse(suffix);
                    yield return index;
                }
            }
        }

        /// <summary>
        /// Create a stream sequence in an NTFS directory
        /// </summary>
        /// <param name="directory">the directory to use</param>
        /// <param name="streamNameBase">the base for streams in the sequence</param>
        public FileStreamSequence(string directory, string streamNameBase)
        {
            this.directory = directory;
            this.streamNameBase = streamNameBase;
        }
    }
}
