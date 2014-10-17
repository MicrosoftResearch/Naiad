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
using System.Threading.Tasks;

using Microsoft.Research.Peloponnese.Shared;

using Microsoft.Research.Naiad;
using Microsoft.Research.Naiad.Dataflow;
using Microsoft.Research.Naiad.Frameworks.Lindi;
using Microsoft.Research.Naiad.Frameworks.WorkGenerator;

namespace Microsoft.Research.Naiad.Frameworks.Storage.Dfs
{
    /// <summary>
    /// The Dfs framework includes base classes to support reading and writing Hdfs files, and is intended to be extensible to other
    /// block-based file systems
    /// </summary>
    class NamespaceDoc
    {

    }

    #region helper classes
    /// <summary>
    /// Work item describing an Dfs block to be read at a worker.
    /// </summary>
    /// <remarks>
    /// This is currently somewhat HDFS-specific, and would probably be generalized if another file
    /// system were added
    /// </remarks>
    [Serializable]
    public class DfsBlock
    {
        /// <summary>
        /// path of the file being read
        /// </summary>
        public Uri path;
        /// <summary>
        /// total length of the file
        /// </summary>
        public long fileLength;
        /// <summary>
        /// offset of the block in the file
        /// </summary>
        public long offset;
        /// <summary>
        /// length of the block to read
        /// </summary>
        public long length;
        /// <summary>
        /// address and port of the preferred datanode to use to read the block from; this saves one redirection
        /// through the namenode during the webhdfs protocol
        /// </summary>
        public IPEndPoint dataNodeAddress;
    }
    #endregion

    #region base implementation for Dfs work coordinators
    /// <summary>
    /// base class for Dfs coordinators, that expands directories into sets of files to read, and keeps track
    /// of the mapping from IPAddresses to IPEndpoints
    /// </summary>
    /// <remarks>
    /// This is currently somewhat HDFS-specific, and would probably be generalized if another file
    /// system were added
    /// </remarks>
    /// <typeparam name="TWorkDescription">the type used by workers to identify themselves, e.g. including their IP addresses</typeparam>
    public abstract class DfsBaseCoordinator<TWorkDescription>
        : MatchingCoordinator<Uri, IPEndPoint, TWorkDescription, TWorkDescription, IPAddress[]>
    {
        /// <summary>
        /// The client for doing basic DFS operations.
        /// </summary>
        /// <remarks>
        /// This is currently an HDFS base client that supports either Java or WebHdfs protocols, but expects an HDFS-like way
        /// of operating that would be generalized for other DFSs
        /// </remarks>
        protected readonly HdfsClientBase client;
        /// <summary>
        /// mapping of IP address to datanode endpoint. The MatchingCoordinator queues are indexed by datanode endpoint
        /// so we know how to read the block once it is removed from a queue, but the workers identify themselves by IP
        /// address, so we need to be able to look up the corresponding queue given an IP address
        /// </summary>
        private readonly Dictionary<IPAddress, IPEndPoint> dataNodes;

        /// <summary>
        /// called whenever a queue is added: keep our index of addresses up to date
        /// </summary>
        /// <param name="queue">new queue</param>
        protected override void NotifyQueueAddition(IPEndPoint queue)
        {
            // we are assuming there is only one datanode per ip address
            this.dataNodes.Add(queue.Address, queue);
        }

        /// <summary>
        /// called whenever a queue is removed: keep our index of addresses up to date
        /// </summary>
        /// <param name="queue">queue being removed</param>
        protected override void NotifyQueueRemoval(IPEndPoint queue)
        {
            this.dataNodes.Remove(queue.Address);
        }

        /// <summary>
        /// Called when a worker announces that it is ready for another work item, to find a work item on the
        /// worker's matching queue, if any. The worker may have multiple IP addresses, so it returns them all,
        /// and if any matches an address the datanode is listening on, then the worker is matched to that datanode
        /// </summary>
        /// <param name="workerAddresses">the IP addresses of the worker's network interfaces</param>
        /// <param name="matchingDataNode">the datanode endpoint of a matching queue, if any</param>
        /// <returns>the matching queue, if there is one, otherwise null</returns>
        protected override MatchQueue MapWorkerToQueue(IPAddress[] workerAddresses, ref IPEndPoint matchingDataNode)
        {
            // look at each ip address that the worker can use, to see if any has a matching queue with work waiting
            foreach (IPAddress choice in workerAddresses)
            {
                // if there is a matching datanode, store it in matchingDataNode
                if (this.dataNodes.TryGetValue(choice, out matchingDataNode))
                {
                    // if there is a matching datanode then there must be a queue with that datanode, so return it
                    return this.waitingWork[matchingDataNode];
                }
            }

            // there is no matching queue
            return null;
        }

        /// <summary>
        /// given an hdfs file, return a sequence of work items, each with a set of matching categories
        /// </summary>
        /// <remarks>
        /// this is currently HDFS-specific, although the HdfsFile class could easily be extended to support
        /// other DFSs
        /// </remarks>
        /// <param name="file">file to expand</param>
        /// <returns>a sequence of work items, each with a set of matching categories</returns>
        protected abstract IEnumerable<Match> EnumerateFileWork(HdfsFile file);

        /// <summary>
        /// given an input string of a file or directory, expand it into a set of files, and then expand each file
        /// into a set of matches using the derived-class implementation of EnumerateFileWork
        /// </summary>
        /// <param name="fileOrDirectory">dfs file or directory to be read</param>
        /// <returns>set of work item matches for the file or directory</returns>
        protected override IEnumerable<Match> EnumerateWork(Uri fileOrDirectory)
        {
            return client
                .ExpandFileOrDirectoryToFile(fileOrDirectory)
                .SelectMany(file => EnumerateFileWork(file));
        }

        /// <summary>
        /// return a new coordinator for a dfs reader
        /// </summary>
        /// <param name="client">hdfs client</param>
        public DfsBaseCoordinator(HdfsClientBase client)
        {
            this.client = client;
            this.dataNodes = new Dictionary<IPAddress, IPEndPoint>();
        }
    }
    #endregion

    #region DfsBaseCoordinator implementation for the file-based dfs work coordinator
    /// <summary>
    /// base coordinator for workers that read an entire hdfs file at a time, rather than split the file into blocks.
    /// For each file the coordinator tries to match it to a worker that holds a large proportion of the relevant data
    /// </summary>
    public class DfsFileCoordinator : DfsBaseCoordinator<DfsBlock>
    {
        /// <summary>
        /// Return the length of a particular block in a file. All blocks except the last one are the same length
        /// </summary>
        /// <param name="index">index of the block in the file</param>
        /// <param name="file">file being read</param>
        /// <returns>length in bytes of the requested block</returns>
        private long BlockLength(int index, HdfsFile file)
        {
            // start location of the block in the file
            long offset = (long)index * file.blockSize;
            // number of bytes after the start of the block
            long bytesAfterBlockStart = file.length - offset;
            // either the standard block length, or the length of the final block if it is shorter
            return Math.Min(bytesAfterBlockStart, file.blockSize);
        }

        /// <summary>
        /// given a file, determine how much of that file's data are stored on each datanode. Return every datanode that stores a
        /// threshold percentage of the file's data as a candidate match for that file
        /// </summary>
        /// <param name="file">file to be matched</param>
        /// <returns>a match including a (possibly-empty) set of candidate data nodes</returns>
        protected override IEnumerable<Match> EnumerateFileWork(HdfsFile file)
        {
            long numberOfBlocks = (file.length + file.blockSize - 1) / file.blockSize;
            long threshold;
            if (numberOfBlocks > 4)
            {
                // this is a 'real' multi-block file; only take a datanode that contains at least a third of it
                threshold = file.length / 3;
            }
            else
            {
                // this file either has a single block or only a few: only return a matching node if it stores the whole file.
                // this will select the node that wrote (all the blocks in) the file rather than one of the replicas, in the case of a file with only a
                // couple of blocks
                threshold = file.length;
            }

            Match match = new Match
            {
                categories = this.client.GetBlockLocations(file)
                    // first flatten the list of block locations, into a sequence of pairs of 'endpoint,length' each indicating that length
                    // bytes are stored at endpoint
                    .SelectMany((endpoints, index) =>
                        endpoints.Select(endpoint => new KeyValuePair<IPEndPoint, long>(endpoint, BlockLength(index, file))))
                    // then group by endpoint
                    .GroupBy(x => x.Key)
                    // within each group, sum the bytes to determine how many bytes in total are stored at each endpoint
                    .Select(g => new KeyValuePair<IPEndPoint, long>(g.Key, g.Select(elt => elt.Value).Sum()))
                    // keep only endpoints that store more than 33% of the file
                    .Where(x => x.Value >= threshold)
                    // return the flattened array of candidate endpoints, if any
                    .Select(x => x.Key).ToArray(),

                // if there isn't a matching worker, use null as the default endpoint, meaning the read will be redirected to the
                // name node. Set the block to indicate the entire file
                workStub = new DfsBlock
                {
                    path = file.path,
                    fileLength = file.length,
                    offset = 0,
                    length = file.length,
                    dataNodeAddress = null
                }
            };

            yield return match;
        }

        /// <summary>
        /// if the work item was matched to a worker on the same computer, fill in the datanode endpoint before sending the work item
        /// </summary>
        /// <param name="usedMatchingQueue">true if the item was matched to a worker</param>
        /// <param name="endpoint">endpoint corresponding to the matched worker, if usedMatchingQueue==true</param>
        /// <param name="stub">work item with no endpoint filled in</param>
        /// <returns>work item with the endpoint filled in, if there was a match</returns>
        protected override DfsBlock ExpandWorkItem(bool usedMatchingQueue, IPEndPoint endpoint, DfsBlock stub)
        {
            if (usedMatchingQueue)
            {
                stub.dataNodeAddress = endpoint;
            }

            return stub;
        }

        /// <summary>
        /// create a new coordinator for file-at-a-time dfs reads
        /// </summary>
        /// <param name="client">hdfs client</param>
        public DfsFileCoordinator(HdfsClientBase client) : base(client)
        {
        }
    }
    #endregion

    #region IWorker implementation for the Hdfs file-based reader worker
    /// <summary>
    /// base worker implementation for the worker to read Hdfs files an entire file at a time, rather than block by block
    /// </summary>
    /// <typeparam name="TOutput">the type of records to be read</typeparam>
    public class DfsFileWorker<TOutput> : IWorker<DfsBlock, IPAddress[], TOutput>
    {
        /// <summary>
        /// a cache of the local IP interfaces, read on startup from DNS
        /// </summary>
        private readonly IPAddress[] localAddresses;
        /// <summary>
        /// the function that takes a stream and returns batches of records
        /// </summary>
        private readonly Func<Stream, IEnumerable<ArraySegment<TOutput>>> deserialize;
        /// <summary>
        /// the Hdfs client used to read files
        /// </summary>
        protected readonly HdfsClientBase client;

        /// <summary>
        /// Return a description of the worker that the coordinator will use when matching work items to workers. This is called
        /// once before any work item has been assigned, and once after each work item is performed.
        /// </summary>
        /// <returns>The IP addresses that the worker node is listening on, to be matched to file block locations for trying
        /// to schedule local reads</returns>
        public IPAddress[] DescribeWorker()
        {
            return this.localAddresses;
        }

        /// <summary>
        /// Execute a work item, reading an HDFS file and generating a sequence of output records
        /// </summary>
        /// <param name="workItem">The work item to be executed, corresponding to an entire Hdfs file</param>
        /// <returns>A sequence of array segments, each containing a sequence of records to be output</returns>
        public IEnumerable<ArraySegment<TOutput>> DoWork(DfsBlock workItem)
        {
            // ask the Hdfs client for a stream corresponding to the file. Use a 1k buffer for byte-at-a-time reads.
            using (Stream reader = client.GetDfsStreamReader(workItem.path, workItem.offset, workItem.length, 1024, workItem.dataNodeAddress))
            {
                foreach (ArraySegment<TOutput> segment in this.deserialize(reader))
                {
                    yield return segment;
                }
            }
        }

        /// <summary>
        /// create a worker for a file-at-a-time hdfs reader
        /// </summary>
        /// <param name="client">Hdfs client used to read files</param>
        /// <param name="deserialize">function to take a stream consisting of an entire webhdfs file, and return a sequence
        /// of batches, each containing an arraysegment of output records</param>
        public DfsFileWorker(
            HdfsClientBase client, Func<Stream, IEnumerable<ArraySegment<TOutput>>> deserialize)
        {
            // cache all the addresses the local node is listening on
            this.localAddresses = Dns.GetHostAddresses(Dns.GetHostName());
            this.deserialize = deserialize;
            this.client = client;
        }
    }
    #endregion

    #region DfsBaseCoordinator implementation for the block-based dfs work coordinator
    /// <summary>
    /// Implementation of a MatchingCoordinator that manages work for reading dfs files in parallel, split by blocks
    /// </summary>
    /// <typeparam name="TItem">The concrete type of work item, which may include metadata specific to a particular
    /// serializer or file type</typeparam>
    public abstract class DfsBlockCoordinator<TItem> : DfsBaseCoordinator<TItem> where TItem : DfsBlock
    {
        /// <summary>
        /// For a given file, map a sequence of locations to a sequence of "base" matches, where the workStub
        /// component of the match has any filetype-specific metadata filled in and the categories component of the
        /// match has been set to the locations of that block. The HdfsBlock fields will
        /// all be filled in later after this sequence has been returned
        /// </summary>
        /// <param name="file">The file being read</param>
        /// <param name="blocks">The block locations for the file</param>
        /// <returns>A sequence of Matches with the categories set, and any file-specific metadata set</returns>
        protected abstract IEnumerable<Match> MakeBaseMatches(HdfsFile file, IEnumerable<IPEndPoint[]> blocks);

        /// <summary>
        /// called to convert an input file into a list of blocks
        /// </summary>
        /// <param name="file">the input file</param>
        /// <returns>the blocks in the file, along with a set of datanodes where each block is stored</returns>
        protected override IEnumerable<Match> EnumerateFileWork(HdfsFile file)
        {
            // get the blocks in the file, and convert each block to a base match, with file-specific metadata
            // filled in to the DfsBlock
            IEnumerable<Match> rawMatches = MakeBaseMatches(file, this.client.GetBlockLocations(file));

            // fill in the rest of the DfsBlock fields
            return rawMatches.Select((match, index) =>
            {
                match.workStub.path = file.path;
                match.workStub.fileLength = file.length;
                // all the blocks are the same size
                match.workStub.offset = (long)index * file.blockSize;
                long bytesRemaining = file.length - match.workStub.offset;
                match.workStub.length = Math.Min(file.blockSize, bytesRemaining);
                // this address will be used if the block is going to be read by a worker on a remote machine
                // otherwise the correct address will be filled in when the worker is chosen
                match.workStub.dataNodeAddress = match.categories.First();

                return match;
            });
        }

        /// <summary>
        /// Called when a work item is going to be sent to a worker. If usedMatchingQueue is true then dataNode
        /// is the endpoint of the dataNode that matches the worker
        /// </summary>
        /// <param name="usedMatchingQueue">true if the work item was pulled from a queue whose datanode is on
        /// the same computer as the worker</param>
        /// <param name="dataNode">datanode endpoint if the work item was pulled from a matching queue</param>
        /// <param name="stub">work item stub to fill in</param>
        /// <returns>work item with the datanode address filled in correctly</returns>
        protected override TItem ExpandWorkItem(bool usedMatchingQueue, IPEndPoint dataNode, TItem stub)
        {
            if (usedMatchingQueue)
            {
                // the worker is on the same machine as a datanode that is storing the block, so read from
                // that datanode
                stub.dataNodeAddress = dataNode;
            }
            // else the worker is on a machine that isn't storing the block, so just use the default datanode
            // that was stored in the stub in EnumerateBlocks

            return stub;
        }

        /// <summary>
        /// return a new coordinator for a block-based HDFS reader
        /// </summary>
        /// <param name="client">hdfs client used to read files and metadata</param>
        public DfsBlockCoordinator(HdfsClientBase client) : base(client)
        {
        }
    }
    #endregion

    #region IWorker implementation for the dfs block-based worker
    /// <summary>
    /// IWorker implementation for the dfs worker that reads data from blocks. This is further specialized to different file
    /// formats by passing in functions for syncing to record boundaries and deserializing data
    /// </summary>
    /// <typeparam name="TItem">The work item passed by the matching coordinator, which inherits from DfsBlock but may contain
    /// metadata used in syncing to record boundaries or deserializing</typeparam>
    /// <typeparam name="TOutput">The type of deserialized records produced by the worker</typeparam>
    public class DfsBlockWorker<TItem, TOutput> : IWorker<TItem, IPAddress[], TOutput> where TItem : DfsBlock
    {
        /// <summary>
        /// a cache of the local IP interfaces, read on startup from DNS
        /// </summary>
        private readonly IPAddress[] localAddresses;
        /// <summary>
        /// the number of bytes to use for each WebHdfs request when seeking past the end of the block for the start of the following
        /// record. If records are expected to be small, this should also be small to avoid pre-fetching a lot of the next block
        /// </summary>
        private readonly int syncRequestLength;
        /// <summary>
        /// the function used to sync to the next record in a stream
        /// </summary>
        private readonly Action<TItem, Stream> syncToNextRecord;
        /// <summary>
        /// the function used to deserialize records from a stream
        /// </summary>
        private readonly Func<TItem, Stream, IEnumerable<ArraySegment<TOutput>>> deserialize;
        /// <summary>
        /// the client used for reading Hdfs data
        /// </summary>
        protected readonly HdfsClientBase client;

        /// <summary>
        /// the IWorker implementation used to identify this worker to the WebHdfs coordinator, so that it can be sent work items
        /// of blocks stored at the same machine
        /// </summary>
        /// <returns>the IP addresses of this computer as reported by DNS</returns>
        public IPAddress[] DescribeWorker()
        {
            return this.localAddresses;
        }

        /// <summary>
        /// Find the start of the first record that begins after the end of the block we have been instructed to read. This indicates
        /// the end of the range of data that this block corresponds to
        /// </summary>
        /// <param name="workItem">the block we are reading</param>
        /// <returns></returns>
        private long FindSpillExtent(TItem workItem)
        {
            // compute the number of bytes remaining in the file past the end of our block
            long spillBytesRemaining = workItem.fileLength - workItem.offset - workItem.length;
            if (spillBytesRemaining <= 0)
            {
                // this is the last block, so our range runs exactly to the end of the block
                return workItem.length;
            }

            // get a stream that starts immediately after the end of the block, and continues until the end of the file.
            using (Stream spillReader = client.GetDfsStreamReader(
                workItem.path,
                // read from the end of this block for the rest of the file
                workItem.offset + workItem.length, spillBytesRemaining,
                // use small requests if we expect records to be fairly small, so we don't prefetch and buffer a lot of data in the next block
                this.syncRequestLength))
            {
                // call into the format-specific function to find the start of the next record. Potentially this spins all the way to the end of the
                // stream
                this.syncToNextRecord(workItem, spillReader);

                // return the offset of the next record after the block, relative to the start of the block
                return workItem.length + spillReader.Position;
            }
        }

        /// <summary>
        /// find the range of valid records in the block (those that start within the block), deserialize them, and return them in batches
        /// </summary>
        /// <param name="workItem">a description of the block to read</param>
        /// <returns>a sequence of batches of output records</returns>
        public IEnumerable<ArraySegment<TOutput>> DoWork(TItem workItem)
        {
            //Console.WriteLine("Starting work for " + workItem.path.AbsoluteUri + " " + workItem.offset + " " + workItem.length);

            // find the number of bytes from the start of the block to the end of the range, i.e. the start of the first record
            // that begins after the end of the block
            long endOfRange = FindSpillExtent(workItem);

            // create a reader for the range. Use the size of the range as the size of each underlying WebHdfs request so we
            // will make a single webhdfs request for all of the data that is stored in this block.
            using (Stream blockReader = client.GetDfsStreamReader(workItem.path, workItem.offset, endOfRange, this.syncRequestLength))
            {
                if (workItem.offset > 0)
                {
                    // unless we are the first block, scan forward from the start of the block to find the start of the next record,
                    // since the (partial) record at the beginning of the block will be read by the preceding block reader. If no records
                    // start within the block, the stream will end up positioned at endOfRange, so we won't deserialize anything
                    this.syncToNextRecord(workItem, blockReader);
                }

                // deserialize all the records in the range
                foreach (ArraySegment<TOutput> segment in this.deserialize(workItem, blockReader))
                {
                    yield return segment;
                }
            }
        }

        /// <summary>
        /// create a worker to read dfs files broken into blocks
        /// </summary>
        /// <param name="syncRequestLength">size of each dfs request when seeking past the end of the block for the start of the
        /// next record. If records are expected to be small this should also be small, to avoid prefetching and buffering a lot of the
        /// next block's data</param>
        /// <param name="syncToNextRecord">action to sync to the start of the next record. The first argument is the block item
        /// being read, which may contain metadata about sync markers. The second argument is the stream to scan.</param>
        /// <param name="deserialize">function to deserialize records in a stream</param>
        /// <param name="client">client used to read hdfs data</param>
        public DfsBlockWorker(
            int syncRequestLength,
            Action<TItem, Stream> syncToNextRecord,
            Func<TItem, Stream, IEnumerable<ArraySegment<TOutput>>> deserialize,
            HdfsClientBase client)
        {
            // cache the local IP addresses
            this.localAddresses = Dns.GetHostAddresses(Dns.GetHostName());

            this.syncRequestLength = syncRequestLength;
            this.syncToNextRecord = syncToNextRecord;
            this.deserialize = deserialize;
            this.client = client;
        }
    }
    #endregion

    #region Hdfs text reader classes
    /// <summary>
    /// the coordinator class for a text reader. No additional metadata is needed to describe a block, so this just uses DfsBlocks
    /// directly as work items
    /// </summary>
    public class DfsTextCoordinator : DfsBlockCoordinator<DfsBlock>
    {
        /// <summary>
        /// For a given file, map a sequence of locations to a sequence of "base" matches. The workStub
        /// component of the match doesn't need any file-specific metadata filled in. The categories component of the
        /// match is set to the locations of that block. The DfsBlock fields will
        /// all be filled in after this sequence has been returned
        /// </summary>
        /// <param name="file">The file being read</param>
        /// <param name="blocks">The block locations for the file</param>
        /// <returns>A sequence of Matches with the categories set, and any file-specific metadata set</returns>
        protected override IEnumerable<Match> MakeBaseMatches(HdfsFile file, IEnumerable<IPEndPoint[]> blocks)
        {
            return blocks.Select(endpoints => new Match { workStub = new DfsBlock(), categories = endpoints });
        }

        /// <summary>
        /// create a coordinator for reading Dfs files with fixed-length blocks, made of text records
        /// </summary>
        /// <param name="client">client used for reading Hdfs data and metadata</param>
        public DfsTextCoordinator(HdfsClientBase client) : base(client)
        {
        }
    }

    /// <summary>
    /// the worker class for a text reader for files with fixed-length blocks and text records. It uses DfsBlocks as work
    /// items, and parses data into lines represented as strings
    /// </summary>
    public class DfsTextWorker : DfsBlockWorker<DfsBlock, string>
    {
        /// <summary>
        /// sync forwards in a stream leaving it positioned on the first character after an end-of-line mark. Supports '\r\n', '\n' and
        /// '\r' as end-of-line. '\r' is considered the end of a line if it is followed by any character other than '\n'.
        /// </summary>
        /// <remarks>
        /// this assumes the stream is seekable, and that seeking backward by one character is efficient
        /// </remarks>
        /// <param name="stream">stream to scan forward in</param>
        static private void SyncToNextLine(Stream stream)
        {
            while (true)
            {
                int currentByte = stream.ReadByte();
                if (currentByte == -1)
                {
                    // we reached the end of the stream without seeing a line terminator, so leave the stream positioned at its end
                    return;
                }
                else if (currentByte == '\n')
                {
                    // we saw a line terminator, and are now positioned to read the first character of the next line
                    return;
                }
                else if (currentByte == '\r')
                {
                    // we saw a carriage return. If the next character is a newline then the next line starts after that,
                    // otherwise we are currently positioned on it. So read the next character, to check
                    int followingByte = stream.ReadByte();
                    if (followingByte == -1)
                    {
                        // we reached the end of the stream just after the CR, so leave the stream positioned at its end
                        return;
                    }
                    else if (followingByte == '\n')
                    {
                        // we saw '\r\n' and are now positioned at the first character after '\n'
                        return;
                    }
                    else
                    {
                        // we have moved one character too many; back up before returning so we are positioned on the first
                        // character after the '\r'
                        stream.Seek(-1, SeekOrigin.Current);
                    }
                }
            }
        }

        /// <summary>
        /// Deserialize all the lines in a stream, which is assumed to be positioned on the start of a line. Return the lines
        /// in batches
        /// </summary>
        /// <param name="stream">stream to deserialize</param>
        /// <param name="batchSize">number of lines to return per batch</param>
        /// <returns>sequence of batches of lines</returns>
        static private IEnumerable<ArraySegment<string>> Deserialize(Stream stream, int batchSize)
        {
            using (StreamReader reader = new StreamReader(stream, Encoding.UTF8, true, 1024*1024))
            {
                // count how much of the batch array we have filled
                int index;
                do
                {
                    // array to store the current batch
                    string[] batch = new string[batchSize];
                    for (index = 0; index < batchSize; ++index)
                    {
                        string line = reader.ReadLine();
                        if (line == null)
                        {
                            // we reached the end of the stream
                            break;
                        }
                        // fill in the next line in the batch
                        batch[index] = line;
                    }

                    if (index > 0)
                    {
                        // return all the lines that got filled in
                        yield return new ArraySegment<string>(batch, 0, index);
                    }
                    // if we didn't fill a complete batch then the stream ended, so exit
                } while (index == batchSize);
            }
        }

        /// <summary>
        /// create a worker for deserializing lines of text from an Hdfs file
        /// </summary>
        /// <param name="client">Hdfs client to use for reading data</param>
        /// <param name="batchSize">number of lines to return at a time</param>
        public DfsTextWorker(HdfsClientBase client, int batchSize)
            : base(
                // use 4k blocks when scanning past the end of the block to find the end of the final line
                4 * 1024,
                (item, stream) => SyncToNextLine(stream),
                (item, stream) => Deserialize(stream, batchSize),
                client)
        {
        }
    }
    #endregion
}
