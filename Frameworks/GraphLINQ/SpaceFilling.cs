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
using System.Text;
using System.Threading.Tasks;

using System.Linq.Expressions;

using Microsoft.Research.Naiad;
using Microsoft.Research.Naiad.Dataflow;
using Microsoft.Research.Naiad.Frameworks.Lindi;
using Microsoft.Research.Naiad.Dataflow.PartitionBy;
using Microsoft.Research.Naiad.Dataflow.StandardVertices;

namespace Microsoft.Research.Naiad.Frameworks.GraphLINQ
{

    /// <summary>
    /// Bellerophon is a graph processing system, where a n x n graph is broken into r x c tiles each of size (n / r) x (n / c).
    /// Matrix-vector multiplication is performed by breaking an n vector into c parts of size (n / c) and distributing to each tile, 
    /// multiplying, then aggregating the results. Within each tile, the graph is laid out according to the Hilbert space-filling curve.
    /// </summary>

    public static class SpaceFillingExtensionMethods
    {
        /// <summary>
        /// Reduces a collection of values along a supplied graph.
        /// </summary>
        /// <typeparam name="V">value type</typeparam>
        /// <typeparam name="T">time type</typeparam>
        /// <param name="values">value stream</param>
        /// <param name="edges">edge stream</param>
        /// <param name="aggregate">aggregation function</param>
        /// <param name="geometry">tiling geometry</param>
        /// <param name="isDefault">test for default values</param>
        /// <returns>stream of aggregates of the neighboring values of each node</returns>
        public static Stream<NodeValueBlock<V>, T> GraphReduce<V, T>(this Stream<NodeValueBlock<V>, T> values, Stream<Edge, T> edges, Func<V, V, V> aggregate, Geometry geometry, Func<V, bool> isDefault)
            where T : Time<T>
        {
            edges = edges.Select(x => new Edge(x.target, x.source));

            // the matrix will be broken into row x col parts, taking entry (i, j) => (i % row, j % col). 
            // We assume row * col = workers * processors.
            // Each process takes responsibility for tiles of shape (processRows, processCols), where processRows * processCols = workers.

            var workers = geometry.RowsPerProcess * geometry.ColsPerProcess;

            var processesPerCol = geometry.Rows / geometry.RowsPerProcess;  // seems backwards, but is correct.
            var processesPerRow = geometry.Cols / geometry.ColsPerProcess;  // seems backwards, but is correct.

            var process = new int[geometry.Rows, geometry.Cols];
            for (int i = 0; i < geometry.Rows; i++)
                for (int j = 0; j < geometry.Cols; j++)
                    process[i, j] = (i / geometry.RowsPerProcess) * processesPerRow + (j / geometry.ColsPerProcess);

            var worker = new int[geometry.Rows, geometry.Cols];
            for (int i = 0; i < geometry.Rows; i++)
                for (int j = 0; j < geometry.Cols; j++)
                    worker[i, j] = process[i, j] * workers + (i % geometry.RowsPerProcess) * geometry.ColsPerProcess + j % geometry.ColsPerProcess;

            //Func<int, int, int> intFunc = ((x, y) => shard[x, y]);
            //Expression<Func<NodeValueBlock<V>, int>> keyFunc = (y => shard[y.RowMod, y.ColMod]);

            return values.Select(y => new NodeValueBlockPacked<V>(y, isDefault))
                         .Select(y => new NodeValueBlockPacked<V>(y, 0, y.RowMod))    // first transpose the vector, routing row data to columns

                         // make one copy for each of the interested processes.
                         .SelectMany(y => Enumerable.Range(0, processesPerCol)
                                                    .Select(i => new NodeValueBlockPacked<V>(y, i * geometry.ColsPerProcess + y.Offset % geometry.ColsPerProcess, y.ColMod)))
                         .PartitionBy(y => worker[y.RowMod, y.ColMod])

                         // making one copy for each of the interested workers.
                         .SelectMany(y => Enumerable.Range(0, geometry.RowsPerProcess)
                                                    .Select(i => new NodeValueBlockPacked<V>(y, geometry.RowsPerProcess * (y.RowMod / geometry.RowsPerProcess) + i, y.ColMod)))
                         .PartitionBy(y => worker[y.RowMod, y.ColMod])             // should be a process-local exchange.

                         .Select(y => y.Unpack())
                         .GraphShuffle<V, T>(edges, aggregate, (x, y) => worker[x, y], y => worker[y.RowMod, y.ColMod], geometry)

                         // now shuffle the data within each processes so that each (rowmod, offset) is with a unique worker.
                         .Select(y => new NodeValueBlock<V>(y.RowMod, geometry.ColsPerProcess * (y.ColMod / geometry.ColsPerProcess) + y.Offset % geometry.ColsPerProcess, y.Offset, y.Data))
                         .PartitionBy(y => worker[y.RowMod, y.ColMod])
                         .FixedFanInAggregate(aggregate, geometry.Cols)

                         // now shuffle the data so that each each offset is with a unique worker (inter-process exchange)
                         .Select(y => new NodeValueBlock<V>(y.RowMod, y.Offset % geometry.Cols, y.Offset, y.Data))
                         .Select(y => new NodeValueBlockPacked<V>(y, isDefault))
                         .PartitionBy(y => worker[y.RowMod, y.ColMod])
                         .Select(y => y.Unpack())
                         .FixedFanInAggregate(aggregate, geometry.Cols / geometry.ColsPerProcess);
        }

        /// <summary>
        /// Reduces a collection of values along a supplied graph.
        /// </summary>
        /// <typeparam name="V">value type</typeparam>
        /// <typeparam name="T">time type</typeparam>
        /// <param name="values">value stream</param>
        /// <param name="edges">edge stream</param>
        /// <param name="aggregate">aggregation function</param>
        /// <param name="geometry">tiling geometry</param>
        /// <returns>stream of aggregates of the neighboring values of each node</returns>
        public static Stream<NodeValueBlock<V>, T> GraphReduce<V, T>(this Stream<NodeValueBlock<V>, T> values, Stream<Edge, T> edges, Func<V, V, V> aggregate, Geometry geometry)
            where T : Time<T>
        {
            // flip direction of edges
            edges = edges.Select(x => new Edge(x.target, x.source));

            // the matrix will be broken into row x col parts, taking entry (i, j) => (i % row, j % col). 
            // We assume row * col = workers * processors.
            // Each process takes responsibility for tiles of shape (processRows, processCols), where processRows * processCols = workers.

            var workers = geometry.RowsPerProcess * geometry.ColsPerProcess;

            var processesPerCol = geometry.Rows / geometry.RowsPerProcess;  // seems backwards, but is correct.
            var processesPerRow = geometry.Cols / geometry.ColsPerProcess;  // seems backwards, but is correct.

            var process = new int[geometry.Rows, geometry.Cols];
            for (int i = 0; i < geometry.Rows; i++)
                for (int j = 0; j < geometry.Cols; j++)
                    process[i, j] = (i / geometry.RowsPerProcess) * processesPerCol + (j / geometry.ColsPerProcess);

            var shard = new int[geometry.Rows, geometry.Cols];
            for (int i = 0; i < geometry.Rows; i++)
                for (int j = 0; j < geometry.Cols; j++)
                    shard[i, j] = process[i, j] * workers + (i % geometry.RowsPerProcess) * geometry.ColsPerProcess + j % geometry.ColsPerProcess;

            //Func<int, int, int> intFunc = ((x, y) => shard[x, y]);
            //Expression<Func<NodeValueBlock<V>, int>> keyFunc = (y => shard[y.RowMod, y.ColMod]);

            return values.Select(y => new NodeValueBlock<V>(y, 0, y.RowMod))    // first transpose the vector, routing row data to columns

                        // make one copy for each of the interested processes.
                        .SelectMany(y => Enumerable.Range(0, processesPerCol)
                                                   .Select(i => new NodeValueBlock<V>(y, i * geometry.ColsPerProcess + y.Offset % geometry.ColsPerProcess, y.ColMod)))
                        .PartitionBy(y => shard[y.RowMod, y.ColMod])
                // making one copy for each of the interested workers.
                        .SelectMany(y => Enumerable.Range(0, geometry.RowsPerProcess)
                                                   .Select(i => new NodeValueBlock<V>(y, geometry.RowsPerProcess * (y.RowMod / geometry.RowsPerProcess) + i, y.ColMod)))
                        .PartitionBy(y => shard[y.RowMod, y.ColMod])             // should be a process-local exchange.
                        .GraphShuffle<V, T>(edges, aggregate, (x, y) => shard[x, y], y => shard[y.RowMod, y.ColMod], geometry)
                // now shuffle the data within each processes so that each (rowmod, offset) is with a unique worker.
                        .Select(y => new NodeValueBlock<V>(y.RowMod, geometry.ColsPerProcess * (y.ColMod / geometry.ColsPerProcess) + y.Offset % geometry.ColsPerProcess, y.Offset, y.Data))
                        .PartitionBy(y => shard[y.RowMod, y.ColMod])
                        .FixedFanInAggregate(aggregate, geometry.Cols)
                // now shuffle the data so that each each offset is with a unique worker (inter-process exchange)
                        .Select(y => new NodeValueBlock<V>(y.RowMod, y.Offset % geometry.Cols, y.Offset, y.Data))
                        .PartitionBy(y => shard[y.RowMod, y.ColMod])
                        .FixedFanInAggregate(aggregate, geometry.Cols / geometry.ColsPerProcess);
        }

        /// <summary>
        /// Converts a sparse value stream to a dense tiled representation.
        /// </summary>
        /// <typeparam name="V">value type</typeparam>
        /// <typeparam name="T">time type</typeparam>
        /// <param name="values">value stream</param>
        /// <param name="geometry">tiling geometry</param>
        /// <returns>stream of dense tiles</returns>
        public static Stream<NodeValueBlock<V>, T> ConvertToVector<V, T>(this Stream<NodeWithValue<V>, T> values, Geometry geometry)
            where T : Time<T>
        {
            return values.GroupBy(x => new Pair<Int32, Int32>(x.node.index % geometry.Rows, (x.node.index / geometry.Rows) / geometry.BlockSize), (key, list) =>
            {
                var result = new NodeValueBlock<V>(key.First, 0, key.Second, new V[geometry.BlockSize]);
                foreach (var element in list)
                    result.Data[(element.node.index / geometry.Rows) % geometry.BlockSize] = element.value;

                return new[] { result };
            });

        }

        /// <summary>
        /// Joins a value stream against static per-node state.
        /// </summary>
        /// <typeparam name="V">value type</typeparam>
        /// <typeparam name="S">state type</typeparam>
        /// <typeparam name="T">time type</typeparam>
        /// <typeparam name="R">result type</typeparam>
        /// <param name="values">value stream</param>
        /// <param name="states">state stream</param>
        /// <param name="reducer">result selector</param>
        /// <returns>stream of results for each matching (value, state) pair.</returns>
        public static Stream<NodeValueBlock<R>, T> NodeJoin<V, S, T, R>(this Stream<NodeValueBlock<V>, T> values, Stream<NodeValueBlock<S>, T> states, Func<V, S, R> reducer)
            where T : Time<T>
        {
            return Foundry.NewBinaryStage(values, states, (i, v) => new ChunkedNodeJoinVertex<V, S, T, R>(i, v, reducer), x => x.Offset, x => x.Offset, x => x.Offset, "NodeJoin");
        }

        /// <summary>
        /// Aggregates values for each node, producing at most one aggregate per node per time.
        /// </summary>
        /// <typeparam name="V">value type</typeparam>
        /// <typeparam name="T">time type</typeparam>
        /// <param name="values">value stream</param>
        /// <param name="aggregator">aggregation function</param>
        /// <returns>stream of blocks of aggregates</returns>
        public static Stream<NodeValueBlock<V>, T> NodeAggregate<V, T>(this Stream<NodeValueBlock<V>, T> values, Func<V, V, V> aggregator)
            where T : Time<T>
        {
            return Foundry.NewUnaryStage(values, (i, v) => new ChunkedNodeAggregatorVertex<V, T>(i, v, aggregator), x => x.Offset, x => x.Offset, "NodeAggregator");
        }

        internal static Stream<NodeValueBlock<V>, T> GraphShuffle<V, T>(this Stream<NodeValueBlock<V>, T> values, Stream<Edge, T> edges, Func<V, V, V> aggregator, Func<int, int, int> intFunc, Expression<Func<NodeValueBlock<V>, int>> keyFunc, Geometry geometry)
            where T : Time<T>
        {
            return Foundry.NewBinaryStage(edges, values, (i, v) => new GraphReduceVertex<V, T>(i, v, aggregator, geometry.Rows, geometry.Cols, geometry), x => intFunc((x.source.index % geometry.Rows), (x.target.index % geometry.Cols)), keyFunc, null, "GraphShuffle");
        }

        internal static Stream<NodeValueBlock<V>, T> FixedFanInAggregate<V, T>(this Stream<NodeValueBlock<V>, T> values, Func<V, V, V> aggregator, int fanIn)
            where T : Time<T>
        {
            return UnaryVertex<NodeValueBlock<V>, NodeValueBlock<V>, T>.MakeStage(values, (i, v) => new AggregatorVertex<V, T>(i, v, aggregator, fanIn), null, null, "Aggregator");
        }
    }

    /// <summary>
    /// Description of the tiling geometry of the graph
    /// </summary>
    public struct Geometry
    {
        /// <summary>
        /// The number of rows at each process
        /// </summary>
        public int RowsPerProcess;
        /// <summary>
        /// The number of columns at each process
        /// </summary>
        public int ColsPerProcess;
        /// <summary>
        /// The number of rows in the graph
        /// </summary>
        public int Rows;
        /// <summary>
        /// The number of columns in the graph
        /// </summary>
        public int Cols;
        /// <summary>
        /// The size of a block within a tile
        /// </summary>
        public int BlockSize;

        /// <summary>
        /// Constructor for a graph geometry
        /// </summary>
        /// <param name="r">Number of rows in the graph</param>
        /// <param name="c">Number of columns in the graph</param>
        /// <param name="rpp">Number of rows per process</param>
        /// <param name="cpp">Number of columns per process</param>
        /// <param name="b">Block size within a tile</param>
        public Geometry(int r, int c, int rpp, int cpp, int b) { Rows = r; Cols = c; RowsPerProcess = rpp; ColsPerProcess = cpp; BlockSize = b; }
    }

    /// <summary>
    /// Block within a tile
    /// </summary>
    /// <typeparam name="V">Type of data value</typeparam>
    public struct NodeValueBlock<V>
    {
        /// <summary>
        /// to which column modulus does the subvector belong?
        /// </summary>
        public int ColMod;
        /// <summary>
        /// to which row modulus does the subvector belong?
        /// </summary>
        public int RowMod;
        /// <summary>
        /// within that modulus, what is its chunk offset?
        /// </summary>
        public int Offset;
        /// <summary>
        /// data of the subvector
        /// </summary>
        public V[] Data;

        /// <summary>
        /// Convert the block to a string of values
        /// </summary>
        /// <returns>String of the data in the block</returns>
        public override string ToString()
        {
            return string.Join("\t", this.Data.Select(x => x.ToString()));
        }

        /// <summary>
        /// Constructor for a block of data
        /// </summary>
        /// <param name="rm">row modulus the block belongs to</param>
        /// <param name="cm">column modulus the block belongs to</param>
        /// <param name="o">offset within the modulus</param>
        /// <param name="ds">data values</param>
        public NodeValueBlock(int rm, int cm, int o, V[] ds) { RowMod = rm; ColMod = cm; Offset = o; Data = ds; }
        
        /// <summary>
        /// Constructor reusing a block of data for a different modulus
        /// </summary>
        /// <param name="source">Block of data to reuse</param>
        /// <param name="newRowMod">Row modulus the new block belongs to</param>
        /// <param name="newColMod">Column modulus the new block belongs to</param>
        public NodeValueBlock(NodeValueBlock<V> source, int newRowMod, int newColMod) { this.RowMod = newRowMod; this.ColMod = newColMod; this.Offset = source.Offset; this.Data = source.Data; }
    }

    /// <summary>
    /// Block within a tile represented as a sparse array
    /// </summary>
    /// <typeparam name="V">Type of data value</typeparam>
    public struct NodeValueBlockPacked<V>
    {
        /// <summary>
        /// to which row modulus does the subvector belong?
        /// </summary>
        public int RowMod;
        /// <summary>
        /// to which column modulus does the subvector belong?
        /// </summary>
        public int ColMod;
        /// <summary>
        /// within that modulus, what is its chunk offset?
        /// </summary>
        public int Offset;

        /// <summary>
        /// the data values that are not the default value
        /// </summary>
        public V[] PackedData;
        /// <summary>
        /// bitmask indicating location of non-default values
        /// </summary>
        public byte[] NonDefault;
        /// <summary>
        /// number of unpacked elements this block represents (currently 1024)
        /// </summary>
        public int Length;

        /// <summary>
        /// turn the sparse block into a dense block
        /// </summary>
        /// <returns>dense block containing the same data as this sparse block</returns>
        public NodeValueBlock<V> Unpack()
        {
            var result = new NodeValueBlock<V>(this.RowMod, this.ColMod, this.Offset, new V[this.Length]);
            var counter = 0;
            for (int i = 0; i < this.Length; i++)
                if ((this.NonDefault[i / 8] & (byte)(1 << i % 8)) != 0)
                    result.Data[i] = this.PackedData[counter++];

            return result;
        }

        /// <summary>
        /// construct a sparse block of data
        /// </summary>
        /// <param name="source">dense block source of the data</param>
        /// <param name="isDefault">function indicating whether a given value should be considered default or not</param>
        public NodeValueBlockPacked(NodeValueBlock<V> source, Func<V, bool> isDefault)
        {
            this.RowMod = source.RowMod;
            this.ColMod = source.ColMod;
            this.Offset = source.Offset;

            this.NonDefault = new byte[source.Data.Length / 8]; // will blow up if not cleanly divisible by 8

            var counter = 0;
            for (int i = 0; i < source.Data.Length; i++)
            {
                if (!isDefault(source.Data[i]))
                {
                    this.NonDefault[i / 8] |= (byte)(1 << i % 8);
                    counter++;
                }
            }

            this.PackedData = new V[counter];
            counter = 0;
            for (int i = 0; i < source.Data.Length; i++)
                if (!isDefault(source.Data[i]))
                    this.PackedData[counter++] = source.Data[i];

            this.Length = source.Data.Length;
        }

        /// <summary>
        /// construct a sparse block of data from an existing sparse block
        /// </summary>
        /// <param name="source">existing sparse block of data</param>
        /// <param name="newRowMod">new row modulus to use</param>
        /// <param name="newColMod">new column modulus to use</param>
        public NodeValueBlockPacked(NodeValueBlockPacked<V> source, int newRowMod, int newColMod)
        {
            this.RowMod = newRowMod;
            this.ColMod = newColMod;

            this.Offset = source.Offset;
            this.PackedData = source.PackedData;
            this.NonDefault = source.NonDefault;
            this.Length = source.Length;
        }
    }

    internal class GraphReduceVertex<V, T> : BinaryVertex<Edge, NodeValueBlock<V>, NodeValueBlock<V>, T>
        where T : Time<T>
    {
        Geometry Geometry;
        List<Pair<Int32, Int32>> EdgeList;
        Pair<Int32, Int32>[] Edges;

        V[] Source;
        V[] Target;

        Func<V, V, V> Accumulator;

        int rowParts;
        int colParts;

        int rowMod;
        int colMod;

        // received another edge
        public override void OnReceive1(Naiad.Dataflow.Message<Edge, T> message)
        {
            for (int i = 0; i < message.length; i++)
            {
                var record = message.payload[i];
                EdgeList.Add(new Pair<Int32, Int32>(record.source.index / rowParts, record.target.index / colParts));

                rowMod = record.source.index % rowParts;
                colMod = record.target.index % colParts;
            }
        }

        // received a chunk of data to use for multiplication
        public override void OnReceive2(Naiad.Dataflow.Message<NodeValueBlock<V>, T> message)
        {
            for (int i = 0; i < message.length; i++)
            {
                var record = message.payload[i];

                if ((this.rowMod != -1 && this.rowMod != record.RowMod) || (this.colMod != -1 && this.colMod != record.ColMod))
                    Console.Error.WriteLine("Apparent mis-routing of VectorData ({0} {1}) v ({2} {3}) in shard {4}", this.rowMod, this.colMod, record.RowMod, record.ColMod, this.VertexId);

                if (record.Offset * record.Data.Length + record.Data.Length >= Source.Length)
                {
                    var temp = Source;
                    Source = new V[Math.Max(record.Offset * record.Data.Length + record.Data.Length + 1, 2 * Source.Length)];
                    for (int j = 0; j < temp.Length; j++)
                        Source[j] = temp[j];
                }

                for (int j = 0; j < record.Data.Length; j++)
                    Source[record.Offset * record.Data.Length + j] = record.Data[j];
            }

            this.NotifyAt(message.time);
        }

        public override void OnNotify(T time)
        {
            if (this.rowMod == -1 || this.colMod == -1)
                Console.Error.WriteLine("Shard {0} has no edges; improbable", this.VertexId);
            else
            {
                if (Edges == null)
                {
                    // re-arrange the graph, if inclined.
                    //Console.Error.WriteLine("Laying out space-filling curves");

                    Edges = EdgeList.ToArray();

                    var optimize = true;
                    if (optimize)
                    {
                        var keys = Edges.Select(x => HilbertCurve.xy2d(30, x.First, x.Second)).ToArray();
                        Array.Sort(keys, Edges);
                    }

                    var maxRow = 0;
                    for (int i = 0; i < Edges.Length; i++)
                        if (maxRow < Edges[i].First)
                            maxRow = Edges[i].First;

                    var maxCol = 0;
                    for (int i = 0; i < Edges.Length; i++)
                        if (maxCol < Edges[i].Second)
                            maxCol = Edges[i].Second;

                    EdgeList = null;

                    this.Target = new V[maxRow + 1];

                    if (this.Source.Length < maxCol + 1)
                    {
                        var temp = this.Source;
                        this.Source = new V[maxCol + 1];
                        for (int i = 0; i < temp.Length; i++)
                            this.Source[i] = temp[i];
                    }
                }

                for (int i = 0; i < Edges.Length; i++)
                    Target[Edges[i].First] = Accumulator(Target[Edges[i].First], Source[Edges[i].Second]);

                var output = this.Output.GetBufferForTime(time);

                for (int i = 0; i < this.Target.Length; i += Geometry.BlockSize)
                {
                    var toSend = new NodeValueBlock<V>(this.rowMod, this.colMod, i / Geometry.BlockSize, new V[Geometry.BlockSize]);
                    for (int j = 0; i + j < this.Target.Length && j < toSend.Data.Length; j++)
                    {
                        toSend.Data[j] = this.Target[i + j];
                        this.Target[i + j] = default(V);
                    }

                    output.Send(toSend);
                }
            }
        }

        // implemented from wikipedia's hilbert curve description.
        // probably better to do byte-at-a-time, or somesuch.
        public static class HilbertCurve
        {
            // convert (x,y) to d
            public static Int64 xy2d(int logn, int x, int y)
            {
                Int64 d = 0;
                for (var logs = logn - 1; logs >= 0; logs--)
                {
                    int rx = (x >> logs) & 1;
                    int ry = (y >> logs) & 1;

                    d += (1L << (2 * logs)) * ((3 * rx) ^ ry);
                    rot(logs, ref x, ref y, rx, ry);
                }
                return d;
            }

            // convert d to (x,y)
            public static void d2xy(int logn, Int64 d, ref int x, ref int y)
            {
                int logs;
                var t = d;

                x = y = 0;
                for (logs = 0; logs < logn; logs++)
                {
                    var rx = (1 & (t / 2)) == 1 ? 1 : 0;
                    var ry = (1 & (t ^ rx)) == 1 ? 1 : 0;
                    rot(logs, ref x, ref y, rx, ry);
                    x += (1 << logs) * rx;
                    y += (1 << logs) * ry;
                    t /= 4;
                }
            }

            // rotate/flip a quadrant appropriately
            public static void rot(int logs, ref int x, ref int y, int rx, int ry)
            {
                if (ry == 0)
                {
                    if (rx == 1)
                    {
                        x = ((1 << logs) - 1 - x);
                        y = ((1 << logs) - 1 - y);
                    }

                    //Swap x and y
                    int t = x;
                    x = y;
                    y = t;
                }
            }
        }

        public GraphReduceVertex(int index, Naiad.Dataflow.Stage<T> vertex, Func<V, V, V> aggregator, int rowParts, int colParts, Geometry geometry)
            : base(index, vertex)
        {
            this.Accumulator = aggregator;
            this.EdgeList = new List<Pair<int, int>>();
            this.Source = new V[] { };

            this.rowParts = rowParts;
            this.colParts = colParts;

            this.rowMod = -1;
            this.colMod = -1;

            this.Geometry = geometry;
        }
    }

    internal class AggregatorVertex<V, T> : UnaryVertex<NodeValueBlock<V>, NodeValueBlock<V>, T>
         where T : Time<T>
    {
        private Dictionary<Pair<int, int>, NodeValueBlock<V>> Values;
        private Dictionary<Pair<int, int>, int> Counts;

        private readonly Func<V, V, V> Aggregator;
        private readonly int Expected;

        public override void OnReceive(Naiad.Dataflow.Message<NodeValueBlock<V>, T> message)
        {
            for (int i = 0; i < message.length; i++)
            {
                var record = message.payload[i];

                var key = new Pair<Int32, Int32>(record.RowMod, record.Offset);
                if (!Values.ContainsKey(key))
                {
                    Values.Add(key, new NodeValueBlock<V>(record.RowMod, record.ColMod, record.Offset, new V[record.Data.Length]));
                    Counts.Add(key, 0);
                }

                var chunk = Values[key];
                for (int j = 0; j < record.Data.Length; j++)
                    chunk.Data[j] = Aggregator(chunk.Data[j], record.Data[j]);

                Counts[key] += 1;
                if (Counts[key] == this.Expected)
                {
                    this.Output.GetBufferForTime(message.time).Send(Values[key]);
                    this.Values.Remove(key);
                    this.Counts.Remove(key);
                }
            }

            this.NotifyAt(message.time);
        }

        public override void OnNotify(T time)
        {
            var output = this.Output.GetBufferForTime(time);

            foreach (var value in this.Values.Values)
                output.Send(value);

            this.Values.Clear();
            this.Counts.Clear();
        }

        public AggregatorVertex(int index, Stage<T> vertex, Func<V, V, V> aggregator, int expectedAggregands)
            : base(index, vertex)
        {
            this.Aggregator = aggregator;
            this.Values = new Dictionary<Pair<int, int>, NodeValueBlock<V>>();
            this.Counts = new Dictionary<Pair<int, int>, int>();
            this.Expected = expectedAggregands;
        }
    }

    internal class ChunkedNodeJoinVertex<V, S, T, R> : BinaryVertex<NodeValueBlock<V>, NodeValueBlock<S >, NodeValueBlock<R>, T>
        where T : Time<T>
    {
        private readonly Dictionary<Pair<int, int>, NodeValueBlock<S>> Scales = new Dictionary<Pair<int,int>,NodeValueBlock<S>>();
        private readonly Func<V, S, R> Reducer;

        private List<NodeValueBlock<V>> ToProcess = new List<NodeValueBlock<V>>();

        public override void OnReceive1(Naiad.Dataflow.Message<NodeValueBlock<V>, T> message)
        {
            var output = this.Output.GetBufferForTime(message.time);
            for (int i = 0; i < message.length; i++)
            {
                var record = message.payload[i];

                var key = new Pair<Int32, Int32>(record.RowMod, record.Offset);
                if (Scales.ContainsKey(key))
                {
                    var scales = Scales[key];

                    var result = new NodeValueBlock<R>(key.First, key.Second, record.Offset, new R[record.Data.Length]);
                    for (int j = 0; j < result.Data.Length; j++)
                        result.Data[j] = this.Reducer(record.Data[j], scales.Data[j]);

                    output.Send(result);
                }
                else if (this.ToProcess != null)
                {
                    this.ToProcess.Add(record);
                    this.NotifyAt(message.time);
                }
                else
                    Console.Error.WriteLine("Apparently missing a chunk of degrees; probably a bug");
            }
        }

        public override void OnReceive2(Message<NodeValueBlock<S>, T> message)
        {
            for (int i = 0; i < message.length; i++)
            {
                var record = message.payload[i];

                var key = new Pair<Int32, Int32>(record.RowMod, record.Offset);
                if (!Scales.ContainsKey(key))
                    Scales.Add(key, record);
            }
        }

        public override void OnNotify(T time)
        {
            var output = this.Output.GetBufferForTime(time);
            foreach (var record in this.ToProcess)
            { 
                var key = new Pair<Int32, Int32>(record.RowMod, record.Offset);
                if (Scales.ContainsKey(key))
                {
                    var scales = Scales[key];

                    var result = new NodeValueBlock<R>(key.First, key.Second, record.Offset, new R[record.Data.Length]);
                    for (int j = 0; j < result.Data.Length; j++)
                        result.Data[j] = this.Reducer(record.Data[j], scales.Data[j]);

                    output.Send(result);
                }
            }

            this.ToProcess = null;
        }

        public ChunkedNodeJoinVertex(int index, Stage<T> vertex, Func<V, S, R> reducer)
            : base(index, vertex)
        {
            this.Reducer = reducer;
        }
    }

    // a blocking aggregation vertex, reporting the aggregate value for any received keys.
    internal class ChunkedNodeAggregatorVertex<V, T> : UnaryVertex<NodeValueBlock<V>, NodeValueBlock<V>, T>
        where T : Time<T>
    {
        private readonly Dictionary<T, Dictionary<Int32, NodeValueBlock<V>>> Values;

        private readonly Func<V, V, V> Update; // value aggregator

        public override void OnReceive(Message<NodeValueBlock<V>, T> message)
        {
            if (!this.Values.ContainsKey(message.time))
            {
                this.Values.Add(message.time, new Dictionary<int, NodeValueBlock<V>>());
                this.NotifyAt(message.time);
            }

            var dictionary = this.Values[message.time];

            for (int i = 0; i < message.length; i++)
            {
                var record = message.payload[i];
                if (!dictionary.ContainsKey(record.Offset))
                    dictionary.Add(record.Offset, new NodeValueBlock<V>(record.RowMod, record.ColMod, record.Offset, record.Data.ToArray()));
                else
                {
                    var data = dictionary[record.Offset].Data;
                    for (int j = 0; j < record.Data.Length; j++)
                        data[j] = this.Update(record.Data[j], data[j]);
                }
            }
        }

        public override void OnNotify(T time)
        {
            // make sure to fold in any loose values lying around
            if (this.Values.ContainsKey(time))
            {
                var output = this.Output.GetBufferForTime(time);
                foreach (var pair in this.Values[time])
                    output.Send(pair.Value);

                this.Values.Remove(time);
            }
        }

        // partitioned indicates that the data have been partitioned and we can divide out the number of parts without loosing accuracy.
        public ChunkedNodeAggregatorVertex(int index, Stage<T> stage, Func<V, V, V> aggregate)
            : base(index, stage)
        {
            this.Values = new Dictionary<T, Dictionary<int, NodeValueBlock<V>>>();
            this.Update = aggregate;
        }
    }
}
