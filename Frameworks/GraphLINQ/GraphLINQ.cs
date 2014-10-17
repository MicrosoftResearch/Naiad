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
using System.Linq;

using Microsoft.Research.Naiad.Dataflow;
using Microsoft.Research.Naiad.Dataflow.Iteration;
using Microsoft.Research.Naiad.Dataflow.StandardVertices;
using Microsoft.Research.Naiad.Dataflow.PartitionBy;

using Microsoft.Research.Naiad.Frameworks.Lindi;

namespace Microsoft.Research.Naiad.Frameworks.GraphLINQ
{
    /// <summary>
    /// The GraphLINQ framework contains data types and extension methods that add optimized graph-specific
    /// operators for streams.
    /// </summary>
    /// <remarks>
    /// The GraphLINQ operators process static graphs. To perform incremental graph computations,
    /// use the <see cref="N:Microsoft.Research.Naiad.Frameworks.DifferentialDataflow"/> framework instead.
    /// 
    /// The GraphLINQ framework contains only graph-specific operators, and relies on other frameworks, such as 
    /// <see cref="N:Microsoft.Research.Naiad.Frameworks.Lindi"/> to perform extract-transform-load and summarization
    /// tasks.
    /// </remarks>
    class NamespaceDoc
    { }

    #region graph data types

    /// <summary>
    /// Represents a node (vertex) in a graph.
    /// </summary>
    public struct Node : IEquatable<Node>
    {
        /// <summary>
        /// The unique index of the node.
        /// </summary>
        public int index;

        /// <summary>
        /// Implicitly extracts the integer index of a node
        /// </summary>
        /// <param name="node">The node.</param>
        /// <returns>The unique index of the node.</returns>
        public static implicit operator int(Node node) { return node.index; }

        /// <summary>
        /// Creates a <see cref="NodeWithValue{TValue}"/> with this node and the given <paramref name="value"/>.
        /// </summary>
        /// <typeparam name="TValue">The value type.</typeparam>
        /// <param name="value">The value.</param>
        /// <returns>A <see cref="NodeWithValue{TValue}"/> based on this node and the given <paramref name="value"/>.</returns>
        public NodeWithValue<TValue> WithValue<TValue>(TValue value) { return new NodeWithValue<TValue>(this, value); }

        /// <summary>
        /// Constructs a node with the given index.
        /// </summary>
        /// <param name="index">The unique index of the node.</param>
        public Node(int index) { this.index = index; }

        /// <summary>
        /// Returns <c>true</c> if this node and the <paramref name="other"/> node have the same index.
        /// </summary>
        /// <param name="other">The other node.</param>
        /// <returns><c>true</c> if and only if the nodes have the same index.</returns>
        public bool Equals(Node other)
        {
            return this.index == other.index;
        }

        /// <summary>
        /// GetHashCode override
        /// </summary>
        /// <returns>the node index</returns>
        public override int GetHashCode()
        {
            return this.index;
        }

        /// <summary>
        /// Returns a string representation of this node.
        /// </summary>
        /// <returns>A string representation of this node.</returns>
        public override string ToString()
        {
            return string.Format("Node({0})", this.index);
        }
    }

    /// <summary>
    /// Represents an edge in a graph as a pair of <see cref="Node"/>s.
    /// </summary>
    public struct Edge : IEquatable<Edge>
    {
        /// <summary>
        /// The source <see cref="Node"/> of this edge.
        /// </summary>
        public Node source;

        /// <summary>
        /// The target <see cref="Node"/> of the edge.
        /// </summary>
        public Node target;

        /// <summary>
        /// Returns a string representation of this edge.
        /// </summary>
        /// <returns>A string representation of this edge.</returns>
        public override string ToString()
        {
            return string.Format("Edge({0}, {1})", this.source.index, this.target.index);
        }

        /// <summary>
        /// Constructs an edge from two <see cref="Node"/> objects.
        /// </summary>
        /// <param name="source">The source node.</param>
        /// <param name="target">The target node.</param>
        public Edge(Node source, Node target) { this.source = source; this.target = target; }

        /// <summary>
        /// Returns the hash code for this edge.
        /// </summary>
        /// <returns>The hash code for this edge.</returns>
        public override int GetHashCode()
        {
            return source.GetHashCode() + 37 * target.GetHashCode();
        }

        /// <summary>
        /// Returns <c>true</c> if this edge and the <paramref name="other"/> node have the same source and target.
        /// </summary>
        /// <param name="other">The other edge.</param>
        /// <returns><c>true</c> if this edge and the <paramref name="other"/> node have the same source and target.</returns>
        public bool Equals(Edge other)
        {
            return this.source.Equals(other.source) && this.target.Equals(other.target);
        }
    }

    /// <summary>
    /// Represents a <see cref="Node"/> that is associated with a value.
    /// </summary>
    /// <typeparam name="TValue">The value type.</typeparam>
    public struct NodeWithValue<TValue>
    {
        /// <summary>
        /// The node.
        /// </summary>
        public Node node;

        /// <summary>
        /// The value.
        /// </summary>
        public TValue value;

        /// <summary>
        /// GetHashCode override
        /// </summary>
        /// <returns>node index plus value hash code</returns>
        public override int GetHashCode()
        {
            return node.index + value.GetHashCode();
        }

        /// <summary>
        /// Constructs a node-value pair from the given <paramref name="node"/> and <paramref name="value"/>.
        /// </summary>
        /// <param name="node">The node.</param>
        /// <param name="value">The value.</param>
        public NodeWithValue(Node node, TValue value) { this.node = node; this.value = value; }
    }

    /// <summary>
    /// Represents an <see cref="Edge"/> that is associated with a value.
    /// </summary>
    /// <typeparam name="TValue">The value type.</typeparam>
    public struct EdgeWithValue<TValue>
    {
        /// <summary>
        /// The edge.
        /// </summary>
        public Edge edge;

        /// <summary>
        /// The value.
        /// </summary>
        public TValue value;

        /// <summary>
        /// GetHashCode override
        /// </summary>
        /// <returns>edge hashcode plus value hashcode</returns>
        public override int GetHashCode()
        {
            return edge.GetHashCode() + value.GetHashCode();
        }

        /// <summary>
        /// Constructs an edge-value pair from the given <paramref name="edge"/> and <paramref name="value"/>.
        /// </summary>
        /// <param name="edge">The edge.</param>
        /// <param name="value">The value.</param>
        public EdgeWithValue(Edge edge, TValue value) { this.edge = edge; this.value = value; }
    }

    #endregion

    /// <summary>
    /// The GraphLINQ operators are implemented as extension methods on <see cref="Stream{TRecord,TTime}"/> objects.
    /// </summary>
    public static class ExtensionMethods
    {
        #region Methods relating to renaming nodes and edges from streams of other data types.

        /// <summary>
        /// Computes a mapping from unique identifiers in the input stream to mostly dense <see cref="Node"/> indices. 
        /// Indices are dense on each worker, but skew among workers can lead to gaps in the tail of the indices.
        /// </summary>
        /// <typeparam name="TIdentifier">The type of the identifiers.</typeparam>
        /// <param name="identifiers">A stream of (not necessarily distinct) identifiers. All identifiers should be in the first epoch.</param>
        /// <returns>The stream of densely-named nodes each associated with its initial identifier.</returns>
        /// <remarks>The <see cref="RenameEdges{TIdentifier}"/> and <see cref="RenameNodes{TIdentifier}"/> operators can use the results of this operator to rewrite
        /// graph edges and nodes in a more compact representation.</remarks>
        /// <seealso cref="RenameEdges{TIdentifier}"/>
        /// <seealso cref="RenameNodes{TIdentifier}"/>
        public static Stream<NodeWithValue<TIdentifier>, Epoch> GenerateDenseNameMapping<TIdentifier>(this Stream<TIdentifier, Epoch> identifiers)
        {
            if (identifiers == null) throw new ArgumentNullException("identifiers");
            return identifiers.NewUnaryStage((i, v) => new Densifier<TIdentifier>(i, v), x => x.GetHashCode(), x => x.value.GetHashCode(), "GenerateDenseNameMapping");
        }

        /// <summary>
        /// Rewrites the pairs of arbitrary identifiers in <paramref name="edges"/> according to the given <paramref name="renameMapping"/>.
        /// </summary>
        /// <typeparam name="TIdentifier">The type of the identifiers.</typeparam>
        /// <param name="edges">A stream edges named by pairs of identifiers. All pairs should be in the first epoch.</param>
        /// <param name="renameMapping">A mapping from <typeparamref name="TIdentifier"/> values to <see cref="Node"/> objects.</param>
        /// <returns>A stream of <see cref="Edge"/>s corresponding to the given input <paramref name="edges"/> and <paramref name="renameMapping"/>.</returns>
        /// <remarks>
        /// The <paramref name="renameMapping"/> typically uses the output of <see cref="GenerateDenseNameMapping{TIdentifier}"/>.
        /// 
        /// An edge will appear in the output if and only if both endpoints of the edge exist in the <paramref name="renameMapping"/>.
        /// </remarks>
        /// <seealso cref="GenerateDenseNameMapping{TIdentifier}"/>
        /// <seealso cref="RenameNodes{TIdentifier}"/>
        public static Stream<Edge, Epoch> RenameEdges<TIdentifier>(this Stream<Pair<TIdentifier, TIdentifier>, Epoch> edges, Stream<NodeWithValue<TIdentifier>, Epoch> renameMapping)
        {
            if (edges == null) throw new ArgumentNullException("edges");
            if (renameMapping == null) throw new ArgumentNullException("renameMapping");
            // produces a stream of (node1, node2) edge pairs from a stream of (oldname1, oldname2) pairs.
            return edges.RenameUsing(renameMapping, x => x.First).Select(x => x.node.WithValue(x.value.Second))
                        .RenameUsing(renameMapping, x => x.value).Select(x => new Edge(x.value.node, x.node));
        }

        /// <summary>
        /// Rewrites the arbitrary identifiers in <paramref name="nodes"/> according to the given <paramref name="renameMapping"/>.
        /// </summary>
        /// <typeparam name="TIdentifier">The type of the identifiers.</typeparam>
        /// <param name="nodes">A stream of nodes named by identifiers. All identifiers should be in the first epoch.</param>
        /// <param name="renameMapping">A mapping from <typeparamref name="TIdentifier"/> values to <see cref="Node"/> objects.</param>
        /// <returns>A stream of <see cref="Node"/>s corresponding to the given input <paramref name="nodes"/> and <paramref name="renameMapping"/>.</returns>
        /// <remarks>
        /// The <paramref name="renameMapping"/> typically uses the output of <see cref="GenerateDenseNameMapping{TIdentifier}"/>.
        /// 
        /// A node will appear in the output if and only if the identifier exists in the <paramref name="renameMapping"/>.
        /// </remarks>
        /// <seealso cref="GenerateDenseNameMapping{TIdentifier}"/>
        /// <seealso cref="RenameEdges{TIdentifier}"/>
        public static Stream<Node, Epoch> RenameNodes<TIdentifier>(this Stream<TIdentifier, Epoch> nodes, Stream<NodeWithValue<TIdentifier>, Epoch> renameMapping)
        {
            if (nodes == null) throw new ArgumentNullException("nodes");
            if (renameMapping == null) throw new ArgumentNullException("renameMapping");
            // produces a stream of nodes from a stream of identifiers.
            return nodes.RenameUsing(renameMapping, x => x).Select(x => x.node);
        }

        /// <summary>
        /// Associates elements of the given <paramref name="stream"/> with nodes selected by the given <paramref name="identifierSelector"/> and <paramref name="renameMapping"/>.
        /// </summary>
        /// <typeparam name="TInput">The type of the input records.</typeparam>
        /// <typeparam name="TIdentifier">The type of the identifiers.</typeparam>
        /// <param name="stream">The input stream. All elements should be in the first epoch.</param>
        /// <param name="renameMapping">A mapping from <typeparamref name="TIdentifier"/> values to <see cref="Node"/> objects.</param>
        /// <param name="identifierSelector">Function from input record to identifier, which associates an input record with a node.</param>
        /// <returns>A stream of nodes each associated with its respective input record.</returns>
        public static Stream<NodeWithValue<TInput>, Epoch> RenameUsing<TInput, TIdentifier>(this Stream<TInput, Epoch> stream, Stream<NodeWithValue<TIdentifier>, Epoch> renameMapping, Func<TInput, TIdentifier> identifierSelector)
        {
            if (stream == null) throw new ArgumentNullException("stream");
            if (renameMapping == null) throw new ArgumentNullException("renameMapping");
            if (identifierSelector == null) throw new ArgumentNullException("identifierSelector");
            return stream.NewBinaryStage(renameMapping, (i, v) => new RenamerVertex<TInput, TIdentifier>(i, v, identifierSelector), x => identifierSelector(x).GetHashCode(), x => x.value.GetHashCode(), null, "RenameUsing");
        }

        /// <summary>
        /// Uses an AutoRenamer to create and attach names to records in an input stream based on identifiers producer by an identifierSelector function.
        /// </summary>
        /// <typeparam name="TInput">Input record type</typeparam>
        /// <typeparam name="TIdentifier">Identifier type</typeparam>
        /// <param name="stream">The input stream</param>
        /// <param name="renamer">The AutoRenamer</param>
        /// <param name="identifierSelector">Function from input record to identifier</param>
        /// <returns>A stream of pairs of input record and the node corresponding to the record's identifier</returns>
        public static Stream<NodeWithValue<TInput>, IterationIn<Epoch>> RenameUsing<TInput, TIdentifier>(this Stream<TInput, IterationIn<Epoch>> stream, AutoRenamer<TIdentifier> renamer, Func<TInput, TIdentifier> identifierSelector)
        {
            if (stream == null) throw new ArgumentNullException("stream");
            if (renamer == null) throw new ArgumentNullException("renamer");
            if (identifierSelector == null) throw new ArgumentNullException("identifierSelector");
            return renamer.AddRenameTask(stream, identifierSelector);
        }

        /// <summary>
        /// Uses an AutoRenamer to create and attach names to records in an input stream based on identifiers producer by an identifierSelector function.
        /// </summary>
        /// <typeparam name="TInput">Input record type</typeparam>
        /// <typeparam name="TIdentifier">Identifier type</typeparam>
        /// <param name="stream">The input stream</param>
        /// <param name="renamer">The AutoRenamer</param>
        /// <param name="identifierSelector">Function from input record to identifier</param>
        /// <returns>A stream of pairs of input record and the node corresponding to the record's identifier</returns>
        public static Stream<NodeWithValue<TInput>, IterationIn<Epoch>> RenameUsing<TInput, TIdentifier>(this Stream<TInput, Epoch> stream, AutoRenamer<TIdentifier> renamer, Func<TInput, TIdentifier> identifierSelector)
        {
            if (stream == null) throw new ArgumentNullException("stream");
            if (renamer == null) throw new ArgumentNullException("renamer");
            if (identifierSelector == null) throw new ArgumentNullException("identifierSelector");
            if (renamer.Context == null)
                renamer.InitializeContext(stream.Context);

            return renamer.AddRenameTask(renamer.Context.EnterLoop(stream), identifierSelector);
        }

        /// <summary>
        /// Extracts a stream from an AutoRenaming context. Once called, no further RenameUsing invocations may be used.
        /// </summary>
        /// <typeparam name="TRecord">Record type</typeparam>
        /// <typeparam name="TIdentifier">Identifier type</typeparam>
        /// <param name="stream">The stream of records</param>
        /// <param name="renamer">The AutoRenamer</param>
        /// <returns>The same stream of records, outside of the renaming context.</returns>
        public static Stream<TRecord, Epoch> FinishRenaming<TRecord, TIdentifier>(this Stream<TRecord, IterationIn<Epoch>> stream, AutoRenamer<TIdentifier> renamer)
        {
            if (stream == null) throw new ArgumentNullException("stream");
            if (renamer == null) throw new ArgumentNullException("renamer");
            return renamer.Context.ExitLoop(stream);
        }

        #endregion

        #region Methods relating to exchanging data across the edges in a graph.

        /// <summary>
        /// Transmits the value associated with each node in <paramref name="nodes"/> along the matching edges in <paramref name="edges"/>
        /// to produce a stream of <see cref="NodeWithValue{TValue}"/>s for each target of a matching edge.
        /// </summary>
        /// <typeparam name="TValue">The type of value associated with each node.</typeparam>
        /// <typeparam name="TTime">The type of timestamp on each record.</typeparam>
        /// <param name="nodes">The stream of nodes with values.</param>
        /// <param name="edges">The stream of edges.</param>
        /// <returns>The stream of targets with values for each edge in <paramref name="edges"/>.</returns>
        /// <remarks>
        /// This operator may produce several values for a particular target node. The <see cref="GraphReduce{TValue,TTime}"/> operator
        /// produces a unique value for each target node.
        /// </remarks>
        public static Stream<NodeWithValue<TValue>, TTime> TransmitAlong<TValue, TTime>(this Stream<NodeWithValue<TValue>, TTime> nodes, Stream<Edge, TTime> edges)
            where TTime : Time<TTime>
        {
            if (nodes == null) throw new ArgumentNullException("nodes");
            if (edges == null) throw new ArgumentNullException("edges");
            var compacted = edges.Compact();    // could be cached and retrieved as needed

            return compacted.NewBinaryStage(nodes, (i, s) => new GraphJoinVertex<TValue, TTime>(i, s), null, x => x.node.index, null, "TransmitAlong");
        }


        /// <summary>
        /// Transmits the value associated with each node in <paramref name="nodes"/> along the matching edges in <paramref name="edges"/>
        /// to produce a stream of <see cref="NodeWithValue{TValue}"/>s for each target of a matching edge.
        /// </summary>
        /// <typeparam name="TValue">The type of value associated with each node.</typeparam>
        /// <typeparam name="TOutput">The type of value sent to each destination node.</typeparam>
        /// <typeparam name="TTime">The type of timestamp on each record.</typeparam>
        /// <param name="nodes">The stream of nodes with values.</param>
        /// <param name="edges">The stream of edges.</param>
        /// <param name="valueSelector">A function from input (node, value) pair and destination node producing the value to send to that destination.</param>
        /// <returns>The stream of targets with values for each edge in <paramref name="edges"/>.</returns>
        /// <remarks>
        /// This operator may produce several values for a particular target node. The <see cref="GraphReduce{TValue,TTime}"/> operator
        /// produces a unique value for each target node.
        /// </remarks>
        public static Stream<NodeWithValue<TOutput>, TTime> TransmitAlong<TValue, TOutput, TTime>(this Stream<NodeWithValue<TValue>, TTime> nodes, Stream<Edge, TTime> edges, Func<NodeWithValue<TValue>, Node, TOutput> valueSelector)
            where TTime : Time<TTime>
        {
            if (nodes == null) throw new ArgumentNullException("nodes");
            if (edges == null) throw new ArgumentNullException("edges");
            if (valueSelector == null) throw new ArgumentNullException("valueSelector");
            var compacted = edges.Compact();    // could be cached and retrieved as needed

            return compacted.NewBinaryStage(nodes, (i, v) => new GraphJoinVertex<TValue, TOutput, TTime>(i, v, valueSelector), null, x => x.node.index, null, "TransmitAlong");
        }

        /// <summary>
        /// For each target node in <paramref name="edges"/>, aggregates the values at the neighboring <paramref name="nodes"/> using the given <paramref name="combiner"/>.
        /// </summary>
        /// <typeparam name="TValue">The type of value associated with each node.</typeparam>
        /// <typeparam name="TTime">The type of timestamp on each record.</typeparam>
        /// <param name="nodes">The stream of nodes with values.</param>
        /// <param name="edges">The stream of edges.</param>
        /// <param name="combiner">A function from current value and incoming value, to the new value.</param>
        /// <param name="useLocalAggregation">If <c>true</c>, pre-aggregates locally at each worker before the global aggregation.</param>
        /// <returns>The stream of aggregated values for each target of an edge in <paramref name="edges"/>.</returns>
        /// <remarks>
        /// This operator produces a single value for each target node. The <see cref="TransmitAlong{TValue,TTime}"/> operator
        /// produces one value per source node.
        /// </remarks>
        public static Stream<NodeWithValue<TValue>, TTime> GraphReduce<TValue, TTime>(this Stream<NodeWithValue<TValue>, TTime> nodes, Stream<Edge, TTime> edges, Func<TValue, TValue, TValue> combiner, bool useLocalAggregation)
            where TTime : Time<TTime>
        {
            if (nodes == null) throw new ArgumentNullException("nodes");
            if (edges == null) throw new ArgumentNullException("edges");
            if (combiner == null) throw new ArgumentNullException("combiner");
            if (useLocalAggregation)
            {
                return nodes.TransmitAlong(edges)
                            .NodeAggregateLocally(combiner)
                            .NodeAggregate(combiner);
            }
            else
            {
                return nodes.TransmitAlong(edges)
                            .NodeAggregate(combiner);
            }
        }

        /// <summary>
        /// Transforms a stream of edges into a single CompactGraph object.
        /// </summary>
        /// <typeparam name="TTime">Time type</typeparam>
        /// <param name="edges">stream of edges</param>
        /// <returns>stream of single CompactGraph</returns>
        internal static Stream<CompactGraph, TTime> Compact<TTime>(this Stream<Edge, TTime> edges)
            where TTime : Time<TTime>
        {
            return edges.NewUnaryStage((i, v) => new GraphCompactor<TTime>(i, v), x => x.source.index, null, "Compactor");
        }

        #endregion

        #region Methods relating to blocking node-based aggregations.

        /// <summary>
        /// Aggregates the values associated with each node using the given <paramref name="combiner"/> independently for each time.
        /// </summary>
        /// <typeparam name="TValue">The type of value associated with each node.</typeparam>
        /// <typeparam name="TTime">The type of timestamp on each record.</typeparam>
        /// <param name="nodes">The stream of nodes with values.</param>
        /// <param name="combiner">A function from current value and incoming value, to the new value.</param>
        /// <returns>The stream containing at most one aggregated value for each distinct node in <paramref name="nodes"/>.</returns>
        /// <remarks>
        /// This method aggregates values in each locigal time independently. To aggregate across times consider using <see cref="StateMachine{TValue, TState, TTime}(Stream{NodeWithValue{TValue},TTime},Func{TValue,TState,TState})"/>.
        /// </remarks>
        public static Stream<NodeWithValue<TValue>, TTime> NodeAggregate<TValue, TTime>(this Stream<NodeWithValue<TValue>, TTime> nodes, Func<TValue, TValue, TValue> combiner)
            where TTime : Time<TTime>
        {
            if (nodes == null) throw new ArgumentNullException("nodes");
            if (combiner == null) throw new ArgumentNullException("combiner");

#if true
            var stage = Foundry.NewStage(nodes.Context, (i,s) => new NodeAggregatorVertex<TValue, TTime>(i, s, combiner, nodes.ForStage.Placement.Count), "Aggregator");

            Action<NodeWithValue<TValue>[], int[], int> action = (data, dsts, len) => { for (int i = 0; i < len; i++) dsts[i] = data[i].node.index; };

            var input1 = stage.NewInput(nodes, (message, vertex) => vertex.OnReceive(message), x => x.node.index, action);
            var output = stage.NewOutput(vertex => vertex.Output, x => x.node.index);

            return output;
#else
            return nodes.NewUnaryStage((i, v) => new NodeAggregatorVertex<TValue, TTime>(i, v, combiner, nodes.ForStage.Placement.Count), x => x.node.index, x => x.node.index, "Aggregator");
#endif
        }

        /// <summary>
        /// Aggregates the values associated with each node at each process using the given <paramref name="combiner"/> independently for each time.
        /// </summary>
        /// <typeparam name="TValue">The type of value associated with each node.</typeparam>
        /// <typeparam name="TTime">The type of timestamp on each record.</typeparam>
        /// <param name="nodes">The stream of nodes with values.</param>
        /// <param name="combiner">A function from current value and incoming value, to the new value.</param>
        /// <returns>The stream of aggregated values at each process for each unique node in <paramref name="nodes"/>.</returns>
        /// <remarks>
        /// This method aggregates values in each logical time independently. To aggregate across times consider using <see cref="StateMachine{TValue, TState, TTime}(Stream{NodeWithValue{TValue},TTime},Func{TValue,TState,TState})"/>.
        /// </remarks>
        public static Stream<NodeWithValue<TValue>, TTime> NodeAggregateLocally<TValue, TTime>(this Stream<NodeWithValue<TValue>, TTime> nodes, Func<TValue, TValue, TValue> combiner)
            where TTime : Time<TTime>
        {
            if (nodes == null) throw new ArgumentNullException("nodes");
            if (combiner == null) throw new ArgumentNullException("combiner");

            var processId = nodes.ForStage.Computation.Controller.Configuration.ProcessID;
            var workersPerProcess = nodes.ForStage.Placement.Where(x => x.ProcessId == processId).Count();

            return nodes.PartitionBy(x => processId * workersPerProcess + (x.node.index % workersPerProcess))
                        .NewUnaryStage((i, v) => new NodeAggregatorVertex<TValue, TTime>(i, v, combiner, workersPerProcess), null, null, "AggregatorLocal");
        }

        #endregion

        #region Methods relating to streaming node-based state machines.

        /// <summary>
        /// Given a stream of values associated with nodes, maintains a state machine for each node,
        /// and produces outputs on each transition, based on the given <paramref name="transitionSelector"/>.
        /// </summary>
        /// <typeparam name="TValue">The type of value associated with each node.</typeparam>
        /// <typeparam name="TState">The type of state associated with each node.</typeparam>
        /// <typeparam name="TOutput">The type of output produced by each node.</typeparam>
        /// <typeparam name="TTime">The type of timestamp on each record.</typeparam>
        /// <param name="nodes">The stream of nodes with values.</param>
        /// <param name="transitionSelector">A function from current value and state, to new state and output.</param>
        /// <param name="defaultState">The default state associated with a node.</param>
        /// <returns>The stream of outputs produced by transitions.</returns>
        public static Stream<NodeWithValue<TOutput>, TTime> StateMachine<TValue, TState, TOutput, TTime>(this Stream<NodeWithValue<TValue>, TTime> nodes, Func<TValue, TState, Pair<TState, TOutput>> transitionSelector, TState defaultState)
            where TTime : Time<TTime>
        {
            if (nodes == null) throw new ArgumentNullException("nodes");
            if (transitionSelector == null) throw new ArgumentNullException("transitionSelector");

            var stage = Foundry.NewStage(nodes.Context, (i, s) => new NodeUnaryStateMachine<TValue, TState, TOutput, TTime>(i, s, transitionSelector, defaultState), "StateMachine");

            Action<NodeWithValue<TValue>[], int[], int> action = (data, dsts, len) => { for (int i = 0; i < len; i++) dsts[i] = data[i].node.index; };

            var input1 = stage.NewInput(nodes, (message, vertex) => vertex.OnReceive(message), x => x.node.index, action);
            var output = stage.NewOutput(vertex => vertex.Output, x => x.node.index);

            return output;
        }

        /// <summary>
        /// Given a stream of values associated with nodes, maintains a state machine for each node,
        /// and produces a stream of new states on each transition, based on the given <paramref name="transitionSelector"/>.
        /// </summary>
        /// <typeparam name="TValue">The type of value associated with each node.</typeparam>
        /// <typeparam name="TState">The type of state associated with each node.</typeparam>
        /// <typeparam name="TTime">The type of timestamp on each record.</typeparam>
        /// <param name="nodes">The stream of nodes with values.</param>
        /// <param name="transitionSelector">A function from current value and state, to new state.</param>
        /// <param name="defaultState">The default state associated with a node.</param>
        /// <returns>The stream of changed states at each node.</returns>
        public static Stream<NodeWithValue<TState>, TTime> StateMachine<TValue, TState, TTime>(this Stream<NodeWithValue<TValue>, TTime> nodes, Func<TValue, TState, TState> transitionSelector, TState defaultState)
            where TTime : Time<TTime>
            where TState : IEquatable<TState>
        {
            if (nodes == null) throw new ArgumentNullException("nodes");
            if (transitionSelector == null) throw new ArgumentNullException("transitionSelector");

            var stage = Foundry.NewStage(nodes.Context, (i, s) => new NodeUnaryStateMachine<TValue, TState, TTime>(i, s, transitionSelector, defaultState), "StateMachine");

            Action<NodeWithValue<TValue>[], int[], int> action = (data, dsts, len) => { for (int i = 0; i < len; i++) dsts[i] = data[i].node.index; };

            var input1 = stage.NewInput(nodes, (message, vertex) => vertex.OnReceive(message), x => x.node.index, action);
            var output = stage.NewOutput(vertex => vertex.Output, x => x.node.index);

            return output;
        }

        /// <summary>
        /// Given a stream of values and a stream of initial states associated with nodes, maintains a state machine for each node,
        /// and produces outputs on each transition, based on the given <paramref name="transitionSelector"/>.
        /// </summary>
        /// <typeparam name="TValue">The type of value associated with each node.</typeparam>
        /// <typeparam name="TState">The type of state associated with each node.</typeparam>
        /// <typeparam name="TOutput">The type of output produced by each node.</typeparam>
        /// <typeparam name="TTime">The type of timestamp on each record.</typeparam>
        /// <param name="nodes">The stream of nodes with values.</param>
        /// <param name="initialStates">The stream of initial states for each node. All initial states should exist at times no later than the first value.</param>
        /// <param name="transitionSelector">A function from current value and state, to new state and output.</param>
        /// <param name="defaultState">The default state associated with a node.</param>
        /// <returns>The stream of outputs produced by transitions.</returns>
        public static Stream<NodeWithValue<TOutput>, TTime> StateMachine<TValue, TState, TOutput, TTime>(this Stream<NodeWithValue<TValue>, TTime> nodes, Stream<NodeWithValue<TState>, TTime> initialStates, Func<TValue, TState, Pair<TState, TOutput>> transitionSelector, TState defaultState)
            where TTime : Time<TTime>
        {
            if (nodes == null) throw new ArgumentNullException("nodes");
            if (initialStates == null) throw new ArgumentNullException("initialStates");
            if (transitionSelector == null) throw new ArgumentNullException("transitionSelector");
            return nodes.NewBinaryStage(initialStates, (i, v) => new NodeBinaryStateMachine<TValue, TState, TOutput, TTime>(i, v, transitionSelector, defaultState), x => x.node.index, x => x.node.index, x => x.node.index, "NodeStateMachine");
        }

        #region Convenience overloads for unary StateMachine<TValue, TState, TOutput, TTime>

        /// <summary>
        /// Represents either a value or an invalid value
        /// </summary>
        /// <typeparam name="TElement">element type</typeparam>
        internal struct Option<TElement>
        {
            public readonly TElement Value;
            public readonly bool IsValid;

            /// <summary>
            /// Constructs a option with a valid value.
            /// </summary>
            /// <param name="value">value</param>
            public Option(TElement value) { this.Value = value; this.IsValid = true; }
        }

        internal static Stream<NodeWithValue<TRecord>, TTime> FilterOptions<TRecord, TTime>(this Stream<NodeWithValue<Option<TRecord>>, TTime> stream)
            where TTime : Time<TTime>
        {
            return stream.NewUnaryStage((i, s) => new FilterOptionsVertex<TRecord, TTime>(i, s), x => x.node.index, x => x.node.index, "FilterOptions");
        }

        /// <summary>
        /// Given a stream of values associated with nodes, maintains a state machine for each node,
        /// and produces a stream of new states on each transition, based on the given <paramref name="transitionSelector"/>.
        /// </summary>
        /// <typeparam name="TValue">The type of value associated with each node.</typeparam>
        /// <typeparam name="TState">The type of state associated with each node.</typeparam>
        /// <typeparam name="TTime">The type of timestamp on each record.</typeparam>
        /// <param name="nodes">The stream of nodes with values.</param>
        /// <param name="transitionSelector">A function from current value and state, to new state.</param>
        /// <returns>The stream of changed states at each node.</returns>
        public static Stream<NodeWithValue<TState>, TTime> StateMachine<TValue, TState, TTime>(this Stream<NodeWithValue<TValue>, TTime> nodes, Func<TValue, TState, TState> transitionSelector)
            where TTime : Time<TTime>
            where TState : IEquatable<TState>
        {
            if (nodes == null) throw new ArgumentNullException("nodes");
            if (transitionSelector == null) throw new ArgumentNullException("transitionSelector");
            return nodes.StateMachine(transitionSelector, default(TState));
        }



        /// <summary>
        /// Given a stream of values associated with nodes, maintains a state machine for each node,
        /// and produces outputs on each transition, based on the given <paramref name="transitionSelector"/>.
        /// </summary>
        /// <typeparam name="TValue">The type of value associated with each node.</typeparam>
        /// <typeparam name="TState">The type of state associated with each node.</typeparam>
        /// <typeparam name="TOutput">The type of output produced by each node.</typeparam>
        /// <typeparam name="TTime">The type of timestamp on each record.</typeparam>
        /// <param name="nodes">The stream of nodes with values.</param>
        /// <param name="transitionSelector">A function from current value and state, to new state and output.</param>
        /// <returns>The stream of outputs produced by transitions.</returns>
        public static Stream<NodeWithValue<TOutput>, TTime> StateMachine<TValue, TState, TOutput, TTime>(this Stream<NodeWithValue<TValue>, TTime> nodes, Func<TValue, TState, Pair<TState, TOutput>> transitionSelector)
            where TTime : Time<TTime>
        {
            if (nodes == null) throw new ArgumentNullException("nodes");
            if (transitionSelector == null) throw new ArgumentNullException("transitionSelector");
            return nodes.StateMachine(transitionSelector, default(TState));
        }

        #endregion

        #endregion

        #region Convenience methods built out of existing parts

        /// <summary>
        /// Joins a value stream with a static set of per-node states.
        /// </summary>
        /// <typeparam name="TValue">The type of value associated with each node.</typeparam>
        /// <typeparam name="TState">The type of state associated with each node.</typeparam>
        /// <typeparam name="TOutput">The type of output produced by each node.</typeparam>
        /// <typeparam name="TTime">The type of timestamp on each record.</typeparam>
        /// <param name="nodes">The stream of nodes with values.</param>
        /// <param name="states">The stream of states for each node.</param>
        /// <param name="resultSelector">Function from a value and state to an output.</param>
        /// <returns>The stream of nodes with outputs.</returns>
        public static Stream<NodeWithValue<TOutput>, TTime> NodeJoin<TValue, TState, TOutput, TTime>(this Stream<NodeWithValue<TValue>, TTime> nodes, Stream<NodeWithValue<TState>, TTime> states, Func<TValue, TState, TOutput> resultSelector)
            where TTime : Time<TTime>
        {
            if (nodes == null) throw new ArgumentNullException("nodes");
            if (states == null) throw new ArgumentNullException("states");
            if (resultSelector == null) throw new ArgumentNullException("resultSelector");
            return nodes.StateMachine(states, (v, s) => s.PairWith(resultSelector(v, s)), default(TState));
        }

        /// <summary>
        /// Filters the stream of <paramref name="nodes"/> to contain only those nodes in the <paramref name="restriction"/>.
        /// </summary>
        /// <typeparam name="TValue">The type of value associated with each node.</typeparam>
        /// <typeparam name="TTime">The type of timestamp on each record.</typeparam>
        /// <param name="nodes">The stream of nodes with values.</param>
        /// <param name="restriction">The stream of nodes that should appear in the output.</param>
        /// <returns>The filtered stream of nodes with values that contains only nodes that are observed in <paramref name="restriction"/>.</returns>
        public static Stream<NodeWithValue<TValue>, TTime> FilterBy<TValue, TTime>(this Stream<NodeWithValue<TValue>, TTime> nodes, Stream<Node, TTime> restriction)
            where TTime : Time<TTime>
        {
            if (nodes == null) throw new ArgumentNullException("nodes");
            if (restriction == null) throw new ArgumentNullException("restriction");
            return nodes.NodeJoin(restriction.Select(x => x.WithValue(true)), (x, y) => y ? new Option<TValue>(x) : new Option<TValue>())
                        .Where(x => x.value.IsValid)
                        .Select(x => x.node.WithValue(x.value.Value));
        }

        /// <summary>
        /// Computes the distinct set of nodes in the input.
        /// </summary>
        /// <typeparam name="TTime">The type of timestamp on each record.</typeparam>
        /// <param name="nodes">The input stream.</param>
        /// <returns>The stream containing the distinct set of nodes in the input.</returns>
        public static Stream<Node, TTime> DistinctNodes<TTime>(this Stream<Node, TTime> nodes)
            where TTime : Time<TTime>
        {
            if (nodes == null) throw new ArgumentNullException("nodes");
            return nodes.Select(x => x.WithValue(true))
                        .StateMachine((bool a, bool b) => a || b)
                        .Select(x => x.node);
        }

        /// <summary>
        /// Computes the number of occurrences of each node in the input.
        /// </summary>
        /// <typeparam name="TTime">The type of timestamp on each record.</typeparam>
        /// <param name="nodes">The input stream.</param>
        /// <returns>The stream of nodes whose associated value if the number of occurrences of each node in the input.</returns>
        public static Stream<NodeWithValue<Int64>, TTime> CountNodes<TTime>(this Stream<Node, TTime> nodes)
            where TTime : Time<TTime>
        {
            if (nodes == null) throw new ArgumentNullException("nodes");
            return nodes.Select(x => x.WithValue(1L))
                        .NodeAggregate((x, y) => x + y);
        }

        #endregion
    }

    // a blocking aggregation vertex, reporting the aggregate value for any received keys.
    internal class NodeAggregatorVertex<TValue, TTime> : UnaryVertex<NodeWithValue<TValue>, NodeWithValue<TValue>, TTime>
        where TTime : Time<TTime>
    {
        private readonly Dictionary<TTime, TValue[]> values;

        private readonly Func<TValue, TValue, TValue> update; // value aggregator

        private readonly Int32 parts;
        private readonly Int32 index;

        public override void OnReceive(Message<NodeWithValue<TValue>, TTime> message)
        {
            if (!this.values.ContainsKey(message.time))
            {
                this.values.Add(message.time, new TValue[] {});
                this.NotifyAt(message.time);
            }

            var array = this.values[message.time];
            for (int i = 0; i < message.length; i++)
            {
                var record = message.payload[i];

                var localName = record.node.index / this.parts;
                if (array.Length <= localName)
                {
                    var newArray = new TValue[Math.Max(2 * array.Length, localName + 1)];
                    for (int j = 0; j < array.Length; j++)
                        newArray[j] = array[j];

                    this.values[message.time] = newArray;
                    array = newArray;
                }

                array[localName] = this.update(array[localName], record.value);
            }
        }

        public override void OnNotify(TTime time)
        {
            // make sure to fold in any loose values lying around
            if (this.values.ContainsKey(time))
            {
                var output = this.Output.GetBufferForTime(time);
                var array = this.values[time];
                for (int i = 0; i < array.Length; i++)
                    output.Send(new Node((this.parts * i) + this.index).WithValue(array[i]));

                this.values.Remove(time);
            }
        }

        public NodeAggregatorVertex(int index, Stage<TTime> stage, Func<TValue, TValue, TValue> aggregate, int parts)
            : base(index, stage)
        {
            this.Entrancy = 5;

            this.values = new Dictionary<TTime, TValue[]>();
            this.update = aggregate;

            this.parts = parts;
            this.index = index % parts;
        }
    }

    // a streaming state machine vertex, responding to incoming values with a state transition and an output message.
    internal class NodeBinaryStateMachine<TValue, TState, TMessage, TTime> : BinaryVertex<NodeWithValue<TValue>, NodeWithValue<TState>, NodeWithValue<TMessage>, TTime>
        where TTime : Time<TTime>
    {
        private TState[] state;
        private readonly Func<TValue, TState, Pair<TState, TMessage>> transition;

        private readonly int parts;

        private List<NodeWithValue<TValue>> toProcess;

        private readonly TState defaultValue;

        public override void OnReceive1(Message<NodeWithValue<TValue>, TTime> message)
        {
            if (this.toProcess != null)
            {
                for (int i = 0; i < message.length; i++)
                    this.toProcess.Add(message.payload[i]);

                this.NotifyAt(message.time);
            }
            else
            {
                var output = this.Output.GetBufferForTime(message.time);
                for (int i = 0; i < message.length; i++)
                {
                    var record = message.payload[i];
                    var localIndex = record.node.index / this.parts;

                    // we may need to grow the state vector
                    if (localIndex >= this.state.Length)
                    {
                        var newState = new TState[Math.Max(2 * this.state.Length, localIndex + 1)];
                        for (int j = 0; j < this.state.Length; j++)
                            newState[j] = this.state[j];

                        for (int j = this.state.Length; j < newState.Length; j++)
                            newState[j] = defaultValue;

                        this.state = newState;
                    }

                    var transitionResult = this.transition(record.value, this.state[localIndex]);

                    this.state[localIndex] = transitionResult.First;
                    output.Send(record.node.WithValue(transitionResult.Second));
                }
            }
        }

        public override void OnReceive2(Message<NodeWithValue<TState>, TTime> message)
        {
            for (int i = 0; i < message.length; i++)
            {
                var localIndex = message.payload[i].node.index / this.parts;
                if (localIndex >= this.state.Length)
                {
                    var newState = new TState[Math.Max(localIndex + 1, 2 * this.state.Length)];

                    for (int j = 0; j < this.state.Length; j++)
                        newState[j] = this.state[j];

                    for (int j = this.state.Length; j < newState.Length; j++)
                        newState[j] = this.defaultValue;

                    this.state = newState;
                }

                this.state[localIndex] = message.payload[i].value;
            }
        }

        public override void OnNotify(TTime time)
        {
            var list = this.toProcess;
            this.toProcess = null;

            var output = this.Output.GetBufferForTime(time);
            foreach (var record in list)
            {
                var localIndex = record.node.index / this.parts;

                // we may need to grow the state vector
                if (localIndex >= this.state.Length)
                {
                    var newState = new TState[Math.Max(2 * this.state.Length, localIndex + 1)];
                    for (int j = 0; j < this.state.Length; j++)
                        newState[j] = this.state[j];

                    for (int j = this.state.Length; j < newState.Length; j++)
                        newState[j] = defaultValue;

                    this.state = newState;
                }

                var transitionResult = this.transition(record.value, this.state[localIndex]);

                this.state[localIndex] = transitionResult.First;
                output.Send(record.node.WithValue(transitionResult.Second));
            }
        }

        public NodeBinaryStateMachine(int index, Stage<TTime> vertex, Func<TValue, TState, Pair<TState, TMessage>> transition, TState defaultValue)
            : base(index, vertex)
        {
            this.state = new TState[] { };
            this.toProcess = new List<NodeWithValue<TValue>>();
            this.transition = transition;
            this.parts = vertex.Placement.Count;
            this.defaultValue = defaultValue;
        }
    }

    // a streaming state machine vertex, responding to incoming values with a state transition and an output message.
    internal class NodeUnaryStateMachine<TValue, TState, TMessage, TTime> : UnaryVertex<NodeWithValue<TValue>, NodeWithValue<TMessage>, TTime>
        where TTime : Time<TTime>
    {
        private TState[] state;
        private readonly Func<TValue, TState, Pair<TState, TMessage>> transition;

        private readonly int parts;
        private readonly TState defaultState;

        public override void OnReceive(Message<NodeWithValue<TValue>, TTime> message)
        {
            var output = this.Output.GetBufferForTime(message.time);
            for (int i = 0; i < message.length; i++)
            {
                var record = message.payload[i];
                var localIndex = record.node.index / this.parts;

                if (this.state.Length <= localIndex)
                {
                    var newState = new TState[Math.Max(localIndex + 1, 2 * this.state.Length)];
                    for (int j = 0; j < this.state.Length; j++)
                        newState[j] = this.state[j];

                    for (int j = this.state.Length; j < newState.Length; j++)
                        newState[j] = defaultState;

                    this.state = newState;
                }

                var transitionResult = this.transition(record.value, this.state[localIndex]);

                this.state[localIndex] = transitionResult.First;

                output.Send(record.node.WithValue(transitionResult.Second));
            }
        }

        public NodeUnaryStateMachine(int index, Stage<TTime> vertex, Func<TValue, TState, Pair<TState, TMessage>> transition, TState defaultState)
            : base(index, vertex)
        {
            this.state = new TState[] { };

            this.transition = transition;

            this.parts = vertex.Placement.Count;
            this.defaultState = defaultState;
        }
    }

    // a streaming state machine vertex, responding to incoming values with a state transition and an output message.
    internal class NodeUnaryStateMachine<TValue, TState, TTime> : UnaryVertex<NodeWithValue<TValue>, NodeWithValue<TState>, TTime>
        where TTime : Time<TTime>
        where TState : IEquatable<TState>
    {
        private TState[] state;
        private readonly Func<TValue, TState, TState> transition;

        private readonly int parts;
        private readonly TState defaultState;

        public override void OnReceive(Message<NodeWithValue<TValue>, TTime> message)
        {
            var output = this.Output.GetBufferForTime(message.time);
            for (int i = 0; i < message.length; i++)
            {
                var record = message.payload[i];
                var localIndex = record.node.index / this.parts;

                if (this.state.Length <= localIndex)
                {
                    var newState = new TState[Math.Max(localIndex + 1, 2 * this.state.Length)];
                    for (int j = 0; j < this.state.Length; j++)
                        newState[j] = this.state[j];

                    for (int j = this.state.Length; j < newState.Length; j++)
                        newState[j] = defaultState;

                    this.state = newState;
                }

                var transitionResult = this.transition(record.value, this.state[localIndex]);

                if (!this.state[localIndex].Equals(transitionResult))
                {
                    this.state[localIndex] = transitionResult;
                    output.Send(record.node.WithValue(transitionResult));
                }
            }
        }

        public NodeUnaryStateMachine(int index, Stage<TTime> vertex, Func<TValue, TState, TState> transition, TState defaultState)
            : base(index, vertex)
        {
            this.state = new TState[] { };

            this.transition = transition;

            this.parts = vertex.Placement.Count;
            this.defaultState = defaultState;
        }
    }

    internal class FilterOptionsVertex<TRecord, TTime> : UnaryVertex<NodeWithValue<ExtensionMethods.Option<TRecord>>, NodeWithValue<TRecord>, TTime>
        where TTime : Time<TTime>
    {
        public override void OnReceive(Message<NodeWithValue<ExtensionMethods.Option<TRecord>>, TTime> message)
        {
            var output = this.Output.GetBufferForTime(message.time);
            for (int i = 0; i < message.length; i++)
                if (message.payload[i].value.IsValid)
                    output.Send(message.payload[i].node.WithValue(message.payload[i].value.Value));
        }

        public FilterOptionsVertex(int index, Stage<TTime> stage) : base(index, stage) { }
    }

    // vertex managing a CompactGraph fragment, processing corresponding values by applying a reducer.
    internal class GraphJoinVertex<TValue, TOutput, T> : BinaryVertex<CompactGraph, NodeWithValue<TValue>, NodeWithValue<TOutput>, T>
        where T : Time<T>
    {
        private CompactGraph graph;

        private List<NodeWithValue<TValue>> toProcess;

        private readonly Func<NodeWithValue<TValue>, Node, TOutput> valueSelector;

        public override void OnReceive1(Message<CompactGraph, T> message)
        {
            for (int i = 0; i < message.length; i++)
                this.graph = message.payload[i];

            this.NotifyAt(message.time);
        }

        public override void OnReceive2(Message<NodeWithValue<TValue>, T> message)
        {
            if (this.graph.Nodes == null && message.length > 0)
            {
                for (int i = 0; i < message.length; i++)
                    this.toProcess.Add(message.payload[i]);
            }
            else
            {
                var output = this.Output.GetBufferForTime(message.time);
                for (int i = 0; i < message.length; i++)
                {
                    var record = message.payload[i];
                    var localName = record.node.index / this.Stage.Placement.Count;

                    if (localName + 1 < this.graph.Nodes.Length)
                        for (int j = this.graph.Nodes[localName]; j < this.graph.Nodes[localName + 1]; j++)
                            output.Send(this.graph.Edges[j].WithValue(this.valueSelector(record, this.graph.Edges[j])));
                }
            }
        }

        public override void OnNotify(T time)
        {
            var output = this.Output.GetBufferForTime(time);
            foreach (var record in this.toProcess.AsEnumerable())
            {
                var localName = record.node.index / this.Stage.Placement.Count;
                if (localName + 1 < this.graph.Nodes.Length)
                    for (int j = this.graph.Nodes[localName]; j < this.graph.Nodes[localName + 1]; j++)
                        output.Send(this.graph.Edges[j].WithValue(this.valueSelector(record, this.graph.Edges[j])));
            }

            this.toProcess = new List<NodeWithValue<TValue>>();
        }

        public GraphJoinVertex(int index, Stage<T> vertex, Func<NodeWithValue<TValue>, Node, TOutput> valueSelector)
            : base(index, vertex)
        {
            this.graph = new CompactGraph();
            this.toProcess = new List<NodeWithValue<TValue>>();
            this.valueSelector = valueSelector;

            this.Entrancy = 5;
        }
    }

    // vertex managing a CompactGraph fragment, processing corresponding values by applying a reducer.
    internal class GraphJoinVertex<TValue, T> : BinaryVertex<CompactGraph, NodeWithValue<TValue>, NodeWithValue<TValue>, T>
        where T : Time<T>
    {
        private CompactGraph graph;

        private List<NodeWithValue<TValue>> toProcess;

        public override void OnReceive1(Message<CompactGraph, T> message)
        {
            for (int i = 0; i < message.length; i++)
                this.graph = message.payload[i];

            this.NotifyAt(message.time);
        }

        public override void OnReceive2(Message<NodeWithValue<TValue>, T> message)
        {
            if (this.graph.Nodes == null && message.length > 0)
            {
                for (int i = 0; i < message.length; i++)
                    this.toProcess.Add(message.payload[i]);
            }
            else
            {
                var output = this.Output.GetBufferForTime(message.time);
                for (int i = 0; i < message.length; i++)
                {
                    var record = message.payload[i];
                    var localName = record.node.index / this.Stage.Placement.Count;

                    if (localName + 1 < this.graph.Nodes.Length)
                        for (int j = this.graph.Nodes[localName]; j < this.graph.Nodes[localName + 1]; j++)
                            output.Send(this.graph.Edges[j].WithValue(record.value));
                }
            }
        }

        public override void OnNotify(T time)
        {
            var output = this.Output.GetBufferForTime(time);
            foreach (var record in this.toProcess.AsEnumerable())
            {
                var localName = record.node.index / this.Stage.Placement.Count;
                if (localName + 1 < this.graph.Nodes.Length)
                    for (int j = this.graph.Nodes[localName]; j < this.graph.Nodes[localName + 1]; j++)
                        output.Send(this.graph.Edges[j].WithValue(record.value));
            }

            this.toProcess = new List<NodeWithValue<TValue>>();
        }

        public GraphJoinVertex(int index, Stage<T> stage)
            : base(index, stage)
        {
            this.graph = new CompactGraph();
            this.toProcess = new List<NodeWithValue<TValue>>();

            this.Entrancy = 5;
        }
    }

    // dense list of edge destinations and node offsets.
    internal struct CompactGraph
    {
        public readonly Int32[] Nodes;  // offsets in this.edges
        public readonly Node[] Edges;  // names of target nodes

        public CompactGraph(Int32[] n, Node[] e) { this.Nodes = n; this.Edges = e; }
    }

    // vertex managing the compaction of a graph, taking a stream of edge pairs to a single CompactGraph.
    internal class GraphCompactor<T> : UnaryVertex<Edge, CompactGraph, T>
        where T : Time<T>
    {
        private readonly Dictionary<T, CompactGraphBuilder> edges = new Dictionary<T, CompactGraphBuilder>();

        public override void OnReceive(Message<Edge, T> message)
        {
            // Console.Error.WriteLine("Compactor received {0} edges", message.length);
            if (!this.edges.ContainsKey(message.time))
            {
                this.edges.Add(message.time, new CompactGraphBuilder());
                this.NotifyAt(message.time);
            }

            var builder = this.edges[message.time];
            for (int i = 0; i < message.length; i++)
                builder.AddEdge(message.payload[i]);

            this.edges[message.time] = builder;
        }

        public override void OnNotify(T time)
        {
            if (this.edges.ContainsKey(time))
            {
                this.Output.GetBufferForTime(time).Send(this.edges[time].Build(this.Stage.Placement.Count));
                this.edges.Remove(time);
            }
        }

        public GraphCompactor(int index, Stage<T> stage)
            : base(index, stage)
        {
        }
    }

    // storage and logic for building a CompactGraph.
    internal struct CompactGraphBuilder
    {
        private List<Edge> newEdges;

        public CompactGraphBuilder AddEdge(Edge edge)
        {
            if (this.newEdges == null)
                this.newEdges = new List<Edge>();

            newEdges.Add(edge);

            return this;
        }

        public CompactGraph Build(int parts)
        {
            if (this.newEdges == null)
                this.newEdges = new List<Edge>();

            // step 0: determine how many nodes we are talking about here
            var maxNode = 0L;
            for (int i = 0; i < newEdges.Count; i++)
                if (maxNode < newEdges[i].source.index / parts)
                    maxNode = newEdges[i].source.index / parts;

            // step 1: determine degree of each node we have been presented with
            var nodes = new Int32[maxNode + 2];
            var edges = new Node[newEdges.Count];

            // determine degrees
            for (int i = 0; i < newEdges.Count; i++)
                nodes[newEdges[i].source.index / parts]++;

            // accumulate to determine ending offsets
            for (int i = 1; i < nodes.Length; i++)
                nodes[i] += nodes[i - 1];

            // shift to determine starting offsets
            for (int i = nodes.Length - 1; i > 0; i--)
                nodes[i] = nodes[i - 1];

            nodes[0] = 0;

            // step 2: populate this.edges with actual edges
            for (int i = 0; i < newEdges.Count; i++)
                edges[nodes[newEdges[i].source.index / parts]++] = newEdges[i].target;

            // shift to determine return to offsets
            for (int i = nodes.Length - 1; i > 0; i--)
                nodes[i] = nodes[i - 1];

            nodes[0] = 0;

            return new CompactGraph(nodes, edges);
        }
    }

    // turns a stream of S records into a stream of Pair<S, int>, where the integers are unique, and largely dense.
    internal class Densifier<TRecord> : UnaryVertex<TRecord, NodeWithValue<TRecord>, Epoch>
    {
        private HashSet<TRecord> hashSet;
        private readonly int parts;

        public override void OnReceive(Message<TRecord, Epoch> message)
        {
            var output = this.Output.GetBufferForTime(message.time);
            for (int i = 0; i < message.length; i++)
                if (hashSet.Add(message.payload[i]))
                    output.Send(new Node(this.VertexId + this.parts * (this.hashSet.Count - 1)).WithValue(message.payload[i]));

            // notify for garbage collection.
            this.NotifyAt(message.time);
        }

        public override void OnNotify(Epoch time)
        {
            hashSet = new HashSet<TRecord>();
        }

        public Densifier(int index, Stage<Epoch> vertex)
            : base(index, vertex)
        {
            this.hashSet = new HashSet<TRecord>();
            this.parts = this.Stage.Placement.Count;
        }
    }

    // joins on keys of type S to look up integer names provided as input.
    internal class RenamerVertex<TInput, TIdentifier> : BinaryVertex<TInput, NodeWithValue<TIdentifier>, NodeWithValue<TInput>, Epoch>
    {
        private Dictionary<TIdentifier, Node> rename = new Dictionary<TIdentifier, Node>();
        private Dictionary<TIdentifier, List<TInput>> delayed = new Dictionary<TIdentifier, List<TInput>>();

        private readonly Func<TInput, TIdentifier> oldName;

        //Int64 enqueuedWork;
        //Int64 processedWork;
        //Int64 highWaterMark;

        /// <summary>
        /// Callback on receipt of a batch of elements to rename.
        /// </summary>
        /// <param name="message"></param>
        public override void OnReceive1(Message<TInput, Epoch> message)
        {
            var output = this.Output.GetBufferForTime(message.time);
            for (int i = 0; i < message.length; i++)
            {
                var record = message.payload[i];
                var name = oldName(record);

                if (rename.ContainsKey(name))
                {
                    output.Send(rename[name].WithValue(record));
                }
                else
                {
                    if (!this.delayed.ContainsKey(name))
                        this.delayed.Add(name, new List<TInput>());

                    this.delayed[name].Add(message.payload[i]);

                    //enqueuedWork++;

                    this.NotifyAt(message.time);
                }
            }
        }

        /// <summary>
        /// Callback on receipt of a batch of nodes with names.
        /// </summary>
        /// <param name="message">message containing nodes with names</param>
        public override void OnReceive2(Message<NodeWithValue<TIdentifier>, Epoch> message)
        {
            for (int i = 0; i < message.length; i++)
            {
                this.rename.Add(message.payload[i].value, message.payload[i].node);
                if (this.delayed.ContainsKey(message.payload[i].value))
                {
                    var list = this.delayed[message.payload[i].value];
                    this.delayed.Remove(message.payload[i].value);

                    //processedWork += list.Count;

                    var output = this.Output.GetBufferForTime(message.time);
                    foreach (TInput t in list)
                        output.Send(message.payload[i].node.WithValue(t));
                }
            }
        }

        /// <summary>
        /// Confirmation once inputs have been exhausted.
        /// Should not produce new records; would be a bug.
        /// </summary>
        /// <param name="time">completed time</param>
        public override void OnNotify(Epoch time)
        {
            var output = this.Output.GetBufferForTime(time);
            foreach (var pair in delayed)
            {
                if (rename.ContainsKey(pair.Key))
                    for (int j = 0; j < pair.Value.Count; j++)
                        output.Send(rename[pair.Key].WithValue(pair.Value[j]));
            }

            this.rename = new Dictionary<TIdentifier, Node>();
            this.delayed = new Dictionary<TIdentifier, List<TInput>>();
        }

        public RenamerVertex(int index, Stage<Epoch> vertex, Func<TInput, TIdentifier> oldN)
            : base(index, vertex)
        {
            this.oldName = oldN;
        }
    }

    /// <summary>
    /// A renaming context with the ability to choose its own names for identifiers.
    /// </summary>
    /// <typeparam name="TIdentifier">common identifier type</typeparam>
    public class AutoRenamer<TIdentifier> : IDisposable
    {
        private class Vertex : Vertex<IterationIn<Epoch>>
        {
            private Dictionary<TIdentifier, Node> renameMapping = new Dictionary<TIdentifier, Node>();

            internal readonly VertexOutputBuffer<NodeWithValue<TIdentifier>, IterationIn<Epoch>> RenameOutput;

            internal void OnReceive<TInput>(Message<TInput, IterationIn<Epoch>> message,
                                            Func<TInput, TIdentifier> nameSelector,
                                            VertexOutputBuffer<NodeWithValue<TInput>, IterationIn<Epoch>> output)
            {
                var time = message.time;

                var renameForTime = this.RenameOutput.GetBufferForTime(time);
                var outputForTime = output.GetBufferForTime(time);
                for (int i = 0; i < message.length; i++)
                {
                    var name = nameSelector(message.payload[i]);
                    if (!this.renameMapping.ContainsKey(name))
                    {
                        var newNode = new Node((this.renameMapping.Count * this.Stage.Placement.Count) + this.VertexId);
                        this.renameMapping.Add(name, newNode);
                        renameForTime.Send(newNode.WithValue(name));
                    }

                    outputForTime.Send(this.renameMapping[name].WithValue(message.payload[i]));
                }
            }

            public override void OnNotify(IterationIn<Epoch> time)
            {
                this.renameMapping = null;
            }

            internal Vertex(int index, Stage<IterationIn<Epoch>> stage)
                : base(index, stage)
            {
                this.RenameOutput = new VertexOutputBuffer<NodeWithValue<TIdentifier>, IterationIn<Epoch>>(this);

                this.NotifyAt(new IterationIn<Epoch>(new Epoch(Int32.MaxValue), Int32.MaxValue));
                this.Entrancy = 5;
            }
        }

        /// <summary>
        /// Context used to enter and exit the renaming scope.
        /// </summary>
        internal LoopContext<Epoch> Context;

        private Stage<Vertex, IterationIn<Epoch>> stage;
        private readonly List<Action> connectLoopsActions = new List<Action>();

        internal void InitializeContext(TimeContext<Epoch> context)
        {
            if (this.Context == null)
            {
                this.Context = new LoopContext<Epoch>(context, "RenameContext");

                var delay = this.Context.Delay<bool>();

                this.stage = new Stage<Vertex, IterationIn<Epoch>>(delay.Output.Context, (i, s) => new Vertex(i, s), "Renamer");
            }
        }

        /// <summary>
        /// Adds a rename task, taking a stream in the rename context and a name selection function.
        /// </summary>
        /// <typeparam name="TInput">input type</typeparam>
        /// <param name="stream">source stream</param>
        /// <param name="identifierSelector">function from input to identifier</param>
        /// <returns>stream of pairs of inputs and node name of associated identifier</returns>
        public Stream<NodeWithValue<TInput>, IterationIn<Epoch>> AddRenameTask<TInput>(Stream<TInput, IterationIn<Epoch>> stream, Func<TInput, TIdentifier> identifierSelector)
        {
            if (stream == null) throw new ArgumentNullException("stream");
            if (identifierSelector == null) throw new ArgumentNullException("identifierSelector");
            var result = this.Context.Delay<NodeWithValue<TInput>>();

            // shared dictionary of VertexOutputBuffers. not how things should really be structured, but ... =/
            var dictionary = new Dictionary<Vertex, VertexOutputBuffer<NodeWithValue<TInput>, IterationIn<Epoch>>>();

            this.stage.NewInput(stream, (message, vertex) => vertex.OnReceive(message, identifierSelector, dictionary[vertex]), x => identifierSelector(x).GetHashCode());

            this.connectLoopsActions.Add(() => result.Input = this.stage.NewOutput(vertex =>
            {
                if (!dictionary.ContainsKey(vertex))
                    dictionary.Add(vertex, new VertexOutputBuffer<NodeWithValue<TInput>, IterationIn<Epoch>>(vertex));

                return dictionary[vertex];
            }, null));

            return result.Output;
        }

        /// <summary>
        /// Creates and connects outputs to their feedback vertices.
        /// </summary>
        public void Dispose()
        {
            foreach (var action in this.connectLoopsActions)
                action();
        }

        /// <summary>
        /// A stream of renamings produced by the AutoRenamer.
        /// </summary>
        public Stream<NodeWithValue<TIdentifier>, Epoch> FinalRenamings
        {
            get
            {
                return this.stage.NewOutput(vertex => vertex.RenameOutput, x => x.value.GetHashCode())
                                 .FinishRenaming(this);
            }
        }
    }
}
