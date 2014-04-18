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
using System.Linq.Expressions;
using System.Text;
using System.Threading.Tasks;

using Microsoft.Research.Naiad;
using Microsoft.Research.Naiad.Dataflow;
using Microsoft.Research.Naiad.Dataflow.StandardVertices;
using Microsoft.Research.Naiad.Frameworks.Lindi;

using Microsoft.Research.Naiad.Dataflow.Channels;

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
            return stream.NewBinaryStage(renameMapping, (i, v) => new Renamer<TInput, TIdentifier>(i, v, identifierSelector), x => identifierSelector(x).GetHashCode(), x => x.value.GetHashCode(), null, "RenameUsing");
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
            var compacted = edges.Compact();    // could be cached and retreived as needed

            return compacted.NewBinaryStage(nodes, (i, v) => new GraphJoin<TValue, TTime>(i, v), null, x => x.node.index, null, "TransmitAlong");
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
            return nodes.NewUnaryStage((i, v) => new NodeAggregatorVertex<TValue, TTime>(i, v, combiner), x => x.node.index, x => x.node.index, "Aggregator");
        }

        /// <summary>
        /// Aggregates the values associated with each node at each worker using the given <paramref name="combiner"/> independently for each time.
        /// </summary>
        /// <typeparam name="TValue">The type of value associated with each node.</typeparam>
        /// <typeparam name="TTime">The type of timestamp on each record.</typeparam>
        /// <param name="nodes">The stream of nodes with values.</param>
        /// <param name="combiner">A function from current value and incoming value, to the new value.</param>
        /// <returns>The stream of aggregated values at each worker for each unique node in <paramref name="nodes"/>.</returns>
        /// <remarks>
        /// This method aggregates values in each logical time independently. To aggregate across times consider using <see cref="StateMachine{TValue, TState, TTime}(Stream{NodeWithValue{TValue},TTime},Func{TValue,TState,TState})"/>.
        /// </remarks>
        public static Stream<NodeWithValue<TValue>, TTime> NodeAggregateLocally<TValue, TTime>(this Stream<NodeWithValue<TValue>, TTime> nodes, Func<TValue, TValue, TValue> combiner)
            where TTime : Time<TTime>
        {
            return nodes.NewUnaryStage((i, v) => new NodeAggregatorVertex<TValue, TTime>(i, v, combiner), null, null, "AggregatorLocal");
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
            return nodes.NewUnaryStage((i, v) => new NodeUnaryStateMachine<TValue, TState, TOutput, TTime>(i, v, transitionSelector, defaultState), x => x.node.index, x => x.node.index, "NodeStateMachine");
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
            return nodes.NewBinaryStage(initialStates, (i, v) => new NodeBinaryStateMachine<TValue, TState, TOutput, TTime>(i, v, transitionSelector, defaultState), x => x.node.index, x => x.node.index, x => x.node.index, "NodeStateMachine");
        }

        #region Convenience overloads for unary StateMachine<TValue, TState, TOutput, TTime> 

        /// <summary>
        /// Represents either a value or an invalid value
        /// </summary>
        /// <typeparam name="TElement">element type</typeparam>
        private struct Option<TElement>
        {
            public readonly TElement Value;
            public readonly bool isValid;

            /// <summary>
            /// Constructs a option with a valid value.
            /// </summary>
            /// <param name="value">value</param>
            public Option(TElement value) { this.Value = value; this.isValid = true; }
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
            return nodes.StateMachine(transitionSelector, default(TState));
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
            return nodes.StateMachine((v, s) => { var n = transitionSelector(v, s); return n.PairWith(s.Equals(n) ? new Option<TState>() : new Option<TState>(n)); }, defaultState)
                         .Where(x => x.value.isValid)
                         .Select(x => x.node.WithValue(x.value.Value));
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
            return nodes.NodeJoin(restriction.Select(x => x.WithValue(true)), (x, y) => y ? new Option<TValue>(x) : new Option<TValue>())
                        .Where(x => x.value.isValid)
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
            return nodes.Select(x => x.WithValue(1L))
                        .NodeAggregate((x, y) => x + y);
        }

        #endregion
    }
    
    // a blocking aggregation vertex, reporting the aggregate value for any received keys.
    internal class NodeAggregatorVertex<V, T> : UnaryVertex<NodeWithValue<V>, NodeWithValue<V>, T>
        where T : Time<T>
    {
        private readonly Dictionary<T, Dictionary<Node, V>> Values;

        private readonly Func<V, V, V> Update; // value aggregator

        public override void OnReceive(Message<NodeWithValue<V>, T> message)
        {
            if (!this.Values.ContainsKey(message.time))
            {
                this.Values.Add(message.time, new Dictionary<Node, V>());
                this.NotifyAt(message.time);
            }

            for (int i = 0; i < message.length; i++)
            {
                var record = message.payload[i];

                var dictionary = this.Values[message.time];
                if (!dictionary.ContainsKey(record.node))
                    dictionary.Add(record.node, record.value);
                else
                    dictionary[record.node] = this.Update(dictionary[record.node], record.value);
            }
        }

        public override void OnNotify(T time)
        {
            // make sure to fold in any loose values lying around
            if (this.Values.ContainsKey(time))
            {
                var output = this.Output.GetBufferForTime(time);
                foreach (var pair in this.Values[time])
                    output.Send(pair.Key.WithValue(pair.Value));

                this.Values.Remove(time);
            }
        }

        public NodeAggregatorVertex(int index, Stage<T> stage, Func<V, V, V> aggregate)
            : base(index, stage)
        {
            this.Values = new Dictionary<T, Dictionary<Node, V>>();
            this.Update = aggregate;
        }
    }

    // a streaming state machine vertex, responding to incoming values with a state transition and an output message.
    internal class NodeBinaryStateMachine<V, S, M, T> : BinaryVertex<NodeWithValue<V>, NodeWithValue<S>, NodeWithValue<M>, T>
        where T : Time<T>
    {
        private S[] State;
        private readonly Func<V, S, Pair<S, M>> Transition;

        private readonly int Parts;

        private List<NodeWithValue<V>> ToProcess;

        private readonly S defaultValue;

        public override void OnReceive1(Message<NodeWithValue<V>, T> message)
        {
            if (this.ToProcess != null)
            {
                for (int i = 0; i < message.length; i++)
                    this.ToProcess.Add(message.payload[i]);

                this.NotifyAt(message.time);
            }
            else
            {
                var output = this.Output.GetBufferForTime(message.time);
                for (int i = 0; i < message.length; i++)
                {
                    var record = message.payload[i];
                    var localIndex = record.node.index / this.Parts;

                    // we may need to grow the state vector
                    if (localIndex >= this.State.Length)
                    {
                        var newState = new S[Math.Max(2 * this.State.Length, localIndex + 1)];
                        for (int j = 0; j < this.State.Length; j++)
                            newState[j] = this.State[j];

                        for (int j = this.State.Length; j < newState.Length; j++)
                            newState[j] = defaultValue;

                        this.State = newState;
                    }

                    var transition = this.Transition(record.value, this.State[localIndex]);

                    this.State[localIndex] = transition.First;
                    output.Send(record.node.WithValue(transition.Second));
                }
            }
        }

        public override void OnReceive2(Message<NodeWithValue<S>, T> message)
        {
            for (int i = 0; i < message.length; i++)
            {
                var index = message.payload[i].node.index;
                if (index >= this.State.Length)
                {
                    var newState = new S[Math.Max(index + 1, 2 * this.State.Length)];
                    var newValid = new bool[newState.Length];

                    for (int j = 0; j < this.State.Length; j++)
                        newState[j] = this.State[j];

                    for (int j = this.State.Length; j < newState.Length; j++)
                        newState[j] = this.defaultValue;

                    this.State = newState;
                }

                this.State[message.payload[i].node.index / this.Parts] = message.payload[i].value;
            }
        }

        public override void OnNotify(T time)
        {
            var list = this.ToProcess;
            this.ToProcess = null;

            var output = this.Output.GetBufferForTime(time);
            for (int i = 0; i < list.Count; i++)
            {
                var record = list[i];
                var localIndex = record.node.index / this.Parts;

                // we may need to grow the state vector
                if (localIndex >= this.State.Length)
                {
                    var newState = new S[Math.Max(2 * this.State.Length, localIndex + 1)];
                    for (int j = 0; j < this.State.Length; j++)
                        newState[j] = this.State[j];

                    for (int j = this.State.Length; j < newState.Length; j++)
                        newState[j] = defaultValue;

                    this.State = newState;
                }

                var transition = this.Transition(record.value, this.State[localIndex]);

                this.State[localIndex] = transition.First;
                output.Send(record.node.WithValue(transition.Second));
            }
        }

        public NodeBinaryStateMachine(int index, Naiad.Dataflow.Stage<T> vertex, Func<V, S, Pair<S, M>> transition, S defaultValue)
            : base(index, vertex)
        {
            this.State = new S[] { };
            this.ToProcess = new List<NodeWithValue<V>>();
            this.Transition = transition;
            this.Parts = vertex.Placement.Count;
            this.defaultValue = defaultValue;
        }
    }

    // a streaming state machine vertex, responding to incoming values with a state transition and an output message.
    internal class NodeUnaryStateMachine<V, S, M, T> : UnaryVertex<NodeWithValue<V>, NodeWithValue<M>, T>
        where T : Time<T>
    {
        private S[] State;
        private readonly Func<V, S, Pair<S, M>> Transition;

        private readonly int Parts;
        private readonly S defaultState;

        public override void OnReceive(Message<NodeWithValue<V>, T> message)
        {
            var output = this.Output.GetBufferForTime(message.time);
            for (int i = 0; i < message.length; i++)
            {
                var record = message.payload[i];
                var localIndex = record.node.index / this.Parts;

                if (this.State.Length <= localIndex)
                {
                    var newState = new S[Math.Max(localIndex + 1, 2 * this.State.Length)];
                    for (int j = 0; j < this.State.Length; j++)
                        newState[j] = this.State[j];

                    for (int j = this.State.Length; j < newState.Length; j++)
                        newState[j] = defaultState;

                    this.State = newState;
                }

                var transition = this.Transition(record.value, this.State[localIndex]);

                this.State[localIndex] = transition.First;

                output.Send(record.node.WithValue(transition.Second));
            }
        }

        public NodeUnaryStateMachine(int index, Naiad.Dataflow.Stage<T> vertex, Func<V, S, Pair<S, M>> transition, S defaultState)
            : base(index, vertex)
        {
            this.State = new S[] { };

            this.Transition = transition;

            this.Parts = vertex.Placement.Count;
            this.defaultState = defaultState;
        }
    }

    // vertex managing a CompactGraph fragment, processing corresponding values by applying a reducer.
    internal class GraphJoin<S, T> : BinaryVertex<CompactGraph, NodeWithValue<S>, NodeWithValue<S>, T>
        where T : Time<T>
    {
        CompactGraph Graph;

        List<NodeWithValue<S>> ToProcess;

        public override void OnReceive1(Message<CompactGraph, T> message)
        {
            for (int i = 0; i < message.length; i++)
                this.Graph = message.payload[i];

            this.NotifyAt(message.time);
        }

        public override void OnReceive2(Message<NodeWithValue<S>, T> message)
        {
            if (this.Graph.nodes == null && message.length > 0)
            {
                for (int i = 0; i < message.length; i++)
                    this.ToProcess.Add(message.payload[i]);
            }
            else
            {
                var output = this.Output.GetBufferForTime(message.time);
                for (int i = 0; i < message.length; i++)
                {
                    var record = message.payload[i];
                    var localName = record.node.index / this.Stage.Placement.Count;

                    if (localName + 1 < this.Graph.nodes.Length)
                        for (int j = this.Graph.nodes[localName]; j < this.Graph.nodes[localName + 1]; j++)
                            output.Send(this.Graph.edges[j].WithValue(record.value));
                }
            }
        }

        public override void OnNotify(T time)
        {
            var output = this.Output.GetBufferForTime(time);
            foreach (var record in this.ToProcess.AsEnumerable())
            {
                var localName = record.node.index / this.Stage.Placement.Count;
                if (localName + 1 < this.Graph.nodes.Length)
                    for (int j = this.Graph.nodes[localName]; j < this.Graph.nodes[localName + 1]; j++)
                        output.Send(this.Graph.edges[j].WithValue(record.value));
            }

            this.ToProcess = new List<NodeWithValue<S>>();
        }

        public GraphJoin(int index, Stage<T> vertex)
            : base(index, vertex)
        {
            this.Graph = new CompactGraph();
            this.ToProcess = new List<NodeWithValue<S>>();

            this.Entrancy = 5;
        }
    }

    // dense list of edge destinations and node offsets.
    internal struct CompactGraph
    {
        public readonly Int32[] nodes;  // offsets in this.edges
        public readonly Node[] edges;  // names of target nodes

        public CompactGraph(Int32[] n, Node[] e) { this.nodes = n; this.edges = e; }
    }

    // vertex managing the compaction of a graph, taking a stream of edge pairs to a single CompactGraph.
    internal class GraphCompactor<T> : UnaryVertex<Edge, CompactGraph, T>
        where T : Time<T>
    {
        private readonly Dictionary<T, CompactGraphBuilder> Edges = new Dictionary<T, CompactGraphBuilder>();

        public override void OnReceive(Message<Edge, T> message)
        {
            // Console.Error.WriteLine("Compactor received {0} edges", message.length);
            if (!this.Edges.ContainsKey(message.time))
            {
                this.Edges.Add(message.time, new CompactGraphBuilder());
                this.NotifyAt(message.time);
            }

            var builder = this.Edges[message.time];
            for (int i = 0; i < message.length; i++)
                builder.AddEdge(message.payload[i]);

            this.Edges[message.time] = builder;
        }

        public override void OnNotify(T time)
        {
            if (this.Edges.ContainsKey(time))
            {
                this.Output.GetBufferForTime(time).Send(this.Edges[time].Build(this.Stage.Placement.Count));
                this.Edges.Remove(time);
            }
        }

        public GraphCompactor(int index, Stage<T> vertex)
            : base(index, vertex)
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
    internal class Densifier<S> : UnaryVertex<S, NodeWithValue<S>, Epoch>
    {
        HashSet<S> HashSet;
        int Parts;

        public override void OnReceive(Message<S, Epoch> message)
        {
            var output = this.Output.GetBufferForTime(message.time);
            for (int i = 0; i < message.length; i++)
                if (HashSet.Add(message.payload[i]))
                    output.Send(new Node(this.VertexId + this.Parts * (this.HashSet.Count - 1)).WithValue(message.payload[i]));

            // notify for garbage collection.
            this.NotifyAt(message.time);
        }

        public override void OnNotify(Epoch time)
        {
            HashSet = new HashSet<S>();
        }

        public Densifier(int index, Stage<Epoch> vertex)
            : base(index, vertex)
        {
            this.HashSet = new HashSet<S>();
            this.Parts = this.Stage.Placement.Count;
        }
    }

    // joins on keys of type S to look up integer names provided as input.
    internal class Renamer<TInput, TIdentifier> : BinaryVertex<TInput, NodeWithValue<TIdentifier>, NodeWithValue<TInput>, Epoch>
    {
        private Dictionary<TIdentifier, Node> Rename = new Dictionary<TIdentifier, Node>();
        private Dictionary<TIdentifier, List<TInput>> delayed = new Dictionary<TIdentifier, List<TInput>>();

        Func<TInput, TIdentifier> oldName;

        Int64 enqueuedWork;
        Int64 processedWork;
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

                if (Rename.ContainsKey(name))
                {
                    output.Send(Rename[name].WithValue(record));
                }
                else
                {
                    if (!this.delayed.ContainsKey(name))
                        this.delayed.Add(name, new List<TInput>());

                    this.delayed[name].Add(message.payload[i]);

                    enqueuedWork++;

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
                this.Rename.Add(message.payload[i].value, message.payload[i].node);
                if (this.delayed.ContainsKey(message.payload[i].value))
                {
                    var list = this.delayed[message.payload[i].value];
                    this.delayed.Remove(message.payload[i].value);

                    processedWork += list.Count;

                    var output = this.Output.GetBufferForTime(message.time);
                    for (int j = 0; j < list.Count; j++)
                        output.Send(message.payload[i].node.WithValue(list[j]));
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
                if (Rename.ContainsKey(pair.Key))
                    for (int j = 0; j < pair.Value.Count; j++)
                        output.Send(Rename[pair.Key].WithValue(pair.Value[j]));
            }

            this.Rename = new Dictionary<TIdentifier,Node>();
            this.delayed = new Dictionary<TIdentifier,List<TInput>>();
        }

        public Renamer(int index, Stage<Epoch> vertex, Func<TInput, TIdentifier> oldN)
            : base(index, vertex)
        {
            this.oldName = oldN;
        }
    }
}