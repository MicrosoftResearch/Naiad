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

namespace Microsoft.Research.Naiad.Dataflow
{
    /// <summary>
    /// Describes the physical location of a dataflow <see cref="Vertex"/>.
    /// </summary>
    public struct VertexLocation
    {
        /// <summary>
        /// The vertex identifier.
        /// </summary>
        public readonly int VertexId;

        /// <summary>
        /// The process on which the vertex with <see cref="VertexId"/> resides.
        /// </summary>
        public readonly int ProcessId;

        /// <summary>
        /// The worker thread on which the vertex with <see cref="VertexId"/> resides.
        /// </summary>
        public readonly int ThreadId;

        /// <summary>
        /// Constructs a new vertex location.
        /// </summary>
        /// <param name="vertexId">The vertex ID.</param>
        /// <param name="processId">The process ID.</param>
        /// <param name="threadId">The worker thread ID.</param>
        public VertexLocation(int vertexId, int processId, int threadId)
        {
            this.VertexId = vertexId;
            this.ProcessId = processId;
            this.ThreadId = threadId;
        }
    }

    /// <summary>
    /// Represents the placement of physical dataflow <see cref="Vertex"/> objects in a <see cref="Stage{TVertex}"/>.
    /// </summary>
    public abstract class Placement : IEnumerable<VertexLocation>, IEquatable<Placement>
    {
        /// <summary>
        /// Returns location information about the vertex with the given ID.
        /// </summary>
        /// <param name="vertexId">The vertex ID.</param>
        /// <returns>Location information about the vertex with the given ID.</returns>
        public abstract VertexLocation this[int vertexId] { get; }

        /// <summary>
        /// The number of vertices.
        /// </summary>
        public abstract int Count { get; }

        /// <summary>
        /// Returns an object for enumerating location information about every vertex in this placement.
        /// </summary>
        /// <returns>An object for enumerating location information about every vertex in this placement.</returns>
        public abstract IEnumerator<VertexLocation> GetEnumerator();

        /// <summary>
        /// Returns <c>true</c> if and only if each vertex in this placement has the same location as the vertex
        /// with the same ID in the <paramref name="other"/> placement, and vice versa.
        /// </summary>
        /// <param name="other">The other placement.</param>
        /// <returns><c>true</c> if and only if each vertex in this placement has the same location as the vertex
        /// with the same ID in the <paramref name="other"/> placement, and vice versa.</returns>
        public abstract bool Equals(Placement other);

        System.Collections.IEnumerator System.Collections.IEnumerable.GetEnumerator()
        {
            return this.GetEnumerator();
        }


        /// <summary>
        /// Round robin placement
        /// </summary>
        internal class RoundRobin : Placement
        {
            private readonly int numProcs;
            private readonly int numThreads;

            /// <summary>
            /// Constructor
            /// </summary>
            /// <param name="numProcs">number of processes</param>
            /// <param name="numThreads">number of threads per process</param>
            public RoundRobin(int numProcs, int numThreads)
            {
                this.numProcs = numProcs;
                this.numThreads = numThreads;
            }

            /// <summary>
            /// Number of workers
            /// </summary>
            public override int Count { get { return this.numProcs * this.numThreads; } }

            /// <summary>
            /// Indexer
            /// </summary>
            /// <param name="vertexId">vertex identifier</param>
            /// <returns></returns>
            public override VertexLocation this[int vertexId]
            {
                get
                {
                    return new VertexLocation(vertexId, (vertexId / this.numThreads) % this.numProcs, vertexId % this.numThreads);
                }
            }

            /// <summary>
            /// Enumerator
            /// </summary>
            /// <returns></returns>
            public override IEnumerator<VertexLocation> GetEnumerator()
            {
                for (int i = 0; i < this.numProcs * this.numThreads; ++i)
                    yield return this[i];
            }

            /// <summary>
            /// Tests equality between placements
            /// </summary>
            /// <param name="that">other placement</param>
            /// <returns></returns>
            public override bool Equals(Placement that)
            {
                RoundRobin other = that as RoundRobin;
                return this == other
                    || (other != null && this.numProcs == other.numProcs && this.numThreads == other.numThreads);
            }
        }


        /// <summary>
        /// Placement with one vertex
        /// </summary>
        internal class SingleVertex : Placement
        {
            private readonly VertexLocation location;

            /// <summary>
            /// Constructor
            /// </summary>
            /// <param name="processId">process identifier for the vertex</param>
            /// <param name="threadId">thread identifier for the vertex</param>
            public SingleVertex(int processId, int threadId)
            {
                this.location = new VertexLocation(0, processId, threadId);
            }

            /// <summary>
            /// Indexer
            /// </summary>
            /// <param name="vertexId">ignored</param>
            /// <returns></returns>
            public override VertexLocation this[int vertexId] { get { return this.location; } }

            /// <summary>
            /// Returns one
            /// </summary>
            public override int Count { get { return 1; } }

            /// <summary>
            /// Enumerator
            /// </summary>
            /// <returns></returns>
            public override IEnumerator<VertexLocation> GetEnumerator()
            {
                yield return this.location;
            }

            /// <summary>
            /// Test equality with another SingleVertexPlacement
            /// </summary>
            /// <param name="that"></param>
            /// <returns></returns>
            public override bool Equals(Placement that)
            {
                SingleVertex other = that as SingleVertex;
                return this == other
                    || (other != null && this.location.Equals(other.location));
            }
        }

        /// <summary>
        /// Represents a <see cref="Placement"/> based on an explicit <see cref="Vertex"/>-to-location mapping.
        /// </summary>
        public class Explicit : Placement
        {

            private readonly VertexLocation[] locations;

            /// <summary>
            /// Returns location information about the vertex with the given ID.
            /// </summary>
            /// <param name="vertexId">The vertex ID.</param>
            /// <returns>Location information about the vertex with the given ID.</returns>
            public override VertexLocation this[int vertexId]
            {
                get { return this.locations[vertexId]; }
            }

            /// <summary>
            /// The number of vertices.
            /// </summary>
            public override int Count
            {
                get { return this.locations.Length; }
            }

            /// <summary>
            /// Returns an object for enumerating location information about every vertex in this placement.
            /// </summary>
            /// <returns>An object for enumerating location information about every vertex in this placement.</returns>
            public override IEnumerator<VertexLocation> GetEnumerator()
            {
                return this.locations.AsEnumerable().GetEnumerator();
            }

            /// <summary>
            /// Returns <c>true</c> if and only if each vertex in this placement has the same location as the vertex
            /// with the same ID in the <paramref name="other"/> placement, and vice versa.
            /// </summary>
            /// <param name="other">The other placement.</param>
            /// <returns><c>true</c> if and only if each vertex in this placement has the same location as the vertex
            /// with the same ID in the <paramref name="other"/> placement, and vice versa.</returns>
            public override bool Equals(Placement other)
            {
                return this.locations.SequenceEqual(other);
            }

            /// <summary>
            /// Constructs explicit placement from a sequence of <see cref="VertexLocation"/> objects.
            /// </summary>
            /// <param name="locations">The explicit locations of each vertex in this placement.</param>
            public Explicit(IEnumerable<VertexLocation> locations)
            {
                this.locations = locations.ToArray();
            }
        }

        /// <summary>
        /// Placement with one vertex per process
        /// </summary>
        internal class SingleVertexPerProcess : Placement
        {
            private readonly int numProcs;
            private readonly int threadId;

            /// <summary>
            /// Constructor
            /// </summary>
            /// <param name="numProcs">number of processes</param>
            /// <param name="threadId">thread index for the vertex</param>
            public SingleVertexPerProcess(int numProcs, int threadId)
            {
                this.numProcs = numProcs;
                this.threadId = threadId;
            }

            /// <summary>
            /// Indexer
            /// </summary>
            /// <param name="vertexId"></param>
            /// <returns></returns>
            public override VertexLocation this[int vertexId]
            {
                get { return new VertexLocation(vertexId, vertexId, this.threadId); }
            }

            /// <summary>
            /// Count
            /// </summary>
            public override int Count
            {
                get { return this.numProcs; }
            }

            /// <summary>
            /// Equals
            /// </summary>
            /// <param name="that">other placement</param>
            /// <returns></returns>
            public override bool Equals(Placement that)
            {
                SingleVertexPerProcess other = that as SingleVertexPerProcess;
                return this == other
                    || (other != null && this.numProcs == other.numProcs && this.threadId == other.threadId);
            }

            /// <summary>
            /// Enumerator
            /// </summary>
            /// <returns></returns>
            public override IEnumerator<VertexLocation> GetEnumerator()
            {
                for (int i = 0; i < this.Count; ++i)
                    yield return this[i];
            }

        }
    }

}
