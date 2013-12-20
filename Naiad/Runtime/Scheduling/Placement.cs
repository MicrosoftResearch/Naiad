/*
 * Naiad ver. 0.2
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

﻿using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace Naiad.Scheduling
{
    public struct VertexLocation
    {
        public readonly int VertexId;
        public readonly int ProcessId;
        public readonly int ThreadId;

        public VertexLocation(int shardId, int processId, int threadId)
        {
            this.VertexId = shardId;
            this.ProcessId = processId;
            this.ThreadId = threadId;
        }
    }

    public interface Placement : IEnumerable<VertexLocation>, IEquatable<Placement>
    {
        VertexLocation this[int vertexId] { get; }
        int Count { get; }
    }

    public abstract class BasePlacement : Placement
    {
        public abstract VertexLocation this[int shardId] { get; }
        public abstract int Count { get; }
        public abstract IEnumerator<VertexLocation> GetEnumerator();
        public abstract bool Equals(Placement that);

        System.Collections.IEnumerator System.Collections.IEnumerable.GetEnumerator()
        {
            return this.GetEnumerator();
        }
    }

    public class RoundRobinPlacement : BasePlacement
    {
        private readonly int numProcs;
        private readonly int numThreads;

        public RoundRobinPlacement(int numProcs, int numThreads)
        {
            this.numProcs = numProcs;
            this.numThreads = numThreads;
        }

        public override int Count { get { return this.numProcs * this.numThreads; } }
        
        public override VertexLocation this[int shardId]
        {
            get
            {
                return new VertexLocation(shardId, (shardId / this.numThreads) % this.numProcs, shardId % this.numThreads);
            }
        }

        public override IEnumerator<VertexLocation> GetEnumerator()
        {
            for (int i = 0; i < this.numProcs * this.numThreads; ++i)
                yield return this[i];
        }

        public override bool Equals(Placement that)
        {
            RoundRobinPlacement other = that as RoundRobinPlacement;
            return this == other
                || (other != null && this.numProcs == other.numProcs && this.numThreads == other.numThreads);
        }
    }

    public class SingleVertexPlacement : BasePlacement
    {
        private readonly VertexLocation location;

        public SingleVertexPlacement(int processId, int threadId)
        {
            this.location = new VertexLocation(0, processId, threadId);
        }

        public override VertexLocation this[int vertexId] { get { return this.location; } }
        public override int Count { get { return 1; } }

        public override IEnumerator<VertexLocation> GetEnumerator()
        {
            yield return this.location;
        }

        public override bool Equals(Placement that)
        {
            SingleVertexPlacement other = that as SingleVertexPlacement;
            return this == other
                || (other != null && this.location.Equals(other.location));
        }
    }

    public class ExplicitPlacement : BasePlacement
    {

        private readonly VertexLocation[] locations;

        public override VertexLocation this[int shardId]
        {
            get { return this.locations[shardId]; }
        }

        public override int Count
        {
            get { return this.locations.Length; }
        }

        public override IEnumerator<VertexLocation> GetEnumerator()
        {
            return this.locations.AsEnumerable().GetEnumerator();
        }

        public override bool Equals(Placement that)
        {
            return this.locations.SequenceEqual(that);
        }

        public ExplicitPlacement(IEnumerable<VertexLocation> locations)
        {
            this.locations = locations.ToArray();
        }
    }

    public class SingleVertexPerProcessPlacement : BasePlacement
    {
        private readonly int numProcs;
        private readonly int threadId;

        public SingleVertexPerProcessPlacement(int numProcs, int threadId)
        {
            this.numProcs = numProcs;
            this.threadId = threadId;
        }

        public override VertexLocation this[int shardId]
        {
            get { return new VertexLocation(shardId, shardId, this.threadId); }
        }

        public override int Count
        {
            get { return this.numProcs; }
        }

        public override bool Equals(Placement that)
        {
            SingleVertexPerProcessPlacement other = that as SingleVertexPerProcessPlacement;
            return this == other
                || (other != null && this.numProcs == other.numProcs && this.threadId == other.threadId);
        }

        public override IEnumerator<VertexLocation> GetEnumerator()
        {
            for (int i = 0; i < this.Count; ++i)
                yield return this[i];
        }

    }
}
