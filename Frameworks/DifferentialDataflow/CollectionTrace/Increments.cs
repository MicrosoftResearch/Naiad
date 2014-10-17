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
using System.Linq;
using System.Text;

namespace Microsoft.Research.Naiad.Frameworks.DifferentialDataflow.CollectionTrace
{

    internal struct CollectionTraceWithHeapIncrement : IEquatable<CollectionTraceWithHeapIncrement>
    {
        public OffsetLength OffsetLength;
        public int TimeIndex;

        public bool IsEmpty { get { return OffsetLength.IsEmpty; } }

        public bool Equals(CollectionTraceWithHeapIncrement other)
        {
            return this.OffsetLength.Equals(other.OffsetLength) && this.TimeIndex == other.TimeIndex;
        }

        public CollectionTraceWithHeapIncrement(OffsetLength ol, int t) { OffsetLength = ol; TimeIndex = t; }
        public CollectionTraceWithHeapIncrement(int t) { OffsetLength = new OffsetLength(); TimeIndex = t; }
    }

    internal struct CollectionTraceWithAggregationIncrement<S> : IEquatable<CollectionTraceWithAggregationIncrement<S>>
        where S : IEquatable<S>
    {
        public Int64 Weight;
        public int TimeIndex;
        public S Value;

        public bool IsEmpty(Func<S, bool> isZero) { return Weight == 0 && isZero(Value); }

        public CollectionTraceWithAggregationIncrement<S> Add(CollectionTraceWithAggregationIncrement<S> that, Func<Int64, S, S, S> axpy, int scale = 1)
        {
            this.Weight += scale * that.Weight;
            this.Value = axpy(scale, that.Value, this.Value);
            return this;
        }

        public bool Equals(CollectionTraceWithAggregationIncrement<S> other)
        {
            return this.Weight == other.Weight && this.TimeIndex == other.TimeIndex && this.Value.Equals(other.Value);
        }

        public CollectionTraceWithAggregationIncrement(Int64 w, int t, S v) { Weight = w; TimeIndex = t; Value = v; }
        public CollectionTraceWithAggregationIncrement(int t) { Weight = 0; TimeIndex = t; Value = default(S); }
    }

    internal struct CollectionTraceWithoutHeapIncrement : IEquatable<CollectionTraceWithoutHeapIncrement>
    {
        public Int64 Weight;
        public int TimeIndex;

        public bool IsEmpty { get { return Weight == 0; } }

        public bool Equals(CollectionTraceWithoutHeapIncrement other)
        {
            return this.Weight == other.Weight && this.TimeIndex == other.TimeIndex;
        }

        public CollectionTraceWithoutHeapIncrement(Int64 w, int t) { Weight = w; TimeIndex = t; }
        public CollectionTraceWithoutHeapIncrement(int t) { Weight = 0; TimeIndex = t; }
    }

}
