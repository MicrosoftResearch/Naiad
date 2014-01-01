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

using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Linq.Expressions;
using Naiad;
using Naiad.Dataflow;

namespace Naiad.Frameworks.DifferentialDataflow.Operators
{
    internal class SumInt32<K, S, T, R> : Aggregate<K, S, T, R, int>
        where K : IEquatable<K>
        where S : IEquatable<S>
        where T : Naiad.Time<T>
        where R : IEquatable<R>
    {
        public SumInt32(int index, Stage<T> collection, bool inputImmutable, Expression<Func<S, K>> k, Expression<Func<S, int>> v, Expression<Func<K, int, R>> r)
            : base(index, collection, inputImmutable, k, v, (Int64 a, int x, int y) => (int)(a * x + y), x => x == 0, r)
        {
        }
    }

    internal class SumInt64<K, S, T, R> : Aggregate<K, S, T, R, Int64>
        where K : IEquatable<K>
        where S : IEquatable<S>
        where T : Time<T>
        where R : IEquatable<R>
    {
        public SumInt64(int index, Stage<T> collection, bool inputImmutable, Expression<Func<S, K>> k, Expression<Func<S, Int64>> v, Expression<Func<K, Int64, R>> r)
            : base(index, collection, inputImmutable, k, v, (a, x, y) => a * x + y, x => x == 0, r)
        {
        }
    }

    internal class SumFloat<K, S, T, R> : Aggregate<K, S, T, R, float>
        where K : IEquatable<K>
        where S : IEquatable<S>
        where T : Time<T>
        where R : IEquatable<R>
    {
        public SumFloat(int index, Stage<T> collection, bool inputImmutable, Expression<Func<S, K>> k, Expression<Func<S, float>> v, Expression<Func<K, float, R>> r)
            : base(index, collection, inputImmutable, k, v, (a, x, y) => a * x + y, x => x == 0.0, r)
        {
        }
    }

    internal class SumDouble<K, S, T, R> : Aggregate<K, S, T, R, double>
        where K : IEquatable<K>
        where S : IEquatable<S>
        where T : Time<T>
        where R : IEquatable<R>
    {
        public SumDouble(int index, Stage<T> collection, bool inputImmutable, Expression<Func<S, K>> k, Expression<Func<S, double>> v, Expression<Func<K, double, R>> r)
            : base(index, collection, inputImmutable, k, v, (a, x, y) => a * x + y, x => x == 0.0, r)
        {
        }
    }
}
