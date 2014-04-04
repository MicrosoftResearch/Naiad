/*
 * Naiad ver. 0.3
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
using Microsoft.Research.Naiad.DataStructures;
using Microsoft.Research.Naiad.Dataflow;

namespace Microsoft.Research.Naiad.Frameworks.DifferentialDataflow.Operators
{
    internal class Aggregate<K, S, T, R, V> : OperatorImplementations.UnaryStatefulOperatorWithAggregation<K, V, S, T, R>
        where S : IEquatable<S>
        where R : IEquatable<R>
        where T : Microsoft.Research.Naiad.Time<T>
        where K : IEquatable<K>
        where V : IEquatable<V>
    {
        Func<K, V, R> reducer;

        protected override void Reduce(K key, UnaryKeyIndices keyIndices, int time)
        {
            collection.Clear();
            inputTrace.EnumerateCollectionAt(keyIndices.processed, time, collection);

            for (int i = 0; i < collection.Count; i++)
                if (collection.Array[i].weight != 0)
                {
                    var result = reducer(key, collection.Array[i].record);
            
                    outputTrace.Introduce(ref outputWorkspace, reducer(key, collection.Array[i].record), 1, time);
                }
        }

        public Aggregate(int i, Stage<T> c, bool inputImmutable, Expression<Func<S, K>> k, Expression<Func<S, V>> v, Expression<Func<Int64, V, V, V>> axpy, Expression<Func<V, bool>> isZero, Expression<Func<K, V, R>> r)
            : base(i, c, inputImmutable, k, v, axpy, isZero)
        {
            reducer = r.Compile();
        }
    }
}
