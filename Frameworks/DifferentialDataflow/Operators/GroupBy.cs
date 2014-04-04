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

using System.Collections.Concurrent;
using System.Linq.Expressions;
using Microsoft.Research.Naiad.DataStructures;
using Microsoft.Research.Naiad.Dataflow;

namespace Microsoft.Research.Naiad.Frameworks.DifferentialDataflow.Operators
{
    internal class GroupBy<K, V, S, T, R> : OperatorImplementations.ConservativeUnaryStatefulOperator<K, V, S, T, R>
        where K : IEquatable<K>
        where V : IEquatable<V>
        where S : IEquatable<S>
        where T : Microsoft.Research.Naiad.Time<T>
        where R : IEquatable<R>
    {
        public Func<K, IEnumerable<V>, IEnumerable<R>> reducer;

        protected IEnumerable<V> EnumerateCollection()
        {
            for (int i = 0; i < collection.Count; i++)
                for (int j = 0; j < collection.Array[i].weight; j++)
                    yield return collection.Array[i].record;
        }

        protected override void Reduce(K key, UnaryKeyIndices keyIndices, int time)
        {
            collection.Clear();
            inputTrace.EnumerateCollectionAt(keyIndices.processed, time, collection);

            if (collection.Count > 0)
                foreach (var r in reducer(key, EnumerateCollection()))
                    outputTrace.Introduce(ref outputWorkspace, r, 1, time);
        }

        public GroupBy(int index, Stage<T> collection, bool immutableInput, Expression<Func<S, K>> k, Expression<Func<S, V>> v, Func<K, IEnumerable<V>, IEnumerable<R>> r)
            : base(index, collection, immutableInput, k, v)
        {
            reducer = r;
        }
    }
    
}
