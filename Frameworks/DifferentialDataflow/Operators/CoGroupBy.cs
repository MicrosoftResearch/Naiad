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
using System.Linq.Expressions;
using Microsoft.Research.Naiad.DataStructures;
using Microsoft.Research.Naiad.Dataflow;

namespace Microsoft.Research.Naiad.Frameworks.DifferentialDataflow.Operators
{
    internal class CoGroupBy<K, V1, V2, S1, S2, T, R> : OperatorImplementations.ConservativeBinaryStatefulOperator<K, V1, V2, S1, S2, T, R>
        where K : IEquatable<K>
        where V1 : IEquatable<V1>
        where V2 : IEquatable<V2>
        where S1 : IEquatable<S1>
        where S2 : IEquatable<S2>
        where T : Time<T>
        where R : IEquatable<R>
    {
        public Func<K, IEnumerable<Weighted<V1>>, IEnumerable<Weighted<V2>>, IEnumerable<Weighted<R>>> weightedReducer;
        public Func<K, IEnumerable<V1>, IEnumerable<V2>, IEnumerable<R>> reducer;

        protected IEnumerable<Weighted<V1>> EnumerateCollection1()
        {
            for (int i = 0; i < collection1.Count; i++)
                if (collection1.Array[i].weight > 0)
                    yield return collection1.Array[i];
        }

        protected IEnumerable<Weighted<V2>> EnumerateCollection2()
        {
            for (int i = 0; i < collection2.Count; i++)
                if (collection2.Array[i].weight > 0)
                    yield return collection2.Array[i];
        }

        protected override void Reduce(K key, BinaryKeyIndices keyIndices, int time)
        {
            collection1.Clear();
            if (keyIndices.processed1 != 0)
                inputTrace1.EnumerateCollectionAt(keyIndices.processed1, time, collection1);

            collection2.Clear();
            if (keyIndices.processed2 != 0)
                inputTrace2.EnumerateCollectionAt(keyIndices.processed2, time, collection2);

            if (weightedReducer != null)
            {
                if (collection1.Count > 0 || collection2.Count > 0)
                    foreach (var r in weightedReducer(key, EnumerateCollection1(), EnumerateCollection2()))
                        outputTrace.Introduce(ref outputWorkspace, r.record, r.weight, time);
            }
            else
            {
                if (collection1.Count > 0 || collection2.Count > 0)
                {
                    foreach (var r in reducer(key, EnumerateCollection1().SelectMany(x => Enumerable.Repeat(x.record, (int)x.weight)),
                                                   EnumerateCollection2().SelectMany(x => Enumerable.Repeat(x.record, (int)x.weight))))
                        outputTrace.Introduce(ref outputWorkspace, r, 1, time);
                }
            }
        }

        public CoGroupBy(int index,
                         Stage<T> stage, 
                         bool input1Immutable,
                         bool input2Immutable,
                         Expression<Func<S1, K>> k1, 
                         Expression<Func<S2, K>> k2, 
                         Expression<Func<S1, V1>> t1, 
                         Expression<Func<S2, V2>> t2, 
                         Expression<Func<K, IEnumerable<Weighted<V1>>, IEnumerable<Weighted<V2>>, IEnumerable<Weighted<R>>>> r)
            : base(index, stage, input1Immutable, input2Immutable, k1, k2, t1, t2)
        {
            weightedReducer = r.Compile();
        }

        public CoGroupBy(int index,
                         Stage<T> stage,
                         bool input1Immutable,
                         bool input2Immutable,
                         Expression<Func<S1, K>> k1,
                         Expression<Func<S2, K>> k2,
                         Expression<Func<S1, V1>> t1,
                         Expression<Func<S2, V2>> t2,
                         Expression<Func<K, IEnumerable<V1>, IEnumerable<V2>, IEnumerable<R>>> r)
            : base(index, stage, input1Immutable, input2Immutable, k1, k2, t1, t2)
        {
            reducer = r.Compile();
        }
    }
}
