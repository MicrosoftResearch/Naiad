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
using Naiad.DataStructures;
using Naiad.Dataflow;

namespace Naiad.Frameworks.DifferentialDataflow.Operators
{
    internal class Max<K, V, S, T> : OperatorImplementations.UnaryStatefulOperator<K, V, S, T, S>
        where K : IEquatable<K>
        where V : IEquatable<V>
        where S : IEquatable<S>
        where T : Naiad.Time<T>
    {
        Func<K, V, int> valueSelector;
        Func<K, V, S> resultSelector;

        protected override void Reduce(K key, UnaryKeyIndices keyIndices, int timeIndex)
        {
            var maxFound = false;
            var maxValue = Int32.MaxValue;
            var maxEntry = default(V);

            collection.Clear();
            inputTrace.EnumerateCollectionAt(keyIndices.processed, timeIndex, collection);
            //while (collection.AdvanceToValid())
            //foreach (var element in inputTrace.EnumerateCollectionAt(keyIndices.processed, timeIndex))
            for (int i = 0; i < collection.Count; i++)
            {
                var element = collection.Array[i];

                if (element.weight > 0)
                {
                    var value = valueSelector(key, element.record);
                    if (maxValue < value || maxFound == false)
                    {
                        maxFound = true;
                        maxValue = value;
                        maxEntry = element.record;
                    }
                }
            }

            if (maxFound)
                outputTrace.Introduce(ref outputWorkspace, resultSelector(key, maxEntry), 1, timeIndex);
        }

        public Max(int index, Stage<T> collection, bool inputImmutable, Expression<Func<S, K>> k, Expression<Func<S, V>> e, Expression<Func<K, V, int>> v, Expression<Func<K, V, S>> r)
            : base(index, collection, inputImmutable, k, e)
        {
            valueSelector = v.Compile();
            resultSelector = r.Compile();
        }
    }

}
