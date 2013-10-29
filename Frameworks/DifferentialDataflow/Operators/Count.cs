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
using System.Linq.Expressions;
using Naiad.DataStructures;
using Naiad.Dataflow;

namespace Naiad.Frameworks.DifferentialDataflow.Operators
{
    internal class Count<K, S, T, R> : OperatorImplementations.UnaryStatefulOperator<K, K, S, T, R>
        where K : IEquatable<K>
        where S : IEquatable<S>
        where T : Naiad.Time<T>
        where R : IEquatable<R>
    {
        public Func<K, Int64, R> reducer;
        protected override void NewOutputMinusOldOutput(K key, UnaryKeyIndices keyIndices, int timeIndex)
        {
            var newSum = 0L;
            collection.Clear();
            inputTrace.EnumerateCollectionAt(keyIndices.processed, timeIndex, collection);
            for (int i = 0; i < collection.Count; i++)
                newSum += collection.Array[i].weight;

            var oldSum = newSum;
            difference.Clear();
            inputTrace.EnumerateCollectionAt(keyIndices.unprocessed, timeIndex, difference);
            for (int i = 0; i < difference.Count; i++)
                oldSum -= difference.Array[i].weight;

            if (oldSum != newSum)
            {
                if (oldSum > 0)
                    outputTrace.Introduce(ref outputWorkspace, reducer(key, oldSum), -1, timeIndex);
                if (newSum > 0)
                    outputTrace.Introduce(ref outputWorkspace, reducer(key, newSum), +1, timeIndex);
            }
        }

        public Count(int index, Stage<T> collection, bool inputImmutable, Expression<Func<S, K>> k, Expression<Func<K, Int64, R>> r)
            : base(index, collection, inputImmutable, k, k)
        {
            reducer = r.Compile();
        }
    }
}

