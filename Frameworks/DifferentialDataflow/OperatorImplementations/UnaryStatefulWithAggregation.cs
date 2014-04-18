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

using System.Linq.Expressions;

using Microsoft.Research.Naiad.DataStructures;
using Microsoft.Research.Naiad.Dataflow.Channels;
using Microsoft.Research.Naiad.Scheduling;
using Microsoft.Research.Naiad.Frameworks.DifferentialDataflow.CollectionTrace;
using Microsoft.Research.Naiad;
using Microsoft.Research.Naiad.Dataflow;

namespace Microsoft.Research.Naiad.Frameworks.DifferentialDataflow.OperatorImplementations
{
    internal class UnaryStatefulOperatorWithAggregation<K, V, S, T, R> : OperatorImplementations.UnaryStatefulOperator<K, V, S, T, R>
        where K : IEquatable<K>
        where V : IEquatable<V>
        where S : IEquatable<S>
        where T : Time<T>
        where R : IEquatable<R>
    {
        Func<Int64, V, V, V> axpy;
        Func<V, bool> isZero;

        protected override CollectionTraceCheckpointable<V> createInputTrace()
        {
            return new CollectionTraceWithAggregation<V>((i, j) => this.internTable.LessThan(i, j), i => this.internTable.UpdateTime(i), axpy, isZero);
        }

        public UnaryStatefulOperatorWithAggregation(int index, Stage<T> collection, bool immutableInput, Expression<Func<S, K>> k, Expression<Func<S, V>> v, Expression<Func<Int64, V, V, V>> a, Expression<Func<V, bool>> i)
            : base(index, collection, immutableInput, k, v)
        {
            axpy = a.Compile();
            isZero = i.Compile();

            inputTrace = createInputTrace();
        }
    }

}
