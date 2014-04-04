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
using Microsoft.Research.Naiad;
using Microsoft.Research.Naiad.Dataflow;

namespace Microsoft.Research.Naiad.Frameworks.DifferentialDataflow.Operators
{
#if true
    internal class Min<K, V, M, S, T> : OperatorImplementations.ConservativeUnaryStatefulOperator<K, V, S, T, S>
        where K : IEquatable<K>
        where V : IEquatable<V>
        where S : IEquatable<S>
        where M : IComparable<M>
        where T : Microsoft.Research.Naiad.Time<T>
    {
        Func<K, V, M> valueSelector;
        Func<K, V, S> resultSelector;

        protected override void Reduce(K key, UnaryKeyIndices keyIndices, int timeIndex)
        {
            var minFound = false;
            var minValue = default(M);
            var minEntry = default(V);

            collection.Clear();
            inputTrace.EnumerateCollectionAt(keyIndices.processed, timeIndex, collection);

            for (int i = 0; i < collection.Count; i++)
            {
                var element = collection.Array[i];

                if (element.weight > 0)
                {
                    var value = valueSelector(key, element.record);
                    if (minFound == false || minValue.CompareTo(value) > 0)
                    {
                        minFound = true;
                        minValue = value;
                        minEntry = element.record;
                    }
                }
            }

            if (minFound)
                outputTrace.Introduce(ref outputWorkspace, resultSelector(key, minEntry), 1, timeIndex);
        }

        public Min(int index, Stage<T> collection, bool inputImmutable, Expression<Func<S, K>> k, Expression<Func<S, V>> e, Expression<Func<K, V, M>> v, Expression<Func<K, V, S>> r)
            : base(index, collection, inputImmutable, k, e)
        {
            valueSelector = v.Compile();
            resultSelector = r.Compile();
        }
    }
#else
    internal class Min<K, V, M, S, T> : OperatorImplementations.UnaryStatefulOperator<K, V, S, T, S>
        where K : IEquatable<K>
        where V : IEquatable<V>
        where S : IEquatable<S>
        where M : IEquatable<M>, IComparable<M>
        where T : Naiad.Time<T>
    {
        Func<K, V, M> minBySelector;
        Func<K, V, S> resultSelector;

        protected override void NewOutputMinusOldOutput(K key, UnaryKeyIndices keyIndices, int timeIndex)
        {
            var oldFound = false;
            var oldValue = default(M);
            var oldEntry = default(S);
            var oldWeight = 0L;

            toSend.Clear();
            outputTrace.EnumerateCollectionAt(keyIndices.output, timeIndex, toSend);
            for (int i = 0; i < toSend.Count; i++)
            {
                oldFound = true;
                oldEntry = toSend.Array[i].record;
                oldWeight = toSend.Array[i].weight;
                oldValue = minBySelector(key, value(oldEntry));   // something to be non-maxvalue
            }


            var minFound = false;
            var minValue = default(M);
            var minEntry = default(V);

            var newEntry = default(S);

            var minStable = true;
            difference.Clear();
            inputTrace.EnumerateCollectionAt(keyIndices.unprocessed, timeIndex, difference);
            for (int i = 0; i < difference.Count; i++)
            {
                var entry = difference.Array[i];
                if (entry.weight != 0)
                {
                    var value = minBySelector(key, entry.record);
                    if (((value.CompareTo(oldValue) < 0 || !oldFound) && entry.weight > 0) || (value.Equals(oldValue) && entry.weight <= 0))
                        minStable = false;
                }
            }

            // only want to do this if we really need to.
            if (!minStable)
            {
                collection.Clear();
                inputTrace.EnumerateCollectionAt(keyIndices.processed, timeIndex, collection);
                for (int i = 0; i < collection.Count; i++)
                {
                    var element = collection.Array[i];

                    if (element.weight > 0)
                    {
                        var value = minBySelector(key, element.record);

                        if (minValue.CompareTo(value) > 0 || minFound == false)
                        {
                            minFound = true;
                            minValue = value;
                            minEntry = element.record;
                        }
                    }
                }

                if (minFound)
                    newEntry = resultSelector(key, minEntry);

                // if they are the same record, and both going to be output, we don't need to produce them.
                if (!(newEntry.Equals(oldEntry) && oldFound && minFound))
                {
                    if (oldFound)
                        outputTrace.Introduce(ref outputWorkspace, oldEntry, -1, timeIndex);
                    if (minFound)
                        outputTrace.Introduce(ref outputWorkspace, newEntry, +1, timeIndex);
                }
            }
        }

        public Min(int index, Stage<T> collection, bool inputImmutable, Expression<Func<S, K>> k, Expression<Func<S, V>> e, Expression<Func<K, V, M>> v, Expression<Func<K, V, S>> r)
            : base(index, collection, inputImmutable, k, e)
        {
            minBySelector = v.Compile();
            resultSelector = r.Compile();
        }
    }
#endif
    internal class MinIntKeyed<V, M, S, T> : OperatorImplementations.UnaryStatefulIntKeyedOperator<V, S, T, S>
        where V : IEquatable<V>
        where M : IEquatable<M>, IComparable<M>
        where S : IEquatable<S>
        where T : Time<T>
    {
        Func<int, V, M> valueSelector;
        Func<int, V, S> resultSelector;
#if true
        protected override void NewOutputMinusOldOutput(int index, UnaryKeyIndices keyIndices, int timeIndex)
        {
            var key = index * this.Stage.Placement.Count + this.VertexId;

            var oldFound = false;
            var oldValue = default(M);
            var oldEntry = default(S);
            var oldWeight = 0L;

            toSend.Clear();
            outputTrace.EnumerateCollectionAt(keyIndices.output, timeIndex, toSend);
            for (int i = 0; i < toSend.Count; i++)
            {
                oldFound = true;
                oldEntry = toSend.Array[i].record;
                oldWeight = toSend.Array[i].weight;
                oldValue = valueSelector(key, value(oldEntry));   // something to be non-maxvalue
            }


            var minFound = false;
            var minValue = default(M);
            var minEntry = default(V);

            var newEntry = default(S);

            var minStable = true;
            difference.Clear();
            inputTrace.EnumerateCollectionAt(keyIndices.unprocessed, timeIndex, difference);
            for (int i = 0; i < difference.Count; i++)
            {
                var entry = difference.Array[i];
                if (entry.weight != 0)
                {
                    var value = valueSelector(key, entry.record);
                    if (((value.CompareTo(oldValue) < 0 || !oldFound)&& entry.weight > 0) || (value.Equals(oldValue) && entry.weight <= 0))
                        minStable = false;
                }
            }

            // only want to do this if we really need to.
            if (!minStable)
            {
                collection.Clear();
                inputTrace.EnumerateCollectionAt(keyIndices.processed, timeIndex, collection);
                for (int i = 0; i < collection.Count; i++)
                {
                    var element = collection.Array[i];

                    if (element.weight > 0)
                    {
                        var value = valueSelector(key, element.record);
                        if (minValue.CompareTo(value) > 0 || minFound == false)
                        {
                            minFound = true;
                            minValue = value;
                            minEntry = element.record;
                        }
                    }
                }

                if (minFound)
                    newEntry = resultSelector(key, minEntry);

                // if they are the same record, and both going to be output, we don't need to produce them.
                if (!(newEntry.Equals(oldEntry) && oldFound && minFound))
                {
                    if (oldFound)
                        outputTrace.Introduce(ref outputWorkspace, oldEntry, -1, timeIndex);
                    if (minFound)
                        outputTrace.Introduce(ref outputWorkspace, newEntry, +1, timeIndex);
                }
            }
        }
#endif

        public MinIntKeyed(int index, Stage<T> collection, bool inputImmutable, Expression<Func<S, int>> k, Expression<Func<S, V>> e, Expression<Func<int, V, M>> v, Expression<Func<int, V, S>> r)
            : base(index, collection, inputImmutable, k, e)
        {
            valueSelector = v.Compile();
            resultSelector = r.Compile();
        }
    }
}
