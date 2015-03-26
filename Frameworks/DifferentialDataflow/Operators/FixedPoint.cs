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

using System.Collections.Concurrent;
using System.IO;
using Microsoft.Research.Naiad;

namespace Microsoft.Research.Naiad.Frameworks.DifferentialDataflow.Operators.FixedPoint
{
#if false
    internal class Ingress<S, T> : OperatorImplementations.UnaryStatelessOperator<S, T, S, IntPartialLattice<T>>
        where S : IEquatable<S>
        where T : Lattice<T>
    {
        public override void OnRecv(NaiadRecord<S, T> element)
        {
            Send(element.record.ToNaiadRecord(element.weight, new IntPartialLattice<T>(element.time, 1)));
        }

        public Ingress(int index, UnaryCollectionVertex<S, T, S, IntPartialLattice<T>> collection)
            : base(index, collection, x => new IntPartialLattice<T>(x, 1))
        {
        }
    }

    internal class Egress<S, T> : OperatorImplementations.UnaryStatelessOperator<S, IntPartialLattice<T>, S, T>
        where S : IEquatable<S>
        where T : Lattice<T>
    {
        public override void OnRecv(NaiadRecord<S, IntPartialLattice<T>> element)
        {
            Send(element.record.ToNaiadRecord(element.weight, element.time.s));
        }

        public Egress(int index, UnaryCollectionVertex<S, IntPartialLattice<T>, S, T> collection)
            : base(index, collection, x => x.s)
        {
        }
    }

    internal class Advance<S, T> : OperatorImplementations.UnaryStatelessOperator<S, IntPartialLattice<T>, S, IntPartialLattice<T>>
        where S : IEquatable<S>
        where T : Lattice<T>
    {
        Func<S, int, int> newIteration;
        int maxIterations;

        public override void OnRecv(NaiadRecord<S, IntPartialLattice<T>> element)
        {
            var newIter = newIteration(element.record, element.time.t);

            element.time.t = Math.Max(element.time.t, newIter);

            if (element.time.t < maxIterations)
                Send(element);
        }

        public Advance(int index, UnaryCollectionVertex<S, IntPartialLattice<T>, S, IntPartialLattice<T>> collection, Func<S, int, int> nIter, int mi)
            : base(index, collection, x => new IntPartialLattice<T>(x.s, x.t + 1))
        {
            newIteration = nIter;
            maxIterations = mi;
        }
    }

#endif

#if false
    internal class Feedback<S, T> : OperatorImplementations.BinaryStatelessOperator<S, S, S, IntPartialLattice<T>>
        where S : IEquatable<S>
        where T : Lattice<T>
    {
        public override void OnRecv1(NaiadRecord<S, IntPartialLattice<T>> element)
        {
            Send(element);
        }

        public override void OnRecv2(NaiadRecord<S, IntPartialLattice<T>> element)
        {
            Send(element);
        }

        public Feedback(int index, BinaryCollectionVertex<S, S, S, IntPartialLattice<T>> collection)
            : base(index, collection)
        {
        }
    }
#endif
}
