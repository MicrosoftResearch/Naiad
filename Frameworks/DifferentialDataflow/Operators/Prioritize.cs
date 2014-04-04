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
using Microsoft.Research.Naiad;

namespace Microsoft.Research.Naiad.Frameworks.DifferentialDataflow.Operators.Prioritize
{
#if false
    internal class Ingress<S, T> : OperatorImplementations.UnaryStatelessOperator<S, T, S, Naiad.IntTotalLattice<T>>
        where S : IEquatable<S> 
        where T : Naiad.Lattice<T>
    {
        Func<S,int> priorityFunction;

        public override void OnRecv(NaiadRecord<S, T> element)
        {
            Send(element.record.ToNaiadRecord(element.weight, new IntTotalLattice<T>(element.time, priorityFunction(element.record))));
        }

        public Ingress(int index, UnaryCollectionVertex<S,S,IntTotalLattice<T>> collection, Func<S,int> p)
            : base(index, collection, x => new IntTotalLattice<T>(x, 0))
        {            
            priorityFunction = p;
        }
    }

    internal class Egress<S, T> : OperatorImplementations.UnaryStatelessOperator<S, IntTotalLattice<T>, S, T>
        where S : IEquatable<S>
        where T : Naiad.Lattice<T>
    {
        public override void OnRecv(NaiadRecord<S, IntTotalLattice<T>> element)
        {
            Send(element.record.ToNaiadRecord(element.weight, element.time.s));
        }
        
        public Egress(int index, UnaryCollectionVertex<S, S, T> collection)
            : base(index, collection, x => x.s)
        {
        }
    }
#endif
}
