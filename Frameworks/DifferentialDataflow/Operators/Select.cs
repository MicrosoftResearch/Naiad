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
using Naiad.Dataflow;

namespace Naiad.Frameworks.DifferentialDataflow.Operators
{
    internal class Select<S, T, R> : Naiad.Frameworks.UnaryVertex<Weighted<S>, Weighted<R>, T>
        where S : IEquatable<S>
        where T : Naiad.Time<T>
        where R : IEquatable<R>
    {
        public Func<S, R> selector;

        public override void MessageReceived(Message<Pair<Weighted<S>, T>> message)
        {
            for (int i = 0; i < message.length; i++)
                this.Output.Send(this.selector(message.payload[i].v1.record).ToWeighted(message.payload[i].v1.weight), message.payload[i].v2);
        }

        public Select(int index, Stage<T> collection, Expression<Func<S, R>> transformation)
            : base(index, collection)
        {
            selector = transformation.Compile();
        }        
    }
}
