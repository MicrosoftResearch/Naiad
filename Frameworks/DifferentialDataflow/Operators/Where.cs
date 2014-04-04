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
using Microsoft.Research.Naiad.Dataflow;

namespace Microsoft.Research.Naiad.Frameworks.DifferentialDataflow.Operators
{

    internal class Where<S, T> : Microsoft.Research.Naiad.Frameworks.UnaryVertex<Weighted<S>, Weighted<S>, T>
        //OperatorImplementations.UnaryStatelessOperator<S, S, T>
        where S : IEquatable<S>
        where T : Microsoft.Research.Naiad.Time<T> 
    {
        protected Func<S, bool> predicate;
        
        public override void OnReceive(Message<Weighted<S>, T> message)
        {
            var output = this.Output.GetBufferForTime(message.time);
            for (int i = 0; i < message.length; i++)
                if (predicate(message.payload[i].record))
                    output.Send(message.payload[i]);
        }

        public Where(int index, Stage<T> collection, Expression<Func<S, bool>> pred)
            : base(index, collection)
        {
            predicate = pred.Compile();
        }
    }
}
