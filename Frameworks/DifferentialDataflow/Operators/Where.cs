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

using System.Collections.Concurrent;
using System.Linq.Expressions;
using Naiad.Dataflow;

namespace Naiad.Frameworks.DifferentialDataflow.Operators
{

    internal class Where<S, T> : Naiad.Frameworks.UnaryVertex<Weighted<S>, Weighted<S>, T>
        //OperatorImplementations.UnaryStatelessOperator<S, S, T>
        where S : IEquatable<S>
        where T : Naiad.Time<T> 
    {
        protected Func<S, bool> predicate;
        
        public override void MessageReceived(Naiad.Dataflow.Message<Naiad.Pair<Weighted<S>, T>> elements)
        {
            for (int i = 0; i < elements.length; i++)
            {
                if (predicate(elements.payload[i].v1.record))
                {
                    this.Output.Buffer.payload[this.Output.Buffer.length++] = elements.payload[i];

                    if (this.Output.Buffer.length == this.Output.Buffer.payload.Length)
                    {
                        this.Output.Send(this.Output.Buffer);
                        this.Output.Buffer.length = 0;
                    }
                }
            }
        }

        public Where(int index, Stage<T> collection, Expression<Func<S, bool>> pred)
            : base(index, collection)
        {
            predicate = pred.Compile();
        }
    }
}
