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
 * THIS CODE IS PROVIDED *AS IS* BASIS, WITHOUT WARRANTIES OR CONDITIONS
 * OF ANY KIND, EITHER EXPRESS OR IMPLIED, INCLUDING WITHOUT LIMITATION 
 * ANY IMPLIED WARRANTIES OR CONDITIONS OF TITLE, FITNESS FOR A
 * PARTICULAR PURPOSE, MERCHANTABLITY OR NON-INFRINGEMENT.
 *
 * See the Apache Version 2.0 License for specific language governing
 * permissions and limitations under the License.
 */

/*
 * Copyright 2013 Microsoft Corporation
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

using Naiad.Dataflow;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;
using System.Text;
using System.Threading.Tasks;

namespace Naiad.CodeGeneration
{
    class MessageUtils
    {

        public static Expression<Action<Message<T>>> Vectorize<T>(Expression<Action> prelude, Expression<Action<T>> elementAction, Expression<Action> postlude)
        {
            Expression i = Expression.Variable(typeof(int), i);
            Expression message = Expression.Parameter(typeof(Message<T>), "message");
            LabelTarget postludeLabel = Expression.Label("postlude")


            return Expression.Block(prelude,
                Expression.Assign(i, Expression.Constant(0))
                Expression.Loop(Expression.Block(
                    Expression.IfThenElse(Expression.LessThan(i, Expression.Field(message, "length")),
                    Expression.Block(),
                    Expression.Break(postludeLabel))
                    )),
                postludeLabel,
                postlude);
        }

    }
}
