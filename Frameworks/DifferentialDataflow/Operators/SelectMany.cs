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
using Microsoft.Research.Naiad.Dataflow;
using Microsoft.Research.Naiad.Dataflow.StandardVertices;

namespace Microsoft.Research.Naiad.Frameworks.DifferentialDataflow.Operators
{
    internal class SelectMany<S, T, R> : UnaryVertex<Weighted<S>, Weighted<R>, T>
        where S : IEquatable<S>
        where T : Time<T>
        where R : IEquatable<R>
    {
        protected Func<S, IEnumerable<R>> selector;

        public override void OnReceive(Message<Weighted<S>, T> message)
        {
            var output = this.Output.GetBufferForTime(message.time);
            for (int i = 0; i < message.length; i++)
            {
                var record = message.payload[i];
                foreach (var r in selector(record.record))
                    output.Send(new Weighted<R>(r, record.weight));
            }
        }
        
        public override string ToString()
        {
            return "SelectMany";
        }

        public SelectMany(int index, Stage<T> collection, Expression<Func<S, IEnumerable<R>>> transformation)
            : base(index, collection)
        {
            selector = transformation.Compile();
        }
    }

    internal class SelectManyBatch<S, T, R> : UnaryVertex<Weighted<S>, Weighted<R>, T>
        where S : IEquatable<S>
        where T : Microsoft.Research.Naiad.Time<T>
        where R : IEquatable<R>
    {
        protected Func<S, IEnumerable<ArraySegment<R>>> selector;

        public override void OnReceive(Message<Weighted<S>, T> message)
        {
            var output = this.Output.GetBufferForTime(message.time);
            for (int i = 0; i < message.length; i++)
            {
                var record = message.payload[i];
                foreach (var r in selector(record.record))
                    for (int ii = 0; ii < r.Count; ii++)
                        output.Send(new Weighted<R>(r.Array[r.Offset + i], record.weight));
            }
        }

        public SelectManyBatch(int index, Stage<T> collection, Expression<Func<S, IEnumerable<ArraySegment<R>>>> transformation)
            : base(index, collection)
        {
            selector = transformation.Compile();
        }
    }
}
