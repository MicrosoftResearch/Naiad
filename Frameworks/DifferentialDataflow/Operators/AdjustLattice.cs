/*
 * Naiad ver. 0.5
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

using Microsoft.Research.Naiad.Dataflow;
using Microsoft.Research.Naiad.Dataflow.StandardVertices;

using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace Microsoft.Research.Naiad.Frameworks.DifferentialDataflow.Operators
{
    internal class AdjustTime<R, T> : UnaryVertex<Weighted<R>, Weighted<R>, T>
        where T : Time<T>
        where R : IEquatable<R>
    {
        public Func<R, T, T> adjustment;
        
        public override void OnReceive(Message<Weighted<R>, T> message)
        {
            for (int i = 0; i < message.length; i++)
            {
                var record = message.payload[i];
                var adjusted = adjustment(record.record, message.time);
                var newTime = adjusted.Join(message.time);
                this.Output.GetBufferForTime(newTime).Send(message.payload[i]);
            }
        }

        public AdjustTime(int index, Stage<T> collection, Func<R, T, T> transformation)
            : base(index, collection)
        {
            adjustment = transformation;
        }

    }
}
