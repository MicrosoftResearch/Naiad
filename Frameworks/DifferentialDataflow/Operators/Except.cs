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

using Microsoft.Research.Naiad.Dataflow;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace Microsoft.Research.Naiad.Frameworks.DifferentialDataflow.Operators
{
    internal class Except<S,T> : BinaryVertex<Weighted<S>, Weighted<S>, Weighted<S>, T>
        where S : IEquatable<S>
        where T : Microsoft.Research.Naiad.Time<T>
    {
        public override void OnReceive1(Message<Weighted<S>, T> message)
        {
            this.Output.Send(message);
        }

        public override void OnReceive2(Message<Weighted<S>, T> message)
        {
            var outputBuffer = this.Output.GetBufferForTime(message.time);

            for (int i = 0; i < message.length; i++)
            {
                var record = message.payload[i];
                record.weight *= -1;

                outputBuffer.Send(record);
            }
        }

        public Except(int index, Stage<T> collection)
            : base(index, collection)
        {
        }
    }
}
