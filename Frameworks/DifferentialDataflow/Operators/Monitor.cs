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

using System.Diagnostics;
using System.Runtime.InteropServices;
using System.Threading;

using Naiad.Util;
using Naiad.Dataflow;

namespace Naiad.Frameworks.DifferentialDataflow.Operators
{
    internal class Monitor<R, T> : UnaryVertex<Weighted<R>, Weighted<R>, T>
        //OperatorImplementations.UnaryStatefulOperator<R, R, R, T, R>
        where R : IEquatable<R>
        where T : Naiad.Time<T>
    {
        public Action<int, List<NaiadRecord<R,T>>> action;
        public List<NaiadRecord<R,T>> list;

        Int64 count;
        T leastTime;

        private void OnInput(Weighted<R> entry, T time)
        {
            this.Output.Send(entry, time);

            if (action != null)
                list.Add(entry.ToNaiadRecord(time));

            if (count == 0 || leastTime.CompareTo(time) > 0)
                leastTime = time;

            count++;
        }

        public override void MessageReceived(Message<Pair<Weighted<R>, T>> message)
        {
            for (int i = 0; i < message.length; i++)
            {
                this.OnInput(message.payload[i].v1, message.payload[i].v2);
                this.NotifyAt(message.payload[i].v2);
            }
        }

        public override void OnDone(T time)
        {
            if (action != null)
            {
                action(VertexId, list);
                list.Clear();
            }
            else
            { 
                Console.WriteLine("{0}\t{1}\t{2}", this.VertexId, count, leastTime, Logging.Stopwatch.Elapsed);

                count = 0;
            }
        }

        public Monitor(int index, Stage<T> collection, bool immutableInput, Action<int, List<NaiadRecord<R,T>>> a) : base(index, collection)
        {
            action = a;
            list = new List<NaiadRecord<R,T>>();
        }
    }
}