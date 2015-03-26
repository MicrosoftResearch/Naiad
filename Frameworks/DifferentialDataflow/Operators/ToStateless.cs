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
using System.Threading.Tasks;

using Microsoft.Research.Naiad.Dataflow;

namespace Microsoft.Research.Naiad.Frameworks.DifferentialDataflow.Operators
{
    internal class ToStateless<S, T> : OperatorImplementations.UnaryStatefulOperator<S, S, S, T, S>
        where S : IEquatable<S>
        where T : Microsoft.Research.Naiad.Time<T>
    {
        public override void OnNotify(T workTime)
        {
            if (!this.inputImmutable)
                foreach (var record in this.Input.GetRecordsAt(workTime))
                    OnInput(record, workTime);

            timeList.Clear();
            timeList.Add(this.internTable.Intern(workTime));

            foreach (var key in this.keyIndices.Keys.ToArray())
            {
                Update(key);
            }

            inputTrace.Compact();
            outputTrace.Compact();

            keysToProcess.Clear();

            Flush();
        }

        protected override NaiadList<int> InterestingTimes(UnaryKeyIndices keyIndices)
        {
            return timeList;
        }

        protected override void NewOutputMinusOldOutput(S key, UnaryKeyIndices keyIndices, int timeIndex)
        {
            collection.Clear();
            inputTrace.EnumerateCollectionAt(keyIndices.processed, timeIndex, collection);

            for (int i = 0; i < collection.Count; i++)
            {
                var element = collection.Array[i];
                if (element.weight > 0)
                {
                    outputTrace.Introduce(ref outputWorkspace, key, element.weight, timeIndex);
                }
            }
        }

        public ToStateless(int index, Stage<T> collection, bool inputImmutable)
            : base(index, collection, inputImmutable, v => v, v => v)
        {
        }
    }

}
