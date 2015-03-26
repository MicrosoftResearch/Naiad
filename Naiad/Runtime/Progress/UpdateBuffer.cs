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
using Microsoft.Research.Naiad.Scheduling;
using Microsoft.Research.Naiad.Dataflow;

namespace Microsoft.Research.Naiad.Runtime.Progress
{
    internal class ProgressUpdateBuffer<T>
        where T : Time<T>
    {
        public Dictionary<T, Int64> Updates;
        Runtime.Progress.ProgressUpdateProducer producer;

        int stageID;

        T lastSentTime;
        Int64 lastSentCount;

        Pointstamp version;

        public void PushCachedToDelta()
        {
            if (lastSentCount != 0)
            {
                Int64 value;
                if (!Updates.TryGetValue(lastSentTime, out value))
                    Updates.Add(lastSentTime, lastSentCount);
                else
                {
                    if (value + lastSentCount == 0)
                        Updates.Remove(lastSentTime);
                    else
                        Updates[lastSentTime] = value + lastSentCount;
                }

                lastSentCount = 0;
            }
        }

        public void Update(T time, Int64 weight)
        {
            if (!time.Equals(lastSentTime))
            {
                PushCachedToDelta();
                lastSentTime = time;
            }

            lastSentCount += weight;
        }

        public void Flush()
        {
            PushCachedToDelta();

            if (Updates.Count > 0)
            {
                foreach (var pair in Updates)
                {
                    if (pair.Value != 0)
                    {
                        pair.Key.Populate(ref version); // do the type conversion to pointstamp
                        producer.UpdateRecordCounts(version, pair.Value);
                    }
                }

                Updates.Clear();
            }
        }

        public ProgressUpdateBuffer(int name, Runtime.Progress.ProgressUpdateProducer p)
        {
            stageID = name;

            Updates = new Dictionary<T, Int64>();
            producer = p;

            var temp = new Pointstamp(0, new int[] { });
            version = new Pointstamp(name, new int[default(T).Populate(ref temp)]);
        }
    }

}
