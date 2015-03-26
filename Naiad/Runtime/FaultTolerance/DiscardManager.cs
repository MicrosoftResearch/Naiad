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

using Microsoft.Research.Naiad.Runtime.Progress;
using Microsoft.Research.Naiad.Dataflow;

namespace Microsoft.Research.Naiad.Runtime.FaultTolerance
{
    internal class DiscardManager
    {
        public Dictionary<int, FTFrontier[]> DiscardFrontiers { get; private set;}
        public bool DiscardingAny { get { return this.DiscardFrontiers != null; } }

        public void SetDiscardFrontier<T>(Stage<T> stage) where T : Time<T>
        {
            int liveCount = 0;
            FTFrontier[] frontiers = new FTFrontier[stage.Placement.Count];
            for (int i = 0; i < frontiers.Length; ++i)
            {
                frontiers[i] = stage.CurrentCheckpoint(i).GetRestorationFrontier();
                if (!frontiers[i].Empty)
                {
                    ++liveCount;
                }
            }

            if (liveCount == 0)
            {
                return;
            }

            if (this.DiscardFrontiers == null)
            {
                this.DiscardFrontiers = new Dictionary<int, FTFrontier[]>();
            }

            this.DiscardFrontiers.Add(stage.StageId, frontiers);
        }

        public void Reset()
        {
            this.DiscardFrontiers = null;
        }

        public DiscardManager()
        {
            this.Reset();
        }
    }
}
