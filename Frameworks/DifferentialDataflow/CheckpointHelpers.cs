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
using Microsoft.Research.Naiad.Runtime.FaultTolerance;

namespace Microsoft.Research.Naiad.Frameworks.DifferentialDataflow
{
    internal class CheckpointHelpers
    {
        public class IncrementalCheckpointManager<T> where T : Time<T>
        {
            private long entriesInFullCheckpoint;
            private long entriesInIncrementalCheckpoints;
            private readonly double incrementalCheckpointRatio;

            public ICheckpoint<T> CachedFullCheckpoint { get; set; }
            public long CachedFullCheckpointEntries { get; set; }
            public ICheckpoint<T> CachedIncrementalCheckpoint { get; set; }
            public long CachedIncrementalCheckpointEntries { get; set; }

            public ICheckpoint<T> RollbackTimeRange { get; set; }
            public ICheckpoint<T> StableTimeRange { get; set; }
            public bool ForceFullCheckpoint { get; set; }

            public bool NextCheckpointIsIncremental(long entriesSinceLastCheckpoint)
            {
                long newProspectiveEntriesInIncrementalCheckpoints = entriesSinceLastCheckpoint + this.entriesInIncrementalCheckpoints;
                return
                    (!this.ForceFullCheckpoint &&
                     ((double)newProspectiveEntriesInIncrementalCheckpoints /
                      (double)this.entriesInFullCheckpoint) < this.incrementalCheckpointRatio);
            }

            public void RegisterCheckpoint(long entries, ICheckpoint<T> checkpoint)
            {
                this.RegisterCheckpoint(entries, checkpoint, 0, FrontierCheckpointTester<T>.CreateEmpty());
            }

            public void RegisterCheckpoint(long entries, ICheckpoint<T> checkpoint, long incrementalEntries, ICheckpoint<T> incrementalCheckpoint)
            {
                if (checkpoint.IsFullCheckpoint)
                {
                    this.entriesInFullCheckpoint = entries;
                    this.entriesInIncrementalCheckpoints = incrementalEntries;
                    this.ForceFullCheckpoint = false;

                    this.CachedFullCheckpoint = checkpoint;
                    this.CachedFullCheckpointEntries = entries;

                    this.CachedIncrementalCheckpoint = incrementalCheckpoint;
                    this.CachedIncrementalCheckpointEntries = incrementalEntries;
                }
                else
                {
                    this.entriesInIncrementalCheckpoints += entries;

                    this.CachedIncrementalCheckpoint = checkpoint;
                    this.CachedIncrementalCheckpointEntries = this.entriesInIncrementalCheckpoints;
                }
            }

            public IncrementalCheckpointManager(Microsoft.Research.Naiad.Dataflow.Stage stage)
            {
                this.RollbackTimeRange = FrontierCheckpointTester<T>.CreateEmpty();
                this.StableTimeRange = FrontierCheckpointTester<T>.CreateEmpty();
                this.ForceFullCheckpoint = true;
                this.entriesInFullCheckpoint = 0;
                this.entriesInIncrementalCheckpoints = 0;
                this.incrementalCheckpointRatio = stage.IncrementalCheckpointRatio;

                this.CachedFullCheckpoint = FrontierCheckpointTester<T>.CreateEmpty();
                this.CachedFullCheckpointEntries = 0;
                this.CachedIncrementalCheckpoint = FrontierCheckpointTester<T>.CreateEmpty();
                this.CachedIncrementalCheckpointEntries = 0;
            }
        }
    }
}
