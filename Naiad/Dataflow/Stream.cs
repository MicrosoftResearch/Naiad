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
using System.Linq.Expressions;
using System.Text;

using Naiad.Dataflow.Channels;

namespace Naiad.Dataflow
{
    /// <summary>
    /// Represents a stream of records each tagged with a time.
    /// </summary>
    /// <typeparam name="TRecord">record type</typeparam>
    /// <typeparam name="TTime">time type</typeparam>
    public class Stream<TRecord, TTime> where TTime : Time<TTime>
    {
        internal readonly StageOutput<TRecord, TTime> StageOutput;

        /// <summary>
        /// Expression indicating a partitioning property the stream obeys, or null if none exists.
        /// </summary>
        public Expression<Func<TRecord, int>> PartitionedBy { get { return this.StageOutput.PartitionedBy; } }

        /// <summary>
        /// Stage the stream is produced by.
        /// </summary>
        public Dataflow.Stage ForStage { get { return this.StageOutput.ForStage; } }

        /// <summary>
        /// Time context for the stream.
        /// </summary>
        public Dataflow.OpaqueTimeContext<TTime> Context { get { return this.StageOutput.Context; } }

        internal Stream(StageOutput<TRecord, TTime> stageOutput)
        {
            this.StageOutput = stageOutput;
        }
    }
}
