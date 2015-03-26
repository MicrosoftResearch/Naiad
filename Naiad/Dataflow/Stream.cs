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
using System.Linq.Expressions;
using System.Text;

using Microsoft.Research.Naiad.Dataflow;
using Microsoft.Research.Naiad.Runtime.FaultTolerance;

namespace Microsoft.Research.Naiad
{
    /// <summary>
    /// Context of a stream, that can be passed to the constructor e.g. of a LoopContext
    /// </summary>
    public class StreamContext
    {
        internal InternalComputation Computation { get; private set; }

        internal StreamContext(InternalComputation computation)
        {
            this.Computation = computation;
        }
    }

    /// <summary>
    /// Represents a stream of records each tagged with a time.
    /// </summary>
    /// <typeparam name="TRecord">record type</typeparam>
    /// <typeparam name="TTime">time type</typeparam>
    public abstract class Stream<TRecord, TTime> where TTime : Time<TTime>
    {
        internal abstract StageOutput<TRecord, TTime> StageOutput { get; }

        /// <summary>
        /// Expression indicating a partitioning property the stream obeys, or null if none exists.
        /// </summary>
        public Expression<Func<TRecord, int>> PartitionedBy { get { return this.StageOutput.PartitionedBy; } }

        /// <summary>
        /// Stage the stream is produced by.
        /// </summary>
        public Dataflow.Stage ForStage { get { return this.StageOutput.ForStage; } }

        /// <summary>
        /// Context of the stream, used to construct stages, loop contexts, etc.
        /// </summary>
        public StreamContext Context { get { return new StreamContext(this.ForStage.InternalComputation); } }

        /// <summary>
        /// Set the type of checkpointing/logging used at the operator that produces this stream
        /// </summary>
        /// <param name="checkpointType">type of checkpointing/logging</param>
        /// <returns>the stream with the modified checkpointing type</returns>
        public Stream<TRecord, TTime> SetCheckpointType(CheckpointType checkpointType)
        {
            this.ForStage.SetCheckpointType(checkpointType);
            return this;
        }

        /// <summary>
        /// Provides a function indicating when to checkpoint
        /// </summary>
        /// <param name="shouldCheckpointFactory">function to create a checkpoint policy object for a vertex</param>
        /// <returns>the stream with the sender's checkpoint policy updated</returns>
        public Stream<TRecord, TTime> SetCheckpointPolicy(Func<int, ICheckpointPolicy> shouldCheckpointFactory)
        {
            this.ForStage.SetCheckpointPolicy(shouldCheckpointFactory);
            return this;
        }
    }

    internal class FullyTypedStream<TSender, TRecord, TTime> : Stream<TRecord, TTime>
        where TTime : Time<TTime>
        where TSender : Time<TSender>
    {
        internal readonly FullyTypedStageOutput<TSender, TRecord, TTime> TypedStageOutput;
        internal override StageOutput<TRecord, TTime> StageOutput
        {
            get { return this.TypedStageOutput; }
        }

        internal FullyTypedStream(FullyTypedStageOutput<TSender, TRecord, TTime> stageOutput)
        {
            this.TypedStageOutput = stageOutput;
        }
    }
}
