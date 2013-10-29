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

﻿using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Linq.Expressions;
using System.Text;

using Naiad.Dataflow.Channels;

namespace Naiad.Dataflow
{
    internal interface Edge
    {
        void Materialize();
        int ChannelId { get; }

        Stage SourceStage { get; }
        Stage TargetStage { get; }
        bool Exchanges { get; }
    }

    internal class Edge<R,T> : Edge
        where T : Time<T>
    {
        public readonly StageOutput<R, T> Source;
        public readonly StageInput<R, T> Target;
        public readonly Expression<Func<R, int>> PartitionFunction;
        public readonly Dataflow.Channels.Channel.Flags Flags;

        Dataflow.Channels.Cable<R, T> bundle;
        //public Dataflow.Channels.ChannelBundle Bundle { get { return this.bundle; } }

        readonly int channelId;
        public int ChannelId { get { return this.channelId; } }

        public Stage SourceStage { get { return Source.ForStage; } }
        public Stage TargetStage { get { return Target.ForStage; } }

        private bool exchanges;
        public bool Exchanges { get { return exchanges; } }

        bool materialized = false;
        public void Materialize()
        {
            if (materialized)
                throw new Exception("Re-materializing edge");

            materialized = true;

            Debug.Assert(Source.ForStage.InternalGraphManager == Target.ForStage.InternalGraphManager);
            var controller = Source.ForStage.InternalGraphManager.Controller;

            // if direct pipelining is possible, do that
            if (Channel.Pipelineable(Source, Target, PartitionFunction) || Channel.PartitionedEquivalently(Source, Target))
            {
                this.bundle = new Dataflow.Channels.PipelineChannel<R, T>(Source, Target, this.ChannelId);
                this.exchanges = false;
            }
            else
            {
                var compiledKey = PartitionFunction != null ? PartitionFunction.Compile() : (Func<R, int>)null;
                this.bundle = new PostOfficeChannel<R, T>(Source, Target, compiledKey, controller.NetworkChannel, this.ChannelId, this.Flags);
                Logging.Info("Allocated exchange channel {2}: {0} -> {1}", this.bundle.SourceStage, this.bundle.DestinationStage, this.bundle.ChannelId);
                this.exchanges = true;
            }

            Logging.Info("Allocated {0}", this.bundle);

            Source.AttachBundleToSender(this.bundle);
            //Target.AttachBundleToReceiver(this.bundle);
        }

        public override string ToString()
        {
            return String.Format("{0} -> {1}", this.SourceStage, this.TargetStage);
        }

        public Edge(StageOutput<R, T> stream, StageInput<R, T> recvPort, Expression<Func<R, int>> partitionFunction, Dataflow.Channels.Channel.Flags flags)
        {
            if (recvPort.PartitionedBy != partitionFunction)
                Console.Error.WriteLine("Constructing Edge where recvPort.PartitionedBy is incorrect.");

            this.Source = stream;
            this.Target = recvPort;
            this.PartitionFunction = partitionFunction;
            this.Flags = flags;

            this.channelId = stream.ForStage.InternalGraphManager.Register(this);

            this.exchanges = !(Channel.Pipelineable(Source, Target, PartitionFunction) || Channel.PartitionedEquivalently(Source, Target));
        }
    }
}
