/*
 * Naiad ver. 0.4
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
using Microsoft.Research.Naiad.DataStructures;
using System.Diagnostics;
using Microsoft.Research.Naiad.Scheduling;

namespace Microsoft.Research.Naiad.Dataflow.Channels
{
    internal class PipelineChannel<S, T> : Cable<S, T>
        where T : Time<T>
    {
        private class Fiber : SendChannel<S, T>
        {
            private readonly PipelineChannel<S, T> bundle;
            private readonly int index;
            private VertexInput<S, T> receiver;
                        
            public Fiber(PipelineChannel<S, T> bundle, VertexInput<S, T> receiver, int index)
            {
                this.bundle = bundle;
                this.index = index;
                this.receiver = receiver;
            }

            public void Send(Message<S, T> records)
            {
                this.receiver.OnReceive(records, new ReturnAddress());
            }

            public override string ToString()
            {
                return string.Format("Pipeline({0} => {1})", this.bundle.SourceStage, this.bundle.DestinationStage);
            }

            public void Flush()
            {
                this.receiver.Flush();
            }
        }

        private readonly StageOutput<S, T> sender;
        private readonly StageInput<S, T> receiver;

        private readonly Dictionary<int, Fiber> subChannels;

        private readonly int channelId;
        public int ChannelId { get { return channelId; } }

        public PipelineChannel(StageOutput<S, T> sender, StageInput<S, T> receiver, int channelId)
        {
            this.sender = sender;
            this.receiver = receiver;

            this.channelId = channelId;

            this.subChannels = new Dictionary<int, Fiber>();
            foreach (VertexLocation loc in sender.ForStage.Placement)
                if (loc.ProcessId == sender.ForStage.InternalComputation.Controller.Configuration.ProcessID)
                    this.subChannels[loc.VertexId] = new Fiber(this, receiver.GetPin(loc.VertexId), loc.VertexId);
        }

        public SendChannel<S, T> GetSendChannel(int i)
        {
            return this.subChannels[i];
        }
                
        public Dataflow.Stage SourceStage { get { return this.sender.ForStage; } }
        public Dataflow.Stage DestinationStage { get { return this.receiver.ForStage; } }

        public override string ToString()
        {
            return String.Format("Pipeline channel: {0} -> {1}", this.sender.ForStage, this.receiver.ForStage);
        }
    }
}
