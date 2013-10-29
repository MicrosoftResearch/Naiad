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
using System.Linq;
using System.Text;

namespace Naiad.Scheduling
{
    public class SchedulerStartArgs : EventArgs
    {
        public readonly int ThreadId;
        public SchedulerStartArgs(int threadId)
        {
            this.ThreadId = threadId;
        }
    }

    public class SchedulerWakeArgs : EventArgs
    {
        public readonly int ThreadId;
        public SchedulerWakeArgs(int threadId)
        {
            this.ThreadId = threadId;
        }
    }
    public class SchedulerSleepArgs : EventArgs
    {    
        public readonly int ThreadId;
        public SchedulerSleepArgs(int threadId)
        {
            this.ThreadId = threadId;
        }
    }
    public class SchedulerTerminateArgs : EventArgs
    {
        public readonly int ThreadId;
        public SchedulerTerminateArgs(int threadId)
        {
            this.ThreadId = threadId;
        }
    }
    public class OperatorStartArgs : EventArgs
    {
        public readonly int ThreadId;
        public readonly Dataflow.Stage Stage;
        public readonly int ShardId;
        public readonly Pointstamp Pointstamp;

        public OperatorStartArgs(int threadId, Dataflow.Stage stage, int shardId, Pointstamp pointstamp)
        {
            this.ThreadId = threadId;
            this.Stage = stage;
            this.ShardId = shardId;
            this.Pointstamp = pointstamp;
        }
    }
    public class OperatorEndArgs : EventArgs
    {
        public readonly int ThreadId;
        public readonly Dataflow.Stage Stage;
        public readonly int ShardId;
        public readonly Pointstamp Pointstamp;

        public OperatorEndArgs(int threadId, Dataflow.Stage stage, int shardId, Pointstamp pointstamp)
        {
            this.ThreadId = threadId;
            this.Stage = stage;
            this.ShardId = shardId;
            this.Pointstamp = pointstamp;
        }
    }
    public class OperatorEnqArgs : EventArgs
    {
        public readonly int ThreadId;
        public readonly Dataflow.Stage Stage;
        public readonly int ShardId;
        public readonly Pointstamp Pointstamp;

        public OperatorEnqArgs(int threadId, Dataflow.Stage stage, int shardId, Pointstamp pointstamp)
        {
            this.ThreadId = threadId;
            this.Stage = stage;
            this.ShardId = shardId;
            this.Pointstamp = pointstamp;
        }
    }

    public class OperatorReceiveArgs : EventArgs
    {
        public readonly Dataflow.Stage Stage;
        public readonly int ShardId;
        public readonly int ChannelId;
        public readonly int Count;
        public OperatorReceiveArgs(Dataflow.Stage stage, int shardId, int channelId, int count)
        {
            this.Stage = stage;
            this.ShardId = shardId;
            this.ChannelId = channelId;
            this.Count = count;
        }
    }
    public class OperatorSendArgs : EventArgs
    {
        public readonly Dataflow.Stage Stage;
        public readonly int ShardId;
        public readonly int ChannelId;
        public readonly int Count;
        public OperatorSendArgs(Dataflow.Stage stage, int shardId, int channelId, int count)
        {
            this.Stage = stage;
            this.ShardId = shardId;
            this.ChannelId = channelId;
            this.Count = count;
        }
    }
}
