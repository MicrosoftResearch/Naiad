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

using Microsoft.Research.Naiad.Runtime.Progress;

namespace Microsoft.Research.Naiad.Diagnostics
{
    /// <summary>
    /// Arguments of the event that is raised when a worker starts.
    /// </summary>
    public class WorkerStartArgs : EventArgs
    {
        /// <summary>
        /// The worker thread that is starting.
        /// </summary>
        public readonly int ThreadId;

        internal WorkerStartArgs(int threadId)
        {
            this.ThreadId = threadId;
        }
    }

    /// <summary>
    /// Arguments of the event that is raised when a worker is woken from sleep.
    /// </summary>
    public class WorkerWakeArgs : EventArgs
    {
        /// <summary>
        /// The worker thread that is waking.
        /// </summary>
        public readonly int ThreadId;

        internal WorkerWakeArgs(int threadId)
        {
            this.ThreadId = threadId;
        }
    }

    /// <summary>
    /// Arguments of the event that is raised when a worker goes to sleep.
    /// </summary>
    public class WorkerSleepArgs : EventArgs
    {    
        /// <summary>
        /// The worker thread that is sleeping.
        /// </summary>
        public readonly int ThreadId;
        /// <summary>
        /// If all the shared queues have drained when this worker goes to sleep, this contains the total number
        /// of items drained from all the queues, otherwise it is -1
        /// </summary>
        public readonly int QueueHighWaterMark;

        internal WorkerSleepArgs(int threadId, int queueHighWaterMark)
        {
            this.ThreadId = threadId;
            this.QueueHighWaterMark = queueHighWaterMark;
        }
    }

    /// <summary>
    /// Arguments of the event that is raised when a worker terminates.
    /// </summary>
    public class WorkerTerminateArgs : EventArgs
    {
        /// <summary>
        /// The worker thread that is terminating.
        /// </summary>
        public readonly int ThreadId;

        internal WorkerTerminateArgs(int threadId)
        {
            this.ThreadId = threadId;
        }
    }

    /// <summary>
    /// Arguments of the event that is raised when a vertex notification starts.
    /// </summary>
    public class VertexStartArgs : EventArgs
    {
        /// <summary>
        /// The worker thread on which the vertex is starting.
        /// </summary>
        public readonly int ThreadId;

        /// <summary>
        /// The stage of which the vertex is a member.
        /// </summary>
        public readonly Dataflow.Stage Stage;

        /// <summary>
        /// The ID of the vertex within its stage.
        /// </summary>
        public readonly int VertexId;

        /// <summary>
        /// The pointstamp of the notification that is being delivered.
        /// </summary>
        public readonly Pointstamp Pointstamp;

        internal VertexStartArgs(int threadId, Dataflow.Stage stage, int vertexId, Pointstamp pointstamp)
        {
            this.ThreadId = threadId;
            this.Stage = stage;
            this.VertexId = vertexId;
            this.Pointstamp = pointstamp;
        }
    }

    /// <summary>
    /// Arguments of the event that is raised when a vertex notification ends.
    /// </summary>
    public class VertexEndArgs : EventArgs
    {

        /// <summary>
        /// The worker thread on which the vertex is ending.
        /// </summary>
        public readonly int ThreadId;

        /// <summary>
        /// The stage of which the vertex is a member.
        /// </summary>
        public readonly Dataflow.Stage Stage;

        /// <summary>
        /// The ID of the vertex within its stage.
        /// </summary>
        public readonly int VertexId;

        /// <summary>
        /// The pointstamp of the notification that was delivered.
        /// </summary>
        public readonly Pointstamp Pointstamp;

        internal VertexEndArgs(int threadId, Dataflow.Stage stage, int vertexId, Pointstamp pointstamp)
        {
            this.ThreadId = threadId;
            this.Stage = stage;
            this.VertexId = vertexId;
            this.Pointstamp = pointstamp;
        }
    }

    /// <summary>
    /// Arguments of the event that is raised when a vertex notification is enqueued.
    /// </summary>
    public class VertexEnqueuedArgs : EventArgs
    {
        /// <summary>
        /// The worker thread on which the notification is being enqueued.
        /// </summary>
        public readonly int ThreadId;

        /// <summary>
        /// The stage of which the vertex is a member.
        /// </summary>
        public readonly Dataflow.Stage Stage;

        /// <summary>
        /// The ID of the vertex within its stage.
        /// </summary>
        public readonly int VertexId;

        /// <summary>
        /// The pointstamp of the notification that was enqueued.
        /// </summary>
        public readonly Pointstamp Pointstamp;

        internal VertexEnqueuedArgs(int threadId, Dataflow.Stage stage, int vertexId, Pointstamp pointstamp)
        {
            this.ThreadId = threadId;
            this.Stage = stage;
            this.VertexId = vertexId;
            this.Pointstamp = pointstamp;
        }
    }

#if false
    public class OperatorReceiveArgs : EventArgs
    {
        public readonly Dataflow.Stage Stage;
        public readonly int VertexId;
        public readonly int ChannelId;
        public readonly int Count;
        public OperatorReceiveArgs(Dataflow.Stage stage, int vertexId, int channelId, int count)
        {
            this.Stage = stage;
            this.VertexId = vertexId;
            this.ChannelId = channelId;
            this.Count = count;
        }
    }
    public class OperatorSendArgs : EventArgs
    {
        public readonly Dataflow.Stage Stage;
        public readonly int VertexId;
        public readonly int ChannelId;
        public readonly int Count;
        public OperatorSendArgs(Dataflow.Stage stage, int vertexId, int channelId, int count)
        {
            this.Stage = stage;
            this.VertexId = vertexId;
            this.ChannelId = channelId;
            this.Count = count;
        }
    }
#endif
}
