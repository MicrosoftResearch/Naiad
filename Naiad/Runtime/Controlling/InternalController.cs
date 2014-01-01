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
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Linq.Expressions;
using System.Net.Sockets;
using System.Text;
using Naiad.Dataflow.Channels;
using Naiad.FaultTolerance;
using Naiad.Frameworks;
using Naiad.Scheduling;
using System.Collections.Concurrent;
using Naiad.Runtime.Networking;
using System.Threading;

namespace Naiad.Runtime.Controlling
{
    internal interface InternalWorkerGroup : WorkerGroup
    {
        void Start();
        void WakeUp();
        void Abort();
        void Activate();
        void Pause();
        void Resume();

        Scheduler this[int index] { get; }

        void NotifySchedulerStarting(Scheduler scheduler);
        void NotifySchedulerWaking(Scheduler scheduler);
        void NotifyOperatorStarting(Scheduler scheduler, Scheduler.WorkItem work);
        void NotifyOperatorEnding(Scheduler scheduler, Scheduler.WorkItem work);
        void NotifyOperatorEnqueued(Scheduler scheduler, Scheduler.WorkItem work);
        void NotifySchedulerSleeping(Scheduler scheduler);
        void NotifySchedulerTerminating(Scheduler scheduler);
        void NotifyOperatorReceivedRecords(Dataflow.Vertex op, int channelId, int recordsReceived);
        void NotifyOperatorSentRecords(Dataflow.Vertex op, int channelId, int recordsSent);

        /// <summary>
        /// Blocks the scheduler waiting for the event to be signalled.
        /// Used in broadcast wakeup implementation.
        /// </summary>
        /// <param name="selectiveEvent"></param>
        /// <param name="val"></param>
        /// <returns></returns>
        long BlockScheduler(AutoResetEvent selectiveEvent, long val);
    }

#if false
    public interface InternalController : IDisposable
#endif
}
