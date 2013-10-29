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

﻿using Naiad.CodeGeneration;
using Naiad.Dataflow.Channels;
using Naiad.Dataflow.Reporting;
using Naiad.DataStructures;
using Naiad.FaultTolerance;
using Naiad.Scheduling;
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Naiad.Dataflow
{
    /// <summary>
    /// Base class for shards without a time type
    /// </summary>
    public abstract class Vertex : ICheckpointable
    {
        private int entrancy;
        public int Entrancy { get { return this.entrancy; } set { this.entrancy = value; } }

        private List<Action> OnFlushActions;

        internal abstract void PerformAction(Scheduling.Scheduler.WorkItem workItem);

        public virtual void UpdateReachability(NaiadList<Pointstamp> versions)
        {
            if (versions == null && !isShutdown)
                this.ShutDown();
        }

        public virtual void Flush()
        {
            for (int i = 0; i < this.OnFlushActions.Count; i++)
                this.OnFlushActions[i]();
        }

        public void AddOnFlushAction(Action onFlush)
        {
            OnFlushActions.Add(onFlush);
        }

        protected bool isShutdown = false;
        internal void ShutDown()
        {
            if (!this.isShutdown)
            {
                Logging.Info("Shutting down {0}", this);
                this.OnShutdown();
                isShutdown = true;
            }
        }

        protected virtual void OnShutdown()
        {
            this.Flush();
            this.OnFlushActions.Clear();
        }

        public readonly Stage Stage;
        public readonly int VertexId;

        internal Scheduler scheduler = null;
        internal Scheduler Scheduler { set { scheduler = value; scheduler.Register(this, this.Stage.InternalGraphManager); } get { return this.scheduler; } }

        public virtual bool Stateful { get { return true; } }

        internal long currentCheckpointStart = 0;

        public virtual Pair<Stream, Pair<long, long>> Checkpoint(bool isMajor)
        {
            Stream stream = this.LoggingOutput;
            if (isMajor)
            {
                currentCheckpointStart = stream.Position;
                using (NaiadWriter writer = new NaiadWriter(stream))
                {
                    this.Checkpoint(writer);
                }
            }
            if (stream is FileStream)
                ((FileStream)stream).Flush(true);
            else
                stream.Flush();
            return new Pair<Stream, Pair<long, long>>(stream, new Pair<long, long>(currentCheckpointStart, stream.Position));
        }

        private Stream loggingOutput;
        public Stream LoggingOutput
        {
            get
            {
                if (this.loggingOutput == null)
                {
                    this.loggingOutput = this.Stage.InternalGraphManager.Controller.GetLoggingOutputStream(this);
                    // Must start with a complete checkpoint of the stage, which should be cheap if we're empty.
                    this.Checkpoint(false);
                }
                return this.loggingOutput;
            }
        }

        public virtual void Checkpoint(NaiadWriter writer)
        {
        }

        public virtual void Restore(NaiadReader reader)
        {
        }

        private string MyName;
        public override string ToString()
        {
            return MyName;
        }

        public Vertex(int index, Stage stage)
        {
            this.VertexId = index;

            this.Stage = stage;
            this.Scheduler = this.Stage.InternalGraphManager.Controller.Workers[this.Stage.Placement[this.VertexId].ThreadId];

            this.MyName = String.Format("{0}.{1}]", stage.ToString().Remove(stage.ToString().Length - 1), this.VertexId);
            this.OnFlushActions = new List<Action>();
        }
    }

    /// <summary>
    /// Shard with a time time, supporting Contexts and NotifyAt(time) for OnDone(time) calls.
    /// </summary>
    /// <typeparam name="TTime"></typeparam>
    public class Vertex<TTime> : Dataflow.Vertex
        where TTime : Time<TTime>
    {
        private IShardContext<TTime> shardContext;

        internal IShardContext<TTime> Context
        {
            get { return shardContext; }
            set { this.shardContext = value; }
        }

        protected IReporting<TTime> Reporting { get { return this.shardContext.Reporting; } }

        private HashSet<TTime> OutstandingResumes = new HashSet<TTime>();
        private Naiad.Runtime.Progress.ProgressUpdateBuffer<TTime> progressBuffer;

        private TTime lastTime;
        private bool lastTimeValid;

        protected override void OnShutdown()
        {
            base.OnShutdown();
            OutstandingResumes = null;
            progressBuffer.Flush();    // should be unnecessary
            progressBuffer = null;
        }

        public void NotifyAt(TTime time)
        {
            System.Diagnostics.Debug.Assert(!this.isShutdown);
            if (this.isShutdown)
                Console.Error.WriteLine("Scheduling {0} at {1} but already shut down", this, time);

            if (!(lastTimeValid && time.Equals(lastTime)) && !OutstandingResumes.Contains(time))
            {
                OutstandingResumes.Add(time);
                lastTime = time;
                lastTimeValid = true;

                // do some progress magic 
                progressBuffer.Update(time, 1);
                progressBuffer.Flush();

                // inform the scheduler
                this.Scheduler.EnqueueNotify(this, time, true);
            }
        }

        internal override void PerformAction(Scheduling.Scheduler.WorkItem workItem)
        {
            var time = default(TTime).InitializeFrom(workItem.Requirement, workItem.Requirement.Timestamp.Length);

            OutstandingResumes.Remove(time);
            lastTimeValid = !time.Equals(lastTime);

            progressBuffer.Update(time, -1);

            this.Entrancy = this.Entrancy - 1;

            this.OnDone(time);

            this.Entrancy = this.Entrancy + 1;

            this.Flush();
        }

        public override void Flush()
        {
            base.Flush();
            if (this.progressBuffer != null)
                this.progressBuffer.Flush();
        }

        public virtual void OnDone(TTime time)
        {
            throw new NotImplementedException();
        }

        public virtual void OnComplete() { }


        public override void Checkpoint(NaiadWriter writer)
        {
            IList<Scheduler.WorkItem> workItems = this.Scheduler.GetWorkItemsForShard(this);
            writer.Write(workItems.Count, PrimitiveSerializers.Int32);
            foreach (Scheduler.WorkItem workItem in workItems)
                writer.Write(workItem.Requirement, Pointstamp.Serializer);
        }

        public override void Restore(NaiadReader reader)
        {
            int workItemsCount = reader.Read<int>(PrimitiveSerializers.Int32);
            for (int i = 0; i < workItemsCount; ++i)
            {
                Pointstamp version = reader.Read<Pointstamp>(Pointstamp.Serializer);
                this.Scheduler.EnqueueNotify(this, version, version);
            }
        }


        public Vertex(int index, Stage<TTime> stage)
            : base(index, stage)
        {
            progressBuffer = new Runtime.Progress.ProgressUpdateBuffer<TTime>(stage.StageId, this.scheduler.State(this.Stage.InternalGraphManager).Producer);
        }
    }
}
