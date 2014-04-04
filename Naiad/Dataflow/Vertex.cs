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

using Microsoft.Research.Naiad.CodeGeneration;
using Microsoft.Research.Naiad.Dataflow.Channels;
using Microsoft.Research.Naiad.Dataflow.Reporting;
using Microsoft.Research.Naiad.DataStructures;
using Microsoft.Research.Naiad.FaultTolerance;
using Microsoft.Research.Naiad.Scheduling;
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Microsoft.Research.Naiad.Dataflow
{
    /// <summary>
    /// Base class for vertices without a time type
    /// </summary>
    public abstract class Vertex : ICheckpointable
    {
        private int entrancy;
        public int Entrancy { get { return this.entrancy; } set { this.entrancy = value; } }

        public SerializationCodeGenerator CodeGenerator { get { return this.Stage.InternalGraphManager.CodeGenerator; } }

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
                using (NaiadWriter writer = new NaiadWriter(stream, this.CodeGenerator))
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
    /// Vertex with a time time, supporting Contexts and NotifyAt(time) for OnNotify(time) calls.
    /// </summary>
    /// <typeparam name="TTime"></typeparam>
    public class Vertex<TTime> : Dataflow.Vertex
        where TTime : Time<TTime>
    {
        private IVertexContext<TTime> vertexContext;

        internal IVertexContext<TTime> Context
        {
            get { return vertexContext; }
            set { this.vertexContext = value; }
        }

        protected IReporting<TTime> Reporting { get { return this.vertexContext.Reporting; } }

        private HashSet<TTime> OutstandingResumes = new HashSet<TTime>();
        private Microsoft.Research.Naiad.Runtime.Progress.ProgressUpdateBuffer<TTime> progressBuffer;

        //private TTime lastTime;
        //private bool lastTimeValid;

        protected override void OnShutdown()
        {
            base.OnShutdown();
            OutstandingResumes = null;
            progressBuffer.Flush();    // should be unnecessary
            progressBuffer = null;
        }

        public void NotifyAt(TTime time)
        {
            this.NotifyAt(time, time);
        }

        /// <summary>
        /// Requests a notification once all records of time requirement have been delivered.
        /// </summary>
        /// <param name="requirement">Requirement on incoming message deliveries</param>
        /// <param name="capability">Capability to send outgoing messages</param>
        public void NotifyAt(TTime requirement, TTime capability)
        {
            if (!requirement.LessThan(capability))
                Console.Error.WriteLine("Requesting a notification with a requirement not less than the capability");
            else
            {
                System.Diagnostics.Debug.Assert(!this.isShutdown);
                if (this.isShutdown)
                    Console.Error.WriteLine("Scheduling {0} at {1} but already shut down", this, capability);

                if (!OutstandingResumes.Contains(capability))
                {
                    OutstandingResumes.Add(capability);

                    // do some progress magic 
                    progressBuffer.Update(capability, 1);
                    progressBuffer.Flush();

                    // inform the scheduler
                    this.Scheduler.EnqueueNotify(this, requirement, capability, true);
                }
            }
        }

        internal override void PerformAction(Scheduling.Scheduler.WorkItem workItem)
        {
            if (this.isShutdown)
                Console.Error.WriteLine("Scheduling {0} at {1} but already shut down", this, workItem);
            else
            {
                var time = default(TTime).InitializeFrom(workItem.Requirement, workItem.Requirement.Timestamp.Length);

                OutstandingResumes.Remove(time);

                progressBuffer.Update(time, -1);

                this.Entrancy = this.Entrancy - 1;

                this.OnNotify(time);

                this.Entrancy = this.Entrancy + 1;

                this.Flush();
            }
        }

        public override void Flush()
        {
            base.Flush();
            if (this.progressBuffer != null)
                this.progressBuffer.Flush();
        }

        public virtual void OnNotify(TTime time)
        {
            throw new NotImplementedException();
        }

        public virtual void OnComplete() { }


        public override void Checkpoint(NaiadWriter writer)
        {
            IList<Scheduler.WorkItem> workItems = this.Scheduler.GetWorkItemsForVertex(this);
            writer.Write(workItems.Count, this.CodeGenerator.GetSerializer<Int32>());
            foreach (Scheduler.WorkItem workItem in workItems)
                writer.Write(workItem.Requirement,  this.CodeGenerator.GetSerializer<Pointstamp>());
        }

        public override void Restore(NaiadReader reader)
        {
            int workItemsCount = reader.Read<int>(this.CodeGenerator.GetSerializer<Int32>());
            for (int i = 0; i < workItemsCount; ++i)
            {
                Pointstamp version = reader.Read<Pointstamp>(this.CodeGenerator.GetSerializer<Pointstamp>());
                this.Scheduler.EnqueueNotify(this, version, version, false);    // could be set to true if we are sure this executes under the worker
            }
        }


        public Vertex(int index, Stage<TTime> stage)
            : base(index, stage)
        {
            progressBuffer = new Runtime.Progress.ProgressUpdateBuffer<TTime>(stage.StageId, this.scheduler.State(this.Stage.InternalGraphManager).Producer);
        }
    }
}
