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
using Microsoft.Research.Naiad;
using Microsoft.Research.Naiad.Dataflow;
using Microsoft.Research.Naiad.Dataflow.StandardVertices;

namespace Microsoft.Research.Naiad.Frameworks.WorkGenerator
{
    /// <summary>
    /// The WorkGenerator framework contains base classes for implementing a work queue of items to be distributed dynamically to a set
    /// of Naiad workers. The queue is implemented as a Naiad loop; when new work arrives at a coordinator, it sends "wakeup" messages
    /// to all the workers, which respond with "ready" messages. Each time the coordinator receives a ready message from a worker, it matches
    /// an outstanding work item to that worker and sends it back. When the worker completes its work item, it responds with another ready message.
    /// Work items are thus sent out as workers become ready, rather than requiring coordination across workers. The framework allows
    /// workers to send identifying information that the coordinator can use for matching. For example the Hdfs and WebHdfs frameworks 
    /// instantiate work generators in which the workers identify themselves by the IP addresses of the computer they are running on,
    /// allowing file blocks to be preferentially read on the worker computer that is hosting them.
    /// </summary>
    class NamespaceDoc
    {

    }

    #region internal helper record types
    /// <summary>
    /// Internal enumeration distinguishing work item records. This is public only for the benefit of the serialization code
    /// </summary>
    public enum WorkRecordType
    {
        /// <summary>
        /// The record does not contain any work, and is simply a notification that the worker should identify itself
        /// </summary>
        Initialize,
        /// <summary>
        /// The record contains work
        /// </summary>
        DoWork
    }

    /// <summary>
    /// Internal record describing a work item, sent from the Coordinator to a Worker. This is public only for the benefit
    /// of the serialization code
    /// </summary>
    /// <typeparam name="TWorkDescription">Type describing an item of work to be done</typeparam>
    public struct WorkItem<TWorkDescription>
    {
        /// <summary>
        /// Records are partitioned by this field, which identifies the worker that is being assigned the work
        /// </summary>
        public int destination;
        /// <summary>
        /// This indicates whether the record contains an actual work item, or is simply an initialization notification
        /// </summary>
        public WorkRecordType type;
        /// <summary>
        /// If the record type is DoWork, this describes the work to be done
        /// </summary>
        public TWorkDescription description;
    }

    /// <summary>
    /// Internal record describing a worker that is ready to receive a work item, sent by the Worker to the Coordinator when
    /// the Worker has processed a WorkItem. This is public only for the benefit of the serialization code
    /// </summary>
    /// <typeparam name="TWorkerDescription">Type describing a worker, for use matching items to workers</typeparam>
    public struct WorkRequest<TWorkerDescription>
    {
        /// <summary>
        /// This matches the destination of the WorkItem that triggered the send. It is the partitioning key that tells Naiad
        /// how to route items to workers, and it is just passed through unchanged from WorkItem.destination to WorkRequest.source
        /// </summary>
        public int source;
        /// <summary>
        /// This describes the worker that is ready for a new work assignment, and is used to match items to workers
        /// </summary>
        public TWorkerDescription description;
    }
    #endregion

    #region ICoordinator interface defining the behavior of the coordinator
    /// <summary>
    /// Interface specifying the behavior of a coordinator vertex. A coordinator takes a stream of input records via calls to AddInput,
    /// and for each input it generates a set of work items. As workers become free, the coordinator is informed, via AssignWork, and
    /// responds with a work item for that worker, or nothing if there is no more work for that worker to perform.
    /// </summary>
    /// <typeparam name="TInput">Type of the input records</typeparam>
    /// <typeparam name="TWorkDescription">Type of the description of an individual work item</typeparam>
    /// <typeparam name="TWorkerDescription">Type of the description of a worker, used to match items to workers</typeparam>
    public interface ICoordinator<TInput, TWorkDescription, TWorkerDescription>
    {
        /// <summary>
        /// Take an input record, translate it to a set of work items, and add those items to an internal datastructure of
        /// outstanding work.
        /// </summary>
        /// <param name="input">The input describing a new batch of work items</param>
        void AddInput(TInput input);

        /// <summary>
        /// Given a worker that is now free, optionally assign it a work item. If no work item is assigned for that worker, the
        /// worker will never be presented in a subsequent call to AssignWork, i.e. it is assumed that there is no more work for
        /// that worker.
        /// </summary>
        /// <param name="worker">The worker that is now free</param>
        /// <param name="work">The work item assigned to the worker, if any</param>
        /// <returns>true if a work item has been assigned, false otherwise</returns>
        bool AssignWork(TWorkerDescription worker, ref TWorkDescription work);
    }
    #endregion

    #region IWorker interface defining the behavior of a worker vertex
    /// <summary>
    /// Interface specifying the behavior of a worker vertex. A worker takes a stream of work items to perform. Before it is
    /// assigned its first work item, and after each item has been performed, the worker is asked to describe itself; the
    /// coordinator uses this description to match work items to workers. The act of performing a work item causes a sequence of
    /// output items to be generated
    /// </summary>
    /// <typeparam name="TWorkDescription">Type describing a work item</typeparam>
    /// <typeparam name="TWorkerDescription">Type describing a worker, used to match workers to items</typeparam>
    /// <typeparam name="TOutput">Type of the output items generated by work items</typeparam>
    public interface IWorker<TWorkDescription, TWorkerDescription, TOutput>
    {
        /// <summary>
        /// Return a description of the worker that the coordinator will use when matching work items to workers. This is called
        /// once before any work item has been assigned, and once after each work item is performed.
        /// </summary>
        /// <returns>A description of the worker, used to match work items to workers</returns>
        TWorkerDescription DescribeWorker();

        /// <summary>
        /// Execute a work item, generating a sequence of output records
        /// </summary>
        /// <param name="workItem">The work item to be executed</param>
        /// <returns>A sequence of array segments, each containing a sequence of records to be output</returns>
        IEnumerable<ArraySegment<TOutput>> DoWork(TWorkDescription workItem);
    }
    #endregion

    #region main internal class defining the dataflow vertices
    /// <summary>
    /// Class defining the vertices needed to construct a work-generator dataflow subgraph
    /// </summary>
    /// <typeparam name="TInput">Type of input records describing batches of work</typeparam>
    /// <typeparam name="TWorkerDescription">Type describing a worker, used to match work items to workers</typeparam>
    /// <typeparam name="TWorkDescription">Type describing a work item</typeparam>
    /// <typeparam name="TOutput">Type of output records generated by workers</typeparam>
    /// <typeparam name="TTime">Time type for input and output records</typeparam>
    internal class Generator<TInput, TWorkDescription, TWorkerDescription, TOutput, TTime> where TTime : Time<TTime>
    {
        #region coordinator vertex
        /// <summary>
        /// Vertex acting as coordinator for work generation: this takes input records and indications that workers are free,
        /// assigns work items to workers, and sends the work items to the appropriate worker vertices.
        /// 
        /// There is an invariant that each worker has at most one work item outstanding at any given (wall-clock) time.
        /// </summary>
        private class CoordinatorVertex : BinaryVertex<TInput, WorkRequest<TWorkerDescription>, WorkItem<TWorkDescription>, IterationIn<TTime>>
        {
            #region static helpers to create WorkItem records
            /// <summary>
            /// Returns a work item record that is sent to tell a worker to initialize and identify itself: this record does not
            /// include any actual work
            /// </summary>
            /// <param name="destination">The VertexId of the worker this will be sent to</param>
            /// <returns>The record to send</returns>
            private static WorkItem<TWorkDescription> Initial(int destination)
            {
                WorkItem<TWorkDescription> initial;

                // the destination field is used by the partition function on the outgoing edge to identify the destination worker
                initial.destination = destination;
                // this means don't do any work, just respond with a record describing yourself
                initial.type = WorkRecordType.Initialize;
                // fill in the dummy field
                initial.description = default(TWorkDescription);

                return initial;
            }

            /// <summary>
            /// Returns a work item record that is sent to tell a worker to do some work and then identify itself again
            /// </summary>
            /// <param name="destination">The VertexId of the worker this will be sent to</param>
            /// <param name="item">A description of the work to be done</param>
            /// <returns>The record to send</returns>
            private static WorkItem<TWorkDescription> Work(int destination, TWorkDescription item)
            {
                WorkItem<TWorkDescription> work;

                // the destination field is used by the partition function on the outgoing edge to identify the destination worker
                work.destination = destination;
                // this means actually do the work in the description field
                work.type = WorkRecordType.DoWork;
                work.description = item;

                return work;
            }
            #endregion

            #region private member fields
            /// <summary>
            /// user-supplied factory to generate a coordinator object for each outer time, to perform the logic of
            /// generating and assigning work items for that outer time
            /// </summary>
            private readonly Func<TTime, ICoordinator<TInput, TWorkDescription, TWorkerDescription>> coordinatorFactory;
            /// <summary>
            /// Placement used to create the worker vertices; this is needed so we can broadcast an initialization record to every
            /// worker vertex
            /// </summary>
            private readonly Placement workerPlacement;
            /// <summary>
            /// Dictionary of coordinator objects, one for each outer time we see on the input
            /// </summary>
            private readonly Dictionary<TTime, ICoordinator<TInput, TWorkDescription, TWorkerDescription>> coordinators;
            /// <summary>
            /// Set to keep track of which workers we have sent work items to, indexed by outer time (so there is one
            /// set per coordinator object in coordinators). Whenever we receive new input at a given outer time, we send
            /// an initialization request to any worker not in the set for that time. If the worker indicates that it is
            /// free, but we don't have work for it, then it is removed from the set again. This allows us to ensure that
            /// each worker only receives a single outstanding work item at any given outer time.
            /// </summary>
            private readonly Dictionary<TTime, HashSet<int>> liveWorkersByTime;
            #endregion

            #region receive an input record from the outside world
            /// <summary>
            /// Receive input from the outside world, call the ICoordinator logic to translate that record into a
            /// set of work items, and kick all the workers to cause them to request some new work
            /// </summary>
            /// <param name="message">input message</param>
            public override void OnReceive1(Message<TInput, IterationIn<TTime>> message)
            {
                // look up the coordinator and live set for this time, and make new ones if they don't already exist
                ICoordinator<TInput, TWorkDescription, TWorkerDescription> coordinator;
                HashSet<int> liveWorkers;
                if (this.coordinators.TryGetValue(message.time.outerTime, out coordinator))
                {
                    liveWorkers = this.liveWorkersByTime[message.time.outerTime];
                }
                else
                {
                    // make a new coordinator from the user-supplied factory and add it to the dictionary
                    coordinator = this.coordinatorFactory(message.time.outerTime);
                    this.coordinators[message.time.outerTime] = coordinator;

                    // make an empty set of live workers and add it to the dictionary
                    liveWorkers = new HashSet<int>();
                    this.liveWorkersByTime[message.time.outerTime] = liveWorkers;
                    // If there are multiple workers, and the coordinator is situated on the same thread as one
                    // of the workers, then liveWorkers is initialized to contain that VertexId, so that the worker
                    // on the coordinator's thread will never receive any work. This is because work items may take
                    // a long time, and we don't want the coordinator to be blocked from handing out work.
                    if (this.workerPlacement.Count > 1)
                    {
                        int coordinatorProcess = this.Stage.Placement.First().ProcessId;
                        int coordinatorThread = this.Stage.Placement.First().ThreadId;
                        IEnumerable<int> workersToBlock = this.workerPlacement.
                            Where(w => w.ProcessId == coordinatorProcess && w.ThreadId == coordinatorThread).
                            Select(w => w.VertexId);
                        if (workersToBlock.Count() > 0)
                        {
                            // block any worker on our thread from receiving work
                            liveWorkers.Add(workersToBlock.First());
                        }
                    }

                    // add a notification to ensure the coordinator gets discarded when work is finished for
                    // this time
                    this.NotifyAt(new IterationIn<TTime> { outerTime = message.time.outerTime, iteration = Int32.MaxValue - 1 });
                }

                // for each input record in the message, tell the coordinator to turn it into the requisite batch of work items
                for (int i=0; i<message.length; ++i)
                {
                    coordinator.AddInput(message.payload[i]);
                }

                // get the buffer that we're going to use to send output records
                var buffer = this.Output.GetBufferForTime(message.time);

                // send a wakeup request for initialization to each worker that is not currently live. 
                foreach (VertexLocation worker in this.workerPlacement.Where(w => !liveWorkers.Contains(w.VertexId)))
                {
                    // add the worker to the set of workers that have received a message and not yet gone back to sleep
                    liveWorkers.Add(worker.VertexId);
                    buffer.Send(CoordinatorVertex.Initial(worker.VertexId));
                }
            }
            #endregion

            #region receive an input record indicating that a worker is ready for more work
            /// <summary>
            /// Receive input from workers, indicating that they are ready for more work
            /// </summary>
            /// <param name="message">Message containing records describing which workers are ready</param>
            public override void OnReceive2(Message<WorkRequest<TWorkerDescription>, IterationIn<TTime>> message)
            {
                // there must already be a coordinator and live worker set for this time, since there won't be
                // any messages returned from a worker about the time until it has been initialized by us receiving
                // an input above at that time
                ICoordinator<TInput, TWorkDescription, TWorkerDescription> coordinator = this.coordinators[message.time.outerTime];
                HashSet<int> liveWorkers = this.liveWorkersByTime[message.time.outerTime];

                // get a buffer to write outputs into
                var buffer = this.Output.GetBufferForTime(message.time);

                // process each record in this message in turn
                for (int i=0; i<message.length; ++i)
                {
                    WorkRequest<TWorkerDescription> record = message.payload[i];

                    // make a default work item; this will get replaced by a real one if there is work to be assigned
                    TWorkDescription work = default(TWorkDescription);

                    // ask the user-supplied coordinator to try to assign work to the worker that has requested it
                    if (coordinator.AssignWork(record.description, ref work))
                    {
                        // a work item was assigned, and filled in to the work variable. Send a record back to the worker that
                        // indicated it was free, assigned the work item
                        buffer.Send(CoordinatorVertex.Work(record.source, work));
                    }
                    else
                    {
                        // The worker has not received any work, and is 'going to sleep'. Remove it from the set of live
                        // workers. This ensures that if we get more input, the worker will receive a new initialization
                        // and wake up again.
                        liveWorkers.Remove(record.source);
                    }
                }
            }
            #endregion

            #region receive a notification that an outer time is complete
            /// <summary>
            /// Receive a notification once all inputs and work items have been completed for a given outer time
            /// Notifications are always requested at time (outerTime,Int32.MaxInt-1).
            /// </summary>
            /// <param name="time">The time that is now complete</param>
            public override void OnNotify(IterationIn<TTime> time)
            {
                // we aren't going to see any more inputs or work items for this outer time, so discard the state
                // we are keeping for it
                this.coordinators.Remove(time.outerTime);
                this.liveWorkersByTime.Remove(time.outerTime);
            }
            #endregion

            #region constructor
            /// <summary>
            /// constructor for the coordinator vertex
            /// </summary>
            /// <param name="index">vertex index in stage, passed to base class</param>
            /// <param name="stage">stage, passed to base class</param>
            /// <param name="coordinatorFactory">factory to generate an ICoordinator for each time seen on the input</param>
            /// <param name="workerPlacement">placement of the worker stage, used to broadcast initializations to all workers</param>
            public CoordinatorVertex(
                int index, Stage<IterationIn<TTime>> stage,
                Func<TTime, ICoordinator<TInput, TWorkDescription, TWorkerDescription>> coordinatorFactory,
                Placement workerPlacement)
                : base(index, stage)
            {
                this.coordinatorFactory = coordinatorFactory;
                this.workerPlacement = workerPlacement;

                this.coordinators = new Dictionary<TTime, ICoordinator<TInput, TWorkDescription, TWorkerDescription>>();
                this.liveWorkersByTime = new Dictionary<TTime, HashSet<int>>();
            }
            #endregion
        }
        #endregion

        #region worker vertex
        /// <summary>
        /// Vertex that implements a worker, receiving work items on a single input and writing to two outputs. The first
        /// is the 'external' output that receives all the generated records of type TOutput, and the second is the feedback
        /// output that receives identification records indicating to the coordinator that this worker is ready for more work
        /// </summary>
        private class WorkerVertex : Vertex<IterationIn<TTime>>
        {
            #region private member variables
            /// <summary>
            /// The output buffer for writing the generated work records that are the external output of the vertex
            /// </summary>
            private VertexOutputBuffer<TOutput, IterationIn<TTime>> output;
            /// <summary>
            /// The output buffer for writing responses to the Coordinator indicating that this worker is ready for
            /// more work
            /// </summary>
            private VertexOutputBuffer<WorkRequest<TWorkerDescription>, IterationIn<TTime>> requests;

            /// <summary>
            /// The user-supplied factory to create a worker per outer time. Each worker can taking a work item and
            /// generate a stream of outputs, and also generate a self-description to send to the coordinator to
            /// request more work.
            /// </summary>
            private readonly Func<TTime, IWorker<TWorkDescription, TWorkerDescription, TOutput>> workerFactory;

            /// <summary>
            /// We store an IWorker for each outer time we have seen, and garbage-collect them when all work for
            /// that time has been completed
            /// </summary>
            private readonly Dictionary<TTime, IWorker<TWorkDescription, TWorkerDescription, TOutput>> workers;
            #endregion

            #region receive a work item from the coordinator
            /// <summary>
            /// Receive a work record from the coordinator. This may be an initialization request, which contains no
            /// work but just causes the worker to identify itself, or may contain a payload of a work item. The coordinator
            /// promises there is only one outstanding work record at a time for a given worker, so the message should not
            /// contain multiple records.
            /// </summary>
            /// <param name="message">the input message containing the work record</param>
            private void OnReceive(Message<WorkItem<TWorkDescription>, IterationIn<TTime>> message)
            {
                if (message.length != 1)
                {
                    // the coordinator only gives out one record per worker at a given outer time, so this
                    // shouldn't happen
                    throw new ApplicationException("Got mysterious payload length " + message.length);
                }

                IWorker<TWorkDescription, TWorkerDescription, TOutput> worker;
                if (!this.workers.TryGetValue(message.time.outerTime, out worker))
                {
                    // we haven't seen this time before. Generate a new worker from the factory, add it to the
                    // dictionary, and request a notification to be able to garbage collect it when all the work
                    // for this time is complete
                    worker = this.workerFactory(message.time.outerTime);
                    this.workers.Add(message.time.outerTime, worker);
                    this.NotifyAt(new IterationIn<TTime> { outerTime = message.time.outerTime, iteration = Int32.MaxValue - 1 });
                }

                // get the buffer to forward the output records to
                var outputBuffer = this.output.GetBufferForTime(message.time);

                // get the record out of the message
                WorkItem<TWorkDescription> item = message.payload[0];
                if (item.type == WorkRecordType.DoWork)
                {
                    // it has a payload of work, so call the user-supplied function to actually generate a
                    // sequence of output records, and write them all out.
                    IEnumerable<ArraySegment<TOutput>> data = worker.DoWork(item.description);
                    foreach (ArraySegment<TOutput> payload in data)
                    {
                        for (int i = 0; i < payload.Count; ++i)
                        {
                            outputBuffer.Send(payload.Array[i + payload.Offset]);
                        }
                    }
                }

                // get the buffer to forward the coordinator request to
                var requestBuffer = this.requests.GetBufferForTime(message.time);

                // identify ourselves as ready to receive another work item. The item.destination/request.source field is
                // the VertexId that Naiad uses to route requests to this worker, so setting request.source ensures that
                // the work item will get routed back to the same place. We send the request with the same logical time
                // we received the work item at, but the iteration counter will get incremented by the feedback vertex
                WorkRequest<TWorkerDescription> request =
                    new WorkRequest<TWorkerDescription> { source = item.destination, description = worker.DescribeWorker() };
                requestBuffer.Send(request);
            }
            #endregion

            #region receive a notification that all work has completed for an outer time
            /// <summary>
            /// Receive a notification that all work has finished for an outer time. Notifications are always
            /// requested for times (time.outerTime, Int32.MaxValue - 1)
            /// </summary>
            /// <param name="time"></param>
            public override void OnNotify(IterationIn<TTime> time)
            {
                // discard the worker we were using for this time
                this.workers.Remove(time.outerTime);
            }
            #endregion

            #region make a stage consisting of worker vertices
            /// <summary>
            /// Make a stage of worker vertices, receiving input from a stream supplied by the coordinator, and return a pair
            /// of streams corresponding to the external output records, and the identification requests that need to be sent
            /// back to the coordinator
            /// </summary>
            /// <param name="placement">the placement to use for vertices in the stage</param>
            /// <param name="input">the input stream of work requests from the coordinator</param>
            /// <param name="factory">a factory to generate a worker vertex object for each vertex in the stage</param>
            /// <param name="name">the friendly name identifying the stage</param>
            /// <returns></returns>
            public static Pair<Stream<TOutput, IterationIn<TTime>>, Stream<WorkRequest<TWorkerDescription>, IterationIn<TTime>>>
                MakeStage(
                Placement placement,
                Stream<WorkItem<TWorkDescription>, IterationIn<TTime>> input,
                Func<int, Stage<IterationIn<TTime>>, WorkerVertex> factory,
                string name)
            {
                // create a new stage object to hold the vertices
                var stage = Foundry.NewStage(placement, input.Context, factory, name);

                // make an input that responds correctly when messages arrive from the coordinator, routing the messages to the
                // worker described by the destination field
                var stageInput = stage.NewInput(input, (message, vertex) => vertex.OnReceive(message), r => r.destination);

                // make an output where workers can send their generated records, with no partitioning information
                var dataOutput = stage.NewOutput(vertex => vertex.output);

                // make an output where workers can send their identification records back to the coordinator, with
                // no partitioning information
                var requestOutput = stage.NewOutput(vertex => vertex.requests);

                // return the two output streams as a pair
                return new Pair<
                    Stream<TOutput, IterationIn<TTime>>,
                    Stream<WorkRequest<TWorkerDescription>, IterationIn<TTime>>>
                    (dataOutput, requestOutput);
            }
            #endregion

            #region constructor
            /// <summary>
            /// make a new worker vertex
            /// </summary>
            /// <param name="index">index of the vertex in the stage, passed to the base class</param>
            /// <param name="stage">stage the vertex belongs to, passed to the base class</param>
            /// <param name="workerFactory">IWorker object containing the user-supplied logic to generate output records from work items</param>
            public WorkerVertex(
                int index, Stage<IterationIn<TTime>> stage,
                Func<int, TTime, IWorker<TWorkDescription, TWorkerDescription, TOutput>> workerFactory)
                : base(index, stage)
            {
                this.output = new VertexOutputBuffer<TOutput, IterationIn<TTime>>(this);
                this.requests = new VertexOutputBuffer<WorkRequest<TWorkerDescription>, IterationIn<TTime>>(this);

                this.workerFactory = time => workerFactory(index, time);
                this.workers = new Dictionary<TTime, IWorker<TWorkDescription, TWorkerDescription, TOutput>>();
            }
            #endregion
        }
        #endregion

        #region method to generate the dataflow
        /// <summary>
        /// Generate a dataflow that takes a stream of input records and generates a stream of output records,
        /// by dividing each input into a set of work items. A supplied ICoordinator factory indicates how to
        /// divide an input into work items, and how to assign items to workers. A supplied IWorker factory
        /// indicates how to generate output records from a work item, and how to identify the worker so that
        /// the coordinator can decide which worker to assign items to.
        /// 
        /// The inputs corresponding to a particular time all form the same 'batch' and work items within that
        /// batch may be interleaved. If a second batch is started before the first completes then the efficiency
        /// of the work assignment may suffer, since batch A's coordinator cannot 'see' that a worker is busy working
        /// on an item from batch B and may assign work that will be queued behind work from batch B, even if other
        /// workers are idle.
        /// </summary>
        /// <param name="input">The stream of records describing work to be done</param>
        /// <param name="coordinatorFactory">The factory to generate a coordinator for each distinct input time</param>
        /// <param name="workerFactory">The factory to generate a worker for each distinct worker VertexId and
        /// input time</param>
        /// <returns>A stream of output records, with no guaranteed partitioning</returns>
        public static Stream<TOutput, TTime> Generate(
            Stream<TInput, TTime> input,
            Func<TTime, ICoordinator<TInput, TWorkDescription, TWorkerDescription>> coordinatorFactory,
            Func<int, TTime, IWorker<TWorkDescription, TWorkerDescription, TOutput>> workerFactory)
        {
            // determine the placement we are going to use for the workers
            Placement workerPlacement = input.ForStage.Computation.Controller.DefaultPlacement;

            // Make a loop context to hold the coordinator and the worker vertices
            var workLoop = new Dataflow.Iteration.LoopContext<TTime>(input.Context);

            // Make the feedback edge that takes work requests from workers and routes them back to the coordinator
            var feedback = workLoop.Delay<WorkRequest<TWorkerDescription>>();

            // Bring the input records into the loop, adding an extra loop coordinate that will be 0 for all records
            var workInput = workLoop.EnterLoop(input);

            // Make the coordinator stage, and return a stream consisting of work items to be handed to workers
            var coordinatorOutput = CoordinatorVertex.MakeStage(
                // A SingleVertex placement ensures there is just one coordinator vertex handing out all the work.
                new Placement.SingleVertex(),
                // this is the stream of input records that we are going to turn into work items
                workInput,
                // this is the stream of notifications coming from workers when they become idle
                feedback.Output,
                // this is the factory to instantiate the coordinator vertex: it needs to know the placement of
                // the workers, so it can broadcast initialization messages to them
                (i, s) => new CoordinatorVertex(i, s, coordinatorFactory, workerPlacement),
                // these are the partitioning requirements for the inputs. Since the coordinator stage has a single
                // vertex, the partitioning function can be null as all records get routed to the same place
                null, null,
                // this is the partitioning guarantee for the outputs, which is null because they all come from the
                // same coordinator vertex
                null,
                // this is the friendly name for the stage
                "Work Coordinator");

            // Make the worker vertex stage. The output is a pair of streams: the first is the generated output
            // records, and the second is the stream of notifications to send back to the controller when each
            // worker becomes idle. The MakeStage internally uses the desired partition function to route the
            // work items to the appropriate vertex
            var workers = WorkerVertex.MakeStage(
                // this is the placement we are using for the workers. We specify it explicitly to ensure it
                // matches the placement we told the coordinator, so broadcast will work
                workerPlacement,
                // this is the stream of work items coming from the coordinator
                coordinatorOutput,
                // this is the factory used to create each worker in the stage
                (i, s) => new WorkerVertex(i, s, workerFactory),
                // this is the friendly name for the stage
                "Work Generators");

            // connect the idle notifications to the feedback stage, which will forward them back to the coordinator
            feedback.Input = workers.Second;

            // strip off the loop coordinate from the output records, and return the resulting stream to the caller
            return workLoop.ExitLoop(workers.First);
        }
        #endregion
    }
    #endregion

    #region public MatchingCoordinator helper class to build a coordinator that matches work items to workers
    /// <summary>
    /// A coordinator that assigns work items to zero or more matching workers. When a worker is free, it is assigned a matching item if
    /// there is one, otherwise it is assigned an item that matched to no workers if there is one, otherwise it is assigned an item from
    /// a worker that has the maximal number of remaining unassigned items.
    /// </summary>
    /// <typeparam name="TInput">Type of an input record: each input expands to some number of work items</typeparam>
    /// <typeparam name="TCategory">Type of a work item category</typeparam>
    /// <typeparam name="TWorkStub">Type of a work item stub, expanded to a work item once the worker is chosen</typeparam>
    /// <typeparam name="TWorkDescription">Type of a work item description</typeparam>
    /// <typeparam name="TWorkerDescription">Type of a worker description: this is translated to a TQueueKey to find a match</typeparam>
    public abstract class MatchingCoordinator<TInput, TCategory, TWorkStub, TWorkDescription, TWorkerDescription> : ICoordinator<TInput, TWorkDescription, TWorkerDescription>
    {
        #region public Match helper class to describe a work item and the list of workers that match it
        /// <summary>
        /// A work item and the list of categories that match the work item.
        /// </summary>
        public class Match
        {
            /// <summary>
            /// A stub description of the work, to be passed to the worker when the work is assigned. Once the worker is
            /// assigned the stub is converted to a TWorkDescription using the derived ExpandWorkItem method; this allows
            /// for the case that the work item description is dependent on which worker is chosen to execute it
            /// </summary>
            public TWorkStub workStub;

            /// <summary>
            /// A list of queues that match to this work item. If this list is null or empty, the item will
            /// be assigned to an available worker after that worker has executed all matching items.
            /// </summary>
            public IEnumerable<TCategory> categories;

            /// <summary>
            /// Create a new empty match.
            /// </summary>
            public Match()
            {
                taken = false;
            }

            /// <summary>
            /// This is initially false. Once the item is assigned to a worker, Taken is set to true, but the item may be left on
            /// other worker queues (to avoid the cost of keeping track of its location in all the relevant queues). Items with
            /// taken set are skipped over when dequeueing matches from queues.
            /// </summary>
            internal bool taken;
        }
        #endregion

        #region helper class holding a queue of work items
        /// <summary>
        /// Queue of work items that match to a particular worker.
        /// </summary>
        protected class MatchQueue
        {
            /// <summary>
            /// Queue of items that match to this worker. Each Match may be present on multiple queues. If a Match has its taken field
            /// set to true, then it has already been consumed by another worker, and should be skipped while dequeueing.
            /// </summary>
            private readonly Queue<Match> queue;
            /// <summary>
            /// Number of items in queue that have item.taken == false (and are thus available).
            /// </summary>
            private int unusedMatches;
            /// <summary>
            /// A unique id that allows MatchQueues to form a unique sort order.
            /// </summary>
            private readonly int uid;

            /// <summary>
            /// Comparison function to use when sorting MatchQueues. a sorts before b if a contains more available matches
            /// than b, or if they contain the same number of available matches and a's uid is less than b's uid.
            /// </summary>
            /// <param name="a">First MatchQueue to compare</param>
            /// <param name="b">Second MatchQueue to compare</param>
            /// <returns>-1 if a has more available matches than b, or the same number of matches, but a's uid is less than b's. 0 if
            /// a and b have the same number of available matches and the same uid. 1 if a has fewer available matches than b, or the same
            /// number of matches, but a's uid is greater than b's.</returns>
            static public int Compare(MatchQueue a, MatchQueue b)
            {
                if (a.unusedMatches > b.unusedMatches)
                {
                    return -1;
                }
                else if (a.unusedMatches < b.unusedMatches)
                {
                    return 1;
                }
                else
                {
                    if (a.uid < b.uid)
                    {
                        return -1;
                    }
                    else if (a.uid == b.uid)
                    {
                        return 0;
                    }
                    else
                    {
                        return 1;
                    }
                }
            }

            /// <summary>
            /// Create an empty MatchQueue with the specified unique identifier
            /// </summary>
            /// <param name="uid">Unique identifier to allow MatchQueues to form a sort order</param>
            public MatchQueue(int uid)
            {
                this.queue = new Queue<Match>();
                this.unusedMatches = 0;
                this.uid = uid;
            }

            /// <summary>
            /// Add an unused Match to the queue. Throws an exception if the Match has been taken
            /// </summary>
            /// <param name="match">A Match to add to the queue</param>
            public void Enqueue(Match match)
            {
                if (match.taken)
                {
                    throw new ApplicationException("Can't add a taken match to a queue");
                }

                this.queue.Enqueue(match);
                ++this.unusedMatches;
            }

            /// <summary>
            /// The number of unused matches remaining in the queue
            /// </summary>
            public int Count
            {
                get { return this.unusedMatches; }
            }

            /// <summary>
            /// Return the next unused matched in the queue. Throws an exception if there are no remaining
            /// matches. DOES NOT UPDATE THE COUNT OF UNUSED MATCHES. The caller is responsible for decrementing
            /// the count of unused matches for every queue the return value appears in, including this one, by
            /// calling MarkTaken on the relevant queues.
            /// </summary>
            /// <returns>The next previously-unused match in the queue</returns>
            public Match Dequeue()
            {
                if (this.unusedMatches == 0)
                {
                    throw new ApplicationException("Queue is empty");
                }

                Match match;

                // skip over all matches that have been marked as taken by being removed from another queue
                do
                {
                    match = this.queue.Dequeue();
                }
                while (match.taken);

                // mark this match as taken. The unused queue count in this queue, and any other queues the match was
                // added to, must be decremented by calls to MarkTaken
                match.taken = true;

                return match;
            }

            /// <summary>
            /// Indicate that an unused entry in this queue has either been dequeued or marked as taken by another queue.
            /// This must be called appropriately to ensure the invariant that this.unusedMatches is equal to the number
            /// of unused Match elements in this.queue. Throws an exception if there are no unused matches in the queue.
            /// </summary>
            public void MarkTaken()
            {
                if (this.unusedMatches == 0)
                {
                    throw new ApplicationException("Queue is empty");
                }

                --unusedMatches;
            }
        }

        /// <summary>
        /// Class to sort MatchQueues by priority
        /// </summary>
        private class MatchQueueComparer : Comparer<MatchQueue>
        {
            public override int Compare(MatchQueue x, MatchQueue y)
            {
                return MatchQueue.Compare(x, y);
            }
        }
        #endregion

        #region member fields
        /// <summary>
        /// For each category with outstanding work, a queue of unused matches. There is an invariant that every value in
        /// this dictionary has Count greater than 0
        /// </summary>
        protected readonly Dictionary<TCategory, MatchQueue> waitingWork;
        /// <summary>
        /// Queue of unused matches that have no category, and can thus be assigned to any worker
        /// </summary>
        private readonly MatchQueue anyplaceWork;
        /// <summary>
        /// Sorted set of queues. Queues with more unused matches sort earlier. When a worker has no matching work, and
        /// there are no unused matches in this.anyplaceWork, a work item will be assigned from the queue that sorts
        /// earliest in this set
        /// </summary>
        private readonly SortedSet<MatchQueue> priority;

        /// <summary>
        /// The next identifier to be assigned to a MatchQueue. These identifiers exist only to ensure a unique sort
        /// order for MatchQueues
        /// </summary>
        private int matchQueueId;
        #endregion

        #region implementation of ICoordinator AddInput method
        /// <summary>
        /// Implements ICoordinator.AddInput. Takes an input record and calls the user-supplied function provided in
        /// the constructor, to translate the input into a set of work items, each of which is annotated with a list
        /// of zero or more categories. As workers become ready, their work will be assigned from these items.
        /// </summary>
        /// <param name="input">Record describing an input that will be converted to a set of work items</param>
        public void AddInput(TInput input)
        {
            // This dictionary will hold the items returned by the user-supplied function, sorted by category. The items are
            // marshaled into this dictionary before updating our datastructures to avoid hammering on the priority queue
            // with every individual addition
            Dictionary<TCategory, List<Match>> additionalWork = new Dictionary<TCategory,List<Match>>();

            // Call the derived-class function to get the list of work items corresponding to this input
            IEnumerable<Match> workItems = EnumerateWork(input);

            foreach (Match item in workItems)
            {
                if (item.categories == null || item.categories.Count() == 0)
                {
                    // the item has no matching categories, so put it on the queue of unaffiliated work
                    anyplaceWork.Enqueue(item);
                }
                else
                {
                    // go through each candidate category, and add the item to the list of new work we are building
                    // up for that category
                    foreach (TCategory category in item.categories)
                    {
                        List<Match> newWork;
                        if (!additionalWork.TryGetValue(category, out newWork))
                        {
                            newWork = new List<Match>();
                            additionalWork.Add(category, newWork);
                        }
                        newWork.Add(item);
                    }
                }
            }

            // Now we have figured out, for each category, the list of new items to add to its queue
            foreach (var element in additionalWork)
            {
                MatchQueue queue;
                if (this.waitingWork.TryGetValue(element.Key, out queue))
                {
                    // there was already a queue of items waiting for this category. Remove the corresponding
                    // element from the priority set, since the number of items in the queue (and hence the
                    // priority) is going to change
                    this.priority.Remove(queue);
                }
                else
                {
                    // there were no items queued for this category, so make a new queue and add it to the
                    // dictionary. Don't add anything to the priority set yet: that will happen below
                    queue = new MatchQueue(matchQueueId);
                    ++matchQueueId;
                    this.waitingWork.Add(element.Key, queue);
                    // let derived classes know there is a new queue
                    this.NotifyQueueAddition(element.Key);
                }

                // add each of the new matches to the queue for this category
                foreach (Match item in element.Value)
                {
                    queue.Enqueue(item);
                }

                // now that we have added all the new elements for this category, we can reinsert its queue
                // into the set sorted by queue length
                this.priority.Add(queue);
            }
        }
        #endregion

        #region implementation of ICoordinator AssignWork method
        /// <summary>
        /// Implements ICoordinator.AssignWork. Given a worker that is now free, assign it a work item if any remain.
        /// If there is an item matching the worker it will be returned; otherwise if there is an item that wasn't matched
        /// to any worker it will be returned; otherwise an item from another category's queue that has a maximal number of
        /// pending items will be returned. If there are no items remaining to be assigned, this returns false.
        /// </summary>
        /// <param name="worker">The worker that needs work</param>
        /// <param name="work">The work item to be assigned if any</param>
        /// <returns>True if any items remained, false otherwise</returns>
        public bool AssignWork(TWorkerDescription worker, ref TWorkDescription work)
        {
            // This will be set to non-null if we find an item to assign
            Match match = null;

            // This will be set to the category of the matching queue, if there is one
            TCategory queueCategory = default(TCategory);
            // Call the derived-class method to match a worker to a category. If this returns non-null then there was
            // a non-empty queue with work, and its key is returned in queueKey.
            MatchQueue queue = MapWorkerToQueue(worker, ref queueCategory);
            if (queue != null)
            {
                // There is an item in the worker's queue of matching work, so we will assign it; hence, let's remove it
                // from the queue. We will update the counts of unused items in all the queues that match belongs to below
                match = queue.Dequeue();
            }
            else if (this.anyplaceWork.Count > 0)
            {
                // There was no item in the worker's queue, but there is one in the list of items that can be matched
                // anywhere, so we will assign that; let's remove it from that queue
                match = this.anyplaceWork.Dequeue();
                // and update the queue's count of unused items
                this.anyplaceWork.MarkTaken();
            }
            else if (this.priority.Count > 0)
            {
                // The only remaining items would have preferred to be matched somewhere else. Never mind, let's just take
                // one from the queue of a worker with a maximal number of unused items.  We will update the counts of
                // unused items in all the queues that match belongs to below
                match = this.priority.First().Dequeue();
            }

            if (match == null)
            {
                // There was no outstanding work to assign
                return false;
            }

            if (match.categories != null)
            {
                // The item belongs to zero or more categories: we need to update the count of unused items for
                // each of those categories now that this item has been matched
                foreach (TCategory candidate in match.categories)
                {
                    // First, remove the queue from the priority set, since its count (and hence priority) are going to change
                    MatchQueue candidateQueue = this.waitingWork[candidate];
                    this.priority.Remove(candidateQueue);

                    // Now update the count of unused items in the queue
                    candidateQueue.MarkTaken();

                    if (candidateQueue.Count == 0)
                    {
                        // The queue has no more items, so remove it from the set of workers that have waiting work
                        this.waitingWork.Remove(candidate);
                        // let derived classes know the queue has been removed
                        this.NotifyQueueRemoval(candidate);
                    }
                    else
                    {
                        // Put the queue back in the priority set, now that it has a new count
                        this.priority.Add(candidateQueue);
                    }
                }
            }

            // Get a work item by converting the stub. If queue!=null the worker got the item from a
            // matching queue with category queueKey, otherwise the item doesn't match the worker, and
            // queueCategory is default(TCategory)
            work = ExpandWorkItem((queue != null), queueCategory, match.workStub);
            return true;
        }
        #endregion

        #region abstract and virtual methods
        /// <summary>
        /// Given an input item, return a list of work items, each with a list of categories
        /// </summary>
        /// <param name="input">input record to convert to work items</param>
        /// <returns>list of work items</returns>
        protected abstract IEnumerable<Match> EnumerateWork(TInput input);
        /// <summary>
        /// Given a worker, return a matching queue from this.waitingWork (and fill in its key to categoryKey)
        /// or null if there is no matching queue
        /// </summary>
        /// <param name="worker">The worker to match to a category</param>
        /// <param name="categoryKey">The category of the matching queue, if there is one</param>
        /// <returns>The matching queue, or null if there is no matching queue</returns>
        protected abstract MatchQueue MapWorkerToQueue(TWorkerDescription worker, ref TCategory categoryKey);
        /// <summary>
        /// Convert a work item stub to a work item once it is known which category, if any, it was matched to
        /// </summary>
        /// <param name="usedMatchingQueue">True if the item was drawn from a queue that matched the worker it is
        /// being assigned to</param>
        /// <param name="category">Category of the matching queue, if usedMatchingQueue==true</param>
        /// <param name="stub">Work item stub to convert</param>
        /// <returns>Filled-in work item</returns>
        protected abstract TWorkDescription ExpandWorkItem(bool usedMatchingQueue, TCategory category, TWorkStub stub);

        /// <summary>
        /// called whenever a new worker queue is added
        /// </summary>
        /// <param name="queue">key for the queue</param>
        protected virtual void NotifyQueueAddition(TCategory queue)
        {
        }

        /// <summary>
        /// called whenever an existing worker queue becomes empty and is deleted
        /// </summary>
        /// <param name="queue">key for the queue</param>
        protected virtual void NotifyQueueRemoval(TCategory queue)
        {
        }
        #endregion

        #region constructor
        /// <summary>
        /// Create a new MatchingCoordinator
        /// </summary>
        public MatchingCoordinator()
        {
            this.priority = new SortedSet<MatchQueue>(new MatchQueueComparer());
            this.waitingWork = new Dictionary<TCategory, MatchQueue>();
            this.anyplaceWork = new MatchQueue(0);
            // initialize the unique id for matchqueues to be created as work items are added
            this.matchQueueId = 1;
        }
        #endregion
    }
    #endregion

    #region extension methods to build the dataflow
    /// <summary>
    /// Extension methods for the work generator
    /// </summary>
    public static class ExtensionMethods
    {
        /// <summary>
        /// Add a work generator taking a stream of inputs and converting each input to a stream of outputs, by assigning inputs to workers
        /// of a given type
        /// </summary>
        /// <typeparam name="TInput">The type of input records to consume</typeparam>
        /// <typeparam name="TWorkDescription">Description of a work item</typeparam>
        /// <typeparam name="TWorkerDescription">Description of a worker, used to match workers to items</typeparam>
        /// <typeparam name="TOutput">The type of output records produced by workers</typeparam>
        /// <typeparam name="TTime">The time of input records</typeparam>
        /// <param name="input">stream of input records</param>
        /// <param name="coordinatorFactory">factory to make the work coordinator object</param>
        /// <param name="workerFactory">factory to make the worker objects</param>
        /// <returns>stream of output records</returns>
        public static Stream<TOutput, TTime> GenerateWork<TInput, TWorkDescription, TWorkerDescription, TOutput, TTime>(
            this Stream<TInput, TTime> input,
            Func<TTime, ICoordinator<TInput, TWorkDescription, TWorkerDescription>> coordinatorFactory,
            Func<int, TTime, IWorker<TWorkDescription, TWorkerDescription, TOutput>> workerFactory)
            where TTime : Time<TTime>
        {
            return Generator<TInput, TWorkDescription, TWorkerDescription, TOutput, TTime>.Generate(
                input, coordinatorFactory, workerFactory);
        }
    }
    #endregion
}
