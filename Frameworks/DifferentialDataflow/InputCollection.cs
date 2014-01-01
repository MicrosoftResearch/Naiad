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
using System.Text;
using System.Diagnostics;
using System.Collections.Concurrent;
using System.Threading;

using Naiad.DataStructures;
using Naiad.Dataflow.Channels;
using Naiad.Scheduling;
using Naiad.Runtime.Controlling;
using Naiad.CodeGeneration;
using Naiad;

namespace Naiad.Frameworks.DifferentialDataflow
{
    public class RecordObserver<R> : IObserver<R> where R : IEquatable<R>
    {
        private bool completedCalled;
        private readonly IObserver<IEnumerable<Weighted<R>>> collection;
        private readonly List<Weighted<R>> list;

        public RecordObserver(IObserver<IEnumerable<Weighted<R>>> collection)
        {
            this.completedCalled = false;
            this.collection = collection;
            list = new List<Weighted<R>>();
        }

        public void OnNext(R record)
        {
            this.OnNext(record, 1);
        }

        public void OnNext(R record, int weight)
        {
            Debug.Assert(!this.completedCalled);
            list.Add(new Weighted<R>(record, weight));
        }

        public void OnCompleted()
        {
            Debug.Assert(!this.completedCalled);
            this.collection.OnNext(list);
        }

        public void OnError(Exception error)
        {
            throw error;
        }
    }

    public class IncrementalCollection<R> : TypedCollection<R, Epoch>, InputCollection<R>
        where R : IEquatable<R>
    {
        //private readonly Naiad.Dataflow.InputStage<Weighted<R>> inputVertex;
        private readonly Naiad.Dataflow.BatchedDataSource<Weighted<R>> inputVertex;
        private readonly Naiad.Dataflow.Stream<Weighted<R>, Epoch> stream;

        public override Naiad.Dataflow.Stream<Weighted<R>, Epoch> Output
        {
            get { return this.stream; }
        }

        public void OnCompleted()
        {
            inputVertex.OnCompleted();
        }

        public void OnCompleted(IEnumerable<Weighted<R>> value)
        {
            inputVertex.OnCompleted(value);
        }

        public void OnCompleted(IEnumerable<R> values)
        {
            inputVertex.OnCompleted(values.Select(x => x.ToWeighted(1)));
        }

        public void OnCompleted(Weighted<R> value)
        {
            inputVertex.OnCompleted(value);
        }

        public void OnCompleted(R value)
        {
            inputVertex.OnCompleted(new Weighted<R>(value, 1));
        }

        public void OnError(Exception error)
        {
            inputVertex.OnError(error);
        }

        public void OnNext(IEnumerable<Weighted<R>> value)
        {
            inputVertex.OnNext(value);
        }

        internal override Naiad.Dataflow.OpaqueTimeContext<Epoch> Statistics { get { return this.stream.Context; } }

        public IncrementalCollection(Naiad.Runtime.GraphManager manager)
        {
            this.inputVertex = new Dataflow.BatchedDataSource<Weighted<R>>();
            this.stream = manager.NewInput(inputVertex);
        }
    }
}
