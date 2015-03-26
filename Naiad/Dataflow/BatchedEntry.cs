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
using System.Threading.Tasks;

using Microsoft.Research.Naiad;
using Microsoft.Research.Naiad.Dataflow.Iteration;
using Microsoft.Research.Naiad.Input;

namespace Microsoft.Research.Naiad.Dataflow
{
    /// <summary>
    /// Extension methods for creating batch entry vertices
    /// </summary>
    public static class BatchedEntryExtensionMethods
    {
        /// <summary>
        /// Create a new batched entry subgraph
        /// </summary>
        /// <typeparam name="S">type of records exiting the subgraph</typeparam>
        /// <typeparam name="T">time type of records after exiting</typeparam>
        /// <param name="computation">computation graph</param>
        /// <param name="entryComputation">function within the subgraph</param>
        /// <returns>the function after exiting the subbatches</returns>
        public static Stream<S, T> BatchedEntry<S, T>(this Computation computation, Func<LoopContext<T>, Stream<S, IterationIn<T>>> entryComputation)
            where T : Time<T>
        {
            var helper = new LoopContext<T>(computation.Context);

            var batchedOutput = entryComputation(helper);

            return helper.ExitLoop(batchedOutput);
        }
    }
}
