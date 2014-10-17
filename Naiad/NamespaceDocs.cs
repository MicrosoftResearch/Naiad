/*
 * Naiad ver. 0.5
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

namespace Microsoft.Research.Naiad
{
    /// <summary>
    /// The Microsoft.Research.Naiad namespace provides the basic classes that are used in all Naiad programs.
    /// </summary>
    class NamespaceDoc
    {
    }

    namespace Dataflow
    {
        /// <summary>
        /// The Dataflow namespace provides classes that enable the programmer to construct dataflow graphs manually.
        /// </summary>
        class NamespaceDoc
        {
        }

        namespace Iteration
        {
            /// <summary>
            /// The Dataflow.Iteration namespace provides classes that enable adding cycles to a dataflow graph.
            /// </summary>
            class NamespaceDoc
            {
            }
        }

        namespace PartitionBy
        {
            /// <summary>
            /// The Dataflow.PartitionBy namespace provides extension methods that enable manual repartitioning of data between vertices.
            /// </summary>
            class NamespaceDoc
            {
            }
        }

        namespace StandardVertices
        {
            /// <summary>
            /// The Dataflow.StandardVertices namespace provides base implementations of commonly-used dataflow vertex types, such as <see cref="UnaryVertex{TInput,TOutput,TTime}"/> and <see cref="BinaryVertex{TInput1,TInput2,TOutput,TTime}"/>.
            /// </summary>
            class NamespaceDoc
            {
            }
        }
    }

    namespace Diagnostics
    {
        /// <summary>
        /// The Diagnostics namespace provides classes that support <see cref="Logging"/>, tracing, and observing various events in the Naiad runtime.
        /// </summary>
        class NamespaceDoc
        {
        }
    }

    namespace Input
    {
        /// <summary>
        /// The Input namespace provides classes and extension methods for ingesting data into a Naiad computation.
        /// </summary>
        class NamespaceDoc
        {
        }
    }

    namespace Runtime.Progress
    {
        /// <summary>
        /// The Runtime.Progress namespace provides classes that support tracking the progress of a Naiad computation.
        /// </summary>
        class NamespaceDoc
        {
        }
    }

    namespace Serialization
    {
        /// <summary>
        /// The Serialization namespace contains classes that support serialization of Naiad data for network and file I/O.
        /// </summary>
        class NamespaceDoc
        {
        }
    }

    namespace Utilities
    {
        /// <summary>
        /// The Utilities namespace contains miscellaneous classes that support Naiad framework authors.
        /// </summary>
        class NamespaceDoc
        {
        }
    }
}
