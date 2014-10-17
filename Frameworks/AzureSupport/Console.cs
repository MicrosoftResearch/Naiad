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

using Microsoft.WindowsAzure.Storage.Blob;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace Microsoft.Research.Naiad.Frameworks.Azure
{
    /// <summary>
    /// Extension methods
    /// </summary>
    public static class ConsoleExtensionMethods
    {
        /// <summary>
        /// Redirects Console.Out to an Azure blob, with the process id in the filename.
        /// </summary>
        /// <param name="controller">Naiad controller</param>
        /// <param name="container">Azure container</param>
        /// <param name="format">Format string expecting the Naiad process id</param>
        public static void SetConsoleOut(this Microsoft.Research.Naiad.Controller controller, CloudBlobContainer container, string format)
        {
            var filename = string.Format(format, controller.Configuration.ProcessID);

            var writer = new System.IO.StreamWriter(container.GetBlockBlobReference(filename).OpenWrite());
            writer.AutoFlush = false;

            Console.SetOut(writer);
        }

        /// <summary>
        /// Redirects Console.Error to an Azure blob, with the process id in the filename.
        /// </summary>
        /// <param name="controller">Naiad controller</param>
        /// <param name="container">Azure container</param>
        /// <param name="format">Format string expecting the Naiad process id</param>
        public static void SetConsoleError(this Microsoft.Research.Naiad.Controller controller, CloudBlobContainer container, string format)
        {
            var filename = string.Format(format, controller.Configuration.ProcessID);

            var writer = new System.IO.StreamWriter(container.GetBlockBlobReference(filename).OpenWrite());
            writer.AutoFlush = false;

            Console.SetError(writer);
        }
    }
}
