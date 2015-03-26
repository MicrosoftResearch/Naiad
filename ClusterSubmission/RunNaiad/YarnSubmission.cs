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

using Microsoft.Research.Peloponnese.Yarn;
using Microsoft.Research.Peloponnese.Shared;
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Microsoft.Research.Naiad.Cluster.Submission
{
    class NativeYarnSubmission : ClusterSubmission
    {
        public NativeYarnSubmission(
            string rmNode, int wsPort, DfsClient dfsClient, string queueName, Uri stagingUri, Uri jobUri, string launcherNode, int launcherPort,
            int amMemoryInMB, int numberOfProcesses, int workerMemoryInMB, string[] args)
            : base(new NativeYarnClient(rmNode, wsPort, dfsClient, jobUri, launcherNode, launcherPort),
                   stagingUri, queueName, amMemoryInMB, numberOfProcesses, workerMemoryInMB, args)
        {
        }

        public NativeYarnSubmission(
            string rmNode, int wsPort, DfsClient dfsClient, string queueName, Uri stagingUri, Uri jobUri, string peloponneseDirectory,
            int amMemoryInMB, int numberOfProcesses, int workerMemoryInMB, string[] args)
            : base(
                   new NativeYarnClient(rmNode, wsPort, dfsClient, jobUri, LauncherJarFile(peloponneseDirectory), YarnDirectory()),
                   stagingUri, queueName, amMemoryInMB, numberOfProcesses, workerMemoryInMB, args)
        {
        }

        private static string LauncherJarFile(string peloponneseDirectory)
        {
            return Path.Combine(peloponneseDirectory, "Microsoft.Research.Peloponnese.YarnLauncher.jar");
        }

        private static string YarnDirectory()
        {
            string yarnDirectory = Environment.GetEnvironmentVariable("HADOOP_COMMON_HOME");

            if (yarnDirectory == null)
            {
                throw new ApplicationException("No HADOOP_COMMON_HOME defined");
            }

            return yarnDirectory;
        }
    }
}
