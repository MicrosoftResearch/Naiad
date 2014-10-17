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
using System.IO;
using System.Linq;
using System.Reflection;
using System.Text;
using System.Threading.Tasks;
using System.Xml;
using System.Xml.Linq;

using Microsoft.Research.Peloponnese.ClusterUtils;

namespace Microsoft.Research.Naiad.Cluster
{
    public class ClusterSubmission : PPMSubmission
    {
        private readonly ClusterClient clusterClient;
        private readonly XDocument launcherConfig;
        private static string[] FrameworkAssemblyNames = { "System", "System.Core", "mscorlib", "System.Xml" };

        private ClusterJob clusterJob;

        protected ClusterSubmission(ClusterClient cluster, Uri stagingRoot, string queueName, int amMemoryInMB, int numberOfProcesses, int workerMemoryInMB, string[] args)
        {
            this.clusterClient = cluster;

            string commandLine;
            IEnumerable<string> commandLineArgs;
            Helpers.MakeCommandLine(args, true, out commandLine, out commandLineArgs);

            string ppmHome = ConfigHelpers.GetPPMHome(null);

            string exePath = args[0];

            this.clusterClient.DfsClient.EnsureDirectory(stagingRoot, true);

            Uri jobStaging = this.clusterClient.DfsClient.Combine(stagingRoot, Environment.UserName, "naiadJob");

            XElement ppmWorkerResources = ConfigHelpers.MakePeloponneseWorkerResourceGroup(this.clusterClient.DfsClient, stagingRoot, ppmHome);
            XElement[] workerResources = { ppmWorkerResources };
            workerResources = workerResources.Concat(MakeJobResourceGroups(exePath, stagingRoot, jobStaging)).ToArray();

            XElement ppmResources = ConfigHelpers.MakePeloponneseResourceGroup(this.clusterClient.DfsClient, stagingRoot, ppmHome);
            XDocument config = Helpers.MakePeloponneseConfig(numberOfProcesses, workerMemoryInMB, "yarn", commandLine, commandLineArgs, false, workerResources);

            string configName = "config.xml";

            XElement configResources = ConfigHelpers.MakeConfigResourceGroup(
                this.clusterClient.DfsClient, jobStaging, config, configName);

            XElement[] launcherResources = { ppmResources, configResources };

            this.launcherConfig = ConfigHelpers.MakeLauncherConfig(
                "Naiad: " + commandLine, configName, queueName, amMemoryInMB, launcherResources,
                this.clusterClient.JobDirectoryTemplate.AbsoluteUri.Replace("_BASELOCATION_", "naiad-jobs"));
        }

        public void Dispose()
        {
            this.clusterClient.Dispose();
        }

        private XElement[] MakeJobResourceGroups(string exeName, Uri stagingRoot, Uri jobStaging)
        {
            if (exeName.ToLower().StartsWith("hdfs://"))
            {
                Uri exeDirectory = new Uri(exeName.Substring(0, exeName.LastIndexOf('/')));
                return new XElement[] { ConfigHelpers.MakeRemoteResourceGroup(this.clusterClient.DfsClient, exeDirectory, false) };
            }
            else
            {
                IEnumerable<string> dependencies = Microsoft.Research.Peloponnese.Shared.DependencyLister.Lister.ListDependencies(exeName);

                if (File.Exists(exeName + ".config"))
                {
                    dependencies = dependencies.Concat(new[] { exeName + ".config" }).ToArray();
                }

                IEnumerable<string> peloponneseDependencies = dependencies.Where(x => Path.GetFileName(x).StartsWith("Microsoft.Research.Peloponnese"));
                XElement peloponneseGroup = ConfigHelpers.MakeResourceGroup(this.clusterClient.DfsClient, this.clusterClient.DfsClient.Combine(stagingRoot, "peloponnese"), true, peloponneseDependencies);

                IEnumerable<string> naiadDependencies = dependencies.Where(x => Path.GetFileName(x).StartsWith("Microsoft.Research.Naiad"));
                XElement naiadGroup = ConfigHelpers.MakeResourceGroup(this.clusterClient.DfsClient, this.clusterClient.DfsClient.Combine(stagingRoot, "naiad"), true, naiadDependencies);

                IEnumerable<string> jobDependencies = dependencies.Where(x => !Path.GetFileName(x).StartsWith("Microsoft.Research.Naiad") && !Path.GetFileName(x).StartsWith("Microsoft.Research.Peloponnese"));
                XElement jobGroup = ConfigHelpers.MakeResourceGroup(this.clusterClient.DfsClient, jobStaging, false, jobDependencies);

                return new XElement[] { peloponneseGroup, naiadGroup, jobGroup };
            }
        }

        public void Submit()
        {
            this.clusterJob = this.clusterClient.Submit(
                this.launcherConfig,
                new Uri(this.clusterClient.JobDirectoryTemplate.AbsoluteUri.Replace("_BASELOCATION_", "naiad-jobs")));
        }

        public int Join()
        {
            this.clusterJob.Join();
            JobStatus status = this.clusterJob.GetStatus();
            if (status == JobStatus.Success)
            {
                return 0;
            }
            else
            {
                Console.Error.WriteLine(this.clusterJob.ErrorMsg);
                return 1;
            }
        }

        public ClusterJob ClusterJob { get { return this.clusterJob; } }
    }
}
