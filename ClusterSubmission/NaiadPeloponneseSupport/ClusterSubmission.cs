/*
 * Naiad ver. 0.3
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

using Microsoft.Research.Peloponnese.Storage;
using Microsoft.Research.Peloponnese.ClusterUtils;

namespace Microsoft.Research.Naiad.Cluster
{
    public class ClusterSubmission : PPMSubmission
    {
        private readonly IDfsClient dfsClient;
        private readonly ClusterClient clusterClient;
        private readonly string exeDirectory;
        private readonly XDocument launcherConfig;
        private static string[] FrameworkAssemblyNames = { "System", "System.Core", "mscorlib", "System.Xml" };

        private ClusterJob clusterJob;

        protected ClusterSubmission(IDfsClient dfs, ClusterClient cluster, int numberOfProcesses, string[] args)
        {
            this.dfsClient = dfs;
            this.clusterClient = cluster;

            string commandLine;
            IEnumerable<string> commandLineArgs;
            Helpers.MakeCommandLine(args, true, out commandLine, out commandLineArgs);

            string ppmHome = ConfigHelpers.GetPPMHome(null);

            string exePath = args[0];
            this.exeDirectory = Path.GetDirectoryName(exePath);

            string jobStaging = dfs.Combine("staging", Environment.UserName, "naiadJob");

            XElement ppmResources = ConfigHelpers.MakePeloponneseResourceGroup(this.dfsClient, ppmHome);
            XElement frameworkResources;
            XElement jobResources;
            MakeJobResourceGroups(exePath, jobStaging, out frameworkResources, out jobResources);

            XElement[] workerResources = { ppmResources, frameworkResources, jobResources };

            XDocument config = Helpers.MakePeloponneseConfig(numberOfProcesses, "yarn", commandLine, commandLineArgs, false, workerResources);

            string configName = "config.xml";

            XElement configResources = ConfigHelpers.MakeConfigResourceGroup(this.dfsClient, jobStaging, config, configName);

            XElement[] launcherResources = { ppmResources, configResources };

            this.launcherConfig = ConfigHelpers.MakeLauncherConfig("Naiad: " + commandLine, configName, launcherResources, this.clusterClient.JobDirectoryTemplate.Replace("_BASELOCATION_", "naiad-jobs"));
        }

        private Assembly DependencyResolveEventHandler(object sender, ResolveEventArgs args)
        {
            string leafName = args.Name.Substring(0, args.Name.IndexOf(","));
            string assemblyPath = Path.Combine(this.exeDirectory, leafName);

            string dll = assemblyPath + ".dll";
            if (File.Exists(dll))
            {
                return Assembly.LoadFrom(dll);
            }

            string exe = assemblyPath + ".exe";
            if (File.Exists(exe))
            {
                return Assembly.LoadFrom(exe);
            }

            throw new ApplicationException("Can't find assembly " + args.ToString());
        }

        /// <summary>
        /// Returns the non-framework assemblies on which a given assembly depends.
        /// </summary>
        /// <param name="source">The initial assembly</param>
        /// <returns>A set of non-framework assemblies on which the given assembly depends</returns>
        private HashSet<Assembly> GetDependenciesInternal(Assembly source)
        {
            HashSet<Assembly> visited = new HashSet<Assembly>();
            Queue<Assembly> assemblyQueue = new Queue<Assembly>();
            assemblyQueue.Enqueue(source);
            visited.Add(source);

            while (assemblyQueue.Count > 0)
            {
                Assembly currentAssembly = assemblyQueue.Dequeue();

                foreach (AssemblyName name in currentAssembly.GetReferencedAssemblies())
                {
                    Assembly referencedAssembly = Assembly.Load(name);
                    if (!visited.Contains(referencedAssembly) && !FrameworkAssemblyNames.Contains(name.Name) && !(name.Name.StartsWith("System")))
                    {
                        visited.Add(referencedAssembly);
                        assemblyQueue.Enqueue(referencedAssembly);
                    }
                }
            }

            return visited;
        }

        /// <summary>
        /// Returns the locations of non-framework assemblies on which the assembly with the given filename depends.
        /// </summary>
        /// <param name="source">The filename of the assembly</param>
        /// <returns>An array of filenames for non-framework assemblies on which the given assembly depends</returns>
        private string[] Dependencies(string assemblyFilename)
        {
            Assembly assembly = Assembly.LoadFrom(assemblyFilename);
            AppDomain.CurrentDomain.AssemblyResolve += new ResolveEventHandler(DependencyResolveEventHandler);
            return GetDependenciesInternal(assembly).Select(x => x.Location).ToArray();
        }

        private void MakeJobResourceGroups(string exeName, string jobStaging, out XElement frameworkGroup, out XElement jobGroup)
        {
            string[] naiadComponentsArray =
            {
                "Naiad.dll",
                "Naiad.pdb"
            };
            HashSet<string> naiadComponents = new HashSet<string>();
            foreach (string c in naiadComponentsArray)
            {
                naiadComponents.Add(c);
            }

            string[] dependencies = Dependencies(exeName);

            if (File.Exists(exeName + ".config"))
            {
                dependencies = dependencies.Concat(new[] { exeName + ".config" }).ToArray();
            }

            IEnumerable<string> frameworkDependencies = dependencies.Where(x => naiadComponents.Contains(Path.GetFileName(x)));
            frameworkGroup = ConfigHelpers.MakeResourceGroup(dfsClient, dfsClient.Combine("staging", "naiad"), true, frameworkDependencies);

            IEnumerable<string> jobDependencies = dependencies.Where(x => !naiadComponents.Contains(Path.GetFileName(x)));
            jobGroup = ConfigHelpers.MakeResourceGroup(dfsClient, jobStaging, false, jobDependencies);
        }

        public void Submit()
        {
            this.clusterJob = this.clusterClient.Submit(this.launcherConfig, this.clusterClient.JobDirectoryTemplate.Replace("_BASELOCATION_", "naiad-jobs"));
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
    }

}
