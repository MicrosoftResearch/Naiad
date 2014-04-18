/*
 * Naiad ver. 0.4
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
using System.Reflection;
using System.Text;
using System.Threading.Tasks;

namespace Microsoft.Research.Naiad.Cluster.DependencyLister
{
    public class Lister : MarshalByRefObject
    {
        private static string[] FrameworkAssemblyNames = { "System", "System.Core", "mscorlib", "System.Xml" };

        /// <summary>
        /// Returns the non-framework assemblies on which a given assembly depends.
        /// </summary>
        /// <param name="source">The initial assembly</param>
        /// <returns>A set of non-framework assemblies on which the given assembly depends</returns>
        private static HashSet<Assembly> Dependencies(Assembly source)
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
        /// <param name="assemblyFilename">The filename of the assembly</param>
        /// <returns>An array of filenames for non-framework assemblies on which the given assembly depends</returns>
        public string[] ListDependencies(string assemblyFilename)
        {
            Assembly assembly = Assembly.LoadFrom(assemblyFilename);
            return Lister.Dependencies(assembly).Select(x => x.Location).ToArray();
        }
    }
}
