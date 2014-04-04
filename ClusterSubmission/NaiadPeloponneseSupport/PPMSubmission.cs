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
using System.Text;
using System.Threading.Tasks;
using System.Xml.Linq;

using Microsoft.Research.Peloponnese.ClusterUtils;

namespace Microsoft.Research.Naiad.Cluster
{
    public interface PPMSubmission
    {
        void Submit();
        int Join();
    }

    public class Helpers
    {
        public static void MakeCommandLine(string[] args, bool stripPath, out string exeName, out IEnumerable<string> commandLineArgs)
        {
            if (stripPath)
            {
                exeName = Path.GetFileName(args[0]);
            }
            else
            {
                exeName = args[0];
            }

            List<string> commandLineArgsList = new List<string>(args.Length);
            for (int i = 1; i < args.Length ;++i)
            {
                commandLineArgsList.Add(args[i]);
            }
            commandLineArgsList.Add("--ppm");

            commandLineArgs = commandLineArgsList;
        }

        public static XDocument MakePeloponneseConfig(
            int numberOfProcesses, string type,
            string commandLine, IEnumerable<string> commandLineArgs, bool addRedirects, IEnumerable<XElement> resources)
        {
            XDocument configDoc = new XDocument();

            XElement docElement = new XElement("PeloponneseConfig");

            XElement serverElement = new XElement("PeloponneseServer");

            XElement portElement = new XElement("Port");
            portElement.Value = "8471";
            serverElement.Add(portElement);

            XElement prefixElement = new XElement("Prefix");
            prefixElement.Value = "/peloponnese/server/";
            serverElement.Add(prefixElement);

            XElement workers = ConfigHelpers.MakeProcessGroup(
                "Worker", type, -1, numberOfProcesses, true,
                commandLine, commandLineArgs, "LOG_DIRS", "stdout.txt", "stderr.txt",
                resources, null);
            serverElement.Add(workers);

            docElement.Add(serverElement);

            configDoc.Add(docElement);

            return configDoc;
        }
    }
}
