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
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Xml.Linq;

using Microsoft.Research.Peloponnese.ClusterUtils;

namespace Microsoft.Research.Naiad.Cluster
{
    class LocalSubmission : PPMSubmission
    {
        private readonly string ppmHome;
        private readonly string workingDirectory;
        private readonly string configPath;

        private Process ppmProcess;

        private string CreateDirectory(string wdBase)
        {
            if (!Directory.Exists(wdBase))
            {
                Directory.CreateDirectory(wdBase);
            }

            var existingDirs = Directory.EnumerateDirectories(wdBase);
            var existingJobs = existingDirs.Select(x => Path.GetFileName(x))
                                           .Select(x => { int jobId; if (int.TryParse(x, out jobId)) return jobId; else return -1; });

            int nextJob = 0;
            if (existingJobs.Count() > 0)
            {
                nextJob = existingJobs.Max() + 1;
            }

            var wd = Path.Combine(wdBase, nextJob.ToString());

            Directory.CreateDirectory(wd);

            return wd;
        }

        public LocalSubmission(int numberOfProcesses, string[] args, string localDirectory)
        {
            string commandLine;
            IEnumerable<string> commandLineArgs;
            Helpers.MakeCommandLine(args, false, out commandLine, out commandLineArgs);

            this.ppmHome = ConfigHelpers.GetPPMHome(null);

            XDocument config = Helpers.MakePeloponneseConfig(numberOfProcesses, -1, "local", commandLine, commandLineArgs, true, null);

            this.workingDirectory = CreateDirectory(localDirectory);

            this.configPath = "config.xml";
            config.Save(Path.Combine(this.workingDirectory, this.configPath));
        }

        public void Dispose()
        {
        }

        public void Submit()
        {
            ProcessStartInfo psi = new ProcessStartInfo();
            psi.FileName = Path.Combine(this.ppmHome, "Microsoft.Research.Peloponnese.PersistentProcessManager.exe");
            psi.Arguments = this.configPath;
            psi.UseShellExecute = false;
            psi.WorkingDirectory = this.workingDirectory;

            this.ppmProcess = new Process();
            this.ppmProcess.StartInfo = psi;
            this.ppmProcess.EnableRaisingEvents = true;

            this.ppmProcess.Start();
        }

        public int Join()
        {
            this.ppmProcess.WaitForExit();
            return this.ppmProcess.ExitCode;
        }
    }
}
