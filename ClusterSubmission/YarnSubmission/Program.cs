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

using Microsoft.Research.Naiad.Util;
using System;
using System.Collections.Generic;
using System.Configuration;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Microsoft.Research.Naiad.Cluster.NativeYarn
{
    public class Program
    {
        private static Flag ShowHelp = Flags.Define("-h,--help", typeof(bool));
        private static Flag NumHosts = Flags.Define("-n,--numhosts", 2);
        private static Flag NumThreads = Flags.Define("-t,--threads", 8);
        private static Flag HeadNodeHostname = Flags.Define("-y,--yarncluster", typeof(string));
        private static Flag PeloponneseHome = Flags.Define("-p,--peloponnesehome", typeof(string));
        
        private const string Usage = @"Usage: NativeYarnSubmission [Azure options] NaiadExecutable.exe [Naiad options]

Runs the given Naiad executable on an YARN cluster.

(N.B. For convenience, each option can be set in the App.config for this program,
      using the long form option name.)

Options:
    -n,--numhosts        Number of Naiad processes (default = 2)
    -t,--threads         Number of worker threads per Naiad process (default = 8)
    -p,--peloponnesehome Location of Peloponnese binaries (default = %PELOPONNESE_HOME%)
    
Azure options:
    -y,--yarncluster     YARN cluster head node hostname";


        public static int Run(string[] args)
        {
            if (Environment.GetEnvironmentVariable("PELOPONNESE_HOME") != null)
                PeloponneseHome.Parse(Environment.GetEnvironmentVariable("PELOPONNESE_HOME"));

            Flags.Parse(ConfigurationManager.AppSettings);

            args = Flags.Parse(args);

            if (ShowHelp.BooleanValue)
            {
                Console.Error.WriteLine(Usage);
                return 0;
            }
            if (!PeloponneseHome.IsSet)
            {
                Console.Error.WriteLine("Error: Peloponnese home directory not set.");
                Console.Error.WriteLine(Usage);
                return -1;
            }
            if (!HeadNodeHostname.IsSet)
            {
                Console.Error.WriteLine("Error: Yarn cluster head node hostname not set.");
                Console.Error.WriteLine(Usage);
                return -1;
            }

            NativeYarnSubmission submission = new NativeYarnSubmission(HeadNodeHostname.StringValue, 9000, 50070, NumHosts, args);

            submission.Submit();
            return submission.Join();
        }

        public static void Main(string[] args)
        {
            try
            {
                int exitCode = Run(args);
                Console.WriteLine("Application return exit code " + exitCode);
            }
            catch (Exception e)
            {
                Console.WriteLine("Exception " + e.Message + "\n" + e.ToString());
            }
        }
    }
}
