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
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Microsoft.Research.Naiad.Cluster.Local
{
    class Program
    {
        private static Flag ShowHelp = Flags.Define("-h,--help", typeof(bool));

        private static Flag NumHosts = Flags.Define("-n,--numhosts", 2);
        private static Flag NumThreads = Flags.Define("-t,--threads", 8);
        private static Flag LocalJobDirectory = Flags.Define("-l,--localdir", "LocalJobs");

        private const string Usage = @"Usage: LocalSubmission [ptions] NaiadExecutable.exe [Naiad options]

Runs the given Naiad executable in multiple processes on the local machine.

(N.B. For convenience, each option can be set in the App.config for this program,
      using the long form option name.)

Options:
    -n,--numhosts        Number of Naiad processes (default = 2)
    -t,--threads         Number of worker threads per Naiad process (default = 8)
    -l,--localdir        Local job working directory (default = '%PWD%\LocalJobs')";

        static int Run(string[] args)
        {
            Flags.Parse(System.Configuration.ConfigurationManager.AppSettings);

            args = Flags.Parse(args);

            if (ShowHelp.BooleanValue)
            {
                Console.Error.WriteLine(Usage);
                return 0;
            }

            LocalSubmission submission = new LocalSubmission(NumHosts, args, LocalJobDirectory);
            submission.Submit();
            return submission.Join();
        }

        static void Main(string[] args)
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
