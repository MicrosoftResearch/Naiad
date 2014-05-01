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
using System.Text;

using Microsoft.Research.Naiad.Frameworks.Azure;
using Microsoft.Research.Naiad.Dataflow.PartitionBy;

namespace Microsoft.Research.Naiad.AzureExamples
{
    public class Repartition : Example
    {
        public string Usage
        {
            get { return "containerName inputDirectory outputDirectory"; }
        }

        public string Help
        {
            get { return "repartitions the text contexts of inputDirectory into as many parts as there are workers, writing the outputs to outputDirectory."; }
        }

        public void Execute(string[] args)
        {
            using (var computation = NewComputation.FromArgs(ref args))
            {
                computation.Controller.SetConsoleOut(computation.DefaultBlobContainer("naiad-outputs"), "out-{0}.txt");
                computation.Controller.SetConsoleError(computation.DefaultBlobContainer("naiad-outputs"), "err-{0}.txt");

                if (args.Length == 4)
                {
                    var containerName = args[1];
                    var inputDirectory = args[2];
                    var outputDirectory = args[3];

                    if (!inputDirectory.Equals(outputDirectory))
                    {
                        var container = computation.DefaultBlobContainer(containerName);

                        computation.ReadTextFromAzureBlobs(container, inputDirectory)
                                   .PartitionBy(x => x.GetHashCode())
                                   .WriteTextToAzureBlobs(container, outputDirectory + "/part-{0}-{1}.txt");
                    }
                    else
                    {
                        Console.Error.WriteLine("ERROR: Input directory name ({0}) equals output directory name ({1})", inputDirectory, outputDirectory);
                    }
                }
                else
                {
                    Console.Error.WriteLine("repartition requires three additional arguments: " + this.Usage);
                }

                computation.Activate();
                computation.Join();

                Console.Out.Close();
                Console.Error.Close();
            }
        }
    }
}
