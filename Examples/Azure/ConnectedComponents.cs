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

using Microsoft.Research.Naiad;
using Microsoft.Research.Naiad.Dataflow;
using Microsoft.Research.Naiad.Frameworks.Lindi;
using Examples.ConnectedComponents;
using Microsoft.Research.Naiad.Frameworks.Azure;
using Microsoft.Research.Naiad.Dataflow.PartitionBy;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Microsoft.WindowsAzure.Storage;
using Microsoft.WindowsAzure.Storage.Table;

namespace Examples.Azure
{
    class ConnectedComponents : Example
    {
        public string Usage
        {
            get { return "containername directoryname outputblobname"; }
        }

        public void Execute(string[] args)
        {
            var containerName = args[1];
            var directoryName = args[2];
            var outputblobName = args[3];

            CloudStorageAccount storageAccount = CloudStorageAccount.DevelopmentStorageAccount;

            var container = storageAccount.CreateCloudBlobClient()
                                          .GetContainerReference(containerName);

            if (!container.Exists())
                throw new Exception("No such container exists");

            // allocate a new controller from command line arguments.
            using (var controller = NewController.FromArgs(ref args))
            {
                // Set Console.Out to point at an Azure blob bearing the process id. 
                // See the important note at end of method about closing Console.Out.
                controller.SetConsoleOut(container, "stdout-{0}.txt");

                // allocate a new graph manager for the computation.
                using (var manager = controller.NewComputation())
                {
                    System.Diagnostics.Stopwatch stopwatch = new System.Diagnostics.Stopwatch();

                    // read the edges from azure storage
                    var edges = manager.ReadBinaryFromAzureBlobs<Pair<int, int>>(container, directoryName);

                    // symmetrize the graph by adding in transposed edges.
                    edges = edges.Select(x => new Pair<int, int>(x.v2, x.v1))
                                 .Concat(edges);

                    // invoke director reachability
                    var result = edges.DirectedReachability();
                         
                    // listen to the output for reporting, and also write the output somewhere in Azure
                    result.Subscribe(list => Console.WriteLine("labeled {0} nodes in {1}", list.Count(), stopwatch.Elapsed));
                    result.WriteBinaryToAzureBlobs(container, outputblobName);

                    stopwatch.Start();

                    // start computation and wait.
                    manager.Activate();
                    manager.Join();
                }

                controller.Join();
            }

            // very important to close the stream to flush writes to Azure.
            Console.Out.Close();
        }
    }
}
