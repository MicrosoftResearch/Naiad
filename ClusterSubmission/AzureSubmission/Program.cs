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

using Microsoft.Research.Naiad.Util;
using Microsoft.Research.Peloponnese.ClusterUtils;
using Microsoft.Research.Peloponnese.Storage;
using System;
using System.Collections.Generic;
using System.Configuration;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Xml.Linq;

namespace Microsoft.Research.Naiad.Cluster.Azure
{
    class Program
    {
        private static Flag ShowHelp = Flags.Define("-h,--help", typeof(bool));

        private static Flag NumHosts = Flags.Define("-n,--numprocs", 2);
        private static Flag NumThreads = Flags.Define("-t,--threads", 8);
        
        private static Flag AzureSubscriptionId = Flags.Define("--subscriptionid", typeof(string));
        private static Flag AzureClusterName = Flags.Define("--clustername", typeof(string));
        private static Flag AzureCertificateThumbprint = Flags.Define("--certthumbprint", typeof(string));
        private static Flag AzureStorageAccountName = Flags.Define("--storageaccount", typeof(string));
        private static Flag AzureStorageAccountKey = Flags.Define("--storagekey", typeof(string));
        private static Flag AzureStorageContainerName = Flags.Define("--container", typeof(string));

        private const string Usage = @"Usage: AzureSubmission [Azure options] NaiadExecutable.exe [Naiad options]

Runs the given Naiad executable on an HDInsight cluster.

(N.B. For convenience, each option can be set in the App.config for this program,
      using the long form option name.)

Options:
    -n,--numhosts        Number of Naiad processes (default = 2)
    -t,--threads         Number of worker threads per Naiad process (default = 8)
    
Azure options:
    --subscriptionid     Azure subscription ID (default = taken from Powershell settings)
    --clustername        HDInsight cluster name (default = cluster if a single cluster is registered to all subscriptions)
    --storageaccount     Azure storage account name for staging resources (default = cluster default storage account)
    --storagekey         Azure storage account key for staging resources (default = cluster default storage acount key)
    --container          Azure storage blob container name for staging resources (default = ""staging"")";

        static int Run(string[] args)
        {

            Flags.Parse(ConfigurationManager.AppSettings);

            args = Flags.Parse(args);

            if (ShowHelp.BooleanValue || args.Length == 0)
            {
                Console.Error.WriteLine(Usage);
                return 0;
            }

            if (!File.Exists(args[0]))
            {
                Console.Error.WriteLine("Error: Naiad program {0} does not exist.", args[0]);
                Console.Error.WriteLine(Usage);
                return -1;
            }

            AzureSubscriptions subscriptionManagement = new AzureSubscriptions();

            if (AzureSubscriptionId.IsSet && AzureCertificateThumbprint.IsSet)
            {
                subscriptionManagement.AddSubscription(AzureSubscriptionId.StringValue, AzureCertificateThumbprint.StringValue);
            }

            string clusterName = null;
            if (AzureClusterName.IsSet)
            {
                clusterName = AzureClusterName.StringValue;

                if (AzureStorageAccountName.IsSet && AzureStorageAccountKey.IsSet)
                {
                    subscriptionManagement.SetClusterAccountAsync(clusterName, AzureStorageAccountName.StringValue, AzureStorageAccountKey.StringValue).Wait();
                }
            }
            else
            {
                IEnumerable<AzureCluster> clusters = subscriptionManagement.GetClusters();
                if (clusters.Count() == 1)
                {
                    clusterName = clusters.Single().Name;
                }
                else
                {
                    Console.Error.WriteLine("Error: Cluster name must be specified unless there is a single configured cluster in default and supplied subscriptions");
                    Console.Error.WriteLine(Usage);
                    return -1;
                }
            }

            AzureCluster cluster;
            try
            {
                cluster = subscriptionManagement.GetClusterAsync(clusterName).Result;
            }
            catch (Exception)
            {
                Console.Error.WriteLine("Error: Failed to find cluster " + clusterName + " in default or supplied subscriptions");
                Console.Error.WriteLine(Usage);
                return -1;
            }
            if (cluster == null)
            {
                Console.Error.WriteLine("Error: Failed to find cluster {0} in default or supplied subscriptions", clusterName);
                Console.Error.WriteLine(Usage);
                return -1;
            }

            string containerName = "staging";
            if (AzureStorageContainerName.IsSet)
            {
                containerName = AzureStorageContainerName.StringValue;
            }

            // The args are augmented with an additional setting containing the Azure connection string.
            args = args.Concat(new string[] { "--addsetting", "Microsoft.Research.Naiad.Cluster.Azure.DefaultConnectionString", string.Format("\"DefaultEndpointsProtocol=https;AccountName={0};AccountKey={1}\"", cluster.StorageAccount.Split('.').First(), cluster.StorageKey) }).ToArray();

            Console.Error.WriteLine("Submitting job with args: {0}", string.Join(" ", args));

            AzureDfsClient azureDfs = new AzureDfsClient(cluster.StorageAccount, cluster.StorageKey, containerName);
            AzureYarnClient azureYarn = new AzureYarnClient(subscriptionManagement, azureDfs, ConfigHelpers.GetPPMHome(null), clusterName);
            AzureYarnSubmission submission = new AzureYarnSubmission(azureDfs, azureYarn, NumHosts, args);

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
