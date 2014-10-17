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
using System.Configuration;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

using Microsoft.Research.Peloponnese.Shared;
using Microsoft.Research.Peloponnese.Hdfs;
using Microsoft.Research.Peloponnese.WebHdfs;
using Microsoft.Research.Peloponnese.Azure;
using Microsoft.Research.Peloponnese.ClusterUtils;
using Microsoft.Research.Naiad.Util;

namespace Microsoft.Research.Naiad.Cluster.Submission
{
    public class Program
    {
        private enum ExecutionType
        {
            Local,
            Yarn,
            Azure
        };

        private static Flag ShowHelp = Flags.Define("-h,--help", typeof(bool));

        private static Flag ForceLocal = Flags.Define("--local", typeof(bool));
        private static Flag ForceAzure = Flags.Define("--azure", typeof(bool));
        private static Flag ForceYarn = Flags.Define("--yarn", typeof(bool));

        private static Flag NumHosts = Flags.Define("-n,--numhosts", 2);
        private static Flag PeloponneseHome = Flags.Define("-p,--peloponnesehome", typeof(string));

        private static Flag RMHostAndPort = Flags.Define("-r,--rmhost", typeof(string));
        private static Flag NameNodeAndPort = Flags.Define("-nn,--namenode", typeof(string));
        private static Flag YarnJobQueue = Flags.Define("-yq,--yarnqueue", typeof(string));
        private static Flag YarnAMMemory = Flags.Define("-amm,--ammemorymb", typeof(int));
        private static Flag YarnWorkerMemory = Flags.Define("-wm,--workermemorymb", typeof(int));
        private static Flag WebHdfsPort = Flags.Define("-w,--webhdfsport", typeof(int));
        private static Flag LauncherHostAndPort = Flags.Define("-l,--launcher", typeof(string));
        private static Flag LogsDumpFile = Flags.Define("-f,--fetch", typeof(string));

        private static Flag AzureSubscriptionId = Flags.Define("--subscriptionid", typeof(string));
        private static Flag AzureClusterName = Flags.Define("-c,--clustername", typeof(string));
        private static Flag AzureCertificateThumbprint = Flags.Define("--certthumbprint", typeof(string));
        private static Flag AzureStorageAccountName = Flags.Define("--storageaccount", typeof(string));
        private static Flag AzureStorageAccountKey = Flags.Define("--storagekey", typeof(string));
        private static Flag AzureStorageContainerName = Flags.Define("--container", typeof(string));

        private static Flag LocalJobDirectory = Flags.Define("-ld,--localdir", typeof(string));

        private const string Usage = @"Usage: RunNaiad [Shared options] [[Azure options]|[Yarn options]|[Local options]] NaiadExecutable.exe [Naiad options]

Runs the given Naiad executable on an Azure HDInsight or YARN cluster, or a set of local processes. If no Azure or Yarn
options are specified, local execution is assumed.

(N.B. For convenience, each option can be set in the App.config for this program,
      using the long form option name.)

Shared options:
    -n,--numhosts        Number of Naiad processes (default = 2)
    -p,--peloponnesehome Location of Peloponnese binaries (defaults to directory of the running binary)
    
Yarn options:
    -r,--rmhost          YARN cluster RM node hostname and optional port. Hostname is required, port defaults to 8088
    -nn,--namenode       YARN cluster namenode and optional port, defaults to rm hostname
    -yq,--yarnqueue      YARN cluster job queue, defaults to cluster's default queue
    -amm,--ammemorymb    YARN container memory requested for AM (coordinator). Default is cluster's maximum container size
    -wm,--workermemorymb YARN container memory requested for workers (Naiad processes). Default is cluster's maximum container size
    -w,--webhdfsport     Optional YARN namenode webhdfs port, defaults to 50070. If provided, RunNaiad will use
                         WebHdfs to upload resources. Otherwise, Java and YARN must be installed on the client computer.
    -l,--launcher        yarnlauncher hostname and optional port. If provided, RunNaiad will launch the job via the launcher
                         process. Otherwise, Java and YARN must be installed on the client computer.
    -f,--fetch           filename. fetch the job logs after the job finishes. yarn.cmd must be in the path for this to work.

Azure options:
    --c,clustername      HDInsight cluster name (required)
    --subscriptionid     Azure subscription ID (default = taken from Powershell settings)
    --certthumbprint     Azure certificate thumbprint (required if and only if subscription ID is provided)
    --storageaccount     Azure storage account name for staging resources (default = cluster default storage account)
    --storagekey         Azure storage account key for staging resources (default = cluster default storage account key)
    --container          Azure storage blob container name for staging resources (default = ""staging"")

Local options:
    -ld,--localdir        Local job working directory (default = '%PWD%\LocalJobs')";

        private static void GetHostAndPort(string input, string defaultHost, int defaultPort, out string host, out int port)
        {
            if (input == null)
            {
                host = defaultHost;
                port = defaultPort;
            }
            else
            {
                string[] parts = input.Split(':');
                host = parts[0].Trim();
                if (parts.Length == 2)
                {
                    if (Int32.TryParse(parts[1], out port))
                    {
                    }
                    else
                    {
                        throw new ApplicationException("Bad port specifier: " + input);
                    }
                }
                else if (parts.Length > 2)
                {
                    throw new ApplicationException("Bad host:port specifier: " + input);
                }
                else
                {
                    port = defaultPort;
                }
            }
        }

        private static void FetchLogs(string dumpFile, string applicationId)
        {
            ProcessStartInfo startInfo = new ProcessStartInfo("cmd.exe");
            startInfo.Arguments = "/c yarn.cmd logs -applicationId " + applicationId + " -appOwner " + Environment.UserName;
            startInfo.RedirectStandardOutput = true;
            startInfo.UseShellExecute = false;

            Console.WriteLine("Fetch logs to '" + dumpFile + "' with command 'cmd.exe " + startInfo.Arguments + "'");

            try
            {
                using (Stream dumpStream = new FileStream(dumpFile, FileMode.Create, FileAccess.Write, FileShare.ReadWrite))
                {
                    Process process = new Process();
                    process.StartInfo = startInfo;
                    bool started = process.Start();
                    if (!started)
                    {
                        Console.Error.WriteLine("Failed to start fetch command");
                        return;
                    }

                    using (StreamReader reader = process.StandardOutput)
                    {
                        Task finishCopy = reader.BaseStream.CopyToAsync(dumpStream);

                        process.WaitForExit();

                        finishCopy.Wait();
                    }
                }
            }
            catch (Exception e)
            {
                Console.Error.WriteLine("Fetching logs got exception: " + e.ToString());
            }
        }

        private static int RunNativeYarn(string[] args)
        {
            if (!RMHostAndPort.IsSet)
            {
                Console.Error.WriteLine("Error: Yarn cluster rm node hostname not set.");
                Console.Error.WriteLine(Usage);
                return 1;
            }

            string rmHost;
            int wsPort;
            GetHostAndPort(RMHostAndPort.StringValue, null, 8088, out rmHost, out wsPort);

            string nameNode;
            int hdfsPort;
            GetHostAndPort(NameNodeAndPort.IsSet ? NameNodeAndPort.StringValue : null, rmHost, -1, out nameNode, out hdfsPort);

            string queueName = null;
            if (YarnJobQueue.IsSet)
            {
                queueName = YarnJobQueue.StringValue;
            }

            int amMemoryMB = -1;
            if (YarnAMMemory.IsSet)
            {
                amMemoryMB = YarnAMMemory.IntValue;
            }

            int workerMemoryMB = -1;
            if (YarnWorkerMemory.IsSet)
            {
                workerMemoryMB = YarnWorkerMemory.IntValue;
            }

            string launcherNode;
            int launcherPort;
            GetHostAndPort(
                LauncherHostAndPort.IsSet ? LauncherHostAndPort.StringValue : null, null, -1,
                out launcherNode, out launcherPort);

            DfsClient dfsClient;
            if (WebHdfsPort.IsSet)
            {
                dfsClient = new WebHdfsClient(Environment.UserName, WebHdfsPort.IntValue);
            }
            else
            {
                dfsClient = new HdfsClient();
            }

            if (args[0].ToLower().StartsWith("hdfs://"))
            {
                if (!dfsClient.IsFileExists(new Uri(args[0])))
                {
                    Console.Error.WriteLine("Error: Naiad program {0} does not exist.", args[0]);
                    Console.Error.WriteLine(Usage);
                    return 1;
                }
            }
            else
            {
                if (!File.Exists(args[0]))
                {
                    Console.Error.WriteLine("Error: Naiad program {0} does not exist.", args[0]);
                    Console.Error.WriteLine(Usage);
                    return 1;
                }
            }

            UriBuilder builder = new UriBuilder();
            builder.Scheme = "hdfs";
            builder.Host = nameNode;
            builder.Port = hdfsPort;
            Uri jobRoot = dfsClient.Combine(builder.Uri, "user", Environment.UserName);
            Uri stagingRoot = dfsClient.Combine(builder.Uri, "tmp", "staging");

            NativeYarnSubmission submission;

            if (launcherNode == null)
            {
                submission = new NativeYarnSubmission(rmHost, wsPort, dfsClient, queueName, stagingRoot, jobRoot, PeloponneseHome, amMemoryMB, NumHosts, workerMemoryMB, args);
            }
            else
            {
                submission = new NativeYarnSubmission(rmHost, wsPort, dfsClient, queueName, stagingRoot, jobRoot, launcherNode, launcherPort, amMemoryMB, NumHosts, workerMemoryMB, args);
            }

            submission.Submit();

            Console.WriteLine("Waiting for application to complete");

            int ret = submission.Join();

            if (LogsDumpFile.IsSet)
            {
                FetchLogs(LogsDumpFile.StringValue, submission.ClusterJob.Id);
            }

            submission.Dispose();

            return ret;
        }

        private static int RunHDInsight(string[] args)
        {
            if (!File.Exists(args[0]))
            {
                Console.Error.WriteLine("Error: Naiad program {0} does not exist.", args[0]);
                Console.Error.WriteLine(Usage);
                return 1;
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
                    return 1;
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
                return 1;
            }
            if (cluster == null)
            {
                Console.Error.WriteLine("Error: Failed to find cluster {0} in default or supplied subscriptions", clusterName);
                Console.Error.WriteLine(Usage);
                return 1;
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
            Uri baseUri = Utils.ToAzureUri(cluster.StorageAccount, containerName, "", null, cluster.StorageKey);
            AzureYarnClient azureYarn = new AzureYarnClient(subscriptionManagement, azureDfs, baseUri, ConfigHelpers.GetPPMHome(null), clusterName);
            AzureYarnSubmission submission = new AzureYarnSubmission(azureYarn, baseUri, NumHosts, args);

            submission.Submit();
            return submission.Join();
        }

        private static int RunLocal(string[] args)
        {
            if (!File.Exists(args[0]))
            {
                Console.Error.WriteLine("Error: Naiad program {0} does not exist.", args[0]);
                Console.Error.WriteLine(Usage);
                return 1;
            }

            if (!LocalJobDirectory.IsSet)
            {
                LocalJobDirectory.Parse("LocalJobs");
            }
            LocalSubmission submission = new LocalSubmission(NumHosts, args, LocalJobDirectory);
            submission.Submit();
            return submission.Join();
        }

        public static int Run(string[] args)
        {
            if (Environment.GetEnvironmentVariable("PELOPONNESE_HOME") != null)
            {
                PeloponneseHome.Parse(Environment.GetEnvironmentVariable("PELOPONNESE_HOME"));
            }
            else
            {
                string exeName = System.Diagnostics.Process.GetCurrentProcess().MainModule.FileName;
                PeloponneseHome.Parse(Path.GetDirectoryName(exeName));
            }

            Flags.Parse(ConfigurationManager.AppSettings);

            args = Flags.Parse(args);

            if (ShowHelp.BooleanValue)
            {
                Console.Error.WriteLine(Usage);
                return 0;
            }

            if (args.Length < 1)
            {
                Console.Error.WriteLine("Error: No Naiad program specified.");
                Console.Error.WriteLine(Usage);
                return 1;
            }

            bool isLocal = ForceLocal.IsSet;
            bool isNativeYarn = ForceYarn.IsSet;
            bool isAzureHDInsight = ForceAzure.IsSet;

            // first find out if we forced an execution type with an explicit argument
            if (isLocal)
            {
                if (isNativeYarn)
                {
                    Console.Error.WriteLine("Can't force both Yarn and Local execution.");
                    Console.Error.WriteLine(Usage);
                    return 1;
                }
                if (isAzureHDInsight)
                {
                    Console.Error.WriteLine("Can't force both Azure and Local execution.");
                    Console.Error.WriteLine(Usage);
                    return 1;
                }
            }
            else if (isNativeYarn)
            {
                if (isAzureHDInsight)
                {
                    Console.Error.WriteLine("Can't force both Azure and Yarn execution.");
                    Console.Error.WriteLine(Usage);
                    return 1;
                }
            }
            else if (!isAzureHDInsight)
            {
                // there's no explicit argument to force execution type, so guess based on which arguments are set
                isLocal =
                    (LocalJobDirectory.IsSet);
                isNativeYarn =
                    (RMHostAndPort.IsSet || NameNodeAndPort.IsSet || YarnJobQueue.IsSet || YarnAMMemory.IsSet || YarnWorkerMemory.IsSet ||
                     WebHdfsPort.IsSet || LauncherHostAndPort.IsSet || LogsDumpFile.IsSet);
                isAzureHDInsight =
                    (AzureSubscriptionId.IsSet || AzureClusterName.IsSet || AzureCertificateThumbprint.IsSet ||
                     AzureStorageAccountName.IsSet || AzureStorageAccountKey.IsSet || AzureStorageContainerName.IsSet);
            }

            if (isNativeYarn)
            {
                if (isAzureHDInsight)
                {
                    Console.Error.WriteLine("Can't specify Yarn and Azure options.");
                    Console.Error.WriteLine(Usage);
                    return 1;
                }
                if (isLocal)
                {
                    Console.Error.WriteLine("Can't specify Yarn and local options.");
                    Console.Error.WriteLine(Usage);
                    return 1;
                }
                return RunNativeYarn(args);
            }
            else if (isAzureHDInsight)
            {
                if (isLocal)
                {
                    Console.Error.WriteLine("Can't specify Azure and local options.");
                    Console.Error.WriteLine(Usage);
                    return 1;
                }
                return RunHDInsight(args);
            }
            else
            {
                return RunLocal(args);
            }
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
