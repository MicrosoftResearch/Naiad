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
        //private static Flag AzureStorageIsPublic = Flags.Define("--storageispublic", typeof(bool));

        private const string Usage = @"Usage: AzureSubmission [Azure options] NaiadExecutable.exe [Naiad options]

Runs the given Naiad executable on an HDInsight cluster.

(N.B. For convenience, each option can be set in the App.config for this program,
      using the long form option name.)

Options:
    -n,--numhosts        Number of Naiad processes (default = 2)
    -t,--threads         Number of worker threads per Naiad process (default = 8)
    
Azure options:
    --subscriptionid     Azure subscription ID (default = taken from Powershell settings)
    --clustername        HDInsight cluster name
    --storageaccount     Azure storage account name
    --storagekey         Azure storage account key
    --container          Azure storage blob container name";

        private static void GetPowershellDefaults(out string defaultSubscriptionId, out string defaultCertThumbprint)
        {
            defaultSubscriptionId = null;
            defaultCertThumbprint = null;

            var configDir = Path.Combine(
                Environment.GetFolderPath(Environment.SpecialFolder.ApplicationData),
                "Windows Azure Powershell");
            var defaultFile = Path.Combine(configDir, "WindowsAzureProfile.xml");
            if (File.Exists(defaultFile))
            {
                using (FileStream s = new FileStream(defaultFile, FileMode.Open, FileAccess.Read))
                {
                    XDocument doc = XDocument.Load(s);
                    XNamespace ns = doc.Root.GetDefaultNamespace();
                    IEnumerable<XElement> subs = doc.Descendants(ns + "AzureSubscriptionData");
                    foreach (XElement sub in subs)
                    {
                        try
                        {
                            if (bool.Parse(sub.Descendants(ns + "IsDefault").Single().Value))
                            {
                                defaultCertThumbprint = sub.Descendants(ns + "ManagementCertificate").Single().Value;
                                defaultSubscriptionId = sub.Descendants(ns + "SubscriptionId").Single().Value;
                                break; // should be only one default
                            }
                        }
                        catch { /* swallow any exceptions so we just return null values */ }
                    }
                }
            }
        }

        static int Run(string[] args)
        {
            string defaultSubscriptionId;
            string defaultCertThumbprint;
            GetPowershellDefaults(out defaultSubscriptionId, out defaultCertThumbprint);

            if (!string.IsNullOrEmpty(defaultSubscriptionId))
                AzureSubscriptionId.Parse(defaultSubscriptionId);
            if (!string.IsNullOrEmpty(defaultCertThumbprint))
                AzureCertificateThumbprint.Parse(defaultCertThumbprint);

            Flags.Parse(ConfigurationManager.AppSettings);

            args = Flags.Parse(args);

            if (ShowHelp.BooleanValue || args.Length == 0)
            {
                Console.Error.WriteLine(Usage);
                return 0;
            }
            if (!AzureStorageAccountName.IsSet)
            {
                Console.Error.WriteLine("Error: Azure storage account name not set.");
                Console.Error.WriteLine(Usage);
                return -1;
            }
            if (!AzureStorageAccountKey.IsSet)
            {
                Console.Error.WriteLine("Error: Azure storage key not set.");
                Console.Error.WriteLine(Usage);
                return -1;
            }
            if (!AzureStorageContainerName.IsSet)
            {
                Console.Error.WriteLine("Error: Azure storage container name not set.");
                Console.Error.WriteLine(Usage);
                return -1;
            }
            if (!File.Exists(args[0]))
            {
                Console.Error.WriteLine("Error: Naiad program {0} does not exist.", args[0]);
                Console.Error.WriteLine(Usage);
                return -1;
            }

            AzureDfsClient azureDfs = new AzureDfsClient(AzureStorageAccountName.StringValue, AzureStorageAccountKey.StringValue, AzureStorageContainerName.StringValue, false);
            AzureYarnClient azureYarn = new AzureYarnClient(azureDfs, ConfigHelpers.GetPPMHome(null), AzureClusterName.StringValue, AzureSubscriptionId.StringValue, AzureCertificateThumbprint.StringValue);
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
