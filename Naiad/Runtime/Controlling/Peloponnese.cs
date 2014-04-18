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
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Net;
using System.Text;
using System.Xml.Linq;

using Microsoft.Research.Naiad.Diagnostics;

namespace Microsoft.Research.Naiad
{
    internal class PeloponneseClient
    {
        /// <summary>
        /// the environment variable used to communicate the job Guid to a
        /// spawned process. This is set by the ProcessGroupManager for each
        /// process it creates
        /// </summary>
        private const string EnvJobGuid = "PELOPONNESE_JOB_GUID";

        /// <summary>
        /// the environment variable used to communicate the server's Uri to a
        /// spawned process. This is set by the ProcessGroupManager for each
        /// process it creates
        /// </summary>
        private const string EnvManagerServerUri = "PELOPONNESE_SERVER_URI";

        /// <summary>
        /// the environment variable used to communicate the name of the process group
        /// that a spawned process belongs to. This is set by the ProcessGroupManager for each
        /// process it creates. The spawned process uses this name when it registers itself
        /// with the web server
        /// </summary>
        private const string EnvProcessGroup = "PELOPONNESE_PROCESS_GROUP";

        /// <summary>
        /// the environment variable used to communicate the identifier for a spawned
        /// process. This is set by the ProcessGroupManager for each
        /// process it creates. Identifiers must be unique (within a given group) over
        /// the lifetime of the server. The spawned process uses this identifier when
        /// it registers itself with the web server
        /// </summary>
        private const string EnvProcessIdentifier = "PELOPONNESE_PROCESS_IDENTIFIER";

        private Guid jobGuid;
        private string serverAddress;
        private string procIdentifier;
        private UInt64 epoch;
        private UInt64 version;
        private int targetNumberOfWorkers;

        private IPEndPoint[] knownWorkers;
        public IPEndPoint[] WorkerEndpoints { get { return knownWorkers; } }

        private int selfIndex;
        public int ThisWorkerIndex { get { return selfIndex; } }

        public PeloponneseClient(IPEndPoint socketEndpoint)
        {
            Logging.Info("Creating Peloponnese client");

            string socketAddress;
            if (socketEndpoint.Address == IPAddress.Any)
            {
                socketAddress = Environment.MachineName;
            }
            else
            {
                socketAddress = socketEndpoint.Address.ToString();
            }

            epoch = 0;
            version = 0;
            targetNumberOfWorkers = -1;
            knownWorkers = null;
            selfIndex = -1;

            string guidString = Environment.GetEnvironmentVariable(EnvJobGuid);
            if (guidString == null)
            {
                throw new ApplicationException("Can't find environment variable " + EnvJobGuid);
            }
            this.jobGuid = Guid.Parse(guidString);

            serverAddress = Environment.GetEnvironmentVariable(EnvManagerServerUri);
            if (serverAddress == null)
            {
                throw new ApplicationException("Can't find environment variable " + EnvManagerServerUri);
            }

            string groupName = Environment.GetEnvironmentVariable(EnvProcessGroup);
            if (groupName == null)
            {
                throw new ApplicationException("Can't find environment variable " + EnvProcessGroup);
            }

            procIdentifier = Environment.GetEnvironmentVariable(EnvProcessIdentifier);
            if (procIdentifier == null)
            {
                throw new ApplicationException("Can't find environment variable " + EnvProcessIdentifier);
            }

            XElement server = new XElement("ServerSocket");
            server.SetAttributeValue("hostname", socketAddress);
            server.SetAttributeValue("port", socketEndpoint.Port.ToString());

            XElement details = new XElement("ProcessDetails");
            details.Add(server);

            string status = details.ToString();

            string registration = String.Format("{0}register?guid={1}&group={2}&identifier={3}", serverAddress, jobGuid.ToString(), groupName, procIdentifier);
            HttpWebRequest request = HttpWebRequest.Create(registration) as HttpWebRequest;
            request.Method = "POST";

            Logging.Info("Registering worker with Peloponnese at " + serverAddress);

            using (Stream upload = request.GetRequestStream())
            {
                using (StreamWriter sw = new StreamWriter(upload))
                {
                    sw.Write(status);
                }
            }

            using (HttpWebResponse response = request.GetResponse() as HttpWebResponse)
            {
                // discard the response
            }

            Logging.Info("Registered worker with Peloponnese");
        }

        private XContainer GetStatus()
        {
            StringBuilder sb = new StringBuilder(serverAddress);
            sb.Append("status");

            if (targetNumberOfWorkers <= 0)
            {
                // we haven't seen any status yet. Don't add any predicates to the request,
                // so it will return immediately
                Logging.Info("Getting initial status from Peloponnese");
            }
            else
            {
                // wait until the epoch changes, or we get all the computers
                sb.AppendFormat("?epochGreater={0}", epoch);
                sb.AppendFormat("&thresholdGreater=Worker:{0}", targetNumberOfWorkers - 1);
                Logging.Info("Blocking on status from Peloponnese until number of workers > " + (targetNumberOfWorkers-1));
            }

            HttpWebRequest request = HttpWebRequest.Create(sb.ToString()) as HttpWebRequest;
            request.Timeout = System.Threading.Timeout.Infinite;

            using (HttpWebResponse status = request.GetResponse() as HttpWebResponse)
            {
                using (Stream response = status.GetResponseStream())
                {
                    Logging.Info("Got status response from Peloponnese");

                    using (var reader = System.Xml.XmlReader.Create(response))
                    {
                        return XDocument.Load(reader);
                    }
                }
            }
        }

        public void NotifyCleanShutdown()
        {
            StringBuilder sb = new StringBuilder(serverAddress);
            sb.Append("startshutdown");
            Logging.Info("Telling Peloponnese to start preparing for shutdown");

            HttpWebRequest request = HttpWebRequest.Create(sb.ToString()) as HttpWebRequest;
            request.Timeout = System.Threading.Timeout.Infinite;
            request.Method = "POST";

            using (Stream rStream = request.GetRequestStream())
            {
                // no data
            }

            using (HttpWebResponse status = request.GetResponse() as HttpWebResponse)
            {
                using (Stream response = status.GetResponseStream())
                {
                    // ignore
                    Logging.Info("Got ack from Peloponnese");
                }
            }
        }

        private bool UpdateStatus(XContainer status)
        {
            XElement processes = status.Descendants("RegisteredProcesses").Single();
            UInt64 newEpoch = UInt64.Parse(processes.Attribute("epoch").Value);
            UInt64 newVersion = UInt64.Parse(processes.Attribute("version").Value);

            XElement workers = processes.Descendants("ProcessGroup").Where(pg => pg.Attribute("name").Value == "Worker").Single();
            int newTarget = int.Parse(workers.Attribute("targetNumberOfProcesses").Value);

            Debug.Assert(newEpoch >= epoch);
            if (newEpoch > epoch)
            {
                epoch = newEpoch;
                targetNumberOfWorkers = newTarget;
                version = 0;
            }
            else
            {
                Debug.Assert(newTarget == targetNumberOfWorkers);
            }

            Debug.Assert(newVersion >= version);
            if (newVersion > version)
            {
                version = newVersion;
            }

            List<KeyValuePair<string, IPEndPoint>> newPeer = new List<KeyValuePair<string, IPEndPoint>>();

            foreach (var processElement in workers.Descendants("Process"))
            {
                string id = processElement.Attribute("identifier").Value;
                XElement details = processElement.Descendants("ProcessDetails").Single();
                XElement server = details.Descendants("ServerSocket").Single();
                string hostName = server.Attribute("hostname").Value;
                int port = int.Parse(server.Attribute("port").Value);

                IPAddress[] choices = Dns.GetHostAddresses(hostName);
                IEnumerable<IPAddress> ipv4 = choices.Where(a => a.AddressFamily == System.Net.Sockets.AddressFamily.InterNetwork);
                IPAddress remoteAddress = ipv4.First();

                newPeer.Add(new KeyValuePair<string,IPEndPoint>(id, new IPEndPoint(remoteAddress, port)));
            }

            if (newPeer.Count == targetNumberOfWorkers)
            {
                Logging.Info("Got status response with all " + targetNumberOfWorkers + " workers");
                IEnumerable<KeyValuePair<string, IPEndPoint>> sorted = newPeer.OrderBy(x => x.Key);

                knownWorkers = sorted.Select(x => x.Value).ToArray();
                selfIndex = sorted.
                    Select((x, i) => new KeyValuePair<string, int>(x.Key, i)).
                    Where(x => x.Key == procIdentifier).
                    Single().
                    Value;

                Logging.Info("Self index is " + selfIndex);

                return true;
            }
            else
            {
                Logging.Info("Got status response with " + newPeer.Count + " workers; need to wait for " + targetNumberOfWorkers);
                return false;
            }
        }

        public void WaitForAllWorkers()
        {
            while (true)
            {
                XContainer status = GetStatus();
                if (UpdateStatus(status))
                {
                    return;
                }
            }
        }
    }
}
