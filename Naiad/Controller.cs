/*
 * Naiad ver. 0.2
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

#define NEW_CLOCK_CHANNEL
//#define SEPARATE_CLOCK_NET
using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Text;

using System.Threading;
using Naiad.Data_structures;
using Naiad.Channels;
using Naiad.CodeGeneration;
using Naiad.Operators;
using System.IO;
using System.Threading.Tasks;
using System.Collections.Concurrent;
using System.Net.Sockets;
using System.Net;
using Naiad.Util;
using Naiad.Scheduling;
using Naiad.ControllerImplementations;

namespace Naiad
{
    public enum Mode
    {
        Legacy, // Statically-provisioned cluster.
        Master,
        Worker,
        Client
    }

    public class Configuration
    {

        public static Configuration Default
        {
            get
            {
                string[] emptyArgs = new string[0];
                Configuration ret = Configuration.FromArgs(ref emptyArgs);
                return ret;
            }
        }

        internal int RecordChannel = -1;

        private Mode mode = Mode.Legacy;
        /// <summary>
        /// Indicates in which mode this controller is running, for example whether this
        /// is a client process or a worker.
        /// </summary>
        public Mode Mode { get { return this.mode; } set { this.mode = value; } }

        private Uri serviceUri = null;
        /// <summary>
        /// The WCF endpoint that is used to contact this controller externally.
        /// </summary>
        public Uri ServiceUri { get { return this.serviceUri; } set { this.serviceUri = value; } }

        private int processID = 0;
        /// <summary>
        /// The ID of the local process in this computation (starting at 0).
        /// </summary>
        public int ProcessID { get { return this.processID; } set { this.processID = value; } }

        private int workerCount = 1;
        /// <summary>
        /// The number of workers (i.e. CPUs used) in the local process.
        /// 
        /// N.B. At present, all processes in a computation must use the same worker count.
        /// </summary>
        public int WorkerCount { get { return this.workerCount; } set { this.workerCount = value; } }

        private IPEndPoint[] endpoints = null;
        /// <summary>
        /// The network addresses of the processes in this computation, indexed by process ID.
        /// </summary>
        public IPEndPoint[] Endpoints { get { return this.endpoints.ToArray(); } set { this.endpoints = value.ToArray(); } }

        private int reachabilityInterval = 1000;
        /// <summary>
        /// The periodic interval in milliseconds at which Naiad will attempt to compact its collection state.
        /// </summary>
        public int CompactionInterval { get { return this.reachabilityInterval; } set { this.reachabilityInterval = value; } }

        private bool separateClock = false;
        /// <summary>
        /// EXPERIMENTAL: Setting this to true and running with n > 1 processes will route all clock traffic through process n-1.
        /// </summary>
        public bool SeparateClock { get { return this.separateClock; } set { this.separateClock = value; } }

        private int deadlockTimeout = Timeout.Infinite;
        /// <summary>
        /// Setting this to a value other than Timeout.Infinite will cause Naiad to print diagnostic information
        /// after a process sleeps for that number of milliseconds.
        /// </summary>
        public int DeadlockTimeout { get { return this.deadlockTimeout; } set { this.deadlockTimeout = value; } }

        private bool multipleLocalProcesses = true;
        /// <summary>
        /// Setting this to true ensures that, if cores are pinned to CPUs, subsequent processes are pinned to different
        /// sets of CPUs.
        /// </summary>
        public bool MultipleLocalProcesses { get { return this.multipleLocalProcesses; } set { this.multipleLocalProcesses = value; } }

        /// <summary>
        /// Prints information about the standard Naiad command-line options, which are used to build
        /// a configuration in Configuration.FromArgs().
        /// </summary>
        public static void Usage()
        {
            Console.Error.WriteLine("Naiad options:\n"
                + String.Format("\t--procid,-p\tProcess ID (default 0)\n")
                + String.Format("\t--threads,-t\tNumber of threads (default 1)\n")
                + String.Format("\t--numprocs,-n\tNumber of processes (default 1)\n")
                + "\t--hosts,-h\tList of hostnames in the form host:port\n"
                + "\t--local\t\tShorthand for -h localhost:2101 localhost:2102 ... localhost:2100+numprocs\n"
                + String.Format("\t--log-level,-l\tDebug|Info|Progress|Error|Fatal|Off (default Progress)\n")
                + String.Format("\t--log-type\tConsole|File|Memory (default Console)\n")
                //+ String.Format("\t--reachability\tmilliseconds per reachability (default {0})\n", reachabilityInterval)
                //+ String.Format("\t--deadlocktimeout\tmilliseconds before exit if deadlocked (default {0})\n", deadlockTimeout)
                );
            Console.Error.WriteLine("Runs single-process by default, optionally set -t for number of threads");
            Console.Error.WriteLine("To run multiprocess, specify -p, -n, and -h");
            Console.Error.WriteLine("   Example for 2 machines, M1 and M2, using TCP port 2101:");
            Console.Error.WriteLine("      M1> NaiadExamples.exe pagerank -p 0 -n 2 -h M1:2101 M2:2101");
            Console.Error.WriteLine("      M2> NaiadExamples.exe pagerank -p 1 -n 2 -h M1:2101 M2:2101");
            Console.Error.WriteLine("To run multiprocess on one machine, specify -p, -n and --local for each process");
        }

        /// <summary>
        /// Builds a Naiad configuration from the given command-line arguments, interpreting them according to
        /// the output of Configuration.Usage().
        /// </summary>
        /// <param name="args"></param>
        /// <returns></returns>
        public static Configuration FromArgs(ref string[] args)
        {
            var config = new Configuration();

            List<string> strippedArgs = new List<string>();

            // Defaults
            string[] Prefixes = new string[] { "localhost:2101" };

            bool procIDSet = false;
            bool numProcsSet = false;
            bool hostsSet = false;

            int Processes = 1;

            Flags.Define("-l,--log-level", LoggingLevel.Progress);
            Flags.Define("--log-type", LoggingStyle.Console);
            args = Flags.Parse(args);
            Logging.LogLevel = Flags.Get("log-level").EnumValue<LoggingLevel>();
            Logging.LogStyle = Flags.Get("log-type").EnumValue<LoggingStyle>();
            Logging.Init();
            
            int i = 0;
            while (i < args.Length)
            {
                switch (args[i])
                {
                    case "--usage":
                    case "--help":
                    case "-?":
                        Usage();
                        ++i;
                        break;
                    case "--procid":
                    case "-p":
                        config.ProcessID = Int32.Parse(args[i + 1]);
                        i += 2;
                        procIDSet = true;
                        break;
                    case "--threads":
                    case "-t":
                        config.WorkerCount = Int32.Parse(args[i + 1]);
                        i += 2;
                        break;
                    case "--numprocs":
                    case "-n":
                        Processes = Int32.Parse(args[i + 1]);
                        i += 2;
                        numProcsSet = true;
                        break;
                    case "--hosts":
                    case "-h":
                        if (!numProcsSet)
                        {
                            Logging.Error("Error: --numprocs must be specified before --hosts");
                            Usage();
                            System.Environment.Exit(-1);
                        }
                        if (hostsSet)
                        {
                            Logging.Error("Error: --local cannot be combined with --hosts");
                            Usage();
                            System.Environment.Exit(-1);
                        }
                        Prefixes = new string[Processes];
                        if (args[i + 1].StartsWith("@"))
                        {
                            string hostsFilename = args[i + 1].Substring(1);
                            try
                            {
                                using (StreamReader reader = File.OpenText(hostsFilename))
                                {
                                    for (int j = 0; j < Processes; ++j)
                                    {
                                        if (reader.EndOfStream)
                                        {
                                            Logging.Error("Error: insufficient number of hosts in the given hosts file, {0}", hostsFilename);
                                            System.Environment.Exit(-1);
                                        }
                                        Prefixes[j] = reader.ReadLine().Trim();
                                    }
                                }
                            }
                            catch (Exception)
                            {
                                Logging.Error("Error: cannot read hosts file, {0}", hostsFilename);
                                System.Environment.Exit(-1);
                            }
                            i += 2;
                        }
                        else
                        {
                            Array.Copy(args, i + 1, Prefixes, 0, Processes);
                            i += 1 + Processes;
                        }
                        hostsSet = true;
                        break;
                    case "--local":
                        if (!numProcsSet)
                        {
                            Logging.Error("Error: --numprocs must be specified before --local");
                            Usage();
                            System.Environment.Exit(-1);
                        }
                        if (hostsSet)
                        {
                            Logging.Error("Error: --local cannot be combined with --hosts");
                            Usage();
                            System.Environment.Exit(-1);
                        }
                        Prefixes = Enumerable.Range(2101, Processes).Select(x => string.Format("localhost:{0}", x)).ToArray();
                        i++;
                        hostsSet = true;
                        break;
                    case "--reachability":
                        config.CompactionInterval = Convert.ToInt32(args[i + 1]);
                        i += 2;
                        break;
                    case "--deadlocktimeout":
                        config.DeadlockTimeout = int.Parse(args[i + 1]);
                        i += 2;
                        break;
                    case "--separateclock":
                        config.SeparateClock = true;
                        i++;
                        break;
                    case "--record":
                        config.RecordChannel = int.Parse(args[i + 1]);
                        i += 2;
                        break;
                    default:
                        strippedArgs.Add(args[i]);
                        i++;
                        break;
                }
            }

            if (config.SeparateClock && Processes < 2)
            {
                Logging.Error("Warning: Cannot use a separate clock process with fewer than 2 processes. Using local clock.");
                config.SeparateClock = false;
            }

            if (procIDSet && !numProcsSet)
            {
                Logging.Error("Error: Number of processes not supplied (use the --numprocs option, followed by an integer)");
                Usage();
                System.Environment.Exit(-1);
            }

            if (numProcsSet && Processes > 1 && !hostsSet)
            {
                Logging.Error("Error: List of hosts not supplied (use the --hosts option, followed by {0} hostnames)", Processes);
                System.Environment.Exit(-1);
            }

            var names = Prefixes.Select(x => x.Split(':')).ToArray();

            IPEndPoint[] endpoints = new IPEndPoint[Processes];
            for (int j = 0; j < endpoints.Length; ++j)
            {
                string[] splitName = Prefixes[j].Split(':');
                string hostname = splitName[0];

                int port = 2101;
                if (splitName.Length == 2 && !int.TryParse(splitName[1], out port))
                {
                    {
                        Logging.Error("Error: invalid port number ({0}) for process {1}", splitName[1], j);
                        System.Environment.Exit(-1);
                    }
                }

                IPAddress[] ipAddresses = null;
                try
                {
                    ipAddresses = Dns.GetHostAddresses(hostname);
                }
                catch (Exception)
                {
                    Logging.Error("Error: could not resolve hostname {0} for process {1}", hostname, j);
                    System.Environment.Exit(-1);
                }

                IPAddress ipv4Address;
                ipv4Address = ipAddresses.FirstOrDefault(x => x.AddressFamily == AddressFamily.InterNetwork);
                if (ipv4Address == null)
                {
                    Logging.Error("Error: could not find an IPv4 address for hostname {0} for process {1}", hostname, j);
                    Logging.Error("IPv6 is not currently supported.");
                    System.Environment.Exit(-1);
                }

                try
                {
                    endpoints[j] = new System.Net.IPEndPoint(ipv4Address, port);
                }
                catch (ArgumentOutOfRangeException)
                {
                    Logging.Error("Error: invalid port ({0}) for process {1}", port, j);
                    System.Environment.Exit(-1);
                }
            }
            config.Endpoints = endpoints;
            args = strippedArgs.ToArray();

            return config;
        }
    }

    public interface WorkerGroup
    {
        int Count { get; }

        /// <summary>
        /// This event is fired by each worker when it initially starts.
        /// </summary>
        event EventHandler<SchedulerStartArgs> Starting;
        /// <summary>
        /// This event is fired by each worker when it wakes from sleeping.
        /// </summary>
        event EventHandler<SchedulerWakeArgs> Waking;
        /// <summary>
        /// This event is fired by a worker immediately before executing a work item.
        /// </summary>
        event EventHandler<OperatorStartArgs> WorkItemStarting; 
        /// <summary>
        /// This event is fired by a worker immediately after executing a work item.
        /// </summary>
        event EventHandler<OperatorEndArgs> WorkItemEnding;
        /// <summary>
        /// This event is fired by a worker immediately after enqueueing a work item.
        /// </summary>
        event EventHandler<OperatorEnqArgs> WorkItemEnqueued;
        /// <summary>
        /// This event is fired by a worker when it becomes idle, because it has no work to execute.
        /// </summary>
        event EventHandler<SchedulerSleepArgs> Sleeping;
        /// <summary>
        /// This event is fired by a worker when it has finished all work, and the computation has terminated.
        /// </summary>
        event EventHandler<SchedulerTerminateArgs> Terminating;
        /// <summary>
        /// This event is fired by a worker when a batch of records is delivered to an operator.
        /// </summary>
        event EventHandler<OperatorReceiveArgs> ReceivedRecords;
        /// <summary>
        /// This event is fired by a worker when a batch of records is sent by an operator.
        /// (N.B. This event is currently not used.)
        /// </summary>
        event EventHandler<OperatorSendArgs> SentRecords;
    }

    /// <summary>
    /// Responsible for managing the execution of multiple worker threads within a process.
    /// </summary>
    public class Controller : IDisposable
    {
        
        /// <summary>
        /// Returns information about the local workers controlled by this controller.
        /// </summary>
        public WorkerGroup Workers { get { return this.impl.Workers; } }

        /// <summary>
        /// Constructs and registers a new input collection of type R.
        /// </summary>
        /// <typeparam name="R">Record</typeparam>
        /// <returns>New input collection</returns>
        public InputCollection<R> NewInput<R>()
            where R : IEquatable<R>
        {
            return this.impl.NewInput<R>();
        }

        /// <summary>
        /// Constructs and registers a new remote input collection of type R.
        /// </summary>
        /// <typeparam name="R">Record</typeparam>
        /// <param name="name">Identifying name</param>
        /// <returns>New remote collection</returns>
        public RemoteCollection<R> NewRemoteInput<R>(string name)
            where R : IEquatable<R>
        {
            return this.impl.NewRemoteInput<R>(name);
        }

        /// <summary>
        /// Blocks until all computation is complete and resources are released.
        /// </summary>
        public void Join()
        {
            this.impl.Join();
        }

        /// <summary>
        /// Prints summary statistics about the execution supervised by the controller.
        /// </summary>
        /// <param name="maxReport">Maximum number of dataflow vertices to report on</param>
        /// <param name="writer">Optional TextWriter for output</param>
        /// <param name="reset">If true, will reset the statistics to zero after reporting</param>
        public void Report(int maxReport = int.MaxValue, TextWriter writer = null, bool reset = false)
        {
            if (writer == null)
                writer = Console.Error;
            this.impl.Report(maxReport, writer, reset);
        }

        /// <summary>                           I
        /// The number of processes participating in the computation.
        /// </summary>
        public int Processes { get { return this.impl.Processes; } }

        /// <summary>
        /// The zero-based index of this process.
        /// </summary>
        public int ProcessID { get { return this.impl.ProcessID; } }

        private Placement defaultPlacement;
        public Placement DefaultPlacement { get { return this.impl.DefaultPlacement; } }

        private readonly InternalController impl;

        /// <summary>
        /// Constructs a controller for a new computation.
        /// </summary>
        /// <param name="config">Controller configuration</param>
        public Controller(Configuration config)
        {
            switch (config.Mode)
            {
                case Mode.Legacy:
                    this.impl = new LegacyController(config);
                    break;
                default:
                    throw new NotImplementedException(string.Format("Cannot instantiate a controller in mode {0}", config.Mode));
            }
        }

        /// <summary>
        /// Constructs a controller for a new computation using default configuration.
        /// </summary>
        public Controller() : this(Configuration.Default) { }

        /// <summary>
        /// Blocks until all computation associated with the supplied epoch have been retired.
        /// </summary>
        /// <param name="epoch">Epoch to wait for</param>
        public void Sync(int epoch)
        {
            this.impl.Sync(epoch);
        }

        /// <summary>
        /// Blocks until all computation has been retired.
        /// </summary>
        public void Sync()
        {
            this.impl.Sync();
        }

        public void Dispose()
        {
            this.impl.Dispose();
        }

    }
}
