using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Xml.Linq;

using Microsoft.Research.Peloponnese.ClusterUtils;

namespace Naiad.Util.PPM
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

        public LocalSubmission(int numberOfProcesses, string[] args, string localDirectory, string homeFromArgs)
        {
            string commandLine;
            IEnumerable<string> commandLineArgs;
            Helpers.MakeCommandLine(args, false, out commandLine, out commandLineArgs);

            this.ppmHome = ConfigHelpers.GetPPMHome(homeFromArgs);

            XDocument config = Helpers.MakePeloponneseConfig(numberOfProcesses, "local", commandLine, commandLineArgs, true, null);

            this.workingDirectory = CreateDirectory(localDirectory);

            this.configPath = "config.xml";
            config.Save(Path.Combine(this.workingDirectory, this.configPath));
        }

        public void Submit()
        {
            ProcessStartInfo psi = new ProcessStartInfo();
            psi.FileName = Path.Combine(this.ppmHome, "PersistentProcessManager.exe");
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
