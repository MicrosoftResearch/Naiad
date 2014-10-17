/*
Copyright (c) Microsoft Corporation

All rights reserved.

Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in 
compliance with the License.  You may obtain a copy of the License 
at http://www.apache.org/licenses/LICENSE-2.0   


THIS CODE IS PROVIDED *AS IS* BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, EITHER 
EXPRESS OR IMPLIED, INCLUDING WITHOUT LIMITATION ANY IMPLIED WARRANTIES OR CONDITIONS OF 
TITLE, FITNESS FOR A PARTICULAR PURPOSE, MERCHANTABLITY OR NON-INFRINGEMENT.  


See the Apache Version 2.0 License for specific language governing permissions and 
limitations under the License. 

*/
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Xml.Linq;

namespace Microsoft.Research.Naiad.SolutionUtils
{
    internal class Program
    {

        private static void PrintUsageAndExit()
        {
            string exeName = "UpdateNuGetVersions";
            Console.WriteLine("Usage: {0} version", exeName);
            Environment.Exit(1);
        }

        private static void ConfirmUpdate(string newVersion, List<string> specFiles)
        {
            Console.WriteLine("Please confirm that you want to update the following files to new version '{0}'.",
                newVersion);
            foreach (var file in specFiles)
            {
                Console.WriteLine("\t{0}", file);
            }
            Console.WriteLine("Please enter Y/y to continue");
            var key = Console.ReadKey(true);
            if (Char.ToUpperInvariant(key.KeyChar) != 'Y')
            {
                Console.WriteLine("Update aborted by user.");
                Environment.Exit(0);
            }
        }

        private static void UpdateNuspecFile(string spec, string newVersion, Dictionary<string, string> depVersions)
        {
            var specDoc = XDocument.Load(spec);
            var root = specDoc.Root;

            string id = root.Descendants(root.GetDefaultNamespace() + "id").Single().Value;
            var versionNode = root.Descendants(root.GetDefaultNamespace() + "version").Single();
            string version = versionNode.Value;
            bool updateFile = false;

            if (version != newVersion)
            {
                updateFile = true;
                versionNode.Value = newVersion;
            }

            var deps = root.Descendants(root.GetDefaultNamespace() + "dependency");

            Console.WriteLine("Package {0} has version {1}", id, version);

            foreach (var dep in deps)
            {
                var did = dep.Attribute("id").Value;
                var dver = dep.Attribute("version").Value;
                if (did.StartsWith("Microsoft.Research.Naiad"))
                {
                    if (newVersion != dver)
                    {
                        dep.SetAttributeValue("version", newVersion);
                        updateFile = true;
                    }
                    Console.WriteLine("\t{0}\t{1}", did, dver);
                }
                else 
                {
                    string installedver = depVersions[did];
                    if (installedver != dver)
                    {
                        dep.SetAttributeValue("version", installedver);
                        updateFile = true;
                    }
                }
            }
            if (updateFile)
            {
                specDoc.Save(spec);
            }
        }

        private static void ProcessFile(string file, Dictionary<string, string> versions)
        {
            if (Path.GetDirectoryName(file).EndsWith("TestHttp"))
	        {
		        return;
	        }

            XDocument doc = XDocument.Load(file);
            var pkgs = doc.Root.Descendants("package");
            foreach (var pkg in pkgs)
            {
                string id = pkg.Attribute("id").Value;
                string ver = pkg.Attribute("version").Value;

                if (!id.StartsWith("Microsoft.Research.Naiad"))
                {
                    if (versions.ContainsKey(id))
                    {
                        //we already found this pkg, make sure the version is the same
                        if (versions[id] != ver)
                        {
                            Console.WriteLine("Warning: Package {0} in {1} has version {2}, previously saw version {3}.",
                                id, file, ver, versions[id]);
                        }
                    }
                    else
                    {
                        versions[id] = ver;
                    }
                    //Console.WriteLine("ID: '{0}' ver: '{1}'", id, ver); 
                }
            }

        }

        private static void Main(string[] args)
        {
            if (args.Length != 1)
            {
                PrintUsageAndExit();
            }

            string newVersion = args[0]; //"0.7.6-alpha058";
            string outputDirectory = "packages-" + newVersion;

            if (Directory.Exists(outputDirectory))
            {
                Console.WriteLine("Output directory '{0}' already exists.", outputDirectory);
                Environment.Exit(1);
            } 
            else
            {
                Directory.CreateDirectory(outputDirectory);
            }

            var packageFiles = Directory.EnumerateFiles("..", "packages.config", SearchOption.AllDirectories);
            Dictionary<string, string> depVersions = new Dictionary<string, string>();
            foreach (var pkgFile in packageFiles)
            {
                ProcessFile(pkgFile, depVersions);
            }


            //TODO: Take a directory as a program argument
            //TODO: Take a parameter of other nuspec files to check, andd update their dependencies
            var specFiles = Directory.EnumerateFiles("..", "*.nuspec", SearchOption.AllDirectories).ToList();

            ConfirmUpdate(newVersion, specFiles);
            StreamWriter sw = new StreamWriter("BuildNugetPackages.bat");


            foreach (var spec in specFiles)
            {
                UpdateNuspecFile(spec, newVersion, depVersions);
                sw.WriteLine("nuget.exe pack {0} -OutputDirectory {1}", spec, outputDirectory);
            }
            sw.Close();

            Console.WriteLine("The NuSpec files have been updated.  Please run BuildNugetPackages.bat to create packages in the directory '{0}'", outputDirectory);
        }
    }
}

