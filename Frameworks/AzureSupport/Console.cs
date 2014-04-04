using Microsoft.WindowsAzure.Storage.Blob;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace Microsoft.Research.Naiad.Frameworks.Azure
{
    public static class ConsoleExtensionMethods
    {
        /// <summary>
        /// Redirects Console.Out to an Azure blob, with the process id in the filename.
        /// </summary>
        /// <param name="controller">Naiad controller</param>
        /// <param name="container">Azure container</param>
        /// <param name="format">Format string expecting the Naiad process id</param>
        public static void SetConsoleOut(this Microsoft.Research.Naiad.Controller controller, CloudBlobContainer container, string format)
        {
            var filename = string.Format(format, controller.Configuration.ProcessID);

            var writer = new System.IO.StreamWriter(container.GetBlockBlobReference(filename).OpenWrite());
            writer.AutoFlush = true;

            Console.SetOut(writer);
        }

        /// <summary>
        /// Redirects Console.Error to an Azure blob, with the process id in the filename.
        /// </summary>
        /// <param name="controller">Naiad controller</param>
        /// <param name="container">Azure container</param>
        /// <param name="format">Format string expecting the Naiad process id</param>
        public static void SetConsoleError(this Microsoft.Research.Naiad.Controller controller, CloudBlobContainer container, string format)
        {
            var filename = string.Format(format, controller.Configuration.ProcessID);

            var writer = new System.IO.StreamWriter(container.GetBlockBlobReference(filename).OpenWrite());
            writer.AutoFlush = true;

            Console.SetError(writer);
        }
    }
}
