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

using System;
using System.Diagnostics;
using System.Linq;
using System.IO;
using System.Runtime.InteropServices;
using System.Reflection.Emit;
using Microsoft.Research.Naiad.Util;

namespace Microsoft.Research.Naiad
{
    public enum LoggingLevel
    {
        Debug,
        Info,
        Progress,   // Just enough to see that things are making progress
        Error,
        Fatal,
        Off
    }

    public enum LoggingStyle
    {
        File,
        //InMemory,
        Console
    }

    // Raised in response to NaiadLog.Fatal.
    // Should not be caught.
    public class FatalException : Exception { }

    public static class Logging
    {
        public static System.Diagnostics.Stopwatch Stopwatch = System.Diagnostics.Stopwatch.StartNew();

        // Strings when field is unknown or not applicable
        public const string NotApplicable = "*";
        public const string NotKnown = "?";

        // Needs to be public so can log with RemoteOutput clients
        public static LoggingStyle LogStyle = LoggingStyle.Console;

        // Information for a single log statement.
        public struct LogInfo
        {
            public int ThreadId;
            public string OperatorName;
            public string MethodName;
            public string ClassName;
            public string FileName;
            public int LineNumber;

            public LogInfo(int threadId, string operatorName, string className, string methodName, string fileName, int lineNumber)
            {
                ClassName = className;
                ThreadId = threadId;
                OperatorName = operatorName;
                MethodName = methodName;
                FileName = fileName;
                LineNumber = lineNumber;
            }
        }

        private static LoggingLevel logLevel = LoggingLevel.Error;

        /// <summary>
        /// Gets and sets logging level.
        /// </summary>
        public static LoggingLevel LogLevel
        {
            get { return logLevel; }
            set
            {
                logLevel = value;
            }
        }

        private static Object logLock = new Object();
        private static bool Inited = false; // Set for file and in-memory logging, not required for console logging

        #region File logging internals

        public static String LogFileName;
        public static FileStream LogFileStream;
        public static StreamWriter LogWriter;
        private static CircularLogBuffer<string> buf;
        private static String CR = Environment.NewLine;

        private static int proc;  // Current process id
#if false
        private static bool includeLineNumbers = true;
#endif

        /// <summary>
        /// Flushes the log and clears the buffer.  Caller holds the lock.
        /// </summary>
        private static void flush()
        {
            while (!buf.isEmpty())
            {
                // flush to disk and clear
                LogWriter.Write(buf.Remove());
            }
            LogWriter.Flush();
        }

        /// <summary>
        /// Initialises file logging. 
        /// </summary>
        /// <returns>True if initialization actually happened.</returns>
        private static bool initFileLogging()
        {
            bool res = false;

            LogFileName = "log.txt";

            // Write some useful info at the top of the log file
            String preamble = String.Format("# Naiad log {0}{1}", DateTime.Now, CR);
            preamble += String.Format("# (timestamps are Windows file times){0}", CR);
            preamble += String.Format("# process_id, timestamp, elapsed_ms, mngd_thread_id, message{0}", CR);

            lock (logLock)
            {
                if (!Inited)
                {
                    buf = new CircularLogBuffer<string>(4, "");
                    buf.OverwriteIfFull = false; // don't lose any entries

                    LogFileStream = new FileStream(LogFileName, FileMode.Create);
                    LogWriter = new StreamWriter(LogFileStream);
                    LogWriter.Write(preamble);
                    LogWriter.Flush();

                    Inited = true;
                    res = true;
                }
            }
            return res;
        }

        // Log msg to file with Naiad thread id and current operator name
        private static void logToFile(int tid, String msg)
        {
            String s = String.Format("{0}, {1}, {2}, {3}, {4}{5}", proc,
                System.DateTime.Now.ToFileTime(), Logging.Stopwatch.ElapsedMilliseconds,
                tid, msg, CR);

            lock (logLock)
            {
                if (buf.isFull())
                {
                    flush();
                }
                buf.Add(s);
            }
        }

        /// <summary>
        /// Stops file logging cleanly by flushing the buffer, closing the log FileStream
        /// and setting logging to be uninitialized.
        /// </summary>
        private static void stopFileLogging()
        {
            String s = String.Format("{0}, {1}, {2}, {3}, {4}{5}", proc,
                System.DateTime.Now.ToFileTime(), Logging.Stopwatch.ElapsedMilliseconds,
                NotApplicable, "End of log", CR);

            lock (logLock)
            {
                if (!Inited)
                {
                    return;
                }

                if (buf.isFull())
                {
                    flush();
                }
                buf.Add(s);
                flush();
                LogWriter.Close();
                Inited = false;
                Console.Error.WriteLine("Stopped logging to file {0}", LogFileName);
            }
        }

        #endregion

        #region In-memory buffer logging internals
        private static bool initInMemoryLogging()
        {
            bool res = false;

            lock (logLock)
            {
                if (!Inited)
                {
                    buf = new CircularLogBuffer<string>(64, "");
                    buf.OverwriteIfFull = true;
                    Inited = true;
                    res = true;
                }
            }
            return res;
        }

        // Log msg to buffer with Naiad thread id and current operator name
        private static void logToBuffer(int tid, String msg)
        {
            if (!Inited)
                initInMemoryLogging();

            long ts = System.DateTime.Now.ToFileTime();
            String s = String.Format("{0}, {1}, {2}\n", ts, tid, msg);

            lock (logLock)
            {
                // If the buffer is full, the oldest entry is overwritten
                buf.Add(s);
            }
        }

        /// <summary>
        /// Method to enable inspection of buffer contents from the debugger.
        /// When in a breakpoint, invoke this method from the Immediate window
        /// and then look at the buffer by typing "NaiadLog.buf"
        /// </summary>
        public static void InspectBuffer()
        {
            if (!Inited)
            {
                return;
            }
        }

        #endregion

        #region Console logging internals

        private static void logToConsole(String msg)
        {
            if (LogLevel >= LoggingLevel.Progress)
                Console.Error.Write("{0}, {1}{2}", Logging.Stopwatch.Elapsed, msg, CR);
            else
                Console.Error.Write("{0}, {1}, {2}{3}", System.DateTime.Now, Logging.Stopwatch.ElapsedMilliseconds, msg, CR);
        }
        #endregion

        
        // Initializes logging according to the current LoggingStyle.
        // Allocates the necessary resources (eg create a file or reserve memory) even
        // if logging is currently disabled.  This is so logging can be turned on and
        // off as the program runs.
        internal static void Init()
        {
            bool res = false;
            String s = "";
            proc = Process.GetCurrentProcess().Id;

            switch (LogStyle)
            {
                case LoggingStyle.File:
                    res = initFileLogging();
                    s = String.Format("file {0}", LogFileName);
                    break;
                case LoggingStyle.Console:
                    res = true;
                    s = "console";
                    break;
            }
            if (res)
            {
                if (LogLevel != LoggingLevel.Off)
                    Console.Error.WriteLine("Logging initialized to {0}", s);
            }
            else
                Console.Error.WriteLine("Initialization of logging failed: {0}", s);
        }

        /// <summary>
        /// Stops logging cleanly.
        /// </summary>
        public static void Stop()
        {
            switch (LogStyle)
            {
                case LoggingStyle.File:
                    stopFileLogging();
                    break;
                default:
                    break;
            }
        }

        [Conditional("DEBUG")]
        public static void Debug(string msg, params object[] args)
        {
            Log(LoggingLevel.Debug, msg, args);
        }

        public static void Info(string msg, params object[] args)
        {
            Log(LoggingLevel.Info, msg, args);
        }

        public static void Progress(string msg, params object[] args)
        {
            Log(LoggingLevel.Progress, msg, args);
        }

        public static void Error(string msg, params object[] args)
        {
            Log(LoggingLevel.Error, msg, args);
        }

        public static void Fatal(string msg, params object[] args)
        {
            Log(LoggingLevel.Fatal, msg, args);
        }

        /// <summary>
        /// Writes a freetext log entry according to the current LoggingStyle.
        /// Returns immediately if logging is not initialized or not enabled.
        /// </summary>
        /// <param name="level">The log level for this message.  Messages below LogLevel are ignored.</param>
        /// <param name="msg">The format string to be logged, as in String.Format</param>
        /// <param name="args">Arguments to be formatted.</param>
        public static void Log(LoggingLevel level, string msg, params object[] args)
        {
            if (level < LogLevel)
            {
                return;
            }

            if (!Inited)
            {
                bool initedNow = false;
                switch (LogStyle)
                {
                    case LoggingStyle.File:
                        initedNow = initFileLogging();
                        break;
                    case LoggingStyle.Console:
                        initedNow = true;
                        break;
                }
                if (!initedNow)
                    return;
            }

            // Go ahead and format the message now.
            msg = String.Format(msg, args);

            int thrd = System.Threading.Thread.CurrentThread.ManagedThreadId;

            switch (LogStyle)
            {
                case LoggingStyle.File:
                    logToFile(thrd, msg);
                    break;
                case LoggingStyle.Console:
                    logToConsole(msg);
                    break;
            }
        }

        private static String FormatTime()
        {
            return System.DateTime.Now.ToString("yy/MM/dd/HH:mm:ss:fff");
        }

        public static void Assert(bool condition)
        {
            Assert(condition, "Assertion failed.");
        }

        public static void Assert(bool condition, string format, params object[] args)
        {
            if (!condition)
            {
                Fatal(format, args);
            }
        }
    }
}
