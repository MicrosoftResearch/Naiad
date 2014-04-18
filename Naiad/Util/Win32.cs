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
using System.Linq;
using System.Text;
using System.Runtime.InteropServices;
using System.Threading;

using Microsoft.Research.Naiad.Diagnostics;

namespace Microsoft.Research.Naiad.Utilities
{
    internal class Win32
    {
        [DllImport("kernel32.dll")]
        internal static extern UInt32 GetLastError();

        [DllImport("kernel32.dll", SetLastError = true)]
        internal static extern IntPtr GetCurrentThread();

        [DllImport("kernel32.dll", SetLastError = true)]
        internal static extern UInt32 GetCurrentThreadId();

        [DllImport("kernel32.dll", SetLastError = true)]
        internal static extern IntPtr SetThreadAffinityMask(IntPtr hThread, IntPtr dwThreadAffinityMask);

        [DllImport("kernel32.dll", CharSet = CharSet.Auto, SetLastError = true)]
        internal static extern int SetThreadIdealProcessor(IntPtr handle, int processor);

        [DllImport("kernel32.dll", SetLastError = true)]
        internal static extern IntPtr OpenThread(uint DesiredAccess, bool InheritHandle, uint ThreadId);

        [DllImport("kernel32.dll", SetLastError = true)]
        internal static extern bool CloseHandle(IntPtr Handle);

        [DllImport("kernel32.dll", SetLastError = true)]
        internal static extern bool QueryThreadCycleTime([In] IntPtr ThreadHandle, [Out] out ulong CycleTime);

        [DllImport("kernel32.dll", SetLastError = true)]
        internal static extern UInt32 GetCurrentProcessId();

        [DllImport("kernel32.dll", SetLastError = true)]
        internal static extern IntPtr OpenProcess(uint DesiredAccess, bool InheritHandle, uint ProcessId);

        [DllImport("kernel32.dll", SetLastError = true)]
        internal static extern bool GetProcessWorkingSetSize([In] IntPtr ProcessHandle, [Out] out ulong MinBytes, [Out] out ulong MaxBytes);

        [DllImport("ws2_32.dll", SetLastError = true)]
        internal static extern int WSAIoctl([In] IntPtr Socket, UInt32 controlcode,
            [In] byte[] inBuf, int cbInBuf,
            [In] IntPtr outBuf, int cbOutBuf, // Not used - nust be zero
            [Out] out UInt32 pBytesRet,
            [In] IntPtr lpOverlapped,
            [In] IntPtr lpCompletionRoutine);
            /*
            WSAAPI
WSAIoctl(
    _In_ SOCKET s,
    _In_ DWORD dwIoControlCode,
    _In_reads_bytes_opt_(cbInBuffer) LPVOID lpvInBuffer,
    _In_ DWORD cbInBuffer,
    _Out_writes_bytes_to_opt_(cbOutBuffer, *lpcbBytesReturned) LPVOID lpvOutBuffer,
    _In_ DWORD cbOutBuffer,
    _Out_ LPDWORD lpcbBytesReturned,
    _Inout_opt_ LPWSAOVERLAPPED lpOverlapped,
    _In_opt_ LPWSAOVERLAPPED_COMPLETION_ROUTINE lpCompletionRoutine
    );
             */
        const UInt32 SIO_KEEPALIVE_VALS = 0x98000004;

        public static void SetKeepaliveOptions(IntPtr sockHandle)
        {
            /*
                struct tcp_keepalive arg;
                DWORD cb;
                arg.onoff = 1;
                arg.keepalivetime = 10;
                arg.keepaliveinterval = 2000;
                winStatus = WSAIoctl(s1, SIO_KEEPALIVE_VALS, &arg, sizeof(arg), NULL, 0, &cb, NULL, NULL);
                if (winStatus != ERROR_SUCCESS) {
                    wprintf(L"\nFailed to set SIO_KEEPALIVE_VALS. Error %d", winStatus);
                    goto bail;
                }
             * 
             * http://msdn.microsoft.com/en-us/library/windows/desktop/dd877220(v=vs.85).aspx
             * If the onoff member is set to a nonzero value, TCP keep-alive is enabled and the other members in the structure are used. 
             * The keepalivetime member specifies the timeout, in milliseconds, with no activity until the first keep-alive packet is sent. 
             * The keepaliveinterval member specifies the interval, in milliseconds, between when successive keep-alive packets are sent 
             * if no acknowledgement is received. 
             * 
             * ie: Keepalive timout is the time from last sent/recvd data packet until a keepalive is sent.  
             * KAInterval is the time between subsquenet keepalives if no keepalive ack is received
             *  
             */
            byte[] keepalivevals = new byte[12];
            int interval = 10;
            keepalivevals[0] = 1;
            keepalivevals[4] = 10;
            keepalivevals[8] = (byte)(interval & 0xff);
            keepalivevals[9] = (byte)(interval >> 8);
            UInt32 cbRet = 0;

            int ret = WSAIoctl(sockHandle, SIO_KEEPALIVE_VALS, 
                keepalivevals, 12, 
                IntPtr.Zero, 0, 
                out cbRet, 
                IntPtr.Zero, IntPtr.Zero);

            if (ret != 0) Console.WriteLine("WSAIoctl returned {0} {1}", ret, cbRet);
        }

        [DllImport("winmm.dll", EntryPoint="timeBeginPeriod", SetLastError=true)]
        internal static extern uint TimeBeginPeriod(uint uMilliseconds);

        [DllImport("winmm.dll", EntryPoint="timeEndPeriod", SetLastError=true)]
        internal static extern uint TimeEndPeriod(uint uMilliseconds);
    }


    internal class PinnedThread : IDisposable
    {
        internal IntPtr processHandle;    // handle to the process object
        public UInt32 processId;        // process id
        internal IntPtr OSThreadHandle;   // handle to the Windows thread object
        public UInt32 OSThreadId;       // Windows thread id
        public int runtimeThreadId;  // .NET thread id
        public int cpu;

        /// <summary>
        /// Gets the current thread cycle count by calling QueryThreadCycleTime
        /// </summary>
        /// <returns>Number of CPU frontier cycles</returns>
        public ulong QueryThreadCycles()
        {
            ulong cycles = 0;
            if (Win32.QueryThreadCycleTime(this.OSThreadHandle, out cycles) == false)
            {
                Console.Error.WriteLine("QueryThreadCycleTime error {0}", Win32.GetLastError());
            }
            return cycles;
        }

        public ulong QueryMaxWorkingSetSize()
        {
            ulong maxbytes = 0, minbytes = 0;
            if (Win32.GetProcessWorkingSetSize(this.processHandle, out minbytes, out maxbytes) == false)
            {
                Console.Error.WriteLine("GetProcessWorkingSetSize error {0}", Win32.GetLastError());
            }
            return maxbytes;
        }

        /// <summary>
        /// Affinitizes the current runtime thread to the OS thread and the OS thread to the specified processor
        /// </summary>
        /// <param name="cpu">Processor number</param>
        /// <param name="soft">if true use soft affinity</param>
        public PinnedThread(int cpu, bool soft=false)
        {
            // Tie the runtime thread to the OS thread
            Thread.BeginThreadAffinity();

            uint ret = Win32.TimeBeginPeriod(1);
            if (ret != 0) Console.Error.WriteLine("TimeBeginPeriod returned {0}", ret);

            // Get the OS thread handle
            this.OSThreadId = Win32.GetCurrentThreadId();
            this.OSThreadHandle = Win32.OpenThread(0x001fffff, false, this.OSThreadId); // THREAD_ALL_ACCESS
            if (this.OSThreadHandle == null || this.OSThreadHandle.ToInt64() == 0)
            {
                Console.Error.WriteLine("OpenThread error {0}", Win32.GetLastError());
            }


            if (soft) {
                Win32.SetThreadIdealProcessor(Win32.GetCurrentThread(), cpu);
            }
            else
            {
                // Tie the OS thread to a processor
                IntPtr oldmask;
                IntPtr newmask;
                if ((oldmask = Win32.SetThreadAffinityMask(Win32.GetCurrentThread(), new IntPtr(1L << cpu))) == IntPtr.Zero)
                {
                    Logging.Error("SetThreadAffinityMask error 0x{0:x} for thread 0x{1:x}  (#1)", Win32.GetLastError(), OSThreadId);
                }
                if ((newmask = Win32.SetThreadAffinityMask(Win32.GetCurrentThread(), new IntPtr(1L << cpu))) == IntPtr.Zero)
                {
                    Logging.Error("SetThreadAffinityMask error 0x{0:x} for thread 0x{1:x} (#2)", Win32.GetLastError(), OSThreadId);
                }
                //Console.Error.WriteLine("Thread {0:x} affinitized to {1:x} (attempted {2:x}, was {3:x})", 
                //    OSThreadId, newmask.ToInt64(), 1L << cpu, oldmask.ToInt64());
            }

            this.cpu = cpu;
            this.runtimeThreadId = Thread.CurrentThread.ManagedThreadId;

            this.processId = Win32.GetCurrentProcessId();
            this.processHandle = Win32.OpenProcess(0x1F0FFF, false, this.processId);  // PROCESS_ALL_ACCESS
            if (this.processHandle == null || this.processHandle.ToInt64() == 0)
            {
                Logging.Error("OpenProcess error {0}", Win32.GetLastError());
            }
        }

        public PinnedThread(ulong affinitymask)
        {
            // Tie the runtime thread to the OS thread
            Thread.BeginThreadAffinity();

            uint ret = Win32.TimeBeginPeriod(1);
            if (ret != 0) Console.Error.WriteLine("TimeBeginPeriod returned {0}", ret);

            // Get the OS thread handle
            this.OSThreadId = Win32.GetCurrentThreadId();
            this.OSThreadHandle = Win32.OpenThread(0x001fffff, false, this.OSThreadId); // THREAD_ALL_ACCESS
            if (this.OSThreadHandle == null || this.OSThreadHandle.ToInt64() == 0)
            {
                Console.Error.WriteLine("OpenThread error {0}", Win32.GetLastError());
            }
            // Tie the OS thread to a processor
            IntPtr oldmask;
            IntPtr newmask;
            if ((oldmask = Win32.SetThreadAffinityMask(Win32.GetCurrentThread(), new IntPtr((long)affinitymask))) == IntPtr.Zero)
            {
                Logging.Error("SetThreadAffinityMask error 0x{0:x} for thread 0x{1:x}  (#1)", Win32.GetLastError(), OSThreadId);
            }
            if ((newmask = Win32.SetThreadAffinityMask(Win32.GetCurrentThread(), new IntPtr((long)affinitymask))) == IntPtr.Zero)
            {
                Logging.Error("SetThreadAffinityMask error 0x{0:x} for thread 0x{1:x} (#2)", Win32.GetLastError(), OSThreadId);
            }

            this.cpu = -1;
            this.runtimeThreadId = Thread.CurrentThread.ManagedThreadId;

            this.processId = Win32.GetCurrentProcessId();
            this.processHandle = Win32.OpenProcess(0x1F0FFF, false, this.processId);  // PROCESS_ALL_ACCESS
            if (this.processHandle == null || this.processHandle.ToInt64() == 0)
            {
                Logging.Error("OpenProcess error {0}", Win32.GetLastError());
            }
        }

        public void Dispose()
        {
            Win32.TimeEndPeriod(1);
            
            if (Win32.CloseHandle(this.OSThreadHandle) == false)
            {
                Logging.Error("CloseHandle(thread) error {0}", Win32.GetLastError());
            }
            if (Win32.CloseHandle(this.processHandle) == false)
            {
                Logging.Error("CloseHandle(process) error {0}", Win32.GetLastError());
            }
            Thread.EndThreadAffinity();
        }
    }
}
