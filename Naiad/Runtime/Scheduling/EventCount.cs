/*
 * Naiad ver. 0.6
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

//#define USESPINLOCK

using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using System.Runtime.CompilerServices;
using System.Collections.Concurrent;

using Microsoft.Research.Naiad.Diagnostics;

namespace Microsoft.Research.Naiad.Scheduling
{

    internal class EventCount
    {
        internal class WaitBlock
        {
            public int waiters;
            public ManualResetEvent ev;
            public WaitBlock next;

            public WaitBlock()
            {
                this.waiters = 0;
                this.ev = new ManualResetEvent(false);
                this.next = null;
            }
        }

#if USESPINLOCK
        private SpinLock m_lock;
#endif
        private long value;
        private WaitBlock current;
        private WaitBlock freelist;

        public EventCount()
        {
#if USESPINLOCK
            this.m_lock = new SpinLock();
            Console.Error.WriteLine("Using EventCount with spinlock");
#else
            Console.Error.WriteLine("Using EventCount with monitor");
#endif
            this.value = 0;
            this.current = null;
            this.freelist = null;
        }

        public long Read()
        {
            return this.value;
        }

        public void Await(AutoResetEvent selectiveEvent, long waitfor)
        {

            //Tracing.Trace("{Await");

            // Repeatedly wait until the condition is satisfied
            while (waitfor > this.value)
            {
                WaitBlock wb;

#if !USESPINLOCK
                lock (this)
                {
#else
                    bool taken = false;
                    m_lock.Enter(ref taken);
#endif

                    //Console.WriteLine("Awaiting for {0}, currently {1}", waitfor, this.value);
                    // Check condition again, now we hold the lock
                    if (this.value >= waitfor)
                    {
#if USESPINLOCK
                        m_lock.Exit();
#endif
                        break;
                    }

                    // Make sure there is a waitblock
                    if (this.current == null)
                    {
                        if (this.freelist != null)
                        {
                            this.current = this.freelist;
                            this.freelist = this.freelist.next;
                        }
                        else
                        {
                            KernelLoggerTracing.PostKernelLoggerMarkEvent("Await new waitblock");
                            Console.Error.WriteLine("EventCount Await(): NEW WAITBLOCK");
                            this.current = new WaitBlock();
                        }
                    }
                    wb = this.current;
                    Logging.Assert(wb.waiters >= 0);
                    wb.waiters++;
#if USESPINLOCK
                m_lock.Exit();
#else
                }
#endif

                // Do the wait
                int wokenBy = WaitHandle.WaitAny(new WaitHandle[] { selectiveEvent, wb.ev });

                if (wokenBy == 0)
                {
                    // We were woken by the selective event so we need to work out the status 
                    // of the eventcount
#if !USESPINLOCK
                    lock (this)
                    {
#else
                        taken = false;
                        m_lock.Enter(ref taken);
#endif
                        if (wb == this.current)
                        {
                            // Not signalled so we can back out of the wait
                            wb.waiters--;
                            //Tracing.Trace("}Await");
#if USESPINLOCK
                            m_lock.Exit();
#endif
                            return;
                        }
#if !USESPINLOCK
                    }
#endif
                }

                // Last man standing frees the waitblock
                if (Interlocked.Decrement(ref wb.waiters) == 0)
                {
                    // re-initialize the waitblock and put on the freelist
                    wb.ev.Reset();
#if !USESPINLOCK
                    lock (this)
                    {
#else
                        taken = false;
                        m_lock.Enter(ref taken);
#endif
                        wb.next = this.freelist;
                        this.freelist = wb;
#if USESPINLOCK
                        m_lock.Exit();
#else
                    }
#endif
                }

                if (wokenBy == 0)
                {
                    //Tracing.Trace("}Await");
#if USESPINLOCK
                    m_lock.Exit();
#endif
                    return;
                }
            }
            //Tracing.Trace("}Await");
        }

        public void Advance()
        {
            WaitBlock wb;
#if !USESPINLOCK
            lock (this)
            {
#else
                bool taken = false;
                m_lock.Enter(ref taken);
#endif
                // Advance the count
                this.value++;
                //Console.WriteLine("Advance {0}", this.value);

                // If we have waiters then we will wake them outside the lock
                wb = this.current;
                this.current = null;
#if USESPINLOCK
                m_lock.Exit();
#else
            }
#endif

            // Unblock the waiters
            if (wb != null)
            {
                NaiadTracing.Trace.RegionStart(NaiadTracingRegion.SetEvent);
                wb.ev.Set();
                NaiadTracing.Trace.RegionStop(NaiadTracingRegion.SetEvent);
            }
        }

    }
}
    
