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

using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

using System.Threading;
using System.Diagnostics;
using System.Collections.Concurrent;
using System.Reflection.Emit;

namespace Microsoft.Research.Naiad
{
    // thread-local buffer pools
    static internal class ThreadLocalBufferPools<T>
    {
        public static ThreadLocal<BufferPool<T>> pool = new ThreadLocal<BufferPool<T>>(() => new ThreadLocalBufferPool<T>(16));
    }

    static public class BufferPoolUtils
    {

        public static int Log2(int x)
        {
            var result = 0;

            while ((1 << result) < x)
                result++;

            return result;
        }

    }

    public interface BufferPool<T>
    {
        T[] CheckOut(int size);
        T[] CheckOutInitialized(int size);
        void CheckIn(T[] array);
        T[] Empty { get; }
    }

    internal interface NonblockingBufferPool<T> : BufferPool<T>
    {
        bool TryCheckOut(int size, out T[] buffer);
        bool TryCheckOut(int size, out T[] buffer, Action unblockAction);
    }

    internal static class ObjectSize
    {
        // From http://msdn.microsoft.com/en-us/library/eahchzkf.aspx
        // GetManagedSize() returns the size of a structure whose type
        // is 'type', as stored in managed memory. For any referenec type
        // this will simply return the size of a pointer (4 or 8).
        /// <summary>
        /// Dynamically determines the size in bytes of a generic type struct
        /// </summary>
        /// <param name="type">The type</param>
        /// <param name="size">Set to the size in bytes</param>
        public static int ManagedSize(Type type)
        {
#if  true
            return -1;
#else

            // all this just to invoke one opcode with no arguments!
            var method = new DynamicMethod("GetManagedSizeImpl",
                typeof(uint), new Type[0],
                typeof(Type),
                false);

            ILGenerator gen = method.GetILGenerator();
            gen.Emit(OpCodes.Sizeof, type);
            gen.Emit(OpCodes.Ret);

            var func = (Func<uint>)method.CreateDelegate(typeof(Func<uint>));
            return checked((int)func());
#endif
        }
    }

    internal class DummyBufferPool<T> : BufferPool<T>
    {

        public T[] CheckOut(int size)
        {
            return new T[size];
        }

        public T[] CheckOutInitialized(int size)
        {
            return new T[size];
        }

        public void CheckIn(T[] array)
        {
            
        }

        public T[] Empty
        {
            get { return new T[0]; }
        }

        public static DummyBufferPool<T> Pool = new DummyBufferPool<T>();
    }

    internal class ThreadLocalBufferPool<T> : BufferPool<T>
    {
        public const int MaximumStackLength = 256;

        protected readonly Stack<T[]>[] stacks;    // indexed by log of the buffer size.

        // Variables for logging
        public readonly int id; // unique identifier of this buffer pool
        public readonly int typesize;   // size in bytes of T
        
        public int Captured()
        {
            var result = 0;
            for (int i = 0; i < stacks.Length; i++)
                result += stacks[i].Count << i;

            return result;
        }

        public T[] CheckOut(int size)
        {
            var logSize = BufferPoolUtils.Log2(size);

            if ((1 << logSize) < size)
                throw new Exception("TLBP Exception !!!!!!!!!");

            if (logSize < stacks.Length && stacks[logSize].Count > 0)
                return stacks[logSize].Pop();
            else
            {
                try
                {
                    T[] res = new T[1 << logSize];
                    return res;
                }
                catch (Exception e)
                {
                    Debug.Assert(false);
                    Logging.Fatal("BufferPool.CheckOut exception {0}", e);
                    Logging.Fatal("Size {0}, Type {1}", logSize, typeof(T));
                    Logging.Fatal(e.StackTrace);
                    throw new FatalException();
                }
            }
        }

        public T[] CheckOutInitialized(int size)
        {
            size = BufferPoolUtils.Log2(size);

            if (size < stacks.Length && stacks[size].Count > 0)
            {
                var array = stacks[size].Pop();

                Array.Clear(array, 0, array.Length);

                return array;
            }
            else
            {
                return new T[1 << size];
            }
        }

        public T[] Empty { get { return this.empty; } }
        public readonly T[] empty = new T[0];

        public void CheckIn(T[] array)
        {
            if (array != null && array.Length > 0)
            {
                var size = BufferPoolUtils.Log2(array.Length);

                while ((1 << size) > array.Length)
                    size--;

                if (size < stacks.Length && stacks[size].Count < MaximumStackLength)
                {
                        stacks[size].Push(array);
                }
            }
        }

        public ThreadLocalBufferPool(int maxsize)
        {
            stacks = new Stack<T[]>[maxsize + 1];
            for (int i = 0; i < stacks.Length; i++)
                stacks[i] = new Stack<T[]>(1);

            this.id = this.GetHashCode();
            this.typesize = ObjectSize.ManagedSize(typeof(T));
        }
    }

    /// <summary>
    /// Pool with locks for concurrent access.
    /// </summary>
    /// <typeparam name="T">Type of buffers in pool</typeparam>
    public class LockedBufferPool<T> : BufferPool<T>
    {
        public const int MaximumStackLength = 10;// Int32.MaxValue;

        protected Stack<T[]>[] stacks;    // indexed by log of the buffer size.

        // Variables for logging
        public readonly int id; // unique identifier of this buffer pool
        public readonly int typesize;   // size in bytes of T

        public int Captured()
        {
            var result = 0;
            for (int i = 0; i < stacks.Length; i++)
                result += stacks[i].Count << i;

            return result;
        }

        public static int Log2(int x)
        {
            var result = 0;

            while ((1 << result) < x)
                result++;

            return result;
        }

        public T[] CheckOut(int size)
        {
            size = Log2(size);

            if (size < stacks.Length)
            {
                lock (stacks[size])
                {
                    if (stacks[size].Count > 0)
                        return stacks[size].Pop();
                }
            }

            try
            {
                return new T[1 << size];
            }
            catch (Exception e)
            {
                Logging.Fatal("Locked buffer pool exception. Size {1}\tType: {2}\nException: {0}", e, size, typeof(T));
                System.Environment.Exit(-1);
                return null;
            }
        }

        public T[] CheckOutInitialized(int size)
        {
            size = Log2(size);

            if (size < stacks.Length)
            {
                T[] array = null;
                lock (stacks[size])
                {
                    if (stacks[size].Count > 0)
                    {
                        array = stacks[size].Pop();
                    }
                }

                if (array != null)
                {
                    Array.Clear(array, 0, array.Length);

                    return array;
                }
            }

            return new T[1 << size];
        }

        public readonly T[] empty = new T[0];
        public T[] Empty { get { return this.empty; } }

        public void CheckIn(T[] array)
        {
            if (array != null && array.Length > 0)
            {
                var size = Log2(array.Length);

                if (size < stacks.Length)
                {
                    lock (stacks[size])
                    {
                        if (stacks[size].Count < MaximumStackLength)
                        {
                            stacks[size].Push(array);
                        }
                    }
                }
            }
        }

        public LockedBufferPool(int maxsize)
        {
            stacks = new Stack<T[]>[maxsize + 1];
            for (int i = 0; i < stacks.Length; i++)
                stacks[i] = new Stack<T[]>(1);

            this.id = this.GetHashCode();
            this.typesize = ObjectSize.ManagedSize(typeof(T));
        }
    }

    internal class BoundedBufferPool2<T> : BufferPool<T>
    {
        private class Waiter
        {
            public readonly ManualResetEventSlim Event;
            public T[] Buffer;

            public Waiter()
            {
                this.Event = new ManualResetEventSlim(false);
                this.Buffer = null;
            }

            public void Dispose()
            {
                this.Event.Dispose();
            }
        }

        public static bool LocalLoggingOn = false;

        private readonly int bufferLength;

        private int capacity;
        private int numAllocated;

        private readonly Queue<T[]> buffers;
        private readonly Queue<Waiter> waiters;
        private SpinLock queuesLock;

        public T[] CheckOut(int size)
        {
            bool successAlt = false;
            this.queuesLock.Enter(ref successAlt);
            Debug.Assert(successAlt);

            // first look for a free buffer in the queue
            if (this.buffers.Count > 0)
            {
                // Return a buffer from the queue of checked-in buffers.
                T[] ret = this.buffers.Dequeue();
                this.queuesLock.Exit();
                return ret;
            }
            else
                this.queuesLock.Exit();

            // if free capacity, consider allocation.
            if (this.numAllocated < this.capacity)
            {
                // grab a ticket, and if it is valid (below capacity) allocate!
                var ticketNumber = Interlocked.Increment(ref this.numAllocated);
                if (ticketNumber < this.capacity)
                    return new T[bufferLength];
            }

            // We have allocated all buffers, so it's time to access the queues.
            bool success = false;
            this.queuesLock.Enter(ref success);
            Debug.Assert(success);

            if (this.buffers.Count > 0)
            {
                // Return a buffer from the queue of checked-in buffers.
                T[] ret = this.buffers.Dequeue();
                this.queuesLock.Exit();
                return ret;
            }
            else
            {
                // Enqueue a waiter for a checked-in buffer.
                Waiter myWaiter = new Waiter();
                this.waiters.Enqueue(myWaiter);
                this.queuesLock.Exit();

                // Wait until my waiter is signalled (after its buffer has been set), and return that buffer.
                myWaiter.Event.Wait();
                Debug.Assert(myWaiter.Buffer != null);
                Debug.Assert(myWaiter.Buffer.Length == this.bufferLength);

                T[] ret = myWaiter.Buffer;
                myWaiter.Dispose();
                return ret;
            }
        }

        public T[] CheckOutInitialized(int size)
        {
            T[] ret = this.CheckOut(size);
            Array.Clear(ret, 0, ret.Length);
            return ret;
        }

        public void CheckIn(T[] array)
        {
            Debug.Assert(array != null);
            Debug.Assert(array.Length == this.bufferLength);

            bool success = false;

            Waiter toSignal = null;

            this.queuesLock.Enter(ref success);
            Debug.Assert(success);

            if (this.waiters.Count > 0)
            {
                toSignal = this.waiters.Dequeue();
                Debug.Assert(toSignal.Buffer == null);
                toSignal.Buffer = array;
            }
            else
            {
                this.buffers.Enqueue(array);
            }

            this.queuesLock.Exit();

            if (toSignal != null)
            {
                //Console.Error.WriteLine("<<< Checkin signalled");
                toSignal.Event.Set();
            }
            else
            {
                //Console.Error.WriteLine("<<< Checkin enqueued");
            }
        }

        public readonly T[] empty = new T[0];
        public T[] Empty { get { return this.empty; } }

        public BoundedBufferPool2(int bufferLength, int capacity)
        {
            Debug.Assert(bufferLength > 0);

            this.bufferLength = bufferLength;
            this.capacity = capacity;

#if DEBUG
            this.queuesLock = new SpinLock(true);
#else
            this.queuesLock = new SpinLock(false);
#endif
            this.buffers = new Queue<T[]>(capacity);
            this.waiters = new Queue<Waiter>(16);

        }
    }

    static internal class GlobalBufferPool<T>
    {
        public static LockedBufferPool<T> pool = new LockedBufferPool<T>(31);
    }
}
