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

using Microsoft.Research.Naiad.CodeGeneration;
using Microsoft.Research.Naiad.Scheduling;
using System.Collections.Concurrent;
using Microsoft.Research.Naiad.DataStructures;
using System.IO;
using Microsoft.Research.Naiad.Frameworks;
using Microsoft.Research.Naiad.Runtime.Controlling;

namespace Microsoft.Research.Naiad.Dataflow.Channels
{
    /// <summary>
    /// Fixed-size and reference-counted array of bytes.  
    /// </summary>
    internal class BufferPage
    {
        private readonly BufferPool<byte> pool;

        public byte[] Buffer;

        /// <summary>
        /// Initially 1 on construction. Incremented for each flushed segment. Decremented when the segment is Disposed().
        /// 
        /// N.B. The user of this page must call Release() after use.
        /// </summary>
        private int refCount;

        public BufferPage(BufferPool<byte> pool, int size)
        {
            this.pool = pool;
            this.Buffer = pool.CheckOut(size);
            this.refCount = 1;
        }

        internal void Acquire()
        {
            int newCount = Interlocked.Increment(ref this.refCount);
        }

        public void Release()
        {
            int newCount = Interlocked.Decrement(ref this.refCount);
            if (newCount == 0)
            {
                this.OnRelease();
            }

            Debug.Assert(newCount >= 0);
        }

        protected virtual void OnRelease()
        {
            pool.CheckIn(this.Buffer);
        }

    }

    /// <summary>
    /// BufferPage used for holding Messages.  Associated with a single SuperMessageSender.
    /// </summary>
    internal class SendBufferPage : BufferPage
    {

        /// <summary>
        /// Represents the index up to which the consumer HAS READ from this buffer.
        /// 
        /// Invariant: consumerPointer \leq validPointer \leq producerPointer.
        /// </summary>
        private int consumerPointer;

        /// <summary>
        /// Represents the index up to which the consumer MAY READ from this buffer.
        /// </summary>
        private int validPointer;

        /// <summary>
        /// Represents the index at which the producer WILL WRITE the next datum.
        /// </summary>
        public int producerPointer;

        /// <summary>
        /// Offset in the buffer at which the header of the current message should be patched in.
        /// </summary>
        private int currentMessageHeaderOffset;

        /// <summary>
        /// Pointer to a location in the message that will be reserved for a later write (e.g. a count after elements
        /// are written).
        /// </summary>
        private int reservedPointer;

        /// <summary>
        /// Number of bytes reserved by last call to ReserveBytes() (for sanity checking).
        /// </summary>
        private int reservedLength;

        private SerializedMessageType Type = SerializedMessageType.Data;

        private MessageHeader? nextHeader;
        internal MessageHeader CurrentMessageHeader
        { get 
            {
                if (nextHeader != null) return (MessageHeader)nextHeader;
                else throw new Exception("Tried to get CurrentMessageHeader when null"); // should never happen
            }
        }

        private const int NOT_SET = -37;

        internal SendBufferPage(BufferPool<byte> sendBufferPool, int size)
            : base(sendBufferPool, size)
        {
            this.producerPointer = 0;
            this.validPointer = 0;
            this.consumerPointer = 0;

            this.reservedPointer = NOT_SET;

            this.currentMessageHeaderOffset = NOT_SET;

            this.nextHeader = null;
        }

        internal void Reset()
        {
            this.producerPointer = 0;
            this.validPointer = 0;
            this.consumerPointer = 0;

            this.currentMessageHeaderOffset = NOT_SET;
        }


        public bool WriteHeader(MessageHeader header, NaiadSerialization<MessageHeader> serializer)
        {
            Debug.Assert(this.validPointer == this.producerPointer);
            Debug.Assert(this.nextHeader == null);
            SubArray<byte> bufferAsSubarray = new SubArray<byte>(this.Buffer, this.producerPointer);
            bool success = serializer.Serialize(ref bufferAsSubarray, default(MessageHeader)); // auto-generated serialization code
            if (success)
            {
                this.currentMessageHeaderOffset = this.producerPointer;
                this.producerPointer = bufferAsSubarray.Count;
                Debug.Assert(this.producerPointer - this.currentMessageHeaderOffset == MessageHeader.SizeOf);
                this.nextHeader = header;
            }
            return success;
        }

        public MessageHeader FinalizeLastMessage(NaiadSerialization<MessageHeader> headerSerializer)
        {
            MessageHeader header = (MessageHeader)this.nextHeader;

            if (this.currentMessageHeaderOffset != NOT_SET)
            {
                NaiadSerialization<MessageHeader> serializer = headerSerializer;
                SubArray<byte> bufferAsSubarray = new SubArray<byte>(this.Buffer, this.currentMessageHeaderOffset);
                this.currentMessageHeaderOffset = this.producerPointer;
                int currentMessageLength = ((this.producerPointer - this.validPointer) - MessageHeader.SizeOf);
                if (false && (currentMessageLength == 0))
                {
                    this.producerPointer = this.validPointer; // Undo header writing.
                }
                else
                {
                    Debug.Assert(this.nextHeader != null);
                    
                    header.Length = currentMessageLength;
                    bool success = serializer.Serialize(ref bufferAsSubarray, header); // auto-generated serialization code
                    Debug.Assert(success);
                    this.validPointer = this.producerPointer;
                }
                this.currentMessageHeaderOffset = NOT_SET;
                this.nextHeader = null;
            }
            else
            {
                Debug.Assert(false);
            }
            return header;
        }

        public bool Write<S>(NaiadSerialization<S> serializer, S element)
        {
            // Serialization code deals with SubArray<byte>, so "convert" the page before serializing the record
            SubArray<byte> bufferAsSubarray = new SubArray<byte>(this.Buffer, this.producerPointer);
            bool success = serializer.Serialize(ref bufferAsSubarray, element); // auto-generated serialization code

            if (bufferAsSubarray.Count > this.producerPointer)
            {
#if WORRIED_ABOUT_SERIALIZATION
                if (!success)
                {
                    throw new Exception("Failed to reset pointer.");
                }

                if (this.Buffer.Length - this.producerPointer < 20)
                {
                    Debugger.Break();
                    throw new Exception("Hello!");
                }
                var test = default(S);
                var recv = new RecvBuffer(bufferAsSubarray.Array, this.producerPointer, this.Buffer.Length);
                serializer.TryDeserialize(ref recv, ref test);
                if (!test.Equals(element))
                {
                    throw new Exception("Error" + test + " ::::: " + element);
                }
#endif
                this.producerPointer = bufferAsSubarray.Count;
                return true;
            }
            else
            {
                return false;
            }
        }

        public int WriteElements<S>(NaiadSerialization<S> serializer, ArraySegment<S> elements)
        {
            SubArray<byte> bufferAsSubarray = new SubArray<byte>(this.Buffer, this.producerPointer);
            int numWritten = serializer.TrySerializeMany(ref bufferAsSubarray, elements);
            this.producerPointer = bufferAsSubarray.Count;
            return numWritten;
            /*
            for (int i = 0; i < elements.Count; ++i, ++numWritten)
            {
                bool success = serializer.Serialize(ref bufferAsSubarray, elements.Array[i + elements.Offset]);
                if (!success)
                    break;
            }
            this.producerPointer = bufferAsSubarray.Count;
            return numWritten;*/
        }

        public void WriteReserved<S>(NaiadSerialization<S> serializer, S element)
        {
            Debug.Assert(this.reservedPointer != NOT_SET);
            SubArray<byte> bufferAsSubarray = new SubArray<byte>(this.Buffer, this.reservedPointer);
            bool success = serializer.Serialize(ref bufferAsSubarray, element); // auto-generated serialization code
            Debug.Assert(success);
            Debug.Assert(bufferAsSubarray.Count - this.reservedLength == this.reservedPointer);
            this.reservedPointer = NOT_SET;
            this.reservedLength = NOT_SET;
        }

        public bool ReserveBytes(int count)
        {
            if (this.Buffer.Length - this.producerPointer >= count)
            {
                this.reservedPointer = this.producerPointer;
                this.reservedLength = count;
                this.producerPointer += count;
                return true;
            }
            else
                return false;
        }

        public BufferSegment Consume()
        {
            BufferSegment ret = new BufferSegment(this, this.consumerPointer, this.validPointer - this.consumerPointer);
            ret.Type = this.Type;
            this.consumerPointer = this.validPointer;
            this.Acquire();
            return ret;
        }

        public static SendBufferPage CreateSpecialPage(MessageHeader header, int seqno, NaiadSerialization<MessageHeader> serializer)
        {
            header.SequenceNumber = seqno;
            SendBufferPage ret = new SendBufferPage(GlobalBufferPool<byte>.pool, MessageHeader.SizeOf);
            ret.Type = header.Type;
            ret.nextHeader = header;
            Logging.Info("Created page of type {0}", ret.Type.ToString());
            SubArray<byte> bufferAsSubarray = new SubArray<byte>(ret.Buffer, ret.producerPointer);
            bool success = serializer.Serialize(ref bufferAsSubarray, header);
            Debug.Assert(success);
            ret.producerPointer = bufferAsSubarray.Count;
            ret.validPointer = bufferAsSubarray.Count;
            Debug.Assert(ret.producerPointer == MessageHeader.SizeOf);
            return ret;
        }

        public static SendBufferPage CreateShutdownMessagePage(int seqno, NaiadSerialization<MessageHeader> serializer)
        {
            return CreateSpecialPage(MessageHeader.Shutdown, seqno, serializer);
        }
    

    }

    internal enum TryConsumeResult
    {
        Success,
        Fail_InsufficientData,
        Fail_TryWithNextPage,
        Fail_LargeMessage
    }

    internal class RecvBufferPage : BufferPage
    {
        // Default page size is 64 KB.
        public const int PAGE_SIZE = 1 << 16;

        public int consumePointer;
        public int producePointer;

        private readonly RecvBufferSheaf sheaf;
        public readonly int Id;

        public RecvBufferPage(BufferPool<byte> pool, RecvBufferSheaf sheaf, int id)
            : base(pool, RecvBufferPage.PAGE_SIZE)
        {
            this.consumePointer = 0;
            this.producePointer = 0;
            this.sheaf = sheaf;
            this.Id = id;
        }

        public ArraySegment<byte> GetSegmentForProducer()
        {
            return new ArraySegment<byte>(this.Buffer, this.producePointer, this.Buffer.Length - this.producePointer);
        }

        public unsafe bool ConsumeHeader(out MessageHeader header, NaiadSerialization<MessageHeader> serializer)
        {
            header = default(MessageHeader);
            if (this.producePointer - this.consumePointer < MessageHeader.SizeOf)
                return false;

            MessageHeader.ReadHeaderFromBuffer(this.Buffer, this.consumePointer, ref header, serializer);
            this.consumePointer += MessageHeader.SizeOf;
            return true;
        }

        public unsafe bool ConsumeHeader(RecvBufferPage nextPage, out MessageHeader header, NaiadSerialization<MessageHeader> serializer)
        {
            Debug.Assert(nextPage != null);
            header = default(MessageHeader);
            byte[] headerBuffer = new byte[MessageHeader.SizeOf];

            int bytesInThisPage = this.Buffer.Length - this.consumePointer;
            int bytesInNextPage = headerBuffer.Length - bytesInThisPage;

            if (bytesInNextPage > nextPage.producePointer)
                return false;

            Array.Copy(this.Buffer, this.consumePointer, headerBuffer, 0, bytesInThisPage);
            Array.Copy(nextPage.Buffer, 0, headerBuffer, bytesInThisPage, bytesInNextPage);

            this.consumePointer += bytesInThisPage;
            nextPage.consumePointer += bytesInNextPage;

            MessageHeader.ReadHeaderFromBuffer(headerBuffer, 0, ref header, serializer);
            return true;
        }

        public unsafe int ConsumeBytesUpto(byte[] buffer, int offset, int length)
        {
            int bytesToConsume = length;
            if (length > this.producePointer - this.consumePointer)
                bytesToConsume = this.producePointer - this.consumePointer;

            Array.Copy(this.Buffer, this.consumePointer, buffer, offset, bytesToConsume);

            this.consumePointer += bytesToConsume;
            //Console.Error.WriteLine("$$$ Consumed {0} bytes from page; new consumePointer = {1}", bytesToConsume, this.consumePointer);
            return bytesToConsume;
        }

        public TryConsumeResult TryConsumeContiguousBytes(int length, ref RecvBuffer buffer)
        {
            if (length > this.producePointer - this.consumePointer)
            {
                if (this.producePointer == this.Buffer.Length)
                    return TryConsumeResult.Fail_TryWithNextPage;
                else
                    return TryConsumeResult.Fail_InsufficientData;
            }
            else
            {
                this.Acquire();
                buffer = new RecvBuffer(this.Buffer, this.consumePointer, this.consumePointer + length);
                this.consumePointer += length;
                return TryConsumeResult.Success;
            }
        }
        
        protected override void OnRelease()
        {
            this.consumePointer = 0;
            this.producePointer = 0;
            if (this.sheaf != null)
                this.sheaf.OnAllConsumed(this);
        }

    }

    /// <summary>
    /// A managed collection of RecvBufferPage objects, used to provide buffers for receiving/reading
    /// serialized messages.
    /// 
    /// Each page can be in one or more states:
    /// * Empty -- the page contains no serialized data.
    /// * In use -- the page has been passed to a producer (i.e. a read or recv operation).
    /// * Partially produced -- the page contains serialized data that has been written into the page, but more space is available.
    /// * Partially produced/consumed -- same as partially produced, except that the consumer has started to read from the page(?).
    /// * Partially consumed -- the page contains serialized data that has been written into the page and not yet consumed. No more space is available for writing.
    /// </summary>
    internal class RecvBufferSheaf
    {
        public readonly int ForProcessID;

        private readonly LinkedList<RecvBufferPage> producedPages;

        private RecvBufferPage partiallyProducedPage;

        private readonly List<RecvBufferPage> inUsePages;

        //private readonly BlockingCollection<RecvBufferPage> freePages;
        private readonly ConcurrentBag<RecvBufferPage> freePages;

        private readonly BufferPool<byte> pool;

        private const int MAX_PAGES_FOR_RECV = 1; // WAS 16;

        public RecvBufferSheaf(int forProcessID, int capacity, BufferPool<byte> pool)
        {
            this.ForProcessID = forProcessID;
            this.partiallyProducedPage = null;
            this.producedPages = new LinkedList<RecvBufferPage>();
            this.inUsePages = new List<RecvBufferPage>();
            //this.freePages = new BlockingCollection<RecvBufferPage>();
            this.freePages = new ConcurrentBag<RecvBufferPage>();
            this.pool = pool;

            // TODO: replace this with lazy initialization.
            for (int i = 0; i < capacity; ++i)
            {
                RecvBufferPage page = new RecvBufferPage(pool, this, i);
                //this.freePages.Add(page);
                this.freePages.Add(page);
            }
        }

        public ArraySegment<byte> GetFreeSegment()
        {
            RecvBufferPage page;
            if (this.partiallyProducedPage != null)
            {
                page = this.partiallyProducedPage;
                this.partiallyProducedPage = null;
            }
            else if (!this.freePages.TryTake(out page))
            {
                page = new RecvBufferPage(this.pool, this, 0);
                Logging.Debug("RecvPage {0}: was allocated to satisfy demand ({1} bytes free) (after blocking for a page)", page.Id, page.Buffer.Length - page.producePointer);
            }
            this.inUsePages.Add(page);
            return page.GetSegmentForProducer();
        }

        // TODO: This should maybe take an optional parameter to specify the amount of buffer
        //       that is desired (with appropriate blocking behavior).
        public List<ArraySegment<byte>> GetFreeSegments(List<ArraySegment<byte>> ret = null)
        {
            if (ret == null)
                ret = new List<ArraySegment<byte>>();

            while (ret.Count < RecvBufferSheaf.MAX_PAGES_FOR_RECV)
                ret.Add(this.GetFreeSegment());

            Debug.Assert(ret.Sum(x => x.Count) > 0);

            return ret;
        }

        /// <summary>
        /// Called when the previous recv/read operation completes.
        /// </summary>
        /// <param name="bytesProduced">The number of bytes that were produced by the completed operation.</param>
        /// <returns>A list of pages containing new data.</returns>
        public void OnBytesProduced(int bytesProduced)
        {
            int i = 0;
            Debug.Assert(bytesProduced > 0);
            while (bytesProduced > 0)
            {
                int oldProducePointer = this.inUsePages[i].producePointer;
                if (bytesProduced > this.inUsePages[i].Buffer.Length - oldProducePointer)
                {
                    bytesProduced -= this.inUsePages[i].Buffer.Length - oldProducePointer;
                    this.inUsePages[i].producePointer = this.inUsePages[i].Buffer.Length;
                    this.producedPages.AddLast(this.inUsePages[i]);
                }
                else
                {
                    this.inUsePages[i].producePointer += bytesProduced;
                    bytesProduced = 0;

                    if (this.inUsePages[i].producePointer < this.inUsePages[i].Buffer.Length)
                    {
                        this.partiallyProducedPage = this.inUsePages[i];
                    }
                    else
                    {
                        this.producedPages.AddLast(this.inUsePages[i]);
                    }
                }
                ++i;
            }
            while (i < this.inUsePages.Count)
            {
                Debug.Assert(this.inUsePages[i].producePointer == 0);
                freePages.Add(this.inUsePages[i]);

                ++i;
            }
            this.inUsePages.Clear();
        }

        private bool ConsumeNextHeader(ref MessageHeader header, NaiadSerialization<MessageHeader> headerSerializer)
        {


            if (this.producedPages.Count == 0)
            {
                //if (this.partiallyProducedPage != null)
                //    Console.Error.WriteLine("!!! Next header starts at offset: {0}", this.partiallyProducedPage.consumePointer);
                if (this.partiallyProducedPage != null && this.partiallyProducedPage.ConsumeHeader(out header, headerSerializer))
                    return true;
                else
                    return false;
            }

            Debug.Assert(this.producedPages.Count > 0);

            LinkedListNode<RecvBufferPage> firstNode = this.producedPages.First;
            RecvBufferPage firstPage = firstNode.Value;

            //Console.Error.WriteLine("!!! Next header starts at offset: {0}", firstPage.consumePointer);
            if (firstPage.ConsumeHeader(out header, headerSerializer))
            {
                if (firstPage.consumePointer == firstPage.Buffer.Length)
                {
                    // Miraculously, we have consumed the whole page.
                    firstPage.Release();
                    this.producedPages.RemoveFirst();
                }
                return true;
            }

            
            RecvBufferPage secondPage;

            if (this.producedPages.Count > 1)
            {
                Debug.Assert(this.producedPages.Count > 1);

                LinkedListNode<RecvBufferPage> secondNode = firstNode.Next;
                secondPage = secondNode.Value;
            }
            else if (this.partiallyProducedPage != null)
            {
                secondPage = this.partiallyProducedPage;
            }
            else
            {
                return false;
            }

            bool success = firstPage.ConsumeHeader(secondPage, out header, headerSerializer);
            if (success)
            {
                // We've completely consumed the first page, because we had to split onto the second page.
                this.producedPages.RemoveFirst();
                firstPage.Release();
                return true;
            }
            else
                return false;
            
        }

        private int ConsumeBytes(byte[] buffer, int offset, int length)
        {

            if (this.producedPages.Count == 0)
                if (this.partiallyProducedPage != null)
                    return this.partiallyProducedPage.ConsumeBytesUpto(buffer, offset, length);

            int totalBytesConsumed = 0;

            while (length > 0 && this.producedPages.Count > 0)
            {
                LinkedListNode<RecvBufferPage> currentNode = this.producedPages.First;
                RecvBufferPage currentPage = currentNode.Value;

                int bytesConsumed = currentPage.ConsumeBytesUpto(buffer, offset, length);
                totalBytesConsumed += bytesConsumed;
                offset += bytesConsumed;
                length -= bytesConsumed;

                if (currentPage.consumePointer == currentPage.Buffer.Length)
                {
                    this.producedPages.RemoveFirst();
                    currentPage.Release();
                }
            }

            if (length > 0 && this.partiallyProducedPage != null)
            {
                int bytesConsumed = this.partiallyProducedPage.ConsumeBytesUpto(buffer, offset, length);
                totalBytesConsumed += bytesConsumed;
                offset += bytesConsumed;
                length -= bytesConsumed;
            }

            return totalBytesConsumed;

        }

        private TryConsumeResult TryConsumeContiguousBytes(int length, ref RecvBuffer body)
        {
            if (this.producedPages.Count == 0)
                if (this.partiallyProducedPage != null)
                    return this.partiallyProducedPage.TryConsumeContiguousBytes(length, ref body);
                else
                    return TryConsumeResult.Fail_InsufficientData;

            Debug.Assert(this.producedPages.Count > 0);
            LinkedListNode<RecvBufferPage> firstNode = this.producedPages.First;
            RecvBufferPage firstPage = firstNode.Value;

            TryConsumeResult success = firstPage.TryConsumeContiguousBytes(length, ref body);
            if (firstPage.consumePointer == firstPage.Buffer.Length)
            {
                firstPage.Release();
                this.producedPages.RemoveFirst();
            }
            return success;
        }

        private MessageHeader currentMessageHeader;
        private bool currentMessageHeaderValid = false;
        private byte[] currentSplitMessageBody;
        private int currentSplitMessagePosition;

        /// <summary>
        /// Returns a collection of messages that have been produced but not yet consumed.
        /// </summary>
        /// <returns></returns>
        public IEnumerable<SerializedMessage> ConsumeMessages(NaiadSerialization<MessageHeader> headerSerializer)
        {
            while (true)
            {

                if (this.currentSplitMessageBody != null)
                {
                    int bytesConsumed = this.ConsumeBytes(this.currentSplitMessageBody, this.currentSplitMessagePosition, this.currentMessageHeader.Length - this.currentSplitMessagePosition);
                    this.currentSplitMessagePosition += bytesConsumed;
                    //Console.Error.WriteLine("&&& Consumed {0} bytes into split message, now at {1} / {2}", bytesConsumed, this.currentSplitMessagePosition, this.currentMessageHeader.Length);
                    if (this.currentSplitMessagePosition == this.currentMessageHeader.Length)
                    {
                        yield return new SerializedMessage(this.ForProcessID, this.currentMessageHeader, new RecvBuffer(this.currentSplitMessageBody, 0, this.currentSplitMessagePosition));
                        this.currentSplitMessageBody = null;
                        this.currentMessageHeaderValid = false;
                    }
                    else
                        break;
                }
                else
                {
                    if (!this.currentMessageHeaderValid)
                    {
                        bool success = this.ConsumeNextHeader(ref this.currentMessageHeader, headerSerializer);
                        if (!success)
                        {
                            this.currentMessageHeaderValid = false;
                            break;
                        }
                        this.currentMessageHeaderValid = true;
                    }

                    RecvBuffer unsplitBuffer = default(RecvBuffer);
                    TryConsumeResult result = this.TryConsumeContiguousBytes(this.currentMessageHeader.Length, ref unsplitBuffer);
                    switch (result)
                    {
                        case TryConsumeResult.Success:
                            //Console.Error.WriteLine("*** Consumed {0} contiguous bytes", unsplitBuffer.End - unsplitBuffer.CurrentPos);
                            yield return new SerializedMessage(this.ForProcessID, this.currentMessageHeader, unsplitBuffer);
                            this.currentMessageHeaderValid = false;
                            break;
                        case TryConsumeResult.Fail_InsufficientData:
                            //Console.Error.WriteLine("*** Not enough data to proceed :(!");
                            yield break;
                        case TryConsumeResult.Fail_TryWithNextPage:
                            //Console.Error.WriteLine("*** Need to split!");
                            this.currentSplitMessageBody = new byte[this.currentMessageHeader.Length];
                            this.currentSplitMessagePosition = 0;
                            break;
                    }

                }

            }


#if false
            SerializedMessage yret;
            while (true)
            {
                //Console.WriteLine("Switching to state: {0}", this.consumeState);
                switch (this.consumeState)
                {
                    case ConsumeState.StartLargeMessage:
                        {
                            //throw new NotImplementedException();
                            this.currentMessageHeader = this.ConsumeNextHeader();
                            this.currentSplitMessageBody = new byte[this.currentMessageHeader.Length];
                            this.currentSplitMessagePosition = 0;
                            this.consumeState = ConsumeState.ContinueLargeMessage;
                            continue;
                        }
                        break;
                    case ConsumeState.ContinueLargeMessage:
                        {
                            while (this.currentSplitMessagePosition < this.currentMessageHeader.Length)
                            {
                                if (this.producedPages.Count > 0)
                                {
                                    LinkedListNode<RecvBufferPage> currentNode = this.producedPages.First;
                                    RecvBufferPage currentPage = currentNode.Value;

                                    int bytesConsumed = currentPage.ConsumeBytesUpto(this.currentSplitMessageBody, this.currentSplitMessagePosition, this.currentMessageHeader.Length - this.currentSplitMessagePosition);
                                    Console.Error.WriteLine("Consumed part of a large message: bytes [{0}, {1}) of {2}", this.currentSplitMessagePosition, this.currentSplitMessagePosition + bytesConsumed, this.currentMessageHeader.Length);
                                    this.currentSplitMessagePosition += bytesConsumed;

                                    if (currentPage.consumePointer == currentPage.Buffer.Length)
                                    {
                                        currentPage.Release();
                                        this.producedPages.RemoveFirst();
                                    }

                                }
                                else
                                {
                                    if (this.partiallyProducedPage != null)
                                    {
                                        int bytesConsumed = this.partiallyProducedPage.ConsumeBytesUpto(this.currentSplitMessageBody, this.currentSplitMessagePosition, this.currentMessageHeader.Length - this.currentSplitMessagePosition);
                                        Console.Error.WriteLine("Consumed part of a large message: bytes [{0}, {1}) of {2}", this.currentSplitMessagePosition, this.currentSplitMessagePosition + bytesConsumed, this.currentMessageHeader.Length);
                                        this.currentSplitMessagePosition += bytesConsumed;
                                    }
                                    break;
                                }
                            }
                            if (this.currentSplitMessagePosition == this.currentMessageHeader.Length)
                            {
                                // Completed the large message.
                                this.consumeState = ConsumeState.Normal;
                                yield return new SerializedMessage(this.ForProcessID, this.currentMessageHeader, new RecvBuffer(this.currentSplitMessageBody, 0, this.currentSplitMessagePosition));
                            }
                            else
                                yield break;
                        }
                        break;
                        yield break;
                    case ConsumeState.Normal:
                        {
                            if (this.producedPages.Count == 0 && this.partiallyProducedPage != null)
                            {
                                // Special case: only data is on the partially produced page.
                                TryConsumeResult result = this.partiallyProducedPage.TryConsumeNextMessage(out yret);

                                while (result == TryConsumeResult.Success)
                                {
                                    yield return yret;
                                    result = this.partiallyProducedPage.TryConsumeNextMessage(out yret);
                                }
                                if (result == TryConsumeResult.Fail_LargeMessage)
                                {
                                    this.consumeState = ConsumeState.StartLargeMessage;
                                    continue;
                                }
                                yield break;
                            }
                            else if (this.producedPages.Count > 0)
                            {
                                LinkedListNode<RecvBufferPage> currentNode;
                                RecvBufferPage currentPage;
                                TryConsumeResult result;
                                do
                                {
                                    currentNode = this.producedPages.First;
                                    currentPage = currentNode.Value;

                                    result = currentPage.TryConsumeNextMessage(out yret);
                                    while (result == TryConsumeResult.Success)
                                    {
                                        yield return yret;
                                        result = currentPage.TryConsumeNextMessage(out yret);
                                    }
                                    if (result == TryConsumeResult.Fail_LargeMessage)
                                    {
                                        break;
                                    }

                                    if (currentPage.consumePointer < currentPage.Buffer.Length)
                                    {
                                        // Attempt to get a message that is fragmented across two pages.
                                        RecvBufferPage nextPage = currentNode.Next != null ? currentNode.Next.Value : this.partiallyProducedPage;
                                        result = TryConsumeResult.Fail_InsufficientData;
                                        if (nextPage != null)
                                        {
                                            result = currentPage.TryConsumeNextMessage(nextPage, out yret);
                                            if (result == TryConsumeResult.Success)
                                                yield return yret;
                                            else if (result == TryConsumeResult.Fail_LargeMessage)
                                                break;

                                        }

                                        // N.B. If we haven't managed to consume the whole page, we need to keep it on the queue.

                                    }

                                    if (currentPage.consumePointer == currentPage.Buffer.Length)
                                    {
                                        // Miraculously, we have consumed the whole page.
                                        currentPage.Release();
                                        this.producedPages.RemoveFirst();
                                    }
                                    else
                                    {
                                        // We will leave the current page on the queue, because it is fully-produced but not yet consumed.
                                        yield break;
                                    }

                                } while (this.producedPages.Count > 0);
                                if (result == TryConsumeResult.Fail_LargeMessage)
                                {
                                    this.consumeState = ConsumeState.StartLargeMessage;
                                    continue;
                                }

                                // Now do it for the partially-produced page.
                                if (this.partiallyProducedPage != null)
                                {
                                    result = this.partiallyProducedPage.TryConsumeNextMessage(out yret);
                                    while (result == TryConsumeResult.Success)
                                    {
                                        yield return yret;
                                        result = this.partiallyProducedPage.TryConsumeNextMessage(out yret);
                                    }
                                    if (result == TryConsumeResult.Fail_LargeMessage)
                                    {
                                        this.consumeState = ConsumeState.StartLargeMessage;
                                        continue;
                                    }

                                }

                                yield break;


                            }
                            break;

                        }
                }

            }

#endif
        }

        /// <summary>
        /// Called when all messages from a single page have been released.
        /// 
        /// The given page will transition from partially-consumed to empty.
        /// </summary>
        /// <param name="page"></param>
        internal void OnAllConsumed(RecvBufferPage page)
        {
            Debug.Assert(page.producePointer == 0);
            Debug.Assert(page.consumePointer == 0);
            page.Acquire();
            this.freePages.Add(page);
        }
    }

    /// <summary>
    /// Identifies a contiguous region of a BufferPage.  A BufferSegment is the serialized representation of one or more Messages.
    /// </summary>
    internal struct BufferSegment : IDisposable
    {
        private readonly BufferPage page;   // parent BufferPage
        public readonly int startOffset;
        public readonly int Length;
        public SerializedMessageType Type;

        internal BufferSegment(BufferPage page, int startOffset, int length)
        {
            this.page = page;
            this.startOffset = startOffset;
            this.Length = length;
            this.Type = SerializedMessageType.Data;
        }

        public ArraySegment<byte> ToArraySegment()
        {
            return new ArraySegment<byte>(this.page.Buffer, this.startOffset, this.Length);
        }

        public void Dispose()
        {
            this.page.Release();
        }

        public void Copy()
        {
            this.page.Acquire();
        }
    }

    public enum SerializedMessageType
    {
        Data = 1,
        Shutdown = 2,
        Checkpoint = 3,
        CheckpointData = 4,
        Startup = 5,
        Failure = 6
    }

    public struct MessageHeader : IEquatable<MessageHeader>
    {
        public int ChannelID;
        public int DestVertexID;
        public int FromVertexID;
        public int Length;
        public int SequenceNumber;
        public SerializedMessageType Type;

        public MessageHeader(int fromVertexID, int sequenceNumber, int channelID, int destVertexID, SerializedMessageType type)
            : this(fromVertexID, sequenceNumber, channelID, destVertexID, -1, type)
        { }

        public MessageHeader(int fromVertexID, int sequenceNumber, int channelID, int destVertexID, int length, SerializedMessageType type)
        {
            this.FromVertexID = fromVertexID;
            this.SequenceNumber = sequenceNumber;
            this.ChannelID = channelID;
            this.DestVertexID = destVertexID;
            this.Length = length;
            this.Type = type;
        }

        public bool Equals(MessageHeader that)
        {
            return this.FromVertexID == that.FromVertexID
                && this.SequenceNumber == that.SequenceNumber
                && this.ChannelID == that.ChannelID
                && this.DestVertexID == that.DestVertexID
                && this.Length == that.Length;
        }

        public bool IsValidDataHeader
        {
            get
            {
                return this.Type == SerializedMessageType.CheckpointData || (this.ChannelID >= 0
                    // && this.DestVertexID < Naiad.NumberOfTotalVertices // (no longer easily accessible).
                    && this.Length < RecvBufferPage.PAGE_SIZE
                    && this.Length >= 0);
            }
        }

        public static MessageHeader Shutdown
        {
            get { return new MessageHeader(-1, -1, -1, -1, 0, SerializedMessageType.Shutdown); }
        }

        public static MessageHeader GenerateBarrierMessageHeader(int barrierId)
        {
            return new MessageHeader(-1, -1, barrierId, -1, 0, SerializedMessageType.Startup);
        }

        public static MessageHeader GraphFailure(int graphId)
        {
            return new MessageHeader(-1, -1, graphId, -1, 0, SerializedMessageType.Failure);
        }

        public static MessageHeader Checkpoint
        {
            get { return new MessageHeader(-1, -1, -1, -1, 0, SerializedMessageType.Checkpoint); }
        }

        public unsafe static int SizeOf
        {
            get
            {
                return sizeof(MessageHeader);
            }
        }

        public static void ReadHeaderFromBuffer(byte[] array, int offset, ref MessageHeader header, NaiadSerialization<MessageHeader> headerSerializer)
        {
            RecvBuffer buffer = new RecvBuffer(array, offset, offset + MessageHeader.SizeOf);
            bool success = headerSerializer.TryDeserialize(ref buffer, out header);
            Debug.Assert(success);
            Debug.Assert(Enum.IsDefined(typeof(SerializedMessageType), header.Type));
        }

        public static void WriteHeaderToBuffer(byte[] array, int offset, MessageHeader header, NaiadSerialization<MessageHeader> headerSerializer)
        {
            SubArray<byte> bufferAsSubarray = new SubArray<byte>(array, offset);
            headerSerializer.Serialize(ref bufferAsSubarray, header);
            Debug.Assert(bufferAsSubarray.Count == offset + MessageHeader.SizeOf);
        }

    }

    /// <summary>
    /// Comprises a header appended to a SubArray containing serialized NaiadRecords.
    /// </summary>
    public class SerializedMessage : IDisposable, IEquatable<SerializedMessage>
    {
        internal readonly MessageHeader Header;

        public readonly int FromProcessID;

        public int ConnectionSequenceNumber;

        internal readonly SerializedMessageType Type;

        public readonly RecvBuffer Body;
        private readonly RecvBufferPage Page;

        public bool Equals(SerializedMessage other)
        {
            return this.Header.FromVertexID == other.Header.FromVertexID
                && this.Header.SequenceNumber == other.Header.SequenceNumber
                && this.FromProcessID == other.FromProcessID
                && this.Header.ChannelID == other.Header.ChannelID
                && this.Header.DestVertexID == other.Header.DestVertexID;
        }

        internal static SerializedMessage SpecialMessage(int fromProcessID, int channelID, int fromVertexID, int destVertexID, int seqNum, SerializedMessageType type)
        {
            return new SerializedMessage(fromProcessID, channelID, fromVertexID, destVertexID, seqNum, type);
        }

        private SerializedMessage(int fromProcessID, int channelID, int fromVertexID, int destVertexID, int seqNum, SerializedMessageType type)
        {
            this.FromProcessID = fromProcessID;
            this.Header.ChannelID = channelID;
            this.Header.FromVertexID = fromVertexID;
            this.Header.DestVertexID = destVertexID;
            this.Header.SequenceNumber = seqNum;
            this.Type = type;

            this.Body = default(RecvBuffer);
            this.Page = null;
        }

        internal SerializedMessage(int fromProcessID, MessageHeader header, RecvBufferPage page, int offset)
        {
            this.FromProcessID = fromProcessID;
            this.Header = header;
            this.Type = header.Type;
            this.Body = new RecvBuffer(page.Buffer, offset, offset + header.Length);
            this.Page = page;
            this.Page.Acquire();
        }

        internal SerializedMessage(int fromProcessID, MessageHeader header, RecvBuffer buffer)
        {
            this.FromProcessID = fromProcessID;
            this.Header = header;
            this.Type = header.Type;
            this.Body = buffer;
            this.Page = null;
        }

        public void Dispose()
        {
            if (this.Page != null)
                this.Page.Release();
        }
    }

    public static class NaiadSerializationConstants
    {
        public const int CHANNEL_ID = 0x4149414e;
        public const int DEST_VERTEX_ID = 0x0a0d2144;

        // Now implemented using the SerializationCodeGenerator properties.
        //public const int FROM_VERTEX_ID = (MAJOR_VERSION_NUMBER << 16) + MINOR_VERSION_NUMBER;
    }

    public class NaiadWriter : IDisposable
    {
        private const int PAGE_SIZE = 1 << 14;

        private readonly SerializedMessageSender[] senders;
        private SendBufferPage currentPage;
        private int sequenceNumber;

        public int pagesWritten;
        public int objectsWritten;

        private Type lastType;
        private object lastSerializer;

        private readonly SerializationCodeGenerator codeGenerator;

        private readonly NaiadSerialization<MessageHeader> headerSerializer;

        private readonly int versionNumber;

        public NaiadWriter(Stream stream, SerializationCodeGenerator codeGenerator)
            : this(new SerializedMessageSender[] { new StreamSerializedMessageSender(stream, PAGE_SIZE) }, codeGenerator)
        { }

        internal NaiadWriter(IEnumerable<SerializedMessageSender> senders, SerializationCodeGenerator codeGenerator)
        {
            this.senders = senders.ToArray();
            this.currentPage = null;
            this.sequenceNumber = 0;

            this.codeGenerator = codeGenerator;
            this.headerSerializer = codeGenerator.GetSerializer<MessageHeader>();

            this.versionNumber = (codeGenerator.MajorVersion << 16) + codeGenerator.MinorVersion;

            this.lastType = null;
            this.lastSerializer = null;

            this.pagesWritten = 0;
            this.objectsWritten = 0;
        }

        public void Write<S>(S value)
        {
            if (typeof(S) != this.lastType)
            {
                this.lastType = typeof(S);
                this.lastSerializer = this.codeGenerator.GetSerializer<S>();
            }
            this.Write(value, (NaiadSerialization<S>)this.lastSerializer);
        }


        public void Write<S>(S value, NaiadSerialization<S> serializer)
        {

            if (this.currentPage == null)
            {
                this.currentPage = new SendBufferPage(new ThreadLocalBufferPool<byte>(1), PAGE_SIZE);
                this.currentPage.WriteHeader(new MessageHeader(this.versionNumber, this.sequenceNumber++, NaiadSerializationConstants.CHANNEL_ID, NaiadSerializationConstants.DEST_VERTEX_ID, SerializedMessageType.CheckpointData), this.headerSerializer);
            }

            if (!this.currentPage.Write(serializer, value))
            {
                this.FlushCurrentPage();
                this.currentPage = new SendBufferPage(new ThreadLocalBufferPool<byte>(1), PAGE_SIZE);
                this.currentPage.WriteHeader(new MessageHeader(this.versionNumber, this.sequenceNumber++, NaiadSerializationConstants.CHANNEL_ID, NaiadSerializationConstants.DEST_VERTEX_ID, SerializedMessageType.CheckpointData), this.headerSerializer);
                if (!this.currentPage.Write(serializer, value))
                    throw new IOException("Cannot current write a record that is longer than a SendBufferPage (16KB).");
            }

            ++this.objectsWritten;
        }

        public void FlushCurrentPage()
        {
            var hdr = this.currentPage.FinalizeLastMessage(this.headerSerializer);
            BufferSegment segment = this.currentPage.Consume();

            foreach (SerializedMessageSender sender in this.senders)
                sender.SendBufferSegment(hdr, segment);
            segment.Dispose();
            this.currentPage.Release();
            this.currentPage = null;
        }


        public void Flush()
        {
            if (this.currentPage != null)
                this.FlushCurrentPage();
        }

        public void Dispose()
        {
            this.Flush();
        }
    }

    public class NaiadReader : IDisposable
    {
        private const int PAGE_SIZE = 1 << 14;

        private readonly Stream stream;
        private readonly byte[] buffer;

        private RecvBuffer currentPage;

        private Type lastType;
        private object lastSerializer;

        public readonly SerializationCodeGenerator CodeGenerator;
        private NaiadSerialization<MessageHeader> headerSerializer;

        private readonly int versionNumber;

        public NaiadReader(Stream stream, SerializationCodeGenerator codeGenerator)
        {
            this.CodeGenerator = codeGenerator;
            this.versionNumber = (codeGenerator.MajorVersion << 16) + codeGenerator.MinorVersion;
            this.headerSerializer = this.CodeGenerator.GetSerializer<MessageHeader>();

            this.stream = stream;
            this.buffer = GlobalBufferPool<byte>.pool.CheckOut(PAGE_SIZE);
            this.lastType = null;
            this.lastSerializer = null;

            bool nextPageAvailable = this.TryGetNextPage(out this.currentPage);
            if (!nextPageAvailable)
                throw new InvalidDataException("Stream does not contain data");

        }

        public S Read<S>()
        {
            if (typeof(S) != this.lastType)
            {
                this.lastType = typeof(S);
                this.lastSerializer = this.CodeGenerator.GetSerializer<S>();
            }
            return this.Read<S>((NaiadSerialization<S>)this.lastSerializer);
        }

        private bool TryGetNextPage(out RecvBuffer result)
        {
            int bytesRead = this.stream.Read(this.buffer, 0, this.buffer.Length);
            if (bytesRead == 0)
            {
                result = default(RecvBuffer);
                return false;
            }
            else
            {
                MessageHeader parsedHeader = default(MessageHeader);
                MessageHeader.ReadHeaderFromBuffer(this.buffer, 0, ref parsedHeader, this.headerSerializer);

                if (parsedHeader.FromVertexID != this.versionNumber)
                {
                    throw new InvalidDataException(string.Format("Cannot deserialize this file with serializer version {0}.{1} (file uses version {2}.{3})", this.CodeGenerator.MajorVersion, this.CodeGenerator.MinorVersion, parsedHeader.FromVertexID >> 16, parsedHeader.FromVertexID & 0xFFFF));
                }


                result = new RecvBuffer(this.buffer, MessageHeader.SizeOf, MessageHeader.SizeOf + parsedHeader.Length);
                return true;
            }
        }

        public bool TryRead<S>(NaiadSerialization<S> deserializer, out S result)
        {
            if (!deserializer.TryDeserialize(ref this.currentPage, out result))
            {
                bool nextPageAvailable = this.TryGetNextPage(out this.currentPage);
                if (!nextPageAvailable)
                    return false;
                bool success = deserializer.TryDeserialize(ref this.currentPage, out result);
                return success;
            }
            else
            {
                return true;
            }
        }

        public S Read<S>(NaiadSerialization<S> deserializer)
        {
            S ret;
            bool success = this.TryRead<S>(deserializer, out ret);
            if (!success)
            {
                throw new InvalidOperationException("No more records in stream");
            }
            return ret;
        }


        public void Dispose()
        {
            this.stream.Dispose();
            GlobalBufferPool<byte>.pool.CheckIn(this.buffer);
        }
    }

    public class NaiadStreamWriter<S> : IDisposable
    {
        private const int PAGE_SIZE = 1 << 14;

        private readonly Stream stream;
        private readonly NaiadSerialization<S> serializer;
        private SendBufferPage currentPage;
        private ManualResetEvent writtenEvent;
        private int sequenceNumber;

        private readonly int versionNumber;

        private readonly SerializationCodeGenerator codeGenerator;
        private readonly NaiadSerialization<MessageHeader> headerSerializer;

        public NaiadStreamWriter(Stream stream, SerializationCodeGenerator codeGenerator)
        {
            this.stream = stream;
            this.currentPage = null;
            this.codeGenerator = codeGenerator;
            this.versionNumber = (codeGenerator.MajorVersion << 16) + codeGenerator.MinorVersion;
            this.serializer = this.codeGenerator.GetSerializer<S>();
            this.headerSerializer = this.codeGenerator.GetSerializer<MessageHeader>();
            this.writtenEvent = new ManualResetEvent(true);
            this.sequenceNumber = 0;
        }

        public void Write(S element)
        {
            if (this.currentPage == null)
            {
                this.currentPage = new SendBufferPage(GlobalBufferPool<byte>.pool, PAGE_SIZE);
                this.currentPage.WriteHeader(new MessageHeader(this.versionNumber, this.sequenceNumber++, NaiadSerializationConstants.CHANNEL_ID, NaiadSerializationConstants.DEST_VERTEX_ID, SerializedMessageType.Data), this.headerSerializer);
            }

            if (!this.currentPage.Write(this.serializer, element))
            {
                this.FlushCurrentPage();

                // Allocate new page
                int size = PAGE_SIZE;
                this.currentPage = new SendBufferPage(GlobalBufferPool<byte>.pool, size);
                this.currentPage.WriteHeader(new MessageHeader(this.versionNumber, this.sequenceNumber++, NaiadSerializationConstants.CHANNEL_ID, NaiadSerializationConstants.DEST_VERTEX_ID, SerializedMessageType.Data), this.headerSerializer);
                while (!this.currentPage.Write(this.serializer, element))
                {
                    this.currentPage.Release();
                    this.currentPage = null;
                    size <<= 1;
                    Logging.Info("Doubling send buffer size due to long record (new size = {0}) in channel {0}", size, this.ToString());
                    this.currentPage = new SendBufferPage(GlobalBufferPool<byte>.pool, size);
                    this.currentPage.WriteHeader(new MessageHeader(this.versionNumber, this.sequenceNumber, NaiadSerializationConstants.CHANNEL_ID, NaiadSerializationConstants.DEST_VERTEX_ID, SerializedMessageType.Data), this.headerSerializer);
                }
            }
        }

        public void FlushCurrentPage()
        {
            this.currentPage.FinalizeLastMessage(this.headerSerializer);
            using (BufferSegment segment = this.currentPage.Consume())
            {
                ArraySegment<byte> arraySegment = segment.ToArraySegment();
                this.stream.Write(arraySegment.Array, arraySegment.Offset, arraySegment.Count);
            }
            this.currentPage.Release();
            this.currentPage = null;
        }
        
        public void Dispose()
        {
            if (this.currentPage != null)
            {
                this.FlushCurrentPage();
                this.currentPage = null;
            }
            this.writtenEvent.WaitOne();
            this.stream.Dispose();
        }
    }

#if false
    public interface SerializedMessageDecoder<T>
    {
        IEnumerable<T> Elements(SerializedMessage message);
    }
#endif

    internal class CompletedMessageArgs : EventArgs
    {
        public readonly MessageHeader Hdr;
        public readonly BufferSegment Segment;
        public CompletedMessageArgs(MessageHeader hdr, BufferSegment segment)
        {
            this.Hdr = hdr;
            this.Segment = segment;
        }
    }

    internal interface SerializedMessageEncoder<T>
    {
        void Write(T element);
        void Flush();
        event EventHandler<CompletedMessageArgs> CompletedMessage;
    }
  
    internal class AutoSerializedMessageEncoder<S, T> : SerializedMessageEncoder<Pair<S, T>>
        where T : Time<T>
    {
        private readonly int destVertexId;
        private readonly int destMailboxId;
        private readonly SerializedMessageType messageType;
        
        private NaiadSerialization<S> payloadSerializer;
        private NaiadSerialization<T> timeSerializer;
        private NaiadSerialization<MessageHeader> headerSerializer;
        private NaiadSerialization<int> intSerializer;
        private SendBufferPage page;

        private BufferPool<byte> pool;

        private readonly Func<int> sequenceNumberGenerator;
        private int currentSequenceNumber;
        public int CurrentSequenceNumber { get { return currentSequenceNumber; } }

        private readonly int pageSize;

        public event EventHandler<CompletedMessageArgs> CompletedMessage;

        private readonly SerializationCodeGenerator CodeGenerator;

        public int RecordSizeHint
        {
            get
            {
                return this.pageSize - (MessageHeader.SizeOf + sizeof(int) + default(T).Coordinates() * sizeof(int) + sizeof(int));
            }
        }

        public void Flush()
        {
            this.SendCurrentPage();
        }

        private void SendCurrentPage()
        {
            if (this.page != null)
            {
                this.page.WriteReserved<int>(this.intSerializer, this.currentMessageWritten);
                MessageHeader hdr = page.FinalizeLastMessage(this.headerSerializer);
                BufferSegment segment = page.Consume();

                if (segment.Length > 0 && this.currentMessageWritten > 0)
                {
                    this.CompletedMessage(this, new CompletedMessageArgs(hdr, segment));
                }
                else
                {
                    segment.Dispose();
                }
                this.page.Release();
                this.page = null;
            }            
        }

        // This overload allows us to create a message header that holds the source vertex id.
        // Required for tracing.
        private void CreateNextPage(int srcVertexId, int size)
        {
            Debug.Assert(this.page == null);
            this.page = new SendBufferPage(size == this.pageSize ? this.pool : DummyBufferPool<byte>.Pool, size);
            //Console.Error.WriteLine("Next page mode (for mailbox {1}:{2}) is {0}", this.currentMode, this.destMailboxId, this.destVertexId);
            this.currentSequenceNumber = this.sequenceNumberGenerator();
            if (this.payloadSerializer == null)
                this.payloadSerializer = this.CodeGenerator.GetSerializer<S>();
            if (this.timeSerializer == null)
                this.timeSerializer = this.CodeGenerator.GetSerializer<T>();
            if (this.headerSerializer == null)
                this.headerSerializer = this.CodeGenerator.GetSerializer<MessageHeader>();
            if (this.intSerializer == null)
                this.intSerializer = this.CodeGenerator.GetSerializer<int>();

            this.page.WriteHeader(new MessageHeader(srcVertexId,
                this.currentSequenceNumber, this.destMailboxId, this.destVertexId, this.messageType), this.headerSerializer);

#if MESSAGE_HEADER_SENTINEL
            this.page.Write(PrimitiveSerializers.Int32, -1);
            this.page.Write(PrimitiveSerializers.Int32, -1);
            this.page.Write(PrimitiveSerializers.Int32, -1);
#endif

            bool timeSuccess = this.page.Write(this.timeSerializer, this.currentMessageTime);
            Debug.Assert(timeSuccess);
            this.page.ReserveBytes(sizeof(int));

            //this.currentMessageTimeSet = false;
            this.currentMessageWritten = 0;
        }

        public void Write(ArraySegment<S> records, int srcVertexId)
        {
            int numToWrite = records.Count;
            do
            {
                int numWritten = this.WriteElements(records, srcVertexId);
                numToWrite -= numWritten;
                records = new ArraySegment<S>(records.Array, records.Offset + numWritten, records.Count - numWritten); 
            } 
            while (numToWrite > 0);
        }

        public int WriteElements(ArraySegment<S> records, int srcVertexId)
        {
            if (this.page == null)
            {
                this.CreateNextPage(srcVertexId, this.pageSize);
            }

            int numWritten = this.page.WriteElements(this.payloadSerializer, records);
            
            this.currentMessageWritten += numWritten;

            if (numWritten > 0 && numWritten < records.Count)
            {
                // We didn't manage to send all elements, so the page must be full.
                this.SendCurrentPage();
            }
            else if (numWritten == 0)
            {
                // We didn't manage to send any elements.
                
                // The first possibility is that the page was full, so allocate a new one.
                this.SendCurrentPage();
                int size = this.pageSize;
                this.CreateNextPage(srcVertexId, size);
                numWritten = this.page.WriteElements(this.payloadSerializer, records);

                // If writing to an empty page failed, repeatedly double the size of the page until at least
                // one record fits.
                while (numWritten == 0)
                {
                    this.page.Release();
                    this.page = null;
                    size <<= 1;
                    Logging.Info("Doubling send buffer size due to long record (new size = {0}) in channel {0}", size, this.ToString());
                    this.CreateNextPage(srcVertexId, size);
                    numWritten = this.page.WriteElements(this.payloadSerializer, records);
                }

                this.currentMessageWritten += numWritten;
            }

            return numWritten;
        }

        public void Write(S record, int srcVertexId)
        {
            if (this.page == null)
            {
                this.CreateNextPage(srcVertexId, this.pageSize);
            }

            if (!this.WriteElement(record))
            {
                this.SendCurrentPage();

                // Allocate new page
                int size = this.pageSize;
                this.CreateNextPage(srcVertexId, size);
                while (!this.WriteElement(record))
                {
                    this.page.Release();
                    this.page = null;
                    size <<= 1;
                    Logging.Info("Doubling send buffer size due to long record (new size = {0}) in channel {0}", size, this.ToString());
                    this.CreateNextPage(srcVertexId, size);
                }
            }
        }

        // This overload allows us to create a message header that holds the source vertex id.
        // Required for tracing.
        public void Write(Pair<S, T> element, int srcVertexId)
        {
            this.SetCurrentTime(element.v2);
            this.Write(element.v1, srcVertexId);
        }

        private bool currentMessageTimeSet;
        private T currentMessageTime;
        private int currentMessageWritten;

        public void SetCurrentTime(T time)
        {
            if (this.currentMessageTimeSet && !this.currentMessageTime.Equals(time))
            {
                this.SendCurrentPage();
            }

            if (!(this.currentMessageTimeSet && this.currentMessageTime.Equals(time)))
            {
                this.currentMessageTime = time;
                this.currentMessageTimeSet = true;
            }
        }

        private bool WriteElement(S record)
        {
            bool success = this.page.Write(this.payloadSerializer, record);

            if (success)
                this.currentMessageWritten++;

            return success;
        }

        public void Write(Pair<S, T> element) 
        {
            this.Write(element, 0);
        }

        public AutoSerializedMessageEncoder(int destVertexId, int destMailboxId, BufferPool<byte> pool, int pageSize, SerializationCodeGenerator codeGenerator, SerializedMessageType messageType = SerializedMessageType.Data, Func<int> seqNumGen = null)
        {
            this.destVertexId = destVertexId;
            this.destMailboxId = destMailboxId;
            this.messageType = messageType;

            if (pool == null)
                this.pool = DummyBufferPool<byte>.Pool;
            else
               this.pool = pool;


            this.page = null;

            this.pageSize = pageSize;

            // If not given a sequence number generator function, always use 0
            this.currentSequenceNumber = 0;
            this.sequenceNumberGenerator = seqNumGen != null ? seqNumGen : () => 0;

            this.CodeGenerator = codeGenerator;
        }
    }

#if false
    internal class NewMessageCodec<S, T> : SerializedMessageEncoder<Pair<S, T>>, SerializedMessageDecoder<Message<S, T>>
        where T : Time<T>
    {
        private NaiadSerialization<S> payloadSerializer;
        private NaiadSerialization<T> timeSerializer;

        private bool isCurrentTimeValid;
        private T currentTime;

        private SendBufferPage page;
        private int currentSequenceNumber;
        private BufferPool<byte> pool;
        private int pageSize;

        private readonly Func<int> sequenceNumberGenerator;

        public void Write(Pair<S, T> element, int srcVertexId)
        {
            // flush if we have a new time
            if (isCurrentTimeValid && !element.v2.Equals(currentTime))
            {
                this.Flush();
                this.currentTime = element.v2;

                if (this.page == null)
                {
                    this.CreateNextPage(srcVertexId, this.pageSize);
                }

                var success = this.page.Write(timeSerializer, element.v2);

                Debug.Assert(success);

                {
                    this.SendCurrentPage();

                    // Allocate new page
                    int size = this.pageSize;
                    this.CreateNextPage(srcVertexId, size);
                    while (!this.currentSubEncoder.Write(element))
                    {
                        this.page.Release();
                        this.page = null;
                        size <<= 1;
                        Logging.Info("Doubling send buffer size due to long record (new size = {0}) in channel {0}", size, this.ToString());
                        this.CreateNextPage(srcVertexId, size);
                    }
                }
                timeSerializer.Serialize();
            }

            // set if this is the first time
            if (!isCurrentTimeValid)
            {
                this.currentTime = element.v2;
                this.isCurrentTimeValid = true;            
            }




            throw new NotImplementedException();
        }

        private void CreateNextPage(int srcVertexId, int size)
        {
            Debug.Assert(this.page == null);
            this.page = new SendBufferPage(size == this.pageSize ? this.pool : DummyBufferPool<byte>.Pool, size);
            
            this.currentSequenceNumber = this.sequenceNumberGenerator();
            
            if (this.payloadSerializer == null)
                this.payloadSerializer = AutoSerialization.GetSerializer<S>();
            if (this.timeSerializer == null)
                this.timeSerializer = AutoSerialization.GetSerializer<T>();
        }

        public void Flush()
        {
            throw new NotImplementedException();
        }

        public event EventHandler<CompletedMessageArgs> CompletedMessage;

        public IEnumerable<Message<S, T>> Elements(SerializedMessage message)
        {
            throw new NotImplementedException();
        }
    }
#endif

    internal class AutoSerializedMessageDecoder<S, T> // : SerializedMessageDecoder<Pair<S, T>>
        where T : Time<T>
    {
        private NaiadSerialization<S> payloadDeserializer;
        private NaiadSerialization<T> timeDeserializer;
        private NaiadSerialization<Int32> intDeserializer;

        private readonly SerializationCodeGenerator CodeGenerator;

#if false
        private IEnumerable<Pair<S, T>> Elements(SerializedMessage message)
        {
            RecvBuffer messageBody = message.Body;
            if (this.payloadDeserializer == null)
                this.payloadDeserializer = this.CodeGenerator.GetSerializer<S>();
            if (this.timeDeserializer == null)
                this.timeDeserializer = this.CodeGenerator.GetSerializer<T>();
            if (this.intDeserializer == null)
                this.intDeserializer = this.CodeGenerator.GetSerializer<Int32>();

#if MESSAGE_HEADER_SENTINEL
            int dummy;
            PrimitiveSerializers.Int32.TryDeserialize(ref messageBody, out dummy);
            Debug.Assert(dummy == -1);
            PrimitiveSerializers.Int32.TryDeserialize(ref messageBody, out dummy);
            Debug.Assert(dummy == -1); 
            PrimitiveSerializers.Int32.TryDeserialize(ref messageBody, out dummy);
            Debug.Assert(dummy == -1);
#endif

            T time;
            bool timeSuccess = timeDeserializer.TryDeserialize(ref messageBody, out time);
            Debug.Assert(timeSuccess);

            int count;
            bool countSuccess = intDeserializer.TryDeserialize(ref messageBody, out count);
            Debug.Assert(countSuccess);

            S payload;
            while (payloadDeserializer.TryDeserialize(ref messageBody, out payload))
                yield return new Pair<S, T>(payload, time);
        }
#endif

        public IEnumerable<Message<S, T>> AsTypedMessages(SerializedMessage message)
        {
            RecvBuffer messageBody = message.Body;
            if (this.payloadDeserializer == null)
                this.payloadDeserializer = this.CodeGenerator.GetSerializer<S>();
            if (this.timeDeserializer == null)
                this.timeDeserializer = this.CodeGenerator.GetSerializer<T>();
            if (this.intDeserializer == null)
                this.intDeserializer = this.CodeGenerator.GetSerializer<Int32>();

#if MESSAGE_HEADER_SENTINEL
            int dummy;
            PrimitiveSerializers.Int32.TryDeserialize(ref messageBody, out dummy);
            Debug.Assert(dummy == -1);
            PrimitiveSerializers.Int32.TryDeserialize(ref messageBody, out dummy);
            Debug.Assert(dummy == -1); 
            PrimitiveSerializers.Int32.TryDeserialize(ref messageBody, out dummy);
            Debug.Assert(dummy == -1);
#endif

            T time;
            bool timeSuccess = timeDeserializer.TryDeserialize(ref messageBody, out time);
            Debug.Assert(timeSuccess);

            int count;
            bool countSuccess = intDeserializer.TryDeserialize(ref messageBody, out count);
            Debug.Assert(countSuccess);

            var targetMessage = new Message<S, T>(time);
            targetMessage.Allocate();

            S payload;
            while (payloadDeserializer.TryDeserialize(ref messageBody, out payload))
            {
                targetMessage.payload[targetMessage.length++] = payload;

                if (targetMessage.length == targetMessage.payload.Length)
                {
                    yield return targetMessage;

                    targetMessage = new Message<S, T>(time);
                    targetMessage.Allocate();
                }
            }

            // if data remain, transfer message and ownership. otherwise, release the buffer.
            if (targetMessage.length > 0)
            {
                yield return targetMessage;
            }
            else
            {
                targetMessage.Release();
            }

        }

        public Pair<T, int> Time(SerializedMessage message)
        {
            RecvBuffer messageBody = message.Body;
            if (this.payloadDeserializer == null)
                this.payloadDeserializer = this.CodeGenerator.GetSerializer<S>();
            if (this.timeDeserializer == null)
                this.timeDeserializer = this.CodeGenerator.GetSerializer<T>();
            if (this.intDeserializer == null)
                this.intDeserializer = this.CodeGenerator.GetSerializer<Int32>();
#if MESSAGE_HEADER_SENTINEL
            int dummy;
            PrimitiveSerializers.Int32.TryDeserialize(ref messageBody, out dummy);
            Debug.Assert(dummy == -1);
            PrimitiveSerializers.Int32.TryDeserialize(ref messageBody, out dummy);
            Debug.Assert(dummy == -1);
            PrimitiveSerializers.Int32.TryDeserialize(ref messageBody, out dummy);
            Debug.Assert(dummy == -1);
#endif

            T time;
            bool timeSuccess = timeDeserializer.TryDeserialize(ref messageBody, out time);
            Debug.Assert(timeSuccess);

            int count;
            bool countSuccess = intDeserializer.TryDeserialize(ref messageBody, out count);
            Debug.Assert(countSuccess);

            return new Pair<T, int>(time, count);
        }

        public AutoSerializedMessageDecoder(SerializationCodeGenerator codeGenerator)
        {
            this.CodeGenerator = codeGenerator;
        }
    }
}
