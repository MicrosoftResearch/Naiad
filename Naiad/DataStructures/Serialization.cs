/*
 * Naiad ver. 0.5
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

#define VAR_LENGTH_INT
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace Microsoft.Research.Naiad.Serialization
{
    internal static class NativeSASerializers {
        
        public static unsafe NativeSA Serialize(NativeSA target, byte[] array)
        {
            int arrayLength = array.Length;
            if (target.EnsureAvailable(arrayLength * sizeof(byte) + sizeof(int))) {
                target = Serialize(target, arrayLength);



                byte* dest = (byte *)(target.ArrayBegin + target.CurPos);
                for (int i = 0; i < arrayLength; i++) {
                    *dest++ = array[i];
                }
                target.CurPos += arrayLength;

            }
            return target;
        }

        public static unsafe NativeSA Serialize(NativeSA target, float[] array)
        {
            int arrayLength = array.Length;
            if (target.EnsureAvailable(arrayLength * sizeof(float) + sizeof(int))) {
                target = Serialize(target, arrayLength);

                float* dest = (float*)(target.ArrayBegin + target.CurPos);
                for (int i = 0; i < arrayLength; i++) {
                    *dest++ = array[i];
                }
                target.CurPos += arrayLength * sizeof(float);
            }
            return target;
        }

        public static unsafe NativeSA Serialize(NativeSA target, int[] array)
        {
            int arrayLength = array.Length;
            if (target.EnsureAvailable(arrayLength * sizeof(int) + sizeof(int))) {
                target = Serialize(target, arrayLength);

                int* dest = (int*)(target.ArrayBegin + target.CurPos);
                for (int i = 0; i < arrayLength; i++) {
                    *dest++ = array[i];
                }
                
                target.CurPos += arrayLength * sizeof(int);
            }
            return target;
        }

        

        public static unsafe NativeSA Serialize(NativeSA target, string source)
        {
            int stringLength = source.Length;
            if (target.EnsureAvailable(stringLength * sizeof(char) + sizeof(int))) {
                target = Serialize(target, stringLength);
                if (stringLength > 0) {
                    char* targetCharPtr = (char*)(target.ArrayBegin + target.CurPos);
                    fixed (char* strPtr = source) {
                        char* sourceCharPtr = strPtr;
                        for (int i = 0; i < stringLength; ++i) {
                            *targetCharPtr++ = *sourceCharPtr++;
                        }
                    }
                    target.CurPos += stringLength * sizeof(char);
                }
            }
            return target;
        }

        public static unsafe NativeSA Serialize(NativeSA target, long source)
        {
            if (target.EnsureAvailable(sizeof(long))) {
                long* dest = (long *)(target.ArrayBegin + target.CurPos);

                *dest = source;
                target.CurPos += sizeof(long);
            }
            return target;
        }

        public static unsafe NativeSA Serialize(NativeSA target, ulong source)
        {
            if (target.EnsureAvailable(sizeof(ulong))) {
                ulong* dest = (ulong*)(target.ArrayBegin + target.CurPos);

                *dest = source;
                target.CurPos += sizeof(ulong);
            }
            return target; 
        }

        public static unsafe NativeSA Serialize(NativeSA target, short source)
        {
            if (target.EnsureAvailable(sizeof(short))) {
                short* dest = (short*)(target.ArrayBegin + target.CurPos);

                *dest = source;
                target.CurPos += sizeof(short);
            }
            return target;            
        }

        public static unsafe NativeSA Serialize(NativeSA target, int source)
        {
            if (target.EnsureAvailable(sizeof(int))) {
                int* dest = (int*)(target.ArrayBegin + target.CurPos);

                *dest = source;           
                target.CurPos += sizeof(int);                   
            }
            return target;
        }

        public static unsafe NativeSA Serialize(NativeSA target, uint source)
        {
            if (target.EnsureAvailable(sizeof(uint))) {
                uint *dest = (uint *)(target.ArrayBegin + target.CurPos);           
             
                *dest = source;
                target.CurPos += sizeof(uint);
            }
            return target;
        }

        public static unsafe NativeSA Serialize(NativeSA target, bool source)
        {
            return Serialize(target, (int)(source ? 1 : 0));
        }

        public static unsafe NativeSA Serialize(NativeSA target, byte source)
        {
            if (target.EnsureAvailable(sizeof(byte))) {
                byte* dest = (byte*)(target.ArrayBegin + target.CurPos); 
                *dest = source;

                target.CurPos += sizeof(byte);
            }
            return target;
        }

        public static unsafe NativeSA Serialize(NativeSA target, double source)
        {
            if (target.EnsureAvailable(sizeof(double))) {
                double* dest = (double*)(target.ArrayBegin + target.CurPos); 
                *dest = source;
 
                target.CurPos += sizeof(double);
            }
            return target;
        }

        public static unsafe NativeSA Serialize(NativeSA target, float source)
        {
            if (target.EnsureAvailable(sizeof(float))) {
                float* dest = (float*)(target.ArrayBegin + target.CurPos);
                *dest = source;

                target.CurPos += sizeof(float);
            }
            return target;
        }
    }

    /// <summary>
    /// Serializer implementations for primitive types.
    /// </summary>
    public static class Serializers
    {
        /// <summary>
        /// Attempts to serialize the given array to the given target buffer.
        /// </summary>
        /// <param name="target">The target buffer.</param>
        /// <param name="array">The array to be serialized.</param>
        /// <returns>The original target buffer if serialization failed, otherwise the updated target buffer.</returns>
        public static unsafe SubArray<byte> Serialize(this SubArray<byte> target, byte[] array)
        {
            int arrayLength = array.Length;
            if (target.EnsureAvailable(arrayLength * sizeof(byte) + sizeof(int)))
            {
                target = target.Serialize(arrayLength);

#if false
                fixed (byte* dest = &target.Array[target.Count])
                {
                    byte* ptr = dest;
                    for (int i = 0; i < array.Length; ++i)
                    {
                        *ptr++ = array[i];
                    }
                }
#else
                Buffer.BlockCopy(array, 0, target.Array, target.Count, arrayLength * sizeof(byte));
#endif
                target.Count += arrayLength;
            }
            return target;
        }

        /// <summary>
        /// Attempts to serialize the given array to the given target buffer.
        /// </summary>
        /// <param name="target">The target buffer.</param>
        /// <param name="array">The array to be serialized.</param>
        /// <returns>The original target buffer if serialization failed, otherwise the updated target buffer.</returns>
        public static unsafe SubArray<byte> Serialize(this SubArray<byte> target, float[] array)
        {
            int arrayLength = array.Length;
            if (target.EnsureAvailable(arrayLength * sizeof(float) + sizeof(int)))
            {
                target = target.Serialize(array.Length);

#if false
                fixed (byte* dest = &target.Array[target.Count])
                {
                    float* ptr = (float*)dest;
                    for (int i = 0; i < array.Length; ++i)
                    {
                        *ptr++ = array[i];
                    }
                }
#else
                Buffer.BlockCopy(array, 0, target.Array, target.Count, arrayLength * sizeof(float));
#endif

                target.Count += array.Length * sizeof(float);
            }
            return target;
        }

        /// <summary>
        /// Attempts to serialize the given array to the given target buffer.
        /// </summary>
        /// <param name="target">The target buffer.</param>
        /// <param name="array">The array to be serialized.</param>
        /// <returns>The original target buffer if serialization failed, otherwise the updated target buffer.</returns>
        public static unsafe SubArray<byte> Serialize(this SubArray<byte> target, int[] array)
        {
            if (target.EnsureAvailable(array.Length * sizeof(int) + sizeof(int))) {
                target = target.Serialize(array.Length);

#if false
                fixed (byte* dest = &target.Array[target.Count])
                {
                    int* ptr = (int*)dest;
                    for (int i = 0; i < array.Length; ++i)
                    {
                        *ptr++ = array[i];
                    }
                }
#else
                Buffer.BlockCopy(array, 0, target.Array, target.Count, array.Length * sizeof(int));

#endif
                target.Count += array.Length * sizeof(int);
            }
            return target;
        }

        /// <summary>
        /// Attempts to serialize the given string to the given target buffer.
        /// </summary>
        /// <param name="target">The target buffer.</param>
        /// <param name="source">The string to be serialized.</param>
        /// <returns>The original target buffer if serialization failed, otherwise the updated target buffer.</returns>
        public static unsafe SubArray<byte> Serialize(this SubArray<byte> target, string source)
        {
            int stringLength = source.Length;
            if (target.EnsureAvailable(stringLength * sizeof(char) + sizeof(int)))
            {
                target = target.Serialize(stringLength);
                if (stringLength > 0)
                {
                    fixed (byte* ptr = &target.Array[target.Count])
                    {
                        fixed (char* strPtr = source)
                        {
                            char* sourceCharPtr = strPtr;
                            char* targetCharPtr = (char*)ptr;
                            for (int i = 0; i < stringLength; ++i)
                            {
                                *targetCharPtr++ = *sourceCharPtr++;
                            }
                        }
                    }
                    target.Count += stringLength * sizeof(char);
                }
            }
            return target;
        }

        /// <summary>
        /// Attempts to serialize the given value to the given target buffer.
        /// </summary>
        /// <param name="target">The target buffer.</param>
        /// <param name="source">The value to be serialized.</param>
        /// <returns>The original target buffer if serialization failed, otherwise the updated target buffer.</returns>
        public static unsafe SubArray<byte> Serialize(this SubArray<byte> target, long source)
        {
            if (target.EnsureAvailable(sizeof(long)))
            {
                fixed (byte* ptr = &target.Array[target.Count])
                {
                    *(long*)ptr = source;
                }
                target.Count += sizeof(long);
            }

            return target;
        }

        /// <summary>
        /// Attempts to serialize the given value to the given target buffer.
        /// </summary>
        /// <param name="target">The target buffer.</param>
        /// <param name="source">The value to be serialized.</param>
        /// <returns>The original target buffer if serialization failed, otherwise the updated target buffer.</returns>
        public static unsafe SubArray<byte> Serialize(this SubArray<byte> target, ulong source)
        {
            if (target.EnsureAvailable(4))
            {
                fixed (byte* ptr = &target.Array[target.Count])
                {
                    *(ulong*)ptr = source;
                }
                target.Count += 4;
            }

            return target;
        }

        /// <summary>
        /// Attempts to serialize the given value to the given target buffer.
        /// </summary>
        /// <param name="target">The target buffer.</param>
        /// <param name="source">The value to be serialized.</param>
        /// <returns>The original target buffer if serialization failed, otherwise the updated target buffer.</returns>
        public static unsafe SubArray<byte> Serialize(this SubArray<byte> target, short source)
        {
            if (target.EnsureAvailable(sizeof(short)))
            {
                fixed (byte* ptr = &target.Array[target.Count])
                {
                    *(long*)ptr = source;
                }
                target.Count += sizeof(short);
            }

            return target;
        }

        /// <summary>
        /// Attempts to serialize the given value to the given target buffer.
        /// </summary>
        /// <param name="target">The target buffer.</param>
        /// <param name="source">The value to be serialized.</param>
        /// <returns>The original target buffer if serialization failed, otherwise the updated target buffer.</returns>
        public static unsafe SubArray<byte> Serialize(this SubArray<byte> target, int source)
        {


            if (target.EnsureAvailable(4))
            {
                fixed (byte* ptr = &target.Array[target.Count])
                {
                    *(int*)ptr = source;
                }
                target.Count += 4;
                /*
                target.Array[target.Count++] = (byte)((source >> 0) % 256);
                target.Array[target.Count++] = (byte)((source >> 8) % 256);
                target.Array[target.Count++] = (byte)((source >> 16) % 256);
                target.Array[target.Count++] = (byte)((source >> 24) % 256);*/
            }

            return target;
        }

        /// <summary>
        /// Attempts to serialize the given value to the given target buffer.
        /// </summary>
        /// <param name="target">The target buffer.</param>
        /// <param name="source">The value to be serialized.</param>
        /// <returns>The original target buffer if serialization failed, otherwise the updated target buffer.</returns>
        public static unsafe SubArray<byte> Serialize(this SubArray<byte> target, uint source)
        {


            if (target.EnsureAvailable(4))
            {
                fixed (byte* ptr = &target.Array[target.Count])
                {
                    *(uint*)ptr = source;
                }
                target.Count += 4;
            }

            return target;
        }

        /// <summary>
        /// Attempts to serialize the given value to the given target buffer.
        /// </summary>
        /// <param name="target">The target buffer.</param>
        /// <param name="source">The value to be serialized.</param>
        /// <returns>The original target buffer if serialization failed, otherwise the updated target buffer.</returns>
        public static unsafe SubArray<byte> Serialize(this SubArray<byte> target, bool source)
        {
            return Serialize(target, (int)(source ? 1 : 0));
        }

        /// <summary>
        /// Attempts to serialize the given value to the given target buffer.
        /// </summary>
        /// <param name="target">The target buffer.</param>
        /// <param name="source">The value to be serialized.</param>
        /// <returns>The original target buffer if serialization failed, otherwise the updated target buffer.</returns>
        public static SubArray<byte> Serialize(this SubArray<byte> target, byte source)
        {
            if (target.EnsureAvailable(1))
            {
                target.Array[target.Count++] = source;
            }
            return target;
        }

        /// <summary>
        /// Attempts to serialize the given value to the given target buffer.
        /// </summary>
        /// <param name="target">The target buffer.</param>
        /// <param name="source">The value to be serialized.</param>
        /// <returns>The original target buffer if serialization failed, otherwise the updated target buffer.</returns>
        public static unsafe SubArray<byte> Serialize(this SubArray<byte> target, double source)
        {
            if (target.EnsureAvailable(8))
            {
                fixed (byte* ptr = &target.Array[target.Count])
                {
                    *(double*)ptr = source;
                }
                target.Count += 8;
                /*byte[] bytes = BitConverter.GetBytes(source);
                for (int i = 0; i < bytes.Length; ++i)
                    target.Array[target.Count++] = bytes[i];*/
            }
            return target;
        }

        /// <summary>
        /// Attempts to serialize the given value to the given target buffer.
        /// </summary>
        /// <param name="target">The target buffer.</param>
        /// <param name="source">The value to be serialized.</param>
        /// <returns>The original target buffer if serialization failed, otherwise the updated target buffer.</returns>
        public static unsafe SubArray<byte> Serialize(this SubArray<byte> target, float source)
        {
            if (target.EnsureAvailable(sizeof(float)))
            {
                fixed (byte* ptr = &target.Array[target.Count])
                {
                    *(float*)ptr = source;
                }
                target.Count += sizeof(float);
                /*byte[] bytes = BitConverter.GetBytes(source);
                for (int i = 0; i < bytes.Length; ++i)
                    target.Array[target.Count++] = bytes[i];*/
            }
            return target;
        }
    }

    /// <summary>
    /// Deserializer implementations for primitive types.
    /// </summary>
    public static class Deserializers
    {
#if false
        public static bool TryDeserialize<K, V>(ref RecvBuffer source, out Dictionary<K, V> dictionary)
        {
            var thingy = default(Pair<K, V>[]);

            var serializer = CodeGeneration.AutoSerialization.GetSerializer<Pair<K, V>[]>();

            var result = serializer.TryDeserialize(ref source, out thingy);

            dictionary = thingy.ToDictionary(x => x.v1, x => x.v2);

            return result;
        }
#endif

        /// <summary>
        /// Attempts to deserialize a value from the given source buffer.
        /// </summary>
        /// <param name="source">The target buffer. In the event of success, the current position of the buffer will be advanced.</param>
        /// <param name="value">The deserialized value, if this method returns true.</param>
        /// <returns>True if deserialization succeeded, otherwise false.</returns>
        public unsafe static bool TryDeserialize(ref RecvBuffer source, out bool value)
        {
            value = false;

            int b;
            if (TryDeserialize(ref source, out b))
            {
                value = (b == 1);
                return true;
            }

            return false;
        }

        /// <summary>
        /// Attempts to deserialize a value from the given source buffer.
        /// </summary>
        /// <param name="source">The target buffer. In the event of success, the current position of the buffer will be advanced.</param>
        /// <param name="value">The deserialized value, if this method returns true.</param>
        /// <returns>True if deserialization succeeded, otherwise false.</returns>
        public unsafe static bool TryDeserialize(ref RecvBuffer source, out byte value)
        {
            if (source.End - source.CurrentPos < sizeof(byte))
            {
                value = default(byte);
                return false;
            }

            fixed (byte* ptr = &source.Buffer[source.CurrentPos])
            {
                value = *ptr;
            }

            source.CurrentPos += sizeof(byte);
            return true;
        }

        /// <summary>
        /// Attempts to deserialize a value from the given source buffer.
        /// </summary>
        /// <param name="source">The target buffer. In the event of success, the current position of the buffer will be advanced.</param>
        /// <param name="value">The deserialized value, if this method returns true.</param>
        /// <returns>True if deserialization succeeded, otherwise false.</returns>
        public unsafe static bool TryDeserialize(ref RecvBuffer source, out char value)
        {
            if (source.End - source.CurrentPos < sizeof(char))
            {
                value = default(char);
                return false;
            }

            fixed (byte* ptr = &source.Buffer[source.CurrentPos])
            {
                value = *(char*)ptr;
            }

            source.CurrentPos += sizeof(char);
            return true;
        }

        /// <summary>
        /// Attempts to deserialize a value from the given source buffer.
        /// </summary>
        /// <param name="source">The target buffer. In the event of success, the current position of the buffer will be advanced.</param>
        /// <param name="value">The deserialized value, if this method returns true.</param>
        /// <returns>True if deserialization succeeded, otherwise false.</returns>
        public unsafe static bool TryDeserialize(ref RecvBuffer source, out short value)
        {
            if (source.End - source.CurrentPos < sizeof(short))
            {
                value = default(short);
                return false;
            }

            fixed (byte* ptr = &source.Buffer[source.CurrentPos])
            {
                value = *(short*)ptr;
            }

            source.CurrentPos += sizeof(short);
            return true;
        }

        /// <summary>
        /// Attempts to deserialize a value from the given source buffer.
        /// </summary>
        /// <param name="source">The target buffer. In the event of success, the current position of the buffer will be advanced.</param>
        /// <param name="value">The deserialized value, if this method returns true.</param>
        /// <returns>True if deserialization succeeded, otherwise false.</returns>
        public unsafe static bool TryDeserialize(ref RecvBuffer source, out int value)
        {
            if (source.End - source.CurrentPos < sizeof(int))
            {
                value = default(int);
                return false;
            }

            fixed (byte* ptr = &source.Buffer[source.CurrentPos])
            {
                value = *(int*)ptr;
            }

            source.CurrentPos += sizeof(int);
            return true;
        }

        /// <summary>
        /// Attempts to deserialize a value from the given source buffer.
        /// </summary>
        /// <param name="source">The target buffer. In the event of success, the current position of the buffer will be advanced.</param>
        /// <param name="value">The deserialized value, if this method returns true.</param>
        /// <returns>True if deserialization succeeded, otherwise false.</returns>
        public unsafe static bool TryDeserialize(ref RecvBuffer source, out uint value)
        {
            if (source.End - source.CurrentPos < sizeof(uint))
            {
                value = default(uint);
                return false;
            }

            fixed (byte* ptr = &source.Buffer[source.CurrentPos])
            {
                value = *(uint*)ptr;
            }

            source.CurrentPos += sizeof(uint);
            return true;
        }

        /// <summary>
        /// Attempts to deserialize a value from the given source buffer.
        /// </summary>
        /// <param name="source">The target buffer. In the event of success, the current position of the buffer will be advanced.</param>
        /// <param name="value">The deserialized value, if this method returns true.</param>
        /// <returns>True if deserialization succeeded, otherwise false.</returns>
        public unsafe static bool TryDeserialize(ref RecvBuffer source, out long value)
        {
            if (source.End - source.CurrentPos < sizeof(long))
            {
                value = default(long);
                return false;
            }

            fixed (byte* ptr = &source.Buffer[source.CurrentPos])
            {
                value = *(long*)ptr;
            }

            source.CurrentPos += sizeof(long);
            return true;
        }

        /// <summary>
        /// Attempts to deserialize a value from the given source buffer.
        /// </summary>
        /// <param name="source">The target buffer. In the event of success, the current position of the buffer will be advanced.</param>
        /// <param name="value">The deserialized value, if this method returns true.</param>
        /// <returns>True if deserialization succeeded, otherwise false.</returns>
        public unsafe static bool TryDeserialize(ref RecvBuffer source, out ulong value)
        {
            if (source.End - source.CurrentPos < sizeof(ulong))
            {
                value = default(ulong);
                return false;
            }

            fixed (byte* ptr = &source.Buffer[source.CurrentPos])
            {
                value = *(ulong*)ptr;
            }

            source.CurrentPos += sizeof(ulong);
            return true;
        }

        /// <summary>
        /// Attempts to deserialize a value from the given source buffer.
        /// </summary>
        /// <param name="source">The target buffer. In the event of success, the current position of the buffer will be advanced.</param>
        /// <param name="value">The deserialized value, if this method returns true.</param>
        /// <returns>True if deserialization succeeded, otherwise false.</returns>
        public unsafe static bool TryDeserialize(ref RecvBuffer source, out float value)
        {
            if (source.End - source.CurrentPos < sizeof(float))
            {
                value = default(float);
                return false;
            }

            fixed (byte* ptr = &source.Buffer[source.CurrentPos])
            {
                value = *(float*)ptr;
            }

            source.CurrentPos += sizeof(float);
            return true;
        }

        /// <summary>
        /// Attempts to deserialize a value from the given source buffer.
        /// </summary>
        /// <param name="source">The target buffer. In the event of success, the current position of the buffer will be advanced.</param>
        /// <param name="value">The deserialized value, if this method returns true.</param>
        /// <returns>True if deserialization succeeded, otherwise false.</returns>
        public unsafe static bool TryDeserialize(ref RecvBuffer source, out double value)
        {
            if (source.End - source.CurrentPos < sizeof(double))
            {
                value = default(double);
                return false;
            }

            fixed (byte* ptr = &source.Buffer[source.CurrentPos])
            {
                value = *(double*)ptr;
            }

            source.CurrentPos += sizeof(double);
            return true;
        }

        /// <summary>
        /// Attempts to deserialize a string from the given source buffer.
        /// </summary>
        /// <param name="source">The target buffer. In the event of success, the current position of the buffer will be advanced.</param>
        /// <param name="value">The deserialized string, if this method returns true.</param>
        /// <returns>True if deserialization succeeded, otherwise false.</returns>
        public unsafe static bool TryDeserialize(ref RecvBuffer source, out string value)
        {
            int resetPosition = source.CurrentPos;

            int stringLength = -1;
            if (!TryDeserialize(ref source, out stringLength))
            {
                value = default(string);
                return false;
            }

            if (source.End - source.CurrentPos < sizeof(char) * stringLength)
            {
                source.CurrentPos = resetPosition;
                value = default(string);
                return false;
            }

            fixed (byte* bufPtr = source.Buffer)
            {
                char* stringStart = (char*)(bufPtr + source.CurrentPos);
                value = new string(stringStart, 0, stringLength);
            }

            source.CurrentPos += stringLength * sizeof(char);

            return true;
        }

        /// <summary>
        /// Attempts to deserialize an array from the given source buffer.
        /// </summary>
        /// <param name="source">The target buffer. In the event of success, the current position of the buffer will be advanced.</param>
        /// <param name="value">The deserialized array, if this method returns true.</param>
        /// <returns>True if deserialization succeeded, otherwise false.</returns>
        public unsafe static bool TryDeserialize(ref RecvBuffer source, out byte[] value)
        {
            int resetPosition = source.CurrentPos;

            int arrayLength = -1;
            if (!TryDeserialize(ref source, out arrayLength))
            {
                value = default(byte[]);
                return false;
            }

            if (source.End - source.CurrentPos < sizeof(byte) * arrayLength)
            {
                source.CurrentPos = resetPosition;
                value = default(byte[]);
                return false;
            }

            value = new byte[arrayLength];

#if false
            fixed (byte* bufPtr = source.Buffer)
            {
                byte* arrayStart = (byte*)(bufPtr + source.CurrentPos);
                for (int i = 0; i < arrayLength; ++i)
                    value[i] = arrayStart[i];
            }
#else
            Buffer.BlockCopy(source.Buffer, source.CurrentPos, value, 0, arrayLength * sizeof(byte));
#endif
            source.CurrentPos += arrayLength * sizeof(byte);

            return true;
        }

        /// <summary>
        /// Attempts to deserialize an array from the given source buffer.
        /// </summary>
        /// <param name="source">The target buffer. In the event of success, the current position of the buffer will be advanced.</param>
        /// <param name="value">The deserialized array, if this method returns true.</param>
        /// <returns>True if deserialization succeeded, otherwise false.</returns>
        public unsafe static bool TryDeserialize(ref RecvBuffer source, out int[] value)
        {
            int resetPosition = source.CurrentPos;

            int arrayLength = -1;
            if (!TryDeserialize(ref source, out arrayLength))
            {
                value = default(int[]);
                return false;
            }

            if (source.End - source.CurrentPos < sizeof(int) * arrayLength)
            {
                source.CurrentPos = resetPosition;
                value = default(int[]);
                return false;
            }

            value = new int[arrayLength];

#if false
            fixed (byte* bufPtr = source.Buffer)
            {
                int* arrayStart = (int*)(bufPtr + source.CurrentPos);
                for (int i = 0; i < arrayLength; ++i)
                    value[i] = arrayStart[i];
            }
#else
            Buffer.BlockCopy(source.Buffer, source.CurrentPos, value, 0, arrayLength * sizeof(int));
#endif
            source.CurrentPos += arrayLength * sizeof(int);

            return true;
        }

        /// <summary>
        /// Attempts to deserialize an array from the given source buffer.
        /// </summary>
        /// <param name="source">The target buffer. In the event of success, the current position of the buffer will be advanced.</param>
        /// <param name="value">The deserialized array, if this method returns true.</param>
        /// <returns>True if deserialization succeeded, otherwise false.</returns>
        public unsafe static bool TryDeserialize(ref RecvBuffer source, out float[] value)
        {
            int resetPosition = source.CurrentPos;

            int arrayLength = -1;
            if (!TryDeserialize(ref source, out arrayLength))
            {
                value = default(float[]);
                return false;
            }

            if (source.End - source.CurrentPos < sizeof(float) * arrayLength)
            {
                source.CurrentPos = resetPosition;
                value = default(float[]);
                return false;
            }

            value = new float[arrayLength];

#if false
            fixed (byte* bufPtr = source.Buffer)
            {
                float* arrayStart = (float*)(bufPtr + source.CurrentPos);
                for (int i = 0; i < arrayLength; ++i)
                    value[i] = arrayStart[i];
            }
#else
            Buffer.BlockCopy(source.Buffer, source.CurrentPos, value, 0, arrayLength * sizeof(float));
#endif
            source.CurrentPos += arrayLength * sizeof(float);

            return true;
        }
    }
}
