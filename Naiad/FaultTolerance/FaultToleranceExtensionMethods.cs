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
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Text;
using Microsoft.Research.Naiad.Dataflow.Channels;
using Microsoft.Research.Naiad.CodeGeneration;
using Microsoft.Research.Naiad.DataStructures;

namespace Microsoft.Research.Naiad.FaultTolerance
{
    public static class FaultToleranceExtensionMethods
    {
        
        /* Checkpoint format for NaiadList<S>:
         * int     Count
         * S*Count Array
         */
        public static void Checkpoint<S>(this NaiadList<S> list, NaiadWriter writer)
        //    where S : IEquatable<S>
        {
            writer.Write(list.Count);
            for (int i = 0; i < list.Count; ++i)
                writer.Write(list.Array[i]);
        }

        public static void Restore<S>(this NaiadList<S> list, NaiadReader reader)
        //    where S : IEquatable<S>
        {
            list.Clear();
            int count = reader.Read<int>();
            list.EnsureCapacity(count);
            list.Count = count;
            for (int i = 0; i < list.Count; ++i)
                list.Array[i] = reader.Read<S>();
        }

        /* Checkpoint format for List<S>:
         * int     Count
         * S*Count Array
         */
        public static void Checkpoint<S>(this List<S> list, NaiadWriter writer, NaiadSerialization<S> serializer, NaiadSerialization<int> intSerializer)
        //    where S : IEquatable<S>
        {
            writer.Write(list.Count, intSerializer);
            for (int i = 0; i < list.Count; ++i)
                writer.Write(list[i], serializer);
        }

        public static void Restore<S>(this List<S> list, NaiadReader reader, NaiadSerialization<S> serializer, NaiadSerialization<int> intSerializer)
        //    where S : IEquatable<S>
        {
            list.Clear();
            int count = reader.Read<int>(intSerializer);
            for (int i = 0; i < list.Count; ++i)
                list.Add(reader.Read<S>(serializer));
        }

        public static void Checkpoint<K, V>(this Dictionary<K, V> dictionary, NaiadWriter writer)
            //where K : IEquatable<K>
            //where V : IEquatable<V>
        {
            writer.Write(dictionary.Count);
            foreach (KeyValuePair<K, V> kvp in dictionary)
            {
                writer.Write(kvp.Key);
                writer.Write(kvp.Value);
            }
        }

        public static void Restore<K, V>(this Dictionary<K, V> dictionary, NaiadReader reader)
            //where K : IEquatable<K>
            //where V : IEquatable<V>
        {
            dictionary.Clear();
            int count = reader.Read<int>();
            for (int i = 0; i < count; ++i)
            {
                K key = reader.Read<K>();
                V value = reader.Read<V>();
                dictionary[key] = value;
            }
        }

        public static void Checkpoint<T>(this T[] array, int count, NaiadWriter writer)
        {
            Debug.Assert(count <= array.Length);
            writer.Write(count);
            for (int i = 0; i < count; ++i)
                writer.Write(array[i]);
        }

        public static T[] RestoreArray<T>(NaiadReader reader, Func<int, T[]> allocator)
        {
            int count = reader.Read<int>();
            T[] ret = allocator(count);
            for (int i = 0; i < count; ++i)
                ret[i] = reader.Read<T>();
            return ret;
        }

    }
}
