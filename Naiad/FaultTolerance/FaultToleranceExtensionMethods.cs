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

﻿using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Text;
using Naiad.Dataflow.Channels;
using Naiad.CodeGeneration;
using Naiad.DataStructures;

namespace Naiad.FaultTolerance
{
    public static class FaultToleranceExtensionMethods
    {
        private static NaiadSerialization<int> intSerializer = null;

        /* Checkpoint format for NaiadList<S>:
         * int     Count
         * S*Count Array
         */
        public static void Checkpoint<S>(this NaiadList<S> list, NaiadWriter writer, NaiadSerialization<S> serializer)
        //    where S : IEquatable<S>
        {
            if (intSerializer == null)
                intSerializer = AutoSerialization.GetSerializer<int>();

            writer.Write(list.Count, intSerializer);
            for (int i = 0; i < list.Count; ++i)
                writer.Write(list.Array[i], serializer);
        }

        public static void Restore<S>(this NaiadList<S> list, NaiadReader reader, NaiadSerialization<S> serializer)
        //    where S : IEquatable<S>
        {
            if (intSerializer == null)
                intSerializer = AutoSerialization.GetSerializer<int>();

            list.Clear();
            int count = reader.Read<int>(intSerializer);
            list.EnsureCapacity(count);
            list.Count = count;
            for (int i = 0; i < list.Count; ++i)
                list.Array[i] = reader.Read<S>(serializer);
        }

        /* Checkpoint format for List<S>:
         * int     Count
         * S*Count Array
         */
        public static void Checkpoint<S>(this List<S> list, NaiadWriter writer, NaiadSerialization<S> serializer)
        //    where S : IEquatable<S>
        {
            if (intSerializer == null)
                intSerializer = AutoSerialization.GetSerializer<int>();

            writer.Write(list.Count, intSerializer);
            for (int i = 0; i < list.Count; ++i)
                writer.Write(list[i], serializer);
        }

        public static void Restore<S>(this List<S> list, NaiadReader reader, NaiadSerialization<S> serializer)
        //    where S : IEquatable<S>
        {
            if (intSerializer == null)
                intSerializer = AutoSerialization.GetSerializer<int>();

            list.Clear();
            int count = reader.Read<int>(intSerializer);
            for (int i = 0; i < list.Count; ++i)
                list.Add(reader.Read<S>(serializer));
        }

        public static void Checkpoint<K, V>(this Dictionary<K, V> dictionary, NaiadWriter writer, NaiadSerialization<K> keySerializer, NaiadSerialization<V> valueSerializer)
            //where K : IEquatable<K>
            //where V : IEquatable<V>
        {
            if (intSerializer == null)
                intSerializer = AutoSerialization.GetSerializer<int>(); 
           
            writer.Write(dictionary.Count, intSerializer);
            foreach (KeyValuePair<K, V> kvp in dictionary)
            {
                writer.Write(kvp.Key, keySerializer);
                writer.Write(kvp.Value, valueSerializer);
            }
        }

        public static void Restore<K, V>(this Dictionary<K, V> dictionary, NaiadReader reader, NaiadSerialization<K> keySerializer, NaiadSerialization<V> valueSerializer)
            //where K : IEquatable<K>
            //where V : IEquatable<V>
        {
            if (intSerializer == null)
                intSerializer = AutoSerialization.GetSerializer<int>(); 

            dictionary.Clear();
            int count = reader.Read<int>(intSerializer);
            for (int i = 0; i < count; ++i)
            {
                K key = reader.Read<K>(keySerializer);
                V value = reader.Read<V>(valueSerializer);
                dictionary[key] = value;
            }
        }

        public static void Checkpoint<T>(this T[] array, int count, NaiadWriter writer, NaiadSerialization<T> serializer)
        {
            if (intSerializer == null)
                intSerializer = AutoSerialization.GetSerializer<int>(); 
            
            Debug.Assert(count <= array.Length);
            writer.Write(count, intSerializer);
            for (int i = 0; i < count; ++i)
                writer.Write(array[i], serializer);
        }

        public static T[] RestoreArray<T>(NaiadReader reader, Func<int, T[]> allocator, NaiadSerialization<T> serializer)
        {
            if (intSerializer == null)
                intSerializer = AutoSerialization.GetSerializer<int>(); 

            int count = reader.Read<int>(intSerializer);
            T[] ret = allocator(count);
            for (int i = 0; i < count; ++i)
                ret[i] = reader.Read<T>(serializer);
            return ret;
        }

    }
}
