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
using System.Diagnostics;
using System.Linq;
using System.Text;
using Microsoft.Research.Naiad.Dataflow.Channels;
using Microsoft.Research.Naiad.DataStructures;

namespace Microsoft.Research.Naiad.Serialization
{
    /// <summary>
    /// A collection of extension methods that facilitate checkpointing and restoring
    /// standard data structures.
    /// </summary>
    public static class CheckpointRestoreExtensionMethods
    {
        
        /* Checkpoint format for List<S>:
         * int     Count
         * S*Count Array
         */

        /// <summary>
        /// Writes this list to the given writer.
        /// </summary>
        /// <typeparam name="TElement">The type of elements in the list.</typeparam>
        /// <param name="list">The list to be written.</param>
        /// <param name="writer">The writer.</param>
        public static void Checkpoint<TElement>(this List<TElement> list, NaiadWriter writer)
        {
            writer.Write(list.Count);
            for (int i = 0; i < list.Count; ++i)
                writer.Write(list[i]);
        }

        /// <summary>
        /// Reads this list from the given reader.
        /// </summary>
        /// <typeparam name="TElement">The type of elements in the list.</typeparam>
        /// <param name="list">The list to be read.</param>
        /// <param name="reader">The reader.</param>
        public static void Restore<TElement>(this List<TElement> list, NaiadReader reader)
        {
            list.Clear();
            int count = reader.Read<int>();
            for (int i = 0; i < list.Count; ++i)
                list.Add(reader.Read<TElement>());
        }

        /// <summary>
        /// Writes this dictionary to the given writer.
        /// </summary>
        /// <typeparam name="TKey">The type of keys in the dictionary.</typeparam>
        /// <typeparam name="TValue">The type of values in the dictionary.</typeparam>
        /// <param name="dictionary">The dictionary to be written.</param>
        /// <param name="writer">The writer.</param>
        public static void Checkpoint<TKey, TValue>(this Dictionary<TKey, TValue> dictionary, NaiadWriter writer)
        {
            writer.Write(dictionary.Count);
            foreach (KeyValuePair<TKey, TValue> kvp in dictionary)
            {
                writer.Write(kvp.Key);
                writer.Write(kvp.Value);
            }
        }

        /// <summary>
        /// Reads this dictionary from the given reader.
        /// </summary>
        /// <typeparam name="TKey">The type of keys in the dictionary.</typeparam>
        /// <typeparam name="TValue">The type of values in the dictionary.</typeparam>
        /// <param name="dictionary">The dictionary to be read.</param>
        /// <param name="reader">The reader.</param>
        public static void Restore<TKey, TValue>(this Dictionary<TKey, TValue> dictionary, NaiadReader reader)
        {
            dictionary.Clear();
            int count = reader.Read<int>();
            for (int i = 0; i < count; ++i)
            {
                TKey key = reader.Read<TKey>();
                TValue value = reader.Read<TValue>();
                dictionary[key] = value;
            }
        }

        /// <summary>
        /// Writes the given count of elements from this array to the given writer.
        /// </summary>
        /// <typeparam name="TElement">The type of elements in the array.</typeparam>
        /// <param name="array">The array to be written.</param>
        /// <param name="count">The number of elements to be written.</param>
        /// <param name="writer">The writer.</param>
        public static void Checkpoint<TElement>(this TElement[] array, int count, NaiadWriter writer)
        {
            Debug.Assert(count <= array.Length);
            writer.Write(count);
            for (int i = 0; i < count; ++i)
                writer.Write(array[i]);
        }

        /// <summary>
        /// Reads an array from the given reader.
        /// </summary>
        /// <typeparam name="TElement">The type of elements in the array.</typeparam>
        /// <param name="reader">The reader.</param>
        /// <param name="allocator">An allocator function that allocates an array with at least as many elements as its argument.</param>
        /// <returns>The array.</returns>
        public static TElement[] RestoreArray<TElement>(NaiadReader reader, Func<int, TElement[]> allocator)
        {
            int count = reader.Read<int>();
            TElement[] ret = allocator(count);
            for (int i = 0; i < count; ++i)
                ret[i] = reader.Read<TElement>();
            return ret;
        }

    }
}
