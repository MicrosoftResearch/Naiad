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
using System.Threading.Tasks;

namespace Microsoft.Research.Naiad.DataStructures
{
    internal struct StringKey
    {
        public readonly int HashCode;
        public readonly int BufferIndex;
        public readonly int Offset;
        public readonly int Length;

        public StringKey(int hashCode, int bufferIndex, int offset, int length)
        {
            this.HashCode = hashCode;
            this.BufferIndex = bufferIndex;
            this.Offset = offset;
            this.Length = length;
        }

        public StringKey(string externalString)
        {
            this.HashCode = externalString.GetHashCode();
            this.BufferIndex = -1;
            this.Offset = -1;
            this.Length = externalString.Length;
        }
    }

    public class StringKeyedDictionary<V> : IDictionary<string, V>
    {

        private class StringKeyComparer : IEqualityComparer<StringKey>
        {
            private readonly StringKeyedDictionary<V> parent;

            public StringKeyComparer(StringKeyedDictionary<V> parent)
            {
                this.parent = parent;
            }

            public bool Equals(StringKey x, StringKey y)
            {
                return this.parent.KeyEquals(x, y);
            }

            public int GetHashCode(StringKey obj)
            {
                return obj.HashCode;
            }
        }

        private readonly Dictionary<StringKey, V> dictionary;

        private readonly SpinedList<char[]> strings;

        private string currentString;

        private const int BufferLength = 1 << 16;

        private char[] currentBuffer;
        private int currentBufferIndex;
        private int currentBufferOffset;

        public StringKeyedDictionary()
        {
            this.dictionary = new Dictionary<StringKey, V>(new StringKeyComparer(this));
            this.strings = new SpinedList<char[]>();

            this.currentBuffer = new char[BufferLength];
            this.strings.Add(this.currentBuffer);
            this.currentBufferIndex = 0;
            this.currentBufferOffset = 0;
        }

        private unsafe bool KeyEquals(StringKey x, StringKey y)
        {
            if (x.HashCode != y.HashCode || x.Length != y.Length)
                return false;

            if (x.Offset >= 0)
            {
                if (y.Offset >= 0)
                    return x.Offset == y.Offset;

                char[] buffer = this.strings[x.BufferIndex];

                fixed (char* bufferPtr = buffer)
                fixed (char* currentStringPtr = this.currentString)
                {
                    for (int i = 0; i < x.Length; ++i)
                        if (bufferPtr[x.Offset + i] != currentStringPtr[i])
                            return false;
                }

                return true;
            }
            else
                return this.KeyEquals(y, x);
        }

        private StringKey AddKey(string key)
        {
            int length = key.Length;

            if (this.currentBufferOffset + length <= StringKeyedDictionary<V>.BufferLength)
            {
                key.CopyTo(0, this.currentBuffer, this.currentBufferOffset, length);
                StringKey ret = new StringKey(key.GetHashCode(), this.currentBufferIndex, this.currentBufferOffset, length);
                this.currentBufferOffset += length;
                return ret;
            }
            else if (length > StringKeyedDictionary<V>.BufferLength)
            {
                // Special case: allocate a new buffer for the large string.
                this.currentBufferIndex++;
                this.currentBufferOffset = length;

                // Don't worry, we won't write to this buffer, because the currentBufferOffset is larger that the constant BufferLength.
                this.currentBuffer = key.ToCharArray();
                this.strings.Add(this.currentBuffer);

                return new StringKey(key.GetHashCode(), this.currentBufferIndex, 0, length);
            }
            else
            {
                // Buffer is (almost) full, so retry with a newly-allocated buffer.
                this.currentBufferIndex++;
                this.currentBufferOffset = 0;
                this.currentBuffer = new char[StringKeyedDictionary<V>.BufferLength];
                this.strings.Add(this.currentBuffer);

                return this.AddKey(key);
            }
        }

        public void Add(string key, V value)
        {
            if (this.ContainsKey(key))
                throw new ArgumentException("An element with the same key has already been added.");

            StringKey keyPointer = this.AddKey(key);
            this.dictionary.Add(keyPointer, value);
        }

        public bool ContainsKey(string key)
        {
            this.currentString = key;
            bool ret = this.dictionary.ContainsKey(new StringKey(key));
            this.currentString = null;
            return ret;
        }

        public ICollection<string> Keys
        {
            get { throw new NotImplementedException(); }
        }

        public bool Remove(string key)
        {
            this.currentString = key;
            bool ret = this.dictionary.Remove(new StringKey(key));
            this.currentString = null;
            return ret;
        }

        public bool TryGetValue(string key, out V value)
        {
            this.currentString = key;
            bool ret = this.dictionary.TryGetValue(new StringKey(key), out value);
            this.currentString = null;
            return ret;
        }

        public ICollection<V> Values
        {
            get { return this.dictionary.Values; }
        }

        public V this[string key]
        {
            get
            {
                this.currentString = key;
                V ret = this.dictionary[new StringKey(key)];
                this.currentString = null;
                return ret;
            }
            set
            {
                if (this.ContainsKey(key))
                {
                    this.currentString = key;
                    this.dictionary[new StringKey(key)] = value;
                    this.currentString = null;
                }
                else
                    this.Add(key, value);
            }
        }

        public void Add(KeyValuePair<string, V> item)
        {
            this.Add(item.Key, item.Value);
        }

        public void Clear()
        {
            this.dictionary.Clear();
        }

        public bool Contains(KeyValuePair<string, V> item)
        {
            return this.ContainsKey(item.Key) && this[item.Key].Equals(item.Value);
        }

        public void CopyTo(KeyValuePair<string, V>[] array, int arrayIndex)
        {
            foreach (KeyValuePair<StringKey, V> kvp in this.dictionary)
            {
                array[arrayIndex++] = new KeyValuePair<string, V>(this.ToString(kvp.Key), kvp.Value);
            }
        }

        public int Count
        {
            get { return this.dictionary.Count; }
        }

        public bool IsReadOnly
        {
            get { return false; }
        }

        public bool Remove(KeyValuePair<string, V> item)
        {
            if (this.ContainsKey(item.Key) && this[item.Key].Equals(item.Value))
            {
                this.Remove(item.Key);
                return true;
            }
            else
            {
                return false;
            }
        }

        private string ToString(StringKey key)
        {
            return new string(this.strings[key.BufferIndex], key.Offset, key.Length);
        }

        public IEnumerator<KeyValuePair<string, V>> GetEnumerator()
        {
            return this.dictionary.Select(x => new KeyValuePair<string, V>(this.ToString(x.Key), x.Value)).GetEnumerator();
        }

        System.Collections.IEnumerator System.Collections.IEnumerable.GetEnumerator()
        {
            return this.dictionary.Select(x => new KeyValuePair<string, V>(this.ToString(x.Key), x.Value)).GetEnumerator();
        }
    }
}
