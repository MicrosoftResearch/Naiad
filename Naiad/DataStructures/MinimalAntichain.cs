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
using System.Linq;
using System.Text;
using Naiad.Dataflow.Channels;
using Naiad.CodeGeneration;
using Naiad.FaultTolerance;
using Naiad.Scheduling;

namespace Naiad.DataStructures
{
    // used for antichain of pointstamps, and pointstamps only
    public class MinimalAntichain<T> where T : Scheduling.PartialOrder<T>
    {
        public NaiadList<T> Antichain = new NaiadList<T>(0);

        NaiadList<T> elements = new NaiadList<T>(0);
        NaiadList<int> precedents = new NaiadList<int>(0);

        void UpdateAntichain()
        {
            Antichain.Clear();
            for (int i = 0; i < elements.Count; i++)
                if (precedents.Array[i] == 0)
                    Antichain.Add(elements.Array[i]);

            if (Antichain.Count == 0 && elements.Count > 0)
                throw new Exception("Empty Antichain from non-empty set");
        }

        public bool Add(T element)
        {
            var newPrecedents = 0;

            for (int i = 0; i < elements.Count; i++)
            {
                if (element.LessThan(elements.Array[i]))
                    precedents.Array[i]++;
                
                if (elements.Array[i].LessThan(element))
                    newPrecedents++;

                if (element.LessThan(elements.Array[i]) && elements.Array[i].LessThan(element) && !elements.Array[i].Equals(element))
                {
                    element.LessThan(elements.Array[i]);
                    throw new Exception("Ordering violation " + element + "," + elements.Array[i]);
                }
            }

            elements.Add(element);
            precedents.Add(newPrecedents);

            var changes = newPrecedents == 0;
            for (int i = 0; i < Antichain.Count; i++)
                if (element.LessThan(Antichain.Array[i]))
                    changes = true;
                
            if (changes)
                UpdateAntichain();

            return changes;
        }

        public bool Remove(T element)
        {
            int position = elements.Count;
            for (int i = 0; i < elements.Count; i++)
            {
                if (element.LessThan(elements.Array[i]))
                {
                    if (position == elements.Count && element.Equals(elements.Array[i]))
                        position = i;
                    else
                        precedents.Array[i]--;
                }
            }

            if (position == elements.Count)
                throw new Exception("Tried to remove an element not present in MinimalAntichain");
            
            if (position < elements.Count)
            {
                elements.Array[position] = elements.Array[elements.Count - 1];
                elements.Count--;

                precedents.Array[position] = precedents.Array[precedents.Count - 1];
                precedents.Count--;
            }

            var changes = false;
            for (int i = 0; i < Antichain.Count; i++)
                if (element.Equals(Antichain.Array[i]))
                    changes = true;

            if (changes)
                UpdateAntichain();

            return changes;
        }

        public void Clear()
        {
            elements.Clear();
            precedents.Clear();
            Antichain.Clear();
        }

        public void Checkpoint(NaiadWriter writer, NaiadSerialization<T> serializer)
        {
            this.elements.Checkpoint(writer, serializer);
            this.precedents.Checkpoint(writer, PrimitiveSerializers.Int32);
            this.Antichain.Checkpoint(writer, serializer);
        }

        public void Restore(NaiadReader reader, NaiadSerialization<T> serializer)
        {
            this.elements.Restore(reader, serializer);
            this.precedents.Restore(reader, PrimitiveSerializers.Int32);
            this.Antichain.Restore(reader, serializer);
        }
    }

    public class PointstampFrontier
    {
        internal Reachability Reachability;

        public NaiadList<Pointstamp> Antichain = new NaiadList<Pointstamp>(0);

        NaiadList<Pointstamp> elements = new NaiadList<Pointstamp>(0);
        NaiadList<int> precedents = new NaiadList<int>(0);

        internal PointstampFrontier(Reachability reachability)
        {
            this.Reachability = reachability;
        }

        void UpdateAntichain()
        {
            Antichain.Clear();
            for (int i = 0; i < elements.Count; i++)
                if (precedents.Array[i] == 0)
                    Antichain.Add(elements.Array[i]);

            if (Antichain.Count == 0 && elements.Count > 0)
                throw new Exception("Empty Antichain from non-empty set");
        }

        public bool Add(Pointstamp element)
        {
            var newPrecedents = 0;

            for (int i = 0; i < elements.Count; i++)
            {
                if (this.Reachability.LessThan(element, elements.Array[i]))
                    precedents.Array[i]++;

                if (this.Reachability.LessThan(elements.Array[i], element))
                    newPrecedents++;

                if (this.Reachability.LessThan(element, elements.Array[i]) && this.Reachability.LessThan(elements.Array[i], element) && !elements.Array[i].Equals(element))
                {
                    this.Reachability.LessThan(element, elements.Array[i]);
                    throw new Exception("Ordering violation " + element + "," + elements.Array[i]);
                }
            }

            elements.Add(element);
            precedents.Add(newPrecedents);

            var changes = newPrecedents == 0;
            for (int i = 0; i < Antichain.Count; i++)
                if (this.Reachability.LessThan(element, Antichain.Array[i]))
                    changes = true;

            if (changes)
                UpdateAntichain();

            return changes;
        }

        public bool Remove(Pointstamp element)
        {
            int position = elements.Count;
            for (int i = 0; i < elements.Count; i++)
            {
                if (this.Reachability.LessThan(element, elements.Array[i]))
                {
                    if (position == elements.Count && element.Equals(elements.Array[i]))
                        position = i;
                    else
                        precedents.Array[i]--;
                }
            }

            if (position == elements.Count)
                throw new Exception("Tried to remove an element not present in MinimalAntichain");

            if (position < elements.Count)
            {
                elements.Array[position] = elements.Array[elements.Count - 1];
                elements.Count--;

                precedents.Array[position] = precedents.Array[precedents.Count - 1];
                precedents.Count--;
            }

            var changes = false;
            for (int i = 0; i < Antichain.Count; i++)
                if (element.Equals(Antichain.Array[i]))
                    changes = true;

            if (changes)
                UpdateAntichain();

            return changes;
        }

        public void Clear()
        {
            elements.Clear();
            precedents.Clear();
            Antichain.Clear();
        }

        public void Checkpoint(NaiadWriter writer, NaiadSerialization<Pointstamp> serializer)
        {
            this.elements.Checkpoint(writer, serializer);
            this.precedents.Checkpoint(writer, PrimitiveSerializers.Int32);
            this.Antichain.Checkpoint(writer, serializer);
        }

        public void Restore(NaiadReader reader, NaiadSerialization<Pointstamp> serializer)
        {
            this.elements.Restore(reader, serializer);
            this.precedents.Restore(reader, PrimitiveSerializers.Int32);
            this.Antichain.Restore(reader, serializer);
        }
    }
}
