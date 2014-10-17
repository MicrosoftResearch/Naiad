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

using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Microsoft.Research.Naiad.Dataflow.Channels;
using Microsoft.Research.Naiad.Serialization;
using Microsoft.Research.Naiad.Scheduling;

namespace Microsoft.Research.Naiad.Runtime.Progress
{

#if false
    // used for antichain of pointstamps, and pointstamps only
    internal class MinimalAntichain<T> where T : Scheduling.PartialOrder<T>
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

        public void Checkpoint(NaiadWriter writer)
        {
            this.elements.Checkpoint(writer);
            this.precedents.Checkpoint(writer);
            this.Antichain.Checkpoint(writer);
        }

        public void Restore(NaiadReader reader)
        {
            this.elements.Restore(reader);
            this.precedents.Restore(reader);
            this.Antichain.Restore(reader);
        }
    }

#endif

    internal class PointstampFrontier
    {
        internal Reachability Reachability;

        public List<Pointstamp> Antichain = new List<Pointstamp>();

        private List<Pointstamp> elements = new List<Pointstamp>();
        private List<int> precedents = new List<int>();

        internal PointstampFrontier(Reachability reachability)
        {
            this.Reachability = reachability;
        }

        void UpdateAntichain()
        {
            Antichain.Clear();
            for (int i = 0; i < elements.Count; i++)
                if (precedents[i] == 0)
                    Antichain.Add(elements[i]);

            if (Antichain.Count == 0 && elements.Count > 0)
                throw new Exception("Empty Antichain from non-empty set");
        }

        internal bool Add(Pointstamp element)
        {
            var newPrecedents = 0;

            for (int i = 0; i < elements.Count; i++)
            {
                if (this.Reachability.LessThan(element, elements[i]))
                    precedents[i]++;

                if (this.Reachability.LessThan(elements[i], element))
                    newPrecedents++;

                if (this.Reachability.LessThan(element, elements[i]) && this.Reachability.LessThan(elements[i], element) && !elements[i].Equals(element))
                {
                    this.Reachability.LessThan(element, elements[i]);
                    throw new Exception("Ordering violation " + element + "," + elements[i]);
                }
            }

            elements.Add(element);
            precedents.Add(newPrecedents);

            var changes = newPrecedents == 0;
            for (int i = 0; i < Antichain.Count; i++)
                if (this.Reachability.LessThan(element, Antichain[i]))
                    changes = true;

            if (changes)
                UpdateAntichain();

            return changes;
        }

        internal bool Remove(Pointstamp element)
        {
            int position = elements.Count;
            for (int i = 0; i < elements.Count; i++)
            {
                if (this.Reachability.LessThan(element, elements[i]))
                {
                    if (position == elements.Count && element.Equals(elements[i]))
                        position = i;
                    else
                        precedents[i]--;
                }
            }

            if (position == elements.Count)
                throw new Exception("Tried to remove an element not present in MinimalAntichain");

            if (position < elements.Count)
            {
                elements[position] = elements[elements.Count - 1];
                //elements.Count--;
                elements.RemoveAt(elements.Count - 1);

                precedents[position] = precedents[precedents.Count - 1];
                precedents.RemoveAt(precedents.Count - 1);
                //precedents.Count--;
            }

            var changes = false;
            for (int i = 0; i < Antichain.Count; i++)
                if (element.Equals(Antichain[i]))
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

        public void Checkpoint(NaiadWriter writer)
        {
            this.elements.Checkpoint(writer);
            this.precedents.Checkpoint(writer);
            this.Antichain.Checkpoint(writer);
        }

        public void Restore(NaiadReader reader)
        {
            this.elements.Restore(reader);
            this.precedents.Restore(reader);
            this.Antichain.Restore(reader);
        }
    }
}
