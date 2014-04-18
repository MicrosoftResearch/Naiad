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
using System.Linq;
using System.Text;

using Microsoft.Research.Naiad;
using Microsoft.Research.Naiad.DataStructures;
using System.Diagnostics;
using Microsoft.Research.Naiad.Dataflow.Channels;
using Microsoft.Research.Naiad.Serialization;
using Microsoft.Research.Naiad.Runtime.Progress;

namespace Microsoft.Research.Naiad.Frameworks.DifferentialDataflow
{

    internal class LatticeInternTable<T> : IComparer<int>
         where T : Time<T>
    {
        public Dictionary<T, int> indices;
        public T[] times;
        public int count;

        private T lastInterned;
        private int lastInternedResult;

        #region Lattice time advancement

        private int[] redirection;  // redirection[i] != i means that time i has been advanced to some new time (indicated by redirection[i]). 
        // keep applying x => redirection[x] until fixed point.

        private NaiadList<T> reachableTimes = new NaiadList<T>(1); // list of lattice elements capable of reaching this operator/intern _table.
        // used as the basis of lattice advancement

        public int UpdateTime(int index)
        {
            return this.redirection[index];
        }

        /// <summary>
        /// Joins the given time against all elements of reachable times, and returns the meet of these joined times.
        /// </summary>
        /// <param name="s"></param>
        /// <returns></returns>
        private T Advance(T s)
        {
            Debug.Assert(this.reachableTimes != null);
            Debug.Assert(this.reachableTimes.Count > 0);
            
            var meet = this.reachableTimes.Array[0].Join(s);
            for (int i = 1; i < this.reachableTimes.Count; i++)
                meet = meet.Meet(this.reachableTimes.Array[i].Join(s));

            return meet;
        }

        /// <summary>
        /// Updates this intern table's redirection mapping to map each table entry to a hopefully-smaller
        /// set of equivalent times, based on the givne set of versions that can reach this table's operator.
        /// </summary>
        /// <param name="causalTimes"></param>
        public void UpdateReachability(List<Pointstamp> causalTimes)
        {
            // Convert the given VertexVersions into local T times.
            this.reachableTimes.Clear();
            for (int i = 0; i < causalTimes.Count; i++)
                this.reachableTimes.Add(default(T).InitializeFrom(causalTimes[i], causalTimes[i].Timestamp.Length));

            // it would be nice to redirect to the *oldest* equivalent time, 
            // so that as many times as possible stay stable.
#if true
            // Maps T times to intern table indices.
            var redirectionDictionary = new Dictionary<T, int>();

            // For each time in this intern table, attempt to update it reflecting the new reachable times.
            for (int i = 0; i < this.count; i++)
            {
                var newTime = this.Advance(times[i]);
                if (!redirectionDictionary.ContainsKey(newTime) || this.times[redirectionDictionary[newTime]].CompareTo(times[i]) > 0)
                    redirectionDictionary[newTime] = i;
            }

            // Update the redirection for each interned time to the computed equivalent time.
            for (int i = 0; i < count; i++)
            {
                var newTime = this.Advance(this.times[i]);
                this.redirection[i] = redirectionDictionary[newTime];
            }
#else
            // advance any times we have interned, updating their redirection[] entry.
            for (int i = 0; i < count; i++)
                Redirection[i] = Intern(Advance(times[Redirection[i]]));
#endif
        }

        #endregion


        /// <summary>
        /// Returns an integer that uniquely identifies the given time in this table.
        /// </summary>
        /// <param name="time">A time T to intern</param>
        /// <returns>An index in this table that uniquely corresponds to the given time.</returns>
        public int Intern(T time)
        {
            if (!time.Equals(lastInterned))
            {
                if (!indices.TryGetValue(time, out lastInternedResult))
                {
                    if (this.count == times.Length)
                    {
                        var tempTimes = new T[count * 2];
                        for (int i = 0; i < count; i++)
                            tempTimes[i] = times[i];

                        times = tempTimes;

                        var tempRedir = new int[count * 2];
                        for (int i = 0; i < count; i++)
                            tempRedir[i] = this.redirection[i];

                        this.redirection = tempRedir;
                    }

                    times[count] = time;
                    this.redirection[count] = count;

                    indices.Add(time, count);

                    lastInternedResult = count++;
                }

                lastInterned = time;
            }

            return lastInternedResult;
        }

        HashSet<T> timeSet = new HashSet<T>();
        public void InterestingTimes(NaiadList<int> timelist, NaiadList<int> truth, NaiadList<int> delta)
        {

            for (int i = 0; i < delta.Count; i++)
            {
                if (!timeSet.Contains(times[delta.Array[i]]))
                {
                    timelist.Add(delta.Array[i]);
                    timeSet.Add(times[delta.Array[i]]);
                }

                for (int j = 0; j < truth.Count; j++)
                {
                    // we can skip this for times before deltaArray[i]
                    if (!LessThan(truth.Array[j], delta.Array[i]))
                    {
                        var join = times[delta.Array[i]].Join(times[truth.Array[j]]);

                        if (!timeSet.Contains(join))
                        {
                            timelist.Add(this.Intern(join));
                            timeSet.Add(join);
                        }
                    }
                }
            }

            // do this until we have processed all elements
            for (int i = 0; i < timelist.Count; i++)
            {
                // try combining it with each earlier time
                for (int j = 0; j < i; j++)
                {
                    // if we already have an ordering relation, we can skip
                    if (!times[timelist.Array[j]].LessThan(times[timelist.Array[i]]))
                    {
                        var join = times[timelist.Array[i]].Join(times[timelist.Array[j]]);

                        if (!timeSet.Contains(join))
                        {

                            timelist.Add(this.Intern(join));
                            timeSet.Add(join);
                        }
                    }
                }
            }

            timeSet.Clear();

            if (timelist.Count > 1)
                Array.Sort(timelist.Array, 0, timelist.Count, this);
        }

        public int Compare(int i, int j)
        {
            return times[i].CompareTo(times[j]);
        }

        public bool LessThan(int index1, int index2)
        {
            return times[index1].LessThan(times[index2]);
        }

        /* Checkpoint format:
         * int                       indicesCount
         * Pair<T, int>*indicesCount indices
         * int                       count
         * T*count                   times
         */ 

        public void Checkpoint(NaiadWriter writer)
        {
            this.indices.Checkpoint(writer);

            this.times.Checkpoint(this.count, writer);

            this.redirection.Checkpoint(this.redirection.Length, writer);

            writer.Write(this.lastInterned);
            writer.Write(this.lastInternedResult);
        }

        public void Restore(NaiadReader reader)
        {
            this.indices.Restore(reader);

            //Console.Error.WriteLine("% LIT.indices read {0} objects", reader.objectsRead - before);
            //before = reader.objectsRead;

            this.times = CheckpointRestoreExtensionMethods.RestoreArray<T>(reader, n => {
                this.count = n; 
                return this.times.Length >= n ? this.times : new T[n]; 
            });

            this.redirection = CheckpointRestoreExtensionMethods.RestoreArray<int>(reader, n => new int[n]);

            //Console.Error.WriteLine("% LIT.times read {0} objects", reader.objectsRead - before);

            this.lastInterned = reader.Read<T>();

            //Console.Error.WriteLine("% LIT.lastInterned read {0} objects", reader.objectsRead - before);

            this.lastInternedResult = reader.Read<int>();

            //Console.Error.WriteLine("% LIT.lastInternedResult read {0} objects", reader.objectsRead - before);

        }

        public LatticeInternTable()
        {
            this.indices = new Dictionary<T, int>(1);
            this.times = new T[1];
            this.redirection = new int[1];
            this.times[0] = default(T);
            this.indices.Add(default(T), 0);
            this.count = 1;
        }
    }

}
