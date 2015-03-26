/*
 * Naiad ver. 0.6
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

using Microsoft.Research.Naiad.Runtime.Controlling;
using Microsoft.Research.Naiad.Runtime.Progress;
using Microsoft.Research.Naiad.DataStructures;
using Microsoft.Research.Naiad.Serialization;

namespace Microsoft.Research.Naiad.Runtime.FaultTolerance
{
    /// <summary>
    /// Helper classes for fault tolerance
    /// </summary>
    class NamespaceDoc
    {
    }

    /// <summary>
    /// A frontier used for fault tolerance
    /// </summary>
    public struct FTFrontier : IEquatable<FTFrontier>, IComparable<FTFrontier>
    {
        /// <summary>
        /// the maximal element in the frontier. this is public so the frontier can be serialized
        /// </summary>
        public Pointstamp maximalElement;

        /// <summary>
        /// true if the frontier contains no times
        /// </summary>
        public bool Empty { get { return maximalElement.Location < 0; } }

        /// <summary>
        /// true if the frontier contains all times
        /// </summary>
        public bool Complete { get { return maximalElement.Location == Int32.MaxValue; } }

        private static Pointstamp EmptyPointstamp { get { return new Pointstamp(-1, new int[] { }); } }
        private static Pointstamp CompletePointstamp { get { return new Pointstamp(Int32.MaxValue, new int[] { }); } }

        /// <summary>
        /// Make a new empty or complete frontier
        /// </summary>
        /// <param name="complete">if true, the new frontier is complete otherwise it is empty</param>
        public FTFrontier(bool complete)
        {
            if (complete)
            {
                this.maximalElement = CompletePointstamp;
            }
            else
            {
                this.maximalElement = EmptyPointstamp;
            }
        }

        /// <summary>
        /// Convert a minimal antichain of pointstamps to a frontier
        /// </summary>
        /// <param name="frontier">the minimal antichain representing the frontier</param>
        /// <returns>the frontier</returns>
        public static FTFrontier FromPointstamps(Pointstamp[] frontier)
        {
            if (frontier.Length == 0)
            {
                return new FTFrontier(false);
            }
            else if (frontier.Length > 1)
            {
                throw new ApplicationException("Only totally ordered frontiers supported");
            }

            return new FTFrontier(frontier[0]);
        }

        /// <summary>
        /// Convert the frontier to a minimal antichain of pointstamps
        /// </summary>
        /// <typeparam name="T">time type of the frontier</typeparam>
        /// <param name="nodeId">location in the graph of the frontier</param>
        /// <returns>the minimal antichain representing the frontier</returns>
        public Pointstamp[] ToPointstamps<T>(int nodeId) where T : Time<T>
        {
            return this.ToPointstamps(default(T).ToPointstamp(nodeId));
        }

        /// <summary>
        /// Convert the frontier to a minimal antichain of pointstamps
        /// </summary>
        /// <param name="defaultVersion">default pointstamp for the frontier's stage</param>
        /// <returns>the minimal antichain representing the frontier</returns>
        public Pointstamp[] ToPointstamps(Pointstamp defaultVersion)
        {
            if (this.Empty)
            {
                return new Pointstamp[0];
            }
            else if (this.Complete)
            {
                Pointstamp maximal = defaultVersion;
                for (int i = 0; i < maximal.Timestamp.Length; ++i)
                {
                    maximal.Timestamp[i] = int.MaxValue;
                }
                return new Pointstamp[] { maximal };
            }
            else
            {
                return new Pointstamp[] { this.maximalElement };
            }
        }

        /// <summary>
        /// Make a new frontier from an existing one
        /// </summary>
        /// <param name="toCopy">existing frontier</param>
        public FTFrontier(FTFrontier toCopy)
        {
            this.maximalElement = new Pointstamp(toCopy.maximalElement);
        }

        private FTFrontier(Pointstamp maximalElement)
        {
            this.maximalElement = new Pointstamp(maximalElement);
        }

        /// <summary>
        /// Make a new frontier containing all times strictly less than <paramref name="pointstamp"/>
        /// </summary>
        /// <param name="pointstamp">frontier boundary. the new frontier contains all times less than
        /// this</param>
        /// <returns>the new frontier</returns>
        public static FTFrontier SetBelow(Pointstamp pointstamp)
        {
            return new FTFrontier(DecrementLexicographically(pointstamp));
        }

        /// <summary>
        /// true if this frontier contains the same times as <paramref name="other"/>
        /// </summary>
        /// <param name="other">frontier to compare to</param>
        /// <returns>true if the frontiers are equal</returns>
        public bool Equals(FTFrontier other)
        {
            return this.maximalElement.Equals(other.maximalElement);
        }

        /// <summary>
        /// Returns true if the time is within the frontier
        /// </summary>
        /// <param name="time">time to test for containment</param>
        /// <returns>true iff the time is within the frontier</returns>
        public bool Contains(Pointstamp time)
        {
            if (this.Empty)
            {
                return false;
            }
            else if (this.Complete)
            {
                return true;
            }

            return IsLessThanOrEqualTo(time, this.maximalElement);
        }

        /// <summary>
        /// compare to <paramref name="other"/>
        /// </summary>
        /// <param name="other">frontier to compare to</param>
        /// <returns>1 if this contains other, 0 if this=other, -1 otherwise</returns>
        public int CompareTo(FTFrontier other)
        {
            if (this.Equals(other))
            {
                return 0;
            }
            else if (this.Contains(other))
            {
                return 1;
            }
            else
            {
                return -1;
            }
        }

        /// <summary>
        /// compare two frontiers
        /// </summary>
        /// <param name="other">frontier to compare</param>
        /// <returns>true if all the times in <paramref name="other"/> are contained in this</returns>
        public bool Contains(FTFrontier other)
        {
            if (this.Empty)
            {
                return other.Empty;
            }
            else if (this.Complete || other.Empty)
            {
                return true;
            }
            else if (other.Complete)
            {
                return false;
            }

            return IsLessThanOrEqualTo(other.maximalElement, this.maximalElement);
        }

        /// <summary>
        /// add multiple times to the frontier
        /// </summary>
        /// <param name="times">times to add</param>
        /// <returns>true if the frontier was increased by adding the new time</returns>
        public FTFrontier Add(IEnumerable<Pointstamp> times)
        {
            foreach (Pointstamp time in times)
            {
                this.Add(time);
            }
            return this;
        }

        /// <summary>
        /// add a time to the frontier
        /// </summary>
        /// <param name="time">time to add</param>
        /// <returns>true if the frontier was increased by adding the new time</returns>
        public bool Add(Pointstamp time)
        {
            if (this.Contains(time))
            {
                return false;
            }

            this.maximalElement = new Pointstamp(time);

            return true;
        }

        /// <summary>
        /// union two frontiers
        /// </summary>
        /// <param name="other">frontier to be unioned with this</param>
        /// <returns>new frontier that is the union of this and <paramref name="other"/></returns>
        public FTFrontier Union(FTFrontier other)
        {
            if (this.Empty || other.Complete)
            {
                return new FTFrontier(other.maximalElement);
            }
            else if (this.Complete || other.Empty)
            {
                return new FTFrontier(this.maximalElement);
            }
            else
            {
                if (IsLessThanOrEqualTo(this.maximalElement, other.maximalElement))
                {
                    return new FTFrontier(other.maximalElement);
                }
                else
                {
                    return new FTFrontier(this.maximalElement);
                }
            }
        }

        /// <summary>
        /// intersect two frontiers
        /// </summary>
        /// <param name="other">frontier to be intersected with this</param>
        /// <returns>new frontier that is the intersection of this and <paramref name="other"/></returns>
        public FTFrontier Intersect(FTFrontier other)
        {
            if (this.Empty || other.Complete)
            {
                return new FTFrontier(this.maximalElement);
            }
            else if (this.Complete || other.Empty)
            {
                return new FTFrontier(other.maximalElement);
            }
            else
            {
                if (IsLessThanOrEqualTo(other.maximalElement, this.maximalElement))
                {
                    return new FTFrontier(other.maximalElement);
                }
                else
                {
                    return new FTFrontier(this.maximalElement);
                }
            }
        }

        /// <summary>
        /// increase a set to include the next timestamp lexicographically
        /// </summary>
        /// <param name="pointstamp">a pointstamp representing a set of times</param>
        /// <returns>the set of times increased by adding the lexicographically next time</returns>
        private static Pointstamp IncrementLexicographically(Pointstamp pointstamp)
        {
            Pointstamp incremented = new Pointstamp(pointstamp);
            // maxvalue indicates that all the times in this coordinate are already present in the set
            if (incremented.Timestamp[incremented.Timestamp.Length - 1] != Int32.MaxValue)
            {
                ++incremented.Timestamp[incremented.Timestamp.Length - 1];
            }
            return incremented;
        }

        /// <summary>
        /// decrease a set removing the last timestamp lexicographically
        /// </summary>
        /// <param name="pointstamp">a pointstamp representing a set of times</param>
        /// <returns>the set of times decreased by removing the lexicographically last time</returns>
        private static Pointstamp DecrementLexicographically(Pointstamp pointstamp)
        {
            Pointstamp decremented = new Pointstamp(pointstamp);

            // subtract one from the lexicographically-ordered timestamp
            for (int i = decremented.Timestamp.Length - 1; i >= 0; --i)
            {
                if (decremented.Timestamp[i] > 0)
                {
                    // maxvalue indicates that all the times in this coordinate are present in the set;
                    // preserve that property
                    if (decremented.Timestamp[i] != Int32.MaxValue)
                    {
                        // the current coordinate is non-zero, just decrement it and we are done
                        --decremented.Timestamp[i];
                    }
                    return decremented;
                }
                else
                {
                    if (i == 0)
                    {
                        // the truncated timestamp was all zeros, so set the decremented set to empty
                        return EmptyPointstamp;
                    }
                    else
                    {
                        // the current coordinate was at zero, so set it to MaxValue and continue to decrement the outer coordinate
                        decremented.Timestamp[i] = Int32.MaxValue;
                    }
                }
            }

            throw new ApplicationException("logic bug in decrement");
        }

        /// <summary>
        /// projects the frontier to a new node using the identity for phi
        /// </summary>
        /// <param name="nodeId">node to project to</param>
        /// <returns>a new frontier phi(this) at <paramref name="nodeId"/> where phi is the identity</returns>
        public FTFrontier ProjectBasic(int nodeId)
        {
            FTFrontier copy = new FTFrontier(this);

            if (!(this.Empty || this.Complete))
            {
                Pointstamp projected = new Pointstamp(this.maximalElement);
                projected.Location = nodeId;
                copy.maximalElement = projected;
            }

            return copy;
        }

        /// <summary>
        /// projects the frontier to a new node using phi appropriate for an increment node in a loop
        /// </summary>
        /// <param name="nodeId">node to project to</param>
        /// <returns>a new frontier phi(this) at <paramref name="nodeId"/> where phi increments the outer loop index</returns>
        public FTFrontier ProjectIteration(int nodeId)
        {
            FTFrontier copy = new FTFrontier(this);

            if (!(this.Empty || this.Complete))
            {
                Pointstamp projected = new Pointstamp(this.maximalElement);
                projected.Location = nodeId;
                copy.maximalElement = IncrementLexicographically(projected);
            }

            return copy;
        }

        /// <summary>
        /// projects the frontier to a new node using phi appropriate for a loop ingress node
        /// </summary>
        /// <param name="nodeId">node to project to</param>
        /// <returns>a new frontier phi(this) at <paramref name="nodeId"/> where phi adds a new loop coordinate</returns>
        public FTFrontier ProjectIngress(int nodeId)
        {
            FTFrontier copy = new FTFrontier(this);

            if (!(this.Empty || this.Complete))
            {
                Pointstamp projected = new Pointstamp(nodeId, new int[this.maximalElement.Timestamp.Length + 1]);

                for (int i=0; i<this.maximalElement.Timestamp.Length; ++i)
                {
                    projected.Timestamp[i] = this.maximalElement.Timestamp[i];
                }

                projected.Timestamp[projected.Timestamp.Length - 1] = Int32.MaxValue;

                copy.maximalElement = projected;
            }

            return copy;
        }

        /// <summary>
        /// projects the frontier to a new node using phi appropriate for a loop egress node
        /// </summary>
        /// <param name="nodeId">node to project to</param>
        /// <returns>a new frontier phi(this) at <paramref name="nodeId"/> where phi removes the outer loop coordinate</returns>
        public FTFrontier ProjectEgress(int nodeId)
        {
            FTFrontier copy = this;

            if (!(this.Empty || this.Complete))
            {
                if (this.maximalElement.Timestamp.Length < 2)
                {
                    throw new ApplicationException("Logic bug in projection");
                }

                Pointstamp projected = new Pointstamp(nodeId, new int[this.maximalElement.Timestamp.Length - 1]);

                for (int i = 0; i < projected.Timestamp.Length; ++i)
                {
                    projected.Timestamp[i] = maximalElement.Timestamp[i];
                }

                if (this.maximalElement.Timestamp[this.maximalElement.Timestamp.Length - 1] == Int32.MaxValue)
                {
                    copy.maximalElement = projected;
                }
                else
                {
                    copy.maximalElement = DecrementLexicographically(projected);
                }
            }

            return copy;
        }

        /// <summary>
        /// projects the frontier to a new node using phi appropriate for the sender stage
        /// </summary>
        /// <param name="stage">stage the frontier is in</param>
        /// <param name="nodeId">node to project to</param>
        /// <returns>a new frontier phi(this) at <paramref name="nodeId"/> where phi is appropriate for the stage type</returns>
        public FTFrontier Project(Dataflow.Stage stage, int nodeId)
        {
            if (stage.IsIterationAdvance)
            {
                return this.ProjectIteration(nodeId);
            }
            else if (stage.IsIterationIngress)
            {
                return this.ProjectIngress(nodeId);
            }
            else if (stage.IsIterationEgress)
            {
                return this.ProjectEgress(nodeId);
            }
            else
            {
                return this.ProjectBasic(nodeId);
            }
        }

        /// <summary>
        /// compare two pointstamps in the same stage using the lexicographic order
        /// </summary>
        /// <param name="a">first pointstamp</param>
        /// <param name="b">second pointstamp</param>
        /// <returns>true iff the first pointstamp is less than or equal to the second pointstamp</returns>
        public static bool IsLessThanOrEqualTo(Pointstamp a, Pointstamp b)
        {
            if (a.Timestamp.Length != b.Timestamp.Length)
                Console.WriteLine("should have same length!");

            if (a.Location != b.Location)
                Console.WriteLine("meant to be called on pointstamps of the same stage");

            if (a.Location < 0 || b.Location < 0 || a.Location == Int32.MaxValue || b.Location == Int32.MaxValue)
                Console.WriteLine("meant to be called on real pointstamps");

            for (int i = 0; i < a.Timestamp.Length; i++)
            {
                if (a.Timestamp[i] < b.Timestamp[i])
                {
                    return true;
                }
                else if (a.Timestamp[i] > b.Timestamp[i])
                {
                    return false;
                }
            }

            return true;
        }

        internal bool TrySerialize(SendBufferPage sendPage, SerializationFormat serializationFormat)
        {
            NaiadSerialization<Pointstamp> stampSerializer = serializationFormat.GetSerializer<Pointstamp>();
            return sendPage.Write(stampSerializer, this.maximalElement);
        }

        internal bool TryDeserialize(ref RecvBuffer source, SerializationFormat serializationFormat)
        {
            NaiadSerialization<Pointstamp> stampSerializer = serializationFormat.GetSerializer<Pointstamp>();
            return stampSerializer.TryDeserialize(ref source, out this.maximalElement);
        }

        /// <summary>
        /// shows a frontier as a string
        /// </summary>
        /// <returns>string representing the frontier</returns>
        public override string ToString()
        {
            if (this.Empty)
            {
                return "[Empty]";
            }
            else if (this.Complete)
            {
                return "[Complete]";
            }
            else
            {
                return "[" + this.maximalElement.Timestamp + "]";
            }
        }
    }
}
