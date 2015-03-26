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

using System.IO;
using Microsoft.Research.Naiad;

namespace Microsoft.Research.Naiad.Frameworks.DifferentialDataflow
{
    /// <summary>
    /// A record with a signed 64-bit weight, which corresponds to the multiplicity of the record
    /// in a multiset.
    /// </summary>
    /// <typeparam name="TRecord">The type of the record.</typeparam>
    public struct Weighted<TRecord> : IEquatable<Weighted<TRecord>> where TRecord : IEquatable<TRecord>
    {
        /// <summary>
        /// The record.
        /// </summary>
        public TRecord record;

        /// <summary>
        /// The weight.
        /// </summary>
        public Int64 weight;

        /// <summary>
        /// Returns a string representation of this weighted record.
        /// </summary>
        /// <returns>A string representation of this weighted record.</returns>
        public override string ToString()
        {
            if (weight > 0)
                return String.Format("[ {0}, +{1} ]", record.ToString(), weight);
            else
                return String.Format("[ {0}, {1} ]", record.ToString(), weight);
        }

        /// <summary>
        /// Returns <c>true</c> if and only if this and the <paramref name="other"/> object have equal records and weights.
        /// </summary>
        /// <param name="other">The other object.</param>
        /// <returns><c>true</c> if and only if this and the <paramref name="other"/> object have equal records and weights.</returns>
        public bool Equals(Weighted<TRecord> other)
        {
            return this.weight == other.weight && this.record.Equals(other.record);
        }

        /// <summary>
        /// Constructs a new weighted object from the given <paramref name="record"/> and <paramref name="weight"/>.
        /// </summary>
        /// <param name="record">The record.</param>
        /// <param name="weight">The weight.</param>
        public Weighted(TRecord record, Int64 weight) { this.record = record; this.weight = weight; }
    }
}
