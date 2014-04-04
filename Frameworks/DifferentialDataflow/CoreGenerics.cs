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

using System.IO;
using Microsoft.Research.Naiad;

namespace Microsoft.Research.Naiad.Frameworks.DifferentialDataflow
{
    public struct Weighted<T> : IEquatable<Weighted<T>> where T : IEquatable<T>
    {
        public T record;
        public Int64 weight;

        public override string ToString()
        {
            if (weight > 0)
                return String.Format("[ {0}, +{1} ]", record.ToString(), weight);
            else
                return String.Format("[ {0}, {1} ]", record.ToString(), weight);
        }

        public bool Equals(Weighted<T> that)
        {
            return this.weight == that.weight && this.record.Equals(that.record);
        }

        public Weighted(T r, Int64 w) { record = r; weight = w; }
    }

    public struct NaiadRecord<S, T> : IEquatable<NaiadRecord<S,T>>
        where S : IEquatable<S>
        where T : Time<T>
    {
        public S record;
        public Int64 weight;
        public T time;

        public bool Equals(NaiadRecord<S,T> that)
        {
            return record.Equals(that.record) && weight == that.weight && time.Equals(that.time);
        }

        public NaiadRecord<S,T> Negate() { return new NaiadRecord<S,T>(record, -weight, time); }

        public override int GetHashCode() { return record.GetHashCode(); }

        public NaiadRecord(S r, Int64 w, T t) { record = r; weight = w; time = t; }

        public override string ToString()
        {
            if (weight >= 0)
                return string.Format("[{0}, +{1}, {2}]", record, weight, time);

            return string.Format("[{0}, {1}, {2}]", record, weight, time);
        }

        public Weighted<S> ToWeighted() { return new Weighted<S> { record = record, weight = weight }; }
    }
}
