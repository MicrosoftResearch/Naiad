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

namespace Microsoft.Research.Naiad
{

    public static class PairExtensionMethods
    {
        public static Pair<V1, V2> PairWith<V1, V2>(this V1 s, V2 t)
        {
            return new Pair<V1, V2>(s, t);
        }
    }

    public struct Pair<V1, V2> : IEquatable<Pair<V1, V2>>
    {
        public V1 v1;
        public V2 v2;

        public Pair(V1 ss, V2 tt) { v1 = ss; v2 = tt; }

        public override string ToString()
        {
            return "[" + v1.ToString() + " " + v2.ToString() + "]";
        }

        public override int GetHashCode()
        {
            return (this.v1 == null ? 0 : this.v1.GetHashCode()) + 123412324 * (this.v2 == null ? 0 : this.v2.GetHashCode());
        }

        public bool Equals(Pair<V1, V2> other)
        {
            return (EqualityComparer<V1>.Default.Equals(this.v1, other.v1) && EqualityComparer<V2>.Default.Equals(this.v2, other.v2));
        }
    }
}
