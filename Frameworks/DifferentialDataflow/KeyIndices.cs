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
using Microsoft.Research.Naiad.CodeGeneration;

namespace Microsoft.Research.Naiad.Frameworks.DifferentialDataflow
{

    public struct UnaryKeyIndices : IEquatable<UnaryKeyIndices>
    {
        public int unprocessed;
        public int processed;
        public int output;

        public bool Equals(UnaryKeyIndices other)
        {
            return this.unprocessed == other.unprocessed && this.processed == other.processed && this.output == other.output;
        }

        public bool IsEmpty { get { return unprocessed == 0 && processed == 0 && output == 0; } }

        public UnaryKeyIndices(int u, int p, int r) { unprocessed = u; processed = p; output = r; }
    }

    public struct BinaryKeyIndices : IEquatable<BinaryKeyIndices>
    {
        //public K key;
        public int unprocessed1;
        public int unprocessed2;
        public int processed1;
        public int processed2;
        public int output;

        public bool Equals(BinaryKeyIndices other)
        {
            return this.unprocessed1 == other.unprocessed1
                && this.unprocessed2 == other.unprocessed2
                && this.processed1 == other.processed1
                && this.processed2 == other.processed2
                && this.output == other.output;
        }

        public BinaryKeyIndices(int u1, int u2, int p1, int p2, int r) { unprocessed1 = u1; unprocessed2 = u2; processed1 = p1; processed2 = p2; output = r; }
    }

    // Join doesn't need all the KeyIndices cruft
    public struct JoinKeyIndices : IEquatable<JoinKeyIndices>
    {
        public int processed1;
        public int processed2;

        public bool IsEmpty { get { return processed1 == 0 && processed2 == 0; } }

        public bool Equals(JoinKeyIndices other)
        {
            return this.processed1 == other.processed1 && this.processed2 == other.processed2;
        }

        public JoinKeyIndices(int p1, int p2) { processed1 = p1; processed2 = p2; }
    }

    // Join doesn't need all the KeyIndices cruft
    public struct JoinIntKeyIndices
    {
        public int processed1;
        public int processed2;

        public bool IsEmpty { get { return processed1 == 0 && processed2 == 0; } }

        public JoinIntKeyIndices(int p1, int p2) { processed1 = p1; processed2 = p2; }

    }

}
