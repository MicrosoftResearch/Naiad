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

namespace Microsoft.Research.Naiad.Frameworks.DifferentialDataflow.CollectionTrace
{
#if true
    /// <summary>
    /// Munge together an index into the heaps array (length) and an offset
    /// in that array (offset) into a single integer.
    /// 
    /// The top bit is reserved for flagging merged state offsets.
    /// </summary>
    public struct OffsetLength : IEquatable<OffsetLength>
    {
        public int offsetLength;

        public void GetOffsetLength(out int offset, out int length)
        {
            length = 0;
            while ((offsetLength >> length) % 2 == 0)
                length++;

            offset = offsetLength >> (length + 1);
        }

        int Offset
        {
            get
            {
                int o, l;
                GetOffsetLength(out o, out l);
                return o;
            }
        }

        public int Length
        {
            get
            {
                int o, l;
                GetOffsetLength(out o, out l);
                return l;
            }
        }

        public override string ToString()
        {
            if (this.offsetLength == 0)
                return "Null";
            else
                return String.Format("[o:{0}, l:{1}]", Offset, Length);
        }

        public bool Equals(OffsetLength other)
        {
            return this.offsetLength == other.offsetLength;
        }

        public bool IsEmpty { get { return offsetLength == 0; } }

        public OffsetLength(int ol)
        {
            offsetLength = ol;
        }

        public OffsetLength(int o, int l)
        {
            offsetLength = (o << (l + 1)) + (1 << l);
        }
    }

#endif
}
