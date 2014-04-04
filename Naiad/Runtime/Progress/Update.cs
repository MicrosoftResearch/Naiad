using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace Microsoft.Research.Naiad.Runtime.Progress
{
    public struct Update
    {
        public Microsoft.Research.Naiad.Scheduling.Pointstamp Pointstamp;
        public Int64 Delta;

        public Update(Microsoft.Research.Naiad.Scheduling.Pointstamp p, Int64 d) { this.Pointstamp = p; this.Delta = d; }
    }
}
