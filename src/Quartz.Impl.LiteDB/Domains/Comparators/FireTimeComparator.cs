using System.Collections.Generic;

namespace Quartz.Impl.LiteDB.Domains.Comparators
{
    public class FireTimeComparator : IComparer<Trigger>
    {
        public int Compare(Trigger trig1, Trigger trig2)
        {
            var t1 = trig1.NextFireTimeUtc;
            var t2 = trig2.NextFireTimeUtc;

            if (t1 != null || t2 != null)
            {
                if (t1 == null) return 1;

                if (t2 == null) return -1;

                if (t1 < t2) return -1;

                if (t1 > t2) return 1;
            }

            var comp = trig2.Priority - trig1.Priority;
            return comp != 0 ? comp : 0;
        }
    }
}