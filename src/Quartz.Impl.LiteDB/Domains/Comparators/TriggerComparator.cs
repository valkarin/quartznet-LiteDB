using System;
using System.Collections.Generic;

namespace Quartz.Impl.LiteDB.Domains.Comparators
{
    internal class TriggerComparator : IComparer<Trigger>, IEquatable<TriggerComparator>
    {
        private readonly FireTimeComparator _ftc = new FireTimeComparator();

        public int Compare(Trigger trig1, Trigger trig2)
        {
            return _ftc.Compare(trig1, trig2);
        }

        /// <summary>
        ///     Indicates whether the current object is equal to another object of the same type.
        /// </summary>
        /// <returns>
        ///     true if the current object is equal to the <paramref name="other" /> parameter; otherwise, false.
        /// </returns>
        /// <param name="other">An object to compare with this object.</param>
        public bool Equals(TriggerComparator other)
        {
            return true;
        }

        public override bool Equals(object obj)
        {
            return obj is TriggerComparator;
        }

        /// <summary>
        ///     Serves as a hash function for a particular type.
        /// </summary>
        /// <returns>
        ///     A hash code for the current <see cref="T:System.Object" />.
        /// </returns>
        /// <filterpriority>2</filterpriority>
        public override int GetHashCode()
        {
            return _ftc?.GetHashCode() ?? 0;
        }
    }
}