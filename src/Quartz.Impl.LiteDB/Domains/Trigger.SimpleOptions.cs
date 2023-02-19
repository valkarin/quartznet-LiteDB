using System;

namespace Quartz.Impl.LiteDB.Domains
{
    public partial class Trigger
    {
        public class SimpleOptions
        {
            public int RepeatCount { get; set; }
            public TimeSpan RepeatInterval { get; set; }
        }
    }
}