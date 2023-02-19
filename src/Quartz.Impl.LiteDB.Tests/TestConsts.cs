using System;

namespace Quartz.Impl.LiteDB.Tests
{
    public static class TestConsts
    {
        public const string Barrier = "BARRIER";
        public const string DateStamps = "DATE_STAMPS";
        public static readonly TimeSpan TestTimeout = TimeSpan.FromSeconds(125);

        public static string GenerateFilename => $"test_{DateTimeOffset.UtcNow.Ticks}.db";
    }
}