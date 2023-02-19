using System;
using System.IO;

namespace Quartz.Impl.LiteDB.Tests
{
    public class TestCleanUp : IDisposable
    {
        public void Dispose()
        {
            var path = AppDomain.CurrentDomain.BaseDirectory;
            try
            {
                var dbFiles = Directory.GetFiles(path, "test_*.db", new EnumerationOptions
                {
                    IgnoreInaccessible = true
                });
                foreach (var dbFile in dbFiles)
                {
                    File.Delete(dbFile);
                }
            }
            catch
            {
                // ignore
            }
        }
    }
}