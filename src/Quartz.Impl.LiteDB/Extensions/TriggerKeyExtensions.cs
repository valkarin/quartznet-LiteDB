namespace Quartz.Impl.LiteDB.Extensions
{
    internal static class TriggerKeyExtensions
    {
        public static string GetDatabaseId(this TriggerKey triggerKey)
        {
            return $"{triggerKey.Name}/{triggerKey.Group}";
        }
    }
}