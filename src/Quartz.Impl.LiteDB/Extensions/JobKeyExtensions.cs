namespace Quartz.Impl.LiteDB.Extensions
{
    internal static class JobKeyExtensions
    {
        public static string GetDatabaseId(this JobKey jobKey)
        {
            return $"{jobKey.Name}/{jobKey.Group}";
        }
    }
}