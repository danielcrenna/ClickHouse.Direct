namespace ClickHouse.Direct.IntegrationTests;

public static class TableNameExtensions
{
    public static string SanitizeForTfm(this string tableName)
    {
        var tfm = GetCurrentTfm();
        return $"{tableName}_{tfm}";
    }
    
    public static string GenerateTableName()
    {
        return $"test_{Guid.NewGuid():N}".SanitizeForTfm();
    }
    
    private static string GetCurrentTfm()
    {
#if NET9_0
        return "net9";
#elif NET8_0
        return "net8";
#elif NET7_0
        return "net7";
#elif NET6_0
        return "net6";
#else
        var version = Environment.Version;
        return $"net{version.Major}_{version.Minor}";
#endif
    }
}