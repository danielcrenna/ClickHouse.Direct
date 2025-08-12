namespace ClickHouse.Direct.IntegrationTests;

public class TableNameExtensionsTests
{
    [Fact]
    public void SanitizeForTfm_AppendsCorrectTfmSuffix()
    {
        const string tableName = "test_table";
        var sanitized = tableName.SanitizeForTfm();
        
#if NET9_0
        Assert.Equal("test_table_net9", sanitized);
#elif NET8_0
        Assert.Equal("test_table_net8", sanitized);
#elif NET7_0
        Assert.Equal("test_table_net7", sanitized);
#elif NET6_0
        Assert.Equal("test_table_net6", sanitized);
#else
        Assert.StartsWith("test_table_net", sanitized);
#endif
    }
    
    [Theory]
    [InlineData("my_table", "my_table_net")]
    [InlineData("test_int32_bulk", "test_int32_bulk_net")]
    [InlineData("test_perf_100", "test_perf_100_net")]
    public void SanitizeForTfm_PreservesOriginalTableNameStructure(string originalName, string expectedPrefix)
    {
        var sanitized = originalName.SanitizeForTfm();
        Assert.StartsWith(expectedPrefix, sanitized);
    }
    
    [Fact]
    public void SanitizeForTfm_IsConsistent()
    {
        const string tableName = "consistent_test";
        var first = tableName.SanitizeForTfm();
        var second = tableName.SanitizeForTfm();
        
        Assert.Equal(first, second);
    }
}