using ClickHouse.Direct.Abstractions;
using ClickHouse.Direct.Types;
using Xunit.Abstractions;

namespace ClickHouse.Direct.IntegrationTests.Types;

public class StringTypeBlockIntegrationTests(ClickHouseContainerFixture fixture, ITestOutputHelper output)
    : TypeBlockIntegrationTestBase(fixture, output)
{
    [Theory]
    [MemberData(nameof(FormatNames))]
    public async Task BulkInsertAndSelect_ShouldRoundTrip(string formatName)
    {
        var tableName = GetSanitizedTableName($"test_string_block_{formatName.ToLower()}");
        await Transport.ExecuteNonQueryAsync($"DROP TABLE IF EXISTS {tableName}");
        await Transport.ExecuteNonQueryAsync($"""
            CREATE TABLE {tableName} (
                id Int32,
                content String
            ) ENGINE = Memory
            """);
        
        var columns = new List<ColumnDescriptor>
        {
            ColumnDescriptor.Create("id", new Int32Type()),
            ColumnDescriptor.Create("content", new StringType())
        };
        
        const int rowCount = 1000;
        var ids = Enumerable.Range(1, rowCount).ToList();
        var contents = new List<string>(rowCount);
        var random = new Random(42);
        
        for (var i = 0; i < rowCount; i++)
        {
            var length = random.Next(0, 100);
            if (length == 0)
            {
                contents.Add("");
            }
            else
            {
                var chars = new char[length];
                for (var j = 0; j < length; j++)
                    chars[j] = (char)random.Next(32, 127);
                contents.Add(new string(chars));
            }
        }
        
        var columnData = new List<System.Collections.IList> { ids, contents };
        var block = Block.CreateFromColumnData(columns, columnData, rowCount);
        
        Output.WriteLine($"Inserting {rowCount} strings using {formatName} format");
        await SendBlockDataAsync(tableName, formatName, block);
        
        var countStr = await GetScalarValueAsync($"SELECT COUNT(*) FROM {tableName}");
        Assert.Equal(rowCount.ToString(), countStr);
        
        var readBlock = await QueryBlockDataAsync(
            $"SELECT id, content FROM {tableName} ORDER BY id",
            formatName,
            rowCount,
            columns
        );
        
        Assert.Equal(rowCount, readBlock.RowCount);
        Assert.Equal(2, readBlock.ColumnCount);
        
        var readIds = (List<int>)readBlock.GetColumnData(0);
        var readContents = (List<string>)readBlock.GetColumnData(1);
        
        Assert.Equal(ids, readIds);
        Assert.Equal(contents, readContents);
        
        await Transport.ExecuteNonQueryAsync($"DROP TABLE {tableName}");
    }
    
    [Theory]
    [MemberData(nameof(FormatNames))]
    public async Task SpecialCharacters_ShouldHandleCorrectly(string formatName)
    {
        var tableName = GetSanitizedTableName($"test_string_special_{formatName.ToLower()}");
        await Transport.ExecuteNonQueryAsync($"DROP TABLE IF EXISTS {tableName}");
        await Transport.ExecuteNonQueryAsync($"""
            CREATE TABLE {tableName} (
                description String,
                value String
            ) ENGINE = Memory
            """);
        
        var columns = new List<ColumnDescriptor>
        {
            ColumnDescriptor.Create("description", new StringType()),
            ColumnDescriptor.Create("value", new StringType())
        };
        
        var descriptions = new List<string>
        {
            "Empty",
            "Single space",
            "Unicode",
            "Emoji",
            "Newlines and tabs",
            "Special chars",
            "Long string",
            "Mixed content"
        };
        
        var values = new List<string>
        {
            "",
            " ",
            "你好世界 Здравствуй мир",
            "😀😁😂🤣😃😄😅🚀",
            "Line1\nLine2\tTabbed\r\nCRLF",
            "!@#$%^&*()_+-=[]{}|;:'\",.<>?/\\",
            new('A', 10000),
            "Mixed: ABC123!@#你好🚀\n\t"
        };
        
        var columnData = new List<System.Collections.IList> { descriptions, values };
        var block = Block.CreateFromColumnData(columns, columnData, descriptions.Count);
        
        await SendBlockDataAsync(tableName, formatName, block);
        
        var readBlock = await QueryBlockDataAsync(
            $"SELECT description, value FROM {tableName} ORDER BY description",
            formatName,
            descriptions.Count,
            columns
        );
        
        var readDescriptions = (List<string>)readBlock.GetColumnData(0);
        var readValues = (List<string>)readBlock.GetColumnData(1);
        
        var sortedPairs = descriptions.Zip(values, (d, v) => new { Desc = d, Val = v })
            .OrderBy(p => p.Desc)
            .ToList();
        
        Assert.Equal(sortedPairs.Select(p => p.Desc).ToList(), readDescriptions);
        Assert.Equal(sortedPairs.Select(p => p.Val).ToList(), readValues);
        
        await Transport.ExecuteNonQueryAsync($"DROP TABLE {tableName}");
    }
    
    [Theory]
    [MemberData(nameof(FormatNames))]
    public async Task EmptyStrings_ShouldHandleCorrectly(string formatName)
    {
        var tableName = GetSanitizedTableName($"test_string_empty_{formatName.ToLower()}");
        await Transport.ExecuteNonQueryAsync($"DROP TABLE IF EXISTS {tableName}");
        await Transport.ExecuteNonQueryAsync($"""
            CREATE TABLE {tableName} (
                id Int32,
                value String
            ) ENGINE = Memory
            """);
        
        var columns = new List<ColumnDescriptor>
        {
            ColumnDescriptor.Create("id", new Int32Type()),
            ColumnDescriptor.Create("value", new StringType())
        };
        
        var ids = new List<int> { 1, 2, 3, 4, 5 };
        var values = new List<string> { "", "", "", "", "" };
        
        var columnData = new List<System.Collections.IList> { ids, values };
        var block = Block.CreateFromColumnData(columns, columnData, ids.Count);
        
        await SendBlockDataAsync(tableName, formatName, block);
        
        var countStr = await GetScalarValueAsync($"SELECT COUNT(*) FROM {tableName} WHERE value = ''");
        Assert.Equal("5", countStr);
        
        var readBlock = await QueryBlockDataAsync(
            $"SELECT id, value FROM {tableName} ORDER BY id",
            formatName,
            ids.Count,
            columns
        );
        
        var readIds = (List<int>)readBlock.GetColumnData(0);
        var readValues = (List<string>)readBlock.GetColumnData(1);
        
        Assert.Equal(ids, readIds);
        Assert.All(readValues, v => Assert.Equal("", v));
        
        await Transport.ExecuteNonQueryAsync($"DROP TABLE {tableName}");
    }
    
    [Theory]
    [MemberData(nameof(FormatNames))]
    public async Task LargeStrings_PerformanceComparison(string formatName)
    {
        var tableName = GetSanitizedTableName($"test_string_perf_{formatName.ToLower()}");
        await Transport.ExecuteNonQueryAsync($"DROP TABLE IF EXISTS {tableName}");
        await Transport.ExecuteNonQueryAsync($"""
            CREATE TABLE {tableName} (
                id Int32,
                small String,
                medium String,
                large String
            ) ENGINE = Memory
            """);
        
        var columns = new List<ColumnDescriptor>
        {
            ColumnDescriptor.Create("id", new Int32Type()),
            ColumnDescriptor.Create("small", new StringType()),
            ColumnDescriptor.Create("medium", new StringType()),
            ColumnDescriptor.Create("large", new StringType())
        };
        
        const int rowCount = 10000;
        var ids = Enumerable.Range(1, rowCount).ToList();
        var smallStrings = new List<string>(rowCount);
        var mediumStrings = new List<string>(rowCount);
        var largeStrings = new List<string>(rowCount);
        
        var random = new Random(42);
        for (var i = 0; i < rowCount; i++)
        {
            smallStrings.Add(GenerateRandomString(random, 10, 50));
            mediumStrings.Add(GenerateRandomString(random, 100, 500));
            largeStrings.Add(GenerateRandomString(random, 1000, 5000));
        }
        
        var columnData = new List<System.Collections.IList> { ids, smallStrings, mediumStrings, largeStrings };
        var block = Block.CreateFromColumnData(columns, columnData, rowCount);
        
        var sw = System.Diagnostics.Stopwatch.StartNew();
        await SendBlockDataAsync(tableName, formatName, block);
        sw.Stop();
        
        Output.WriteLine($"{formatName} format: Inserted {rowCount} rows with strings in {sw.Elapsed.TotalMilliseconds:F2}ms");
        
        sw.Restart();
        var readBlock = await QueryBlockDataAsync(
            $"SELECT id, small, medium, large FROM {tableName} WHERE id <= 100 ORDER BY id",
            formatName,
            100,
            columns
        );
        sw.Stop();
        
        Output.WriteLine($"{formatName} format: Read 100 rows in {sw.Elapsed.TotalMilliseconds:F2}ms");
        
        Assert.Equal(100, readBlock.RowCount);
        
        var avgLengthStr = await GetScalarValueAsync($"SELECT AVG(LENGTH(small) + LENGTH(medium) + LENGTH(large)) FROM {tableName}");
        Output.WriteLine($"Average total string length per row: {double.Parse(avgLengthStr):F2}");
        
        await Transport.ExecuteNonQueryAsync($"DROP TABLE {tableName}");
    }
    
    private static string GenerateRandomString(Random random, int minLength, int maxLength)
    {
        var length = random.Next(minLength, maxLength + 1);
        var chars = new char[length];
        for (var i = 0; i < length; i++)
            chars[i] = (char)random.Next(32, 127);
        return new string(chars);
    }
}