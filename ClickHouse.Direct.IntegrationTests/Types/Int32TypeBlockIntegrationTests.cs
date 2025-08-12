using ClickHouse.Direct.Abstractions;
using ClickHouse.Direct.Types;
using Xunit.Abstractions;

namespace ClickHouse.Direct.IntegrationTests.Types;

public class Int32TypeBlockIntegrationTests(ClickHouseContainerFixture fixture, ITestOutputHelper output)
    : TypeBlockIntegrationTestBase(fixture, output)
{
    [Theory]
    [MemberData(nameof(FormatNames))]
    public async Task BulkInsertAndSelect_ShouldRoundTrip(string formatName)
    {
        var tableName = GetSanitizedTableName($"test_int32_block_{formatName.ToLower()}");
        await Transport.ExecuteNonQueryAsync($"DROP TABLE IF EXISTS {tableName}");
        await Transport.ExecuteNonQueryAsync($"""
            CREATE TABLE {tableName} (
                id Int32,
                value Int32
            ) ENGINE = Memory
            """);
        
        var columns = new List<ColumnDescriptor>
        {
            ColumnDescriptor.Create("id", new Int32Type()),
            ColumnDescriptor.Create("value", new Int32Type())
        };
        
        const int rowCount = 1000;
        var ids = Enumerable.Range(1, rowCount).ToList();
        var values = new List<int>(rowCount);
        var random = new Random(42);
        
        for (var i = 0; i < rowCount; i++)
            values.Add(random.Next(int.MinValue, int.MaxValue));
        
        var columnData = new List<System.Collections.IList> { ids, values };
        var block = Block.CreateFromColumnData(columns, columnData, rowCount);
        
        Output.WriteLine($"Inserting {rowCount} rows using {formatName} format");
        await SendBlockDataAsync(tableName, formatName, block);
        
        var countStr = await GetScalarValueAsync($"SELECT COUNT(*) FROM {tableName}");
        Assert.Equal(rowCount.ToString(), countStr);
        
        var readBlock = await QueryBlockDataAsync(
            $"SELECT id, value FROM {tableName} ORDER BY id",
            formatName,
            rowCount,
            columns
        );
        
        Assert.Equal(rowCount, readBlock.RowCount);
        Assert.Equal(2, readBlock.ColumnCount);
        
        var readIds = (List<int>)readBlock.GetColumnData(0);
        var readValues = (List<int>)readBlock.GetColumnData(1);
        
        Assert.Equal(ids, readIds);
        Assert.Equal(values, readValues);
        
        await Transport.ExecuteNonQueryAsync($"DROP TABLE {tableName}");
    }
    
    [Theory]
    [MemberData(nameof(FormatNames))]
    public async Task BoundaryValues_ShouldHandleCorrectly(string formatName)
    {
        var tableName = GetSanitizedTableName($"test_int32_boundary_{formatName.ToLower()}");
        await Transport.ExecuteNonQueryAsync($"DROP TABLE IF EXISTS {tableName}");
        await Transport.ExecuteNonQueryAsync($"""
            CREATE TABLE {tableName} (
                description String,
                value Int32
            ) ENGINE = Memory
            """);
        
        var columns = new List<ColumnDescriptor>
        {
            ColumnDescriptor.Create("description", new StringType()),
            ColumnDescriptor.Create("value", new Int32Type())
        };
        
        var descriptions = new List<string>
        {
            "Min Value",
            "Max Value",
            "Zero",
            "Negative One",
            "Positive One"
        };
        
        var values = new List<int>
        {
            int.MinValue,
            int.MaxValue,
            0,
            -1,
            1
        };
        
        var columnData = new List<System.Collections.IList> { descriptions, values };
        var block = Block.CreateFromColumnData(columns, columnData, descriptions.Count);
        
        await SendBlockDataAsync(tableName, formatName, block);
        
        var readBlock = await QueryBlockDataAsync(
            $"SELECT description, value FROM {tableName} ORDER BY value",
            formatName,
            descriptions.Count,
            columns
        );
        
        var readDescriptions = (List<string>)readBlock.GetColumnData(0);
        var readValues = (List<int>)readBlock.GetColumnData(1);
        
        var sortedPairs = descriptions.Zip(values, (d, v) => new { Desc = d, Val = v })
            .OrderBy(p => p.Val)
            .ToList();
        
        Assert.Equal(sortedPairs.Select(p => p.Desc).ToList(), readDescriptions);
        Assert.Equal(sortedPairs.Select(p => p.Val).ToList(), readValues);
        
        await Transport.ExecuteNonQueryAsync($"DROP TABLE {tableName}");
    }
    
    [Theory]
    [MemberData(nameof(FormatNames))]
    public async Task EmptyBlock_ShouldHandleCorrectly(string formatName)
    {
        var tableName = GetSanitizedTableName($"test_int32_empty_{formatName.ToLower()}");
        await Transport.ExecuteNonQueryAsync($"DROP TABLE IF EXISTS {tableName}");
        await Transport.ExecuteNonQueryAsync($"""
            CREATE TABLE {tableName} (
                value Int32
            ) ENGINE = Memory
            """);
        
        var columns = new List<ColumnDescriptor>
        {
            ColumnDescriptor.Create("value", new Int32Type())
        };
        
        var values = new List<int>();
        var columnData = new List<System.Collections.IList> { values };
        var block = Block.CreateFromColumnData(columns, columnData, 0);
        
        await SendBlockDataAsync(tableName, formatName, block);
        
        var countStr = await GetScalarValueAsync($"SELECT COUNT(*) FROM {tableName}");
        Assert.Equal("0", countStr);
        
        await Transport.ExecuteNonQueryAsync($"DROP TABLE {tableName}");
    }
    
    [Theory]
    [MemberData(nameof(FormatNames))]
    public async Task LargeDataset_PerformanceComparison(string formatName)
    {
        var tableName = GetSanitizedTableName($"test_int32_perf_{formatName.ToLower()}");
        await Transport.ExecuteNonQueryAsync($"DROP TABLE IF EXISTS {tableName}");
        await Transport.ExecuteNonQueryAsync($"""
            CREATE TABLE {tableName} (
                seq Int32,
                random Int32
            ) ENGINE = Memory
            """);
        
        var columns = new List<ColumnDescriptor>
        {
            ColumnDescriptor.Create("seq", new Int32Type()),
            ColumnDescriptor.Create("random", new Int32Type())
        };
        
        const int rowCount = 100000;
        var seqValues = Enumerable.Range(1, rowCount).ToList();
        var randomValues = new List<int>(rowCount);
        var random = new Random(42);
        
        for (var i = 0; i < rowCount; i++)
            randomValues.Add(random.Next(int.MinValue, int.MaxValue));
        
        var columnData = new List<System.Collections.IList> { seqValues, randomValues };
        var block = Block.CreateFromColumnData(columns, columnData, rowCount);
        
        var sw = System.Diagnostics.Stopwatch.StartNew();
        await SendBlockDataAsync(tableName, formatName, block);
        sw.Stop();
        
        Output.WriteLine($"{formatName} format: Inserted {rowCount} rows in {sw.Elapsed.TotalMilliseconds:F2}ms");
        
        sw.Restart();
        var readBlock = await QueryBlockDataAsync(
            $"SELECT seq, random FROM {tableName} WHERE seq <= 1000 ORDER BY seq",
            formatName,
            1000,
            columns
        );
        sw.Stop();
        
        Output.WriteLine($"{formatName} format: Read 1000 rows in {sw.Elapsed.TotalMilliseconds:F2}ms");
        
        Assert.Equal(1000, readBlock.RowCount);
        
        await Transport.ExecuteNonQueryAsync($"DROP TABLE {tableName}");
    }
}