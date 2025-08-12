using ClickHouse.Direct.Abstractions;
using ClickHouse.Direct.Types;
using Xunit.Abstractions;

namespace ClickHouse.Direct.IntegrationTests.Types;

public class Int8TypeBlockIntegrationTests(ClickHouseContainerFixture fixture, ITestOutputHelper output)
    : TypeBlockIntegrationTestBase(fixture, output)
{
    [Theory]
    [MemberData(nameof(FormatNames))]
    public async Task BulkInsertAndSelect_ShouldRoundTrip(string formatName)
    {
        var tableName = GetSanitizedTableName($"test_int8_block_{formatName.ToLower()}");
        await Transport.ExecuteNonQueryAsync($"DROP TABLE IF EXISTS {tableName}");
        await Transport.ExecuteNonQueryAsync($"""
            CREATE TABLE {tableName} (
                id Int32,
                value Int8
            ) ENGINE = Memory
            """);
        
        var columns = new List<ColumnDescriptor>
        {
            ColumnDescriptor.Create("id", new Int32Type()),
            ColumnDescriptor.Create("value", new Int8Type())
        };
        
        const int rowCount = 1000;
        var ids = Enumerable.Range(1, rowCount).ToList();
        var values = new List<sbyte>(rowCount);
        var random = new Random(42);
        
        for (var i = 0; i < rowCount; i++)
            values.Add((sbyte)random.Next(sbyte.MinValue, sbyte.MaxValue + 1));
        
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
        var readValues = (List<sbyte>)readBlock.GetColumnData(1);
        
        Assert.Equal(ids, readIds);
        Assert.Equal(values, readValues);
        
        await Transport.ExecuteNonQueryAsync($"DROP TABLE {tableName}");
    }
    
    [Theory]
    [MemberData(nameof(FormatNames))]
    public async Task BoundaryValues_ShouldHandleCorrectly(string formatName)
    {
        var tableName = GetSanitizedTableName($"test_int8_boundary_{formatName.ToLower()}");
        await Transport.ExecuteNonQueryAsync($"DROP TABLE IF EXISTS {tableName}");
        await Transport.ExecuteNonQueryAsync($"""
            CREATE TABLE {tableName} (
                description String,
                value Int8
            ) ENGINE = Memory
            """);
        
        var columns = new List<ColumnDescriptor>
        {
            ColumnDescriptor.Create("description", new StringType()),
            ColumnDescriptor.Create("value", new Int8Type())
        };
        
        var descriptions = new List<string> { "Min", "Max", "Zero", "Negative", "Positive" };
        var values = new List<sbyte> { sbyte.MinValue, sbyte.MaxValue, 0, -42, 42 };
        
        var columnData = new List<System.Collections.IList> { descriptions, values };
        var block = Block.CreateFromColumnData(columns, columnData, descriptions.Count);
        
        await SendBlockDataAsync(tableName, formatName, block);
        
        var minStr = await GetScalarValueAsync($"SELECT value FROM {tableName} WHERE description = 'Min'");
        var maxStr = await GetScalarValueAsync($"SELECT value FROM {tableName} WHERE description = 'Max'");
        var zeroStr = await GetScalarValueAsync($"SELECT value FROM {tableName} WHERE description = 'Zero'");
        
        Assert.Equal(sbyte.MinValue.ToString(), minStr);
        Assert.Equal(sbyte.MaxValue.ToString(), maxStr);
        Assert.Equal("0", zeroStr);
        
        await Transport.ExecuteNonQueryAsync($"DROP TABLE {tableName}");
    }
    
    [Theory]
    [MemberData(nameof(FormatNames))]
    public async Task EmptyBlock_ShouldHandleCorrectly(string formatName)
    {
        var tableName = GetSanitizedTableName($"test_int8_empty_{formatName.ToLower()}");
        await Transport.ExecuteNonQueryAsync($"DROP TABLE IF EXISTS {tableName}");
        await Transport.ExecuteNonQueryAsync($"""
            CREATE TABLE {tableName} (
                value Int8
            ) ENGINE = Memory
            """);
        
        var columns = new List<ColumnDescriptor>
        {
            ColumnDescriptor.Create("value", new Int8Type())
        };
        
        var values = new List<sbyte>();
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
        var tableName = GetSanitizedTableName($"test_int8_large_{formatName.ToLower()}");
        await Transport.ExecuteNonQueryAsync($"DROP TABLE IF EXISTS {tableName}");
        await Transport.ExecuteNonQueryAsync($"""
            CREATE TABLE {tableName} (
                id Int32,
                value Int8,
                flag Int8
            ) ENGINE = Memory
            """);
        
        var columns = new List<ColumnDescriptor>
        {
            ColumnDescriptor.Create("id", new Int32Type()),
            ColumnDescriptor.Create("value", new Int8Type()),
            ColumnDescriptor.Create("flag", new Int8Type())
        };
        
        const int rowCount = 50000;
        var ids = new List<int>(rowCount);
        var values = new List<sbyte>(rowCount);
        var flags = new List<sbyte>(rowCount);
        var random = new Random(42);
        
        for (var i = 0; i < rowCount; i++)
        {
            ids.Add(i + 1);
            values.Add((sbyte)random.Next(sbyte.MinValue, sbyte.MaxValue + 1));
            flags.Add((sbyte)(i % 2));
        }
        
        var columnData = new List<System.Collections.IList> { ids, values, flags };
        var block = Block.CreateFromColumnData(columns, columnData, rowCount);
        
        var sw = System.Diagnostics.Stopwatch.StartNew();
        await SendBlockDataAsync(tableName, formatName, block);
        sw.Stop();
        
        Output.WriteLine($"Inserted {rowCount} rows in {sw.Elapsed.TotalMilliseconds:F2}ms using {formatName}");
        
        var countStr = await GetScalarValueAsync($"SELECT COUNT(*) FROM {tableName}");
        Assert.Equal(rowCount.ToString(), countStr);
        
        var avgStr = await GetScalarValueAsync($"SELECT ROUND(AVG(value)) FROM {tableName}");
        Output.WriteLine($"Average value: {avgStr}");
        
        sw.Restart();
        var readBlock = await QueryBlockDataAsync(
            $"SELECT id, value, flag FROM {tableName} WHERE flag = 1 LIMIT 1000",
            formatName,
            1000,
            columns
        );
        sw.Stop();
        
        Output.WriteLine($"Read 1000 filtered rows in {sw.Elapsed.TotalMilliseconds:F2}ms");
        
        Assert.True(readBlock.RowCount <= 1000);
        var readFlags = (List<sbyte>)readBlock.GetColumnData(2);
        Assert.All(readFlags, flag => Assert.Equal(1, flag));
        
        await Transport.ExecuteNonQueryAsync($"DROP TABLE {tableName}");
    }
}