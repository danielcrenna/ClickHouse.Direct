using ClickHouse.Direct.Abstractions;
using ClickHouse.Direct.Types;
using Xunit.Abstractions;

namespace ClickHouse.Direct.IntegrationTests.Types;

public class UuidTypeBlockIntegrationTests(ClickHouseContainerFixture fixture, ITestOutputHelper output)
    : TypeBlockIntegrationTestBase(fixture, output)
{
    [Theory]
    [MemberData(nameof(FormatNames))]
    public async Task BulkInsertAndSelect_ShouldRoundTrip(string formatName)
    {
        var tableName = GetSanitizedTableName($"test_uuid_block_{formatName.ToLower()}");
        await Transport.ExecuteNonQueryAsync($"DROP TABLE IF EXISTS {tableName}");
        await Transport.ExecuteNonQueryAsync($"""
            CREATE TABLE {tableName} (
                id Int32,
                uuid_value UUID
            ) ENGINE = Memory
            """);
        
        var columns = new List<ColumnDescriptor>
        {
            ColumnDescriptor.Create("id", new Int32Type()),
            ColumnDescriptor.Create("uuid_value", new UuidType())
        };
        
        const int rowCount = 1000;
        var ids = Enumerable.Range(1, rowCount).ToList();
        var uuids = new List<Guid>(rowCount);
        
        for (var i = 0; i < rowCount; i++)
            uuids.Add(Guid.NewGuid());
        
        var columnData = new List<System.Collections.IList> { ids, uuids };
        var block = Block.CreateFromColumnData(columns, columnData, rowCount);
        
        Output.WriteLine($"Inserting {rowCount} UUIDs using {formatName} format");
        await SendBlockDataAsync(tableName, formatName, block);
        
        var countStr = await GetScalarValueAsync($"SELECT COUNT(*) FROM {tableName}");
        Assert.Equal(rowCount.ToString(), countStr);
        
        var readBlock = await QueryBlockDataAsync(
            $"SELECT id, uuid_value FROM {tableName} ORDER BY id",
            formatName,
            rowCount,
            columns
        );
        
        Assert.Equal(rowCount, readBlock.RowCount);
        Assert.Equal(2, readBlock.ColumnCount);
        
        var readIds = (List<int>)readBlock.GetColumnData(0);
        var readUuids = (List<Guid>)readBlock.GetColumnData(1);
        
        Assert.Equal(ids, readIds);
        Assert.Equal(uuids, readUuids);
        
        await Transport.ExecuteNonQueryAsync($"DROP TABLE {tableName}");
    }
    
    [Theory]
    [MemberData(nameof(FormatNames))]
    public async Task SpecialUuids_ShouldHandleCorrectly(string formatName)
    {
        var tableName = GetSanitizedTableName($"test_uuid_special_{formatName.ToLower()}");
        await Transport.ExecuteNonQueryAsync($"DROP TABLE IF EXISTS {tableName}");
        await Transport.ExecuteNonQueryAsync($"""
            CREATE TABLE {tableName} (
                description String,
                uuid_value UUID
            ) ENGINE = Memory
            """);
        
        var columns = new List<ColumnDescriptor>
        {
            ColumnDescriptor.Create("description", new StringType()),
            ColumnDescriptor.Create("uuid_value", new UuidType())
        };
        
        var descriptions = new List<string>
        {
            "Empty GUID",
            "All ones",
            "All twos",
            "Sequential 1",
            "Sequential 2",
            "Sequential 3",
            "Max value",
            "Random 1",
            "Random 2"
        };
        
        var uuids = new List<Guid>
        {
            Guid.Empty,
            Guid.Parse("11111111-1111-1111-1111-111111111111"),
            Guid.Parse("22222222-2222-2222-2222-222222222222"),
            Guid.Parse("550e8400-e29b-41d4-a716-446655440001"),
            Guid.Parse("550e8400-e29b-41d4-a716-446655440002"),
            Guid.Parse("550e8400-e29b-41d4-a716-446655440003"),
            Guid.Parse("ffffffff-ffff-ffff-ffff-ffffffffffff"),
            Guid.NewGuid(),
            Guid.NewGuid()
        };
        
        var columnData = new List<System.Collections.IList> { descriptions, uuids };
        var block = Block.CreateFromColumnData(columns, columnData, descriptions.Count);
        
        await SendBlockDataAsync(tableName, formatName, block);
        
        var readBlock = await QueryBlockDataAsync(
            $"SELECT description, uuid_value FROM {tableName} ORDER BY description",
            formatName,
            descriptions.Count,
            columns
        );
        
        var readDescriptions = (List<string>)readBlock.GetColumnData(0);
        var readUuids = (List<Guid>)readBlock.GetColumnData(1);
        
        var sortedPairs = descriptions.Zip(uuids, (d, u) => new { Desc = d, Uuid = u })
            .OrderBy(p => p.Desc)
            .ToList();
        
        Assert.Equal(sortedPairs.Select(p => p.Desc).ToList(), readDescriptions);
        
        for (var i = 0; i < sortedPairs.Count; i++)
        {
            if (!sortedPairs[i].Desc.StartsWith("Random"))
            {
                Assert.Equal(sortedPairs[i].Uuid, readUuids[i]);
            }
        }
        
        await Transport.ExecuteNonQueryAsync($"DROP TABLE {tableName}");
    }
    
    [Theory]
    [MemberData(nameof(FormatNames))]
    public async Task DuplicateUuids_ShouldHandleCorrectly(string formatName)
    {
        var tableName = GetSanitizedTableName($"test_uuid_duplicates_{formatName.ToLower()}");
        await Transport.ExecuteNonQueryAsync($"DROP TABLE IF EXISTS {tableName}");
        await Transport.ExecuteNonQueryAsync($"""
            CREATE TABLE {tableName} (
                id Int32,
                uuid_value UUID
            ) ENGINE = Memory
            """);
        
        var columns = new List<ColumnDescriptor>
        {
            ColumnDescriptor.Create("id", new Int32Type()),
            ColumnDescriptor.Create("uuid_value", new UuidType())
        };
        
        var duplicateUuid = Guid.Parse("12345678-1234-1234-1234-123456789abc");
        const int rowCount = 100;
        
        var ids = Enumerable.Range(1, rowCount).ToList();
        var uuids = new List<Guid>(rowCount);
        
        for (var i = 0; i < rowCount; i++)
        {
            uuids.Add(i % 10 == 0 ? duplicateUuid : Guid.NewGuid());
        }
        
        var columnData = new List<System.Collections.IList> { ids, uuids };
        var block = Block.CreateFromColumnData(columns, columnData, rowCount);
        
        await SendBlockDataAsync(tableName, formatName, block);
        
        var duplicateCountStr = await GetScalarValueAsync(
            $"SELECT COUNT(*) FROM {tableName} WHERE uuid_value = '{duplicateUuid}'"
        );
        Assert.Equal("10", duplicateCountStr);
        
        var uniqueCountStr = await GetScalarValueAsync(
            $"SELECT COUNT(DISTINCT uuid_value) FROM {tableName}"
        );
        Assert.Equal("91", uniqueCountStr);
        
        await Transport.ExecuteNonQueryAsync($"DROP TABLE {tableName}");
    }
    
    [Theory]
    [MemberData(nameof(FormatNames))]
    public async Task LargeDataset_PerformanceComparison(string formatName)
    {
        var tableName = GetSanitizedTableName($"test_uuid_perf_{formatName.ToLower()}");
        await Transport.ExecuteNonQueryAsync($"DROP TABLE IF EXISTS {tableName}");
        await Transport.ExecuteNonQueryAsync($"""
            CREATE TABLE {tableName} (
                seq Int32,
                uuid1 UUID,
                uuid2 UUID,
                uuid3 UUID
            ) ENGINE = Memory
            """);
        
        var columns = new List<ColumnDescriptor>
        {
            ColumnDescriptor.Create("seq", new Int32Type()),
            ColumnDescriptor.Create("uuid1", new UuidType()),
            ColumnDescriptor.Create("uuid2", new UuidType()),
            ColumnDescriptor.Create("uuid3", new UuidType())
        };
        
        const int rowCount = 50000;
        var seqValues = Enumerable.Range(1, rowCount).ToList();
        var uuid1Values = new List<Guid>(rowCount);
        var uuid2Values = new List<Guid>(rowCount);
        var uuid3Values = new List<Guid>(rowCount);
        
        for (var i = 0; i < rowCount; i++)
        {
            uuid1Values.Add(Guid.NewGuid());
            uuid2Values.Add(Guid.NewGuid());
            uuid3Values.Add(Guid.NewGuid());
        }
        
        var columnData = new List<System.Collections.IList> { seqValues, uuid1Values, uuid2Values, uuid3Values };
        var block = Block.CreateFromColumnData(columns, columnData, rowCount);
        
        var sw = System.Diagnostics.Stopwatch.StartNew();
        await SendBlockDataAsync(tableName, formatName, block);
        sw.Stop();
        
        Output.WriteLine($"{formatName} format: Inserted {rowCount} rows with 3 UUIDs each in {sw.Elapsed.TotalMilliseconds:F2}ms");
        
        sw.Restart();
        var readBlock = await QueryBlockDataAsync(
            $"SELECT seq, uuid1, uuid2, uuid3 FROM {tableName} WHERE seq <= 1000 ORDER BY seq",
            formatName,
            1000,
            columns
        );
        sw.Stop();
        
        Output.WriteLine($"{formatName} format: Read 1000 rows in {sw.Elapsed.TotalMilliseconds:F2}ms");
        
        Assert.Equal(1000, readBlock.RowCount);
        
        var distinctCountStr = await GetScalarValueAsync(
            $"SELECT COUNT(DISTINCT uuid1) + COUNT(DISTINCT uuid2) + COUNT(DISTINCT uuid3) FROM {tableName}"
        );
        Output.WriteLine($"Total distinct UUIDs across 3 columns: {distinctCountStr}");
        
        await Transport.ExecuteNonQueryAsync($"DROP TABLE {tableName}");
    }
}