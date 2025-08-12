using System.Buffers;
using ClickHouse.Direct.Abstractions;
using ClickHouse.Direct.Formats;
using ClickHouse.Direct.Types;
using ClickHouse.Direct.Transports;
using Xunit.Abstractions;

namespace ClickHouse.Direct.IntegrationTests.Protocol;

[Collection("ClickHouse")]
public abstract class FormatSerializerIntegrationTestsBase : IClassFixture<ClickHouseContainerFixture>, IDisposable
{
    private readonly ClickHouseContainerFixture _fixture;
    private readonly ITestOutputHelper _output;
    private readonly IClickHouseTransport _transport;
    
    protected abstract IFormatSerializer CreateSerializer();
    protected abstract string FormatName { get; }
    
    protected FormatSerializerIntegrationTestsBase(ClickHouseContainerFixture fixture, ITestOutputHelper output)
    {
        _fixture = fixture;
        _output = output;
        _transport = new HttpTransport(
            _fixture.Hostname, 
            _fixture.HttpPort,
            ClickHouseContainerFixture.Username,
            ClickHouseContainerFixture.Password
        );
    }
    
    public void Dispose()
    {
        _transport.Dispose();
    }
    
    /// <summary>
    /// Gets a sanitized table name with TFM suffix to prevent conflicts when running tests on multiple TFMs.
    /// </summary>
    protected string GetSanitizedTableName(string baseTableName)
    {
        return baseTableName.SanitizeForTfm();
    }
    
    [Fact]
    public async Task WriteAndRead_BasicTypes_ShouldRoundTrip()
    {
        var tableName = GetSanitizedTableName($"test_{FormatName.ToLower()}_basic");
        await _transport.ExecuteNonQueryAsync($"DROP TABLE IF EXISTS {tableName}");
        await _transport.ExecuteNonQueryAsync($"""
                                               CREATE TABLE {tableName} (
                                                   id Int32,
                                                   name String,
                                                   guid UUID
                                               ) ENGINE = Memory
                                               """);
        
        var columns = new List<ColumnDescriptor>
        {
            ColumnDescriptor.Create("id", new Int32Type()),
            ColumnDescriptor.Create("name", new StringType()),
            ColumnDescriptor.Create("guid", new UuidType())
        };
        
        var ids = new List<int> { 1, 2, 3, 4, 5 };
        var names = new List<string> { "Alice", "Bob", "Charlie", "David", "Eve" };
        var guids = new List<Guid>
        {
            Guid.Parse("550e8400-e29b-41d4-a716-446655440001"),
            Guid.Parse("550e8400-e29b-41d4-a716-446655440002"),
            Guid.Parse("550e8400-e29b-41d4-a716-446655440003"),
            Guid.Parse("550e8400-e29b-41d4-a716-446655440004"),
            Guid.Parse("550e8400-e29b-41d4-a716-446655440005")
        };
        
        var columnData = new List<System.Collections.IList> { ids, names, guids };
        var block = Block.CreateFromColumnData(columns, columnData, 5);
        
        var serializer = CreateSerializer();
        var writer = new ArrayBufferWriter<byte>();
        serializer.WriteBlock(block, writer);
        
        await _transport.SendDataAsync(
            $"INSERT INTO {tableName} FORMAT {FormatName}",
            writer.WrittenMemory
        );
        
        var responseData = await _transport.QueryDataAsync(
            $"SELECT id, name, guid FROM {tableName} ORDER BY id FORMAT {FormatName}"
        );
        
        var sequence = new ReadOnlySequence<byte>(responseData);
        var readBlock = serializer.ReadBlock(5, columns, ref sequence, out _);
        
        Assert.Equal(5, readBlock.RowCount);
        Assert.Equal(3, readBlock.ColumnCount);
        
        var readIds = (List<int>)readBlock.GetColumnData(0);
        var readNames = (List<string>)readBlock.GetColumnData(1);
        var readGuids = (List<Guid>)readBlock.GetColumnData(2);
        
        Assert.Equal(ids, readIds);
        Assert.Equal(names, readNames);
        Assert.Equal(guids, readGuids);
        
        await _transport.ExecuteNonQueryAsync($"DROP TABLE {tableName}");
    }
    
    [Fact]
    public async Task WriteAndRead_LargeDataset_ShouldHandleCorrectly()
    {
        var tableName = GetSanitizedTableName($"test_{FormatName.ToLower()}_large");
        await _transport.ExecuteNonQueryAsync($"DROP TABLE IF EXISTS {tableName}");
        await _transport.ExecuteNonQueryAsync($"""
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
        
        const int rowCount = 10000;
        var ids = Enumerable.Range(1, rowCount).ToList();
        var values = Enumerable.Range(1, rowCount).Select(i => $"Value_{i}").ToList();
        
        var columnData = new List<System.Collections.IList> { ids, values };
        var block = Block.CreateFromColumnData(columns, columnData, rowCount);
        
        var serializer = CreateSerializer();
        var writer = new ArrayBufferWriter<byte>();
        serializer.WriteBlock(block, writer);
        
        await _transport.SendDataAsync(
            $"INSERT INTO {tableName} FORMAT {FormatName}",
            writer.WrittenMemory
        );
        
        var countResult = await _transport.ExecuteQueryAsync($"SELECT COUNT(*) FROM {tableName} FORMAT TabSeparated");
        var countStr = System.Text.Encoding.UTF8.GetString(countResult);
        Assert.Equal(rowCount.ToString(), countStr.Trim());
        
        await _transport.ExecuteNonQueryAsync($"DROP TABLE {tableName}");
    }
    
    [Fact]
    public async Task WriteAndRead_MixedTypes_ShouldHandleCorrectly()
    {
        var tableName = GetSanitizedTableName($"test_{FormatName.ToLower()}_mixed");
        await _transport.ExecuteNonQueryAsync($"DROP TABLE IF EXISTS {tableName}");
        await _transport.ExecuteNonQueryAsync($"""
                                               CREATE TABLE {tableName} (
                                                   int_val Int32,
                                                   str_val String,
                                                   uuid_val UUID
                                               ) ENGINE = Memory
                                               """);
        
        var columns = new List<ColumnDescriptor>
        {
            ColumnDescriptor.Create("int_val", new Int32Type()),
            ColumnDescriptor.Create("str_val", new StringType()),
            ColumnDescriptor.Create("uuid_val", new UuidType())
        };
        
        var integers = new List<int> { int.MinValue, -1, 0, 1, int.MaxValue };
        var strings = new List<string> { "", "a", "Test", "Unicode: 你好", new('x', 1000) };
        var uuids = new List<Guid>
        {
            Guid.Empty,
            Guid.Parse("12345678-1234-1234-1234-123456789abc"),
            Guid.NewGuid(),
            Guid.NewGuid(),
            Guid.Parse("ffffffff-ffff-ffff-ffff-ffffffffffff")
        };
        
        var columnData = new List<System.Collections.IList> { integers, strings, uuids };
        var block = Block.CreateFromColumnData(columns, columnData, 5);
        
        var serializer = CreateSerializer();
        var writer = new ArrayBufferWriter<byte>();
        serializer.WriteBlock(block, writer);
        
        await _transport.SendDataAsync(
            $"INSERT INTO {tableName} FORMAT {FormatName}",
            writer.WrittenMemory
        );
        
        var responseData = await _transport.QueryDataAsync(
            $"SELECT int_val, str_val, uuid_val FROM {tableName} FORMAT {FormatName}"
        );
        
        var sequence = new ReadOnlySequence<byte>(responseData);
        var readBlock = serializer.ReadBlock(5, columns, ref sequence, out _);
        
        Assert.Equal(5, readBlock.RowCount);
        Assert.Equal(3, readBlock.ColumnCount);
        
        var readIntegers = (List<int>)readBlock.GetColumnData(0);
        var readStrings = (List<string>)readBlock.GetColumnData(1);
        var readUuids = (List<Guid>)readBlock.GetColumnData(2);
        
        Assert.Equal(integers, readIntegers);
        Assert.Equal(strings, readStrings);
        
        for (var i = 0; i < uuids.Count; i++)
        {
            Assert.Equal(uuids[i], readUuids[i]);
        }
        
        await _transport.ExecuteNonQueryAsync($"DROP TABLE {tableName}");
    }
    
    [Fact]
    public async Task WriteAndRead_EmptyData_ShouldHandleCorrectly()
    {
        var tableName = GetSanitizedTableName($"test_{FormatName.ToLower()}_empty");
        await _transport.ExecuteNonQueryAsync($"DROP TABLE IF EXISTS {tableName}");
        await _transport.ExecuteNonQueryAsync($"""
                                               CREATE TABLE {tableName} (
                                                   id Int32,
                                                   name String
                                               ) ENGINE = Memory
                                               """);
        
        var columns = new List<ColumnDescriptor>
        {
            ColumnDescriptor.Create("id", new Int32Type()),
            ColumnDescriptor.Create("name", new StringType())
        };
        
        var ids = new List<int>();
        var names = new List<string>();
        
        var columnData = new List<System.Collections.IList> { ids, names };
        var block = Block.CreateFromColumnData(columns, columnData, 0);
        
        var serializer = CreateSerializer();
        var writer = new ArrayBufferWriter<byte>();
        serializer.WriteBlock(block, writer);
        
        await _transport.SendDataAsync(
            $"INSERT INTO {tableName} FORMAT {FormatName}",
            writer.WrittenMemory
        );
        
        var countResult = await _transport.ExecuteQueryAsync($"SELECT COUNT(*) FROM {tableName} FORMAT TabSeparated");
        var countStr = System.Text.Encoding.UTF8.GetString(countResult);
        Assert.Equal("0", countStr.Trim());
        
        await _transport.ExecuteNonQueryAsync($"DROP TABLE {tableName}");
    }
    
    [Fact]
    public async Task HttpTransport_Ping_ShouldSucceed()
    {
        var result = await _transport.PingAsync();
        Assert.True(result, "Ping should succeed when ClickHouse is running");
    }
}