using System.Buffers;
using System.Net.Http.Headers;
using System.Text;
using ClickHouse.Direct.Abstractions;
using ClickHouse.Direct.Protocol;
using ClickHouse.Direct.Types;
using Xunit.Abstractions;

namespace ClickHouse.Direct.IntegrationTests.Protocol;

[Collection("ClickHouse")]
public class FormatSerializerIntegrationTests : IClassFixture<ClickHouseContainerFixture>, IDisposable
{
    private readonly ClickHouseContainerFixture _fixture;
    private readonly ITestOutputHelper _output;
    private readonly HttpClient _httpClient;
    private string BaseUrl => $"http://{_fixture.Hostname}:{_fixture.HttpPort}";
    
    public FormatSerializerIntegrationTests(ClickHouseContainerFixture fixture, ITestOutputHelper output)
    {
        _fixture = fixture;
        _output = output;
        _httpClient = new HttpClient();
        
        var authValue = Convert.ToBase64String(Encoding.UTF8.GetBytes($"{ClickHouseContainerFixture.Username}:{ClickHouseContainerFixture.Password}"));
        _httpClient.DefaultRequestHeaders.Authorization = new AuthenticationHeaderValue("Basic", authValue);
    }
    
    public void Dispose()
    {
        _httpClient.Dispose();
    }
    
    [Fact]
    public async Task NativeFormat_WriteAndRead_ShouldRoundTrip()
    {
        await ExecuteQuery("DROP TABLE IF EXISTS test_native_format");
        await ExecuteQuery(@"
            CREATE TABLE test_native_format (
                id Int32,
                name String,
                guid UUID
            ) ENGINE = Memory
        ");
        
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
        
        var serializer = new NativeFormatSerializer();
        var writer = new ArrayBufferWriter<byte>();
        serializer.WriteBlock(block, writer);
        
        var insertUrl = $"{BaseUrl}/?query=INSERT%20INTO%20test_native_format%20FORMAT%20Native";
        using var insertContent = new ByteArrayContent(writer.WrittenMemory.ToArray());
        insertContent.Headers.ContentType = new MediaTypeHeaderValue("application/octet-stream");
        
        var insertResponse = await _httpClient.PostAsync(insertUrl, insertContent);
        if (!insertResponse.IsSuccessStatusCode)
        {
            var error = await insertResponse.Content.ReadAsStringAsync();
            _output.WriteLine($"Native format insert failed: {insertResponse.StatusCode}");
            _output.WriteLine($"Error: {error}");
        }
        insertResponse.EnsureSuccessStatusCode();
        
        var selectUrl = $"{BaseUrl}/?query=SELECT%20id,%20name,%20guid%20FROM%20test_native_format%20ORDER%20BY%20id%20FORMAT%20Native";
        var selectResponse = await _httpClient.GetAsync(selectUrl);
        selectResponse.EnsureSuccessStatusCode();
        
        var responseBytes = await selectResponse.Content.ReadAsByteArrayAsync();
        var sequence = new ReadOnlySequence<byte>(responseBytes);
        
        var readBlock = serializer.ReadBlock(5, columns, ref sequence, out var bytesConsumed);
        
        Assert.Equal(5, readBlock.RowCount);
        Assert.Equal(3, readBlock.ColumnCount);
        
        var readIds = (List<int>)readBlock.GetColumnData(0);
        var readNames = (List<string>)readBlock.GetColumnData(1);
        var readGuids = (List<Guid>)readBlock.GetColumnData(2);
        
        Assert.Equal(ids, readIds);
        Assert.Equal(names, readNames);
        Assert.Equal(guids, readGuids);
        
        await ExecuteQuery("DROP TABLE test_native_format");
    }
    
    [Fact]
    public async Task RowBinaryFormat_WriteAndRead_ShouldRoundTrip()
    {
        await ExecuteQuery("DROP TABLE IF EXISTS test_rowbinary_format");
        await ExecuteQuery(@"
            CREATE TABLE test_rowbinary_format (
                id Int32,
                name String,
                guid UUID
            ) ENGINE = Memory
        ");
        
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
        
        var serializer = new RowBinaryFormatSerializer();
        var writer = new ArrayBufferWriter<byte>();
        serializer.WriteBlock(block, writer);
        
        var insertUrl = $"{BaseUrl}/?query=INSERT%20INTO%20test_rowbinary_format%20FORMAT%20RowBinary";
        using var insertContent = new ByteArrayContent(writer.WrittenMemory.ToArray());
        insertContent.Headers.ContentType = new MediaTypeHeaderValue("application/octet-stream");
        
        var insertResponse = await _httpClient.PostAsync(insertUrl, insertContent);
        insertResponse.EnsureSuccessStatusCode();
        
        var selectUrl = $"{BaseUrl}/?query=SELECT%20id,%20name,%20guid%20FROM%20test_rowbinary_format%20ORDER%20BY%20id%20FORMAT%20RowBinary";
        var selectResponse = await _httpClient.GetAsync(selectUrl);
        selectResponse.EnsureSuccessStatusCode();
        
        var responseBytes = await selectResponse.Content.ReadAsByteArrayAsync();
        var sequence = new ReadOnlySequence<byte>(responseBytes);
        
        var readBlock = serializer.ReadBlock(5, columns, ref sequence, out var bytesConsumed);
        
        Assert.Equal(5, readBlock.RowCount);
        Assert.Equal(3, readBlock.ColumnCount);
        
        var readIds = (List<int>)readBlock.GetColumnData(0);
        var readNames = (List<string>)readBlock.GetColumnData(1);
        var readGuids = (List<Guid>)readBlock.GetColumnData(2);
        
        Assert.Equal(ids, readIds);
        Assert.Equal(names, readNames);
        Assert.Equal(guids, readGuids);
        
        await ExecuteQuery("DROP TABLE test_rowbinary_format");
    }
    
    [Fact]
    public async Task NativeFormat_LargeDataset_ShouldHandleCorrectly()
    {
        await ExecuteQuery("DROP TABLE IF EXISTS test_native_large");
        await ExecuteQuery(@"
            CREATE TABLE test_native_large (
                id Int32,
                value String
            ) ENGINE = Memory
        ");
        
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
        
        var serializer = new NativeFormatSerializer();
        var writer = new ArrayBufferWriter<byte>();
        serializer.WriteBlock(block, writer);
        
        var insertUrl = $"{BaseUrl}/?query=INSERT%20INTO%20test_native_large%20FORMAT%20Native";
        using var insertContent = new ByteArrayContent(writer.WrittenMemory.ToArray());
        insertContent.Headers.ContentType = new MediaTypeHeaderValue("application/octet-stream");
        
        var insertResponse = await _httpClient.PostAsync(insertUrl, insertContent);
        insertResponse.EnsureSuccessStatusCode();
        
        var countUrl = $"{BaseUrl}/?query=SELECT%20COUNT(*)%20FROM%20test_native_large%20FORMAT%20TabSeparated";
        var countResponse = await _httpClient.GetAsync(countUrl);
        countResponse.EnsureSuccessStatusCode();
        
        var countStr = await countResponse.Content.ReadAsStringAsync();
        Assert.Equal(rowCount.ToString(), countStr.Trim());
        
        await ExecuteQuery("DROP TABLE test_native_large");
    }
    
    [Fact]
    public async Task RowBinaryFormat_MixedTypes_ShouldHandleCorrectly()
    {
        await ExecuteQuery("DROP TABLE IF EXISTS test_rowbinary_mixed");
        await ExecuteQuery(@"
            CREATE TABLE test_rowbinary_mixed (
                int_val Int32,
                str_val String,
                uuid_val UUID
            ) ENGINE = Memory
        ");
        
        var columns = new List<ColumnDescriptor>
        {
            ColumnDescriptor.Create("int_val", new Int32Type()),
            ColumnDescriptor.Create("str_val", new StringType()),
            ColumnDescriptor.Create("uuid_val", new UuidType())
        };
        
        var ints = new List<int> { int.MinValue, -1, 0, 1, int.MaxValue };
        var strings = new List<string> { "", "a", "Test", "Unicode: 你好", new string('x', 1000) };
        var uuids = new List<Guid>
        {
            Guid.Empty,
            Guid.Parse("12345678-1234-1234-1234-123456789abc"),
            Guid.NewGuid(),
            Guid.NewGuid(),
            Guid.Parse("ffffffff-ffff-ffff-ffff-ffffffffffff")
        };
        
        var columnData = new List<System.Collections.IList> { ints, strings, uuids };
        var block = Block.CreateFromColumnData(columns, columnData, 5);
        
        var serializer = new RowBinaryFormatSerializer();
        var writer = new ArrayBufferWriter<byte>();
        serializer.WriteBlock(block, writer);
        
        var insertUrl = $"{BaseUrl}/?query=INSERT%20INTO%20test_rowbinary_mixed%20FORMAT%20RowBinary";
        using var insertContent = new ByteArrayContent(writer.WrittenMemory.ToArray());
        insertContent.Headers.ContentType = new MediaTypeHeaderValue("application/octet-stream");
        
        var insertResponse = await _httpClient.PostAsync(insertUrl, insertContent);
        insertResponse.EnsureSuccessStatusCode();
        
        var selectUrl = $"{BaseUrl}/?query=SELECT%20int_val,%20str_val,%20uuid_val%20FROM%20test_rowbinary_mixed%20FORMAT%20RowBinary";
        var selectResponse = await _httpClient.GetAsync(selectUrl);
        selectResponse.EnsureSuccessStatusCode();
        
        var responseBytes = await selectResponse.Content.ReadAsByteArrayAsync();
        var sequence = new ReadOnlySequence<byte>(responseBytes);
        
        var readBlock = serializer.ReadBlock(5, columns, ref sequence, out var bytesConsumed);
        
        Assert.Equal(5, readBlock.RowCount);
        Assert.Equal(3, readBlock.ColumnCount);
        
        var readInts = (List<int>)readBlock.GetColumnData(0);
        var readStrings = (List<string>)readBlock.GetColumnData(1);
        var readUuids = (List<Guid>)readBlock.GetColumnData(2);
        
        Assert.Equal(ints, readInts);
        Assert.Equal(strings, readStrings);
        
        for (var i = 0; i < uuids.Count; i++)
        {
            Assert.Equal(uuids[i], readUuids[i]);
        }
        
        await ExecuteQuery("DROP TABLE test_rowbinary_mixed");
    }
    
    private async Task ExecuteQuery(string query)
    {
        var url = $"{BaseUrl}/?query={Uri.EscapeDataString(query)}";
        var response = await _httpClient.PostAsync(url, null);
        
        if (!response.IsSuccessStatusCode)
        {
            var error = await response.Content.ReadAsStringAsync();
            _output.WriteLine($"Query failed: {query}");
            _output.WriteLine($"Error: {error}");
        }
        
        response.EnsureSuccessStatusCode();
    }
}