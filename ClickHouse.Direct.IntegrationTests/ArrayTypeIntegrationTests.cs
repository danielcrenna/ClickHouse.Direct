using System.Buffers;
using System.Text;
using ClickHouse.Direct.Abstractions;
using ClickHouse.Direct.Formats;
using ClickHouse.Direct.Transports;
using ClickHouse.Direct.Types;
using Xunit.Abstractions;

namespace ClickHouse.Direct.IntegrationTests;

[Collection("ClickHouse")]
public class ArrayTypeIntegrationTests(ClickHouseContainerFixture fixture, ITestOutputHelper output)
    : IClassFixture<ClickHouseContainerFixture>, IDisposable
{
    private readonly IClickHouseTransport _transport = new HttpTransport(
        fixture.Hostname,
        fixture.HttpPort,
        ClickHouseContainerFixture.Username,
        ClickHouseContainerFixture.Password
    );

    public void Dispose()
    {
        _transport?.Dispose();
    }
    
    [Fact]
    public async Task ArrayOfInt32_RowBinary_RoundTrip()
    {
        var tableName = TableNameExtensions.GenerateTableName();
        
        // Setup table
        await _transport.ExecuteNonQueryAsync($"DROP TABLE IF EXISTS {tableName}");
        await _transport.ExecuteNonQueryAsync($"""
                                              CREATE TABLE {tableName} (
                                                  id UInt32,
                                                  values Array(Int32)
                                              ) ENGINE = Memory
                                              """);
        
        // Prepare test data
        var testArrays = new[]
        {
            [1, 2, 3, 4, 5],
            [-1, -2, -3],
            [int.MaxValue, int.MinValue, 0],
            [],
            new[] { 42 }
        };
        
        // Create block with arrays
        var columns = new List<ColumnDescriptor>
        {
            ColumnDescriptor.Create("id", new UInt32Type()),
            ColumnDescriptor.Create("values", new Int32Type(), isArray: true)
        };
        
        var idData = new List<uint> { 1, 2, 3, 4, 5 };
        var arrayData = new List<int[]>(testArrays);
        
        var block = Block.CreateFromColumnData(columns, [idData, arrayData], 5);
        
        // Serialize to RowBinary
        var serializer = new RowBinaryFormatSerializer();
        var buffer = new ArrayBufferWriter<byte>();
        serializer.WriteBlock(block, buffer);
        
        // Insert data
        using var httpClient = new HttpClient();
        httpClient.BaseAddress = new Uri($"http://{fixture.Hostname}:{fixture.HttpPort}");
        
        var query = $"INSERT INTO {tableName} FORMAT RowBinary";
        var binaryData = buffer.WrittenSpan.ToArray();
        
        var content = new ByteArrayContent(binaryData);
        content.Headers.Clear();
        content.Headers.Add("X-ClickHouse-User", ClickHouseContainerFixture.Username);
        content.Headers.Add("X-ClickHouse-Key", ClickHouseContainerFixture.Password);
        
        var response = await httpClient.PostAsync($"/?query={Uri.EscapeDataString(query)}", content);
        var responseBody = await response.Content.ReadAsStringAsync();
        
        if (!response.IsSuccessStatusCode)
        {
            output.WriteLine($"Insert failed: {response.StatusCode}");
            output.WriteLine($"Response: {responseBody}");
        }
        
        Assert.True(response.IsSuccessStatusCode, $"Insert failed: {responseBody}");
        
        // Read back data
        var selectQuery = $"SELECT id, values FROM {tableName} ORDER BY id FORMAT RowBinary";
        var selectContent = new StringContent(selectQuery);
        selectContent.Headers.Clear();
        selectContent.Headers.Add("X-ClickHouse-User", ClickHouseContainerFixture.Username);
        selectContent.Headers.Add("X-ClickHouse-Key", ClickHouseContainerFixture.Password);
        
        var selectResponse = await httpClient.PostAsync("/", selectContent);
        Assert.True(selectResponse.IsSuccessStatusCode, "Select failed");
        
        var resultBytes = await selectResponse.Content.ReadAsByteArrayAsync();
        var readSequence = new ReadOnlySequence<byte>(resultBytes);
        
        // Deserialize result
        var resultBlock = serializer.ReadBlock(5, columns, ref readSequence, out _);
        
        // Verify results
        Assert.Equal(5, resultBlock.RowCount);
        
        for (var i = 0; i < 5; i++)
        {
            var id = (uint)resultBlock[i, 0]!;
            var values = (int[])resultBlock[i, 1]!;
            
            Assert.Equal((uint)(i + 1), id);
            Assert.Equal(testArrays[i], values);
            
            output.WriteLine($"Row {i}: id={id}, values=[{string.Join(", ", values)}]");
        }
        
        await _transport.ExecuteNonQueryAsync($"DROP TABLE {tableName}");
    }
    
    [Fact]
    public async Task ArrayOfInt32_Native_RoundTrip()
    {
        var tableName = TableNameExtensions.GenerateTableName();
        
        // Setup table
        await _transport.ExecuteNonQueryAsync($"DROP TABLE IF EXISTS {tableName}");
        await _transport.ExecuteNonQueryAsync($@"
            CREATE TABLE {tableName} (
                id UInt32,
                values Array(Int32)
            ) ENGINE = Memory");
        
        // Prepare test data
        var testArrays = new[]
        {
            [10, 20, 30],
            [-100],
            new int[] { }
        };
        
        // Create block with arrays
        var columns = new List<ColumnDescriptor>
        {
            ColumnDescriptor.Create("id", new UInt32Type()),
            ColumnDescriptor.Create("values", new Int32Type(), isArray: true)
        };
        
        var idData = new List<uint> { 1, 2, 3 };
        var arrayData = new List<int[]>(testArrays);
        
        var block = Block.CreateFromColumnData(columns, [idData, arrayData], 3);
        
        // Serialize to Native
        var serializer = new NativeFormatSerializer();
        var buffer = new ArrayBufferWriter<byte>();
        serializer.WriteBlock(block, buffer);
        
        // Insert data
        using var httpClient = new HttpClient();
        httpClient.BaseAddress = new Uri($"http://{fixture.Hostname}:{fixture.HttpPort}");
        
        var query = $"INSERT INTO {tableName} FORMAT Native";
        var binaryData = buffer.WrittenSpan.ToArray();
        
        var content = new ByteArrayContent(binaryData);
        content.Headers.Clear();
        content.Headers.Add("X-ClickHouse-User", ClickHouseContainerFixture.Username);
        content.Headers.Add("X-ClickHouse-Key", ClickHouseContainerFixture.Password);
        
        var response = await httpClient.PostAsync($"/?query={Uri.EscapeDataString(query)}", content);
        var responseBody = await response.Content.ReadAsStringAsync();
        
        if (!response.IsSuccessStatusCode)
        {
            output.WriteLine($"Insert failed: {response.StatusCode}");
            output.WriteLine($"Response: {responseBody}");
        }
        
        Assert.True(response.IsSuccessStatusCode, $"Insert failed: {responseBody}");
        
        // Read back data
        var selectQuery = $"SELECT id, values FROM {tableName} ORDER BY id FORMAT Native";
        var selectContent = new StringContent(selectQuery);
        selectContent.Headers.Clear();
        selectContent.Headers.Add("X-ClickHouse-User", ClickHouseContainerFixture.Username);
        selectContent.Headers.Add("X-ClickHouse-Key", ClickHouseContainerFixture.Password);
        
        var selectResponse = await httpClient.PostAsync("/", selectContent);
        Assert.True(selectResponse.IsSuccessStatusCode, "Select failed");
        
        var resultBytes = await selectResponse.Content.ReadAsByteArrayAsync();
        var readSequence = new ReadOnlySequence<byte>(resultBytes);
        
        // Deserialize result
        var resultBlock = serializer.ReadBlock(3, columns, ref readSequence, out _);
        
        // Verify results
        Assert.Equal(3, resultBlock.RowCount);
        
        for (var i = 0; i < 3; i++)
        {
            var id = (uint)resultBlock[i, 0]!;
            var values = (int[])resultBlock[i, 1]!;
            
            Assert.Equal((uint)(i + 1), id);
            Assert.Equal(testArrays[i], values);
            
            output.WriteLine($"Row {i}: id={id}, values=[{string.Join(", ", values)}]");
        }
        
        await _transport.ExecuteNonQueryAsync($"DROP TABLE {tableName}");
    }
}