using System.Buffers;
using ClickHouse.Direct.Abstractions;
using ClickHouse.Direct.Formats;
using ClickHouse.Direct.Transports;
using ClickHouse.Direct.Types;
using Xunit.Abstractions;

namespace ClickHouse.Direct.IntegrationTests;

[Collection("ClickHouse")]
public class NestedArrayTypeIntegrationTests(ClickHouseContainerFixture fixture, ITestOutputHelper output)
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
    public async Task NestedArrayOfInt32_Native_RoundTrip()
    {
        var tableName = TableNameExtensions.GenerateTableName();
        
        // Setup table with nested arrays (2D arrays)
        await _transport.ExecuteNonQueryAsync($"DROP TABLE IF EXISTS {tableName}");
        await _transport.ExecuteNonQueryAsync($"""
                                              CREATE TABLE {tableName} (
                                                  id UInt32,
                                                  matrix Array(Array(Int32))
                                              ) ENGINE = Memory
                                              """);
        
        // Prepare test data - 2D arrays (matrices)
        var testMatrices = new[]
        {
            [
                [1, 2, 3],
                [4, 5, 6]
            ],
            [
                [10],
                [20],
                [30]
            ],
            [
                []
            ],
            new int[][]
            {
            }
        };
        
        // Create block with nested arrays
        var columns = new List<ColumnDescriptor>
        {
            ColumnDescriptor.Create("id", new UInt32Type()),
            ColumnDescriptor.CreateNestedArray("matrix", new Int32Type(), arrayDepth: 2)
        };
        
        var idData = new List<uint> { 1, 2, 3, 4 };
        var matrixData = new List<int[][]>(testMatrices);
        
        var block = Block.CreateFromColumnData(columns, [idData, matrixData], 4);
        
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
        
        var selectQuery = $"SELECT id, matrix FROM {tableName} ORDER BY id FORMAT Native";
        var selectContent = new StringContent(selectQuery);
        selectContent.Headers.Clear();
        selectContent.Headers.Add("X-ClickHouse-User", ClickHouseContainerFixture.Username);
        selectContent.Headers.Add("X-ClickHouse-Key", ClickHouseContainerFixture.Password);
        
        var selectResponse = await httpClient.PostAsync("/", selectContent);
        Assert.True(selectResponse.IsSuccessStatusCode, "Select failed");
        
        var resultBytes = await selectResponse.Content.ReadAsByteArrayAsync();
        var readSequence = new ReadOnlySequence<byte>(resultBytes);
        
        // Deserialize result
        var resultBlock = serializer.ReadBlock(4, columns, ref readSequence, out _);
        
        // Verify results
        Assert.Equal(4, resultBlock.RowCount);
        
        for (var i = 0; i < 4; i++)
        {
            var id = (uint)resultBlock[i, 0]!;
            var matrix = (int[][])resultBlock[i, 1]!;
            
            Assert.Equal((uint)(i + 1), id);
            Assert.Equal(testMatrices[i].Length, matrix.Length);
            
            for (var j = 0; j < matrix.Length; j++)
            {
                Assert.Equal(testMatrices[i][j], matrix[j]);
            }
            
            output.WriteLine($"Row {i}: id={id}, matrix has {matrix.Length} rows");
        }
        
        await _transport.ExecuteNonQueryAsync($"DROP TABLE {tableName}");
    }
    
    [Fact]
    public async Task TripleNestedArray_Native_RoundTrip()
    {
        var tableName = TableNameExtensions.GenerateTableName();
        
        // Setup table with triple nested arrays (3D arrays)
        await _transport.ExecuteNonQueryAsync($"DROP TABLE IF EXISTS {tableName}");
        await _transport.ExecuteNonQueryAsync($"""
                                              CREATE TABLE {tableName} (
                                                  id UInt32,
                                                  cube Array(Array(Array(Int32)))
                                              ) ENGINE = Memory
                                              """);
        
        // Prepare test data - 3D arrays
        var testCubes = new[]
        {
            [
                [
                    [1, 2],
                    [3, 4]
                ],
                [
                    [5, 6],
                    [7, 8]
                ]
            ],
            new[]
            {
                new int[][]
                {
                    [100]
                }
            }
        };
        
        // Create block with triple nested arrays
        var columns = new List<ColumnDescriptor>
        {
            ColumnDescriptor.Create("id", new UInt32Type()),
            ColumnDescriptor.CreateNestedArray("cube", new Int32Type(), arrayDepth: 3)
        };
        
        var idData = new List<uint> { 1, 2 };
        var cubeData = new List<int[][][]>(testCubes);
        
        var block = Block.CreateFromColumnData(columns, [idData, cubeData], 2);
        
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
        
        var selectQuery = $"SELECT id, cube FROM {tableName} ORDER BY id FORMAT Native";
        var selectContent = new StringContent(selectQuery);
        selectContent.Headers.Clear();
        selectContent.Headers.Add("X-ClickHouse-User", ClickHouseContainerFixture.Username);
        selectContent.Headers.Add("X-ClickHouse-Key", ClickHouseContainerFixture.Password);
        
        var selectResponse = await httpClient.PostAsync("/", selectContent);
        Assert.True(selectResponse.IsSuccessStatusCode, "Select failed");
        
        var resultBytes = await selectResponse.Content.ReadAsByteArrayAsync();
        var readSequence = new ReadOnlySequence<byte>(resultBytes);
        
        // Deserialize result
        var resultBlock = serializer.ReadBlock(2, columns, ref readSequence, out _);
        
        // Verify results
        Assert.Equal(2, resultBlock.RowCount);
        
        for (var i = 0; i < 2; i++)
        {
            var id = (uint)resultBlock[i, 0]!;
            var cube = (int[][][])resultBlock[i, 1]!;
            
            Assert.Equal((uint)(i + 1), id);
            Assert.Equal(testCubes[i].Length, cube.Length);
            
            for (var j = 0; j < cube.Length; j++)
            {
                Assert.Equal(testCubes[i][j].Length, cube[j].Length);
                for (var k = 0; k < cube[j].Length; k++)
                {
                    Assert.Equal(testCubes[i][j][k], cube[j][k]);
                }
            }
            
            output.WriteLine($"Row {i}: id={id}, cube has {cube.Length} 2D arrays");
        }
        
        await _transport.ExecuteNonQueryAsync($"DROP TABLE {tableName}");
    }
}