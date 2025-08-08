using System.Buffers;
using System.Net.Http.Headers;
using System.Text;
using System.Text.Json;
using ClickHouse.Direct.Types;
using Xunit.Abstractions;

namespace ClickHouse.Direct.IntegrationTests.Types;

[Collection("ClickHouse")]
public class UuidTypeIntegrationTests : IClassFixture<ClickHouseContainerFixture>, IDisposable
{
    private readonly ClickHouseContainerFixture _fixture;
    private readonly ITestOutputHelper _output;
    private readonly HttpClient _httpClient;

    public UuidTypeIntegrationTests(ClickHouseContainerFixture fixture, ITestOutputHelper output)
    {
        _fixture = fixture;
        _output = output;
        _httpClient = new HttpClient();
        
        var authValue = Convert.ToBase64String(Encoding.UTF8.GetBytes($"{ClickHouseContainerFixture.Username}:{ClickHouseContainerFixture.Password}"));
        _httpClient.DefaultRequestHeaders.Authorization = new AuthenticationHeaderValue("Basic", authValue);
    }

    [Fact]
    public async Task InsertAndSelect_UsingRowBinary_ShouldRoundTrip()
    {
        await ExecuteQuery("DROP TABLE IF EXISTS test_uuid_rowbinary");
        await ExecuteQuery("""
                           CREATE TABLE test_uuid_rowbinary (
                               test_uuid UUID
                           ) ENGINE = Memory
                           """);

        var testGuids = new[]
        {
            new Guid("01234567-89AB-CDEF-0123-456789ABCDEF"),
            new Guid("dca0e161-9503-41a1-9de2-18528bfffe88"),
            Guid.Empty,
            new Guid("ffffffff-ffff-ffff-ffff-ffffffffffff")
        };

        var writer = new ArrayBufferWriter<byte>();
        foreach (var guid in testGuids)
            UuidType.Instance.WriteValue(writer, guid);

        var binaryData = writer.WrittenSpan.ToArray();
        
        _output.WriteLine($"Inserting {testGuids.Length} UUIDs using RowBinary format");
        _output.WriteLine($"Binary data length: {binaryData.Length} bytes (expected: {testGuids.Length * 16})");
        _output.WriteLine($"Binary data (hex): {Convert.ToHexString(binaryData)}");

        const string insertQuery = "INSERT INTO test_uuid_rowbinary FORMAT RowBinary";
        var insertResponse = await ExecuteBinaryQuery(insertQuery, binaryData);
        
        if (!insertResponse.IsSuccessStatusCode)
        {
            var error = await insertResponse.Content.ReadAsStringAsync();
            _output.WriteLine($"Insert failed: {error}");
        }
        insertResponse.EnsureSuccessStatusCode();

        const string selectQuery = "SELECT test_uuid FROM test_uuid_rowbinary ORDER BY test_uuid FORMAT RowBinary";
        var selectResponse = await ExecuteQuery(selectQuery);
        selectResponse.EnsureSuccessStatusCode();
        
        var responseBytes = await selectResponse.Content.ReadAsByteArrayAsync();
        _output.WriteLine($"Response binary length: {responseBytes.Length} bytes");
        _output.WriteLine($"Response binary (hex): {Convert.ToHexString(responseBytes)}");
        
        // Deserialize using our UuidType
        var sequence = new ReadOnlySequence<byte>(responseBytes);
        var results = new List<Guid>();
        
        while (sequence.Length >= 16)
        {
            var guid = UuidType.Instance.ReadValue(ref sequence, out _);
            results.Add(guid);
            _output.WriteLine($"Read GUID: {guid}");
        }
        
        var expectedSorted = testGuids.OrderBy(g => g).ToArray();
        var actualSorted = results.ToArray();
        Assert.Equal(expectedSorted.Length, actualSorted.Length);
        for (var i = 0; i < expectedSorted.Length; i++)
            Assert.Equal(expectedSorted[i], actualSorted[i]);

        _output.WriteLine($"Successfully round-tripped {results.Count} UUIDs through RowBinary format");
    }
    
    [Fact]
    public async Task SingleUuid_RowBinaryRoundTrip_PreservesExactBytes()
    {
        await ExecuteQuery("DROP TABLE IF EXISTS test_single_uuid_binary");
        await ExecuteQuery("""
                           CREATE TABLE test_single_uuid_binary (
                               test_uuid UUID
                           ) ENGINE = Memory
                           """);

        var testGuid = new Guid("01234567-89AB-CDEF-0123-456789ABCDEF");
        
        var writer = new ArrayBufferWriter<byte>();
        UuidType.Instance.WriteValue(writer, testGuid);
        var ourBytes = writer.WrittenSpan.ToArray();
        
        _output.WriteLine($"Test GUID: {testGuid}");
        _output.WriteLine($"Our serialization: {Convert.ToHexString(ourBytes)}");
        
        var insertResponse = await ExecuteBinaryQuery(
            "INSERT INTO test_single_uuid_binary FORMAT RowBinary",
            ourBytes);
        insertResponse.EnsureSuccessStatusCode();
        
        var selectResponse = await ExecuteQuery("SELECT test_uuid FROM test_single_uuid_binary FORMAT RowBinary");
        selectResponse.EnsureSuccessStatusCode();
        
        var clickHouseBytes = await selectResponse.Content.ReadAsByteArrayAsync();
        _output.WriteLine($"ClickHouse returned: {Convert.ToHexString(clickHouseBytes)}");
        
        Assert.Equal(ourBytes, clickHouseBytes);
        
        var sequence = new ReadOnlySequence<byte>(clickHouseBytes);
        var resultGuid = UuidType.Instance.ReadValue(ref sequence, out _);
        Assert.Equal(testGuid, resultGuid);
    }

    [Fact]
    public async Task MixedFormat_TextInsertBinarySelect_ShouldWork()
    {
        await ExecuteQuery("DROP TABLE IF EXISTS test_mixed_formats");
        await ExecuteQuery("""
                           CREATE TABLE test_mixed_formats (
                               id Int32,
                               uuid_value UUID
                           ) ENGINE = Memory
                           """);

        var testData = new[]
        {
            (1, new Guid("12345678-90ab-cdef-1234-567890abcdef")),
            (2, new Guid("87654321-4321-8765-cba9-cba987654321")),
            (3, Guid.Empty),
            (4, new Guid("ffffffff-ffff-ffff-ffff-ffffffffffff"))
        };

        foreach (var (id, guid) in testData)
            await ExecuteQuery($"INSERT INTO test_mixed_formats VALUES ({id}, '{guid}')");

        // Select using RowBinary format
        var selectResponse = await ExecuteQuery(
            "SELECT uuid_value FROM test_mixed_formats ORDER BY id FORMAT RowBinary");
        selectResponse.EnsureSuccessStatusCode();

        var binaryData = await selectResponse.Content.ReadAsByteArrayAsync();
        _output.WriteLine($"Selected {binaryData.Length} bytes in RowBinary format");

        var sequence = new ReadOnlySequence<byte>(binaryData);
        var results = new List<Guid>();

        while (sequence.Length >= 16)
        {
            var guid = UuidType.Instance.ReadValue(ref sequence, out _);
            results.Add(guid);
        }

        Assert.Equal(testData.Length, results.Count);
        for (var i = 0; i < testData.Length; i++)
        {
            Assert.Equal(testData[i].Item2, results[i]);
            _output.WriteLine($"Verified UUID {i + 1}: {results[i]}");
        }
    }

    [Fact]
    public async Task MixedFormat_BinaryInsertTextSelect_ShouldWork()
    {
        await ExecuteQuery("DROP TABLE IF EXISTS test_binary_to_text");
        await ExecuteQuery("""
                           CREATE TABLE test_binary_to_text (
                               uuid_value UUID
                           ) ENGINE = Memory
                           """);

        var testGuids = new[]
        {
            new Guid("abcdef12-3456-7890-abcd-ef1234567890"),
            new Guid("11111111-2222-3333-4444-555555555555"),
            Guid.NewGuid()
        };

        var writer = new ArrayBufferWriter<byte>();
        foreach (var guid in testGuids)
            UuidType.Instance.WriteValue(writer, guid);

        var insertResponse = await ExecuteBinaryQuery("INSERT INTO test_binary_to_text FORMAT RowBinary",
            writer.WrittenSpan.ToArray());
        insertResponse.EnsureSuccessStatusCode();

        var selectResponse = await ExecuteQuery("SELECT uuid_value FROM test_binary_to_text FORMAT JSON");
        selectResponse.EnsureSuccessStatusCode();

        var jsonResult = await selectResponse.Content.ReadAsStringAsync();
        var jsonDocument = JsonDocument.Parse(jsonResult);
        var rows = jsonDocument.RootElement.GetProperty("data").EnumerateArray().ToArray();

        Assert.Equal(testGuids.Length, rows.Length);

        var returnedGuids = rows.Select(row =>
            new Guid(row.GetProperty("uuid_value").GetString()!)
        ).ToArray();

        Assert.Equal(testGuids.OrderBy(g => g), returnedGuids.OrderBy(g => g));
        
        foreach (var guid in returnedGuids)
            _output.WriteLine($"Retrieved UUID via JSON: {guid}");
    }

    [Theory]
    [InlineData(10)]
    [InlineData(100)]
    [InlineData(1000)]
    public async Task BulkUuidOperations_UsingRowBinary_ShouldHandleVariousSizes(int count)
    {
        _output.WriteLine($"Testing bulk UUID operations with {count} records using RowBinary");
        
        await ExecuteQuery($"DROP TABLE IF EXISTS test_uuid_bulk_{count}");
        await ExecuteQuery($"""
                            CREATE TABLE test_uuid_bulk_{count} (
                                uuid_value UUID
                            ) ENGINE = Memory
                            """);

        var testGuids = Enumerable.Range(0, count)
            .Select(_ => Guid.NewGuid())
            .ToArray();

        var writer = new ArrayBufferWriter<byte>();
        UuidType.Instance.WriteValues(writer, testGuids);
        var binaryData = writer.WrittenSpan.ToArray();
        
        _output.WriteLine($"Inserting {count} UUIDs, binary size: {binaryData.Length} bytes");

        var insertResponse = await ExecuteBinaryQuery($"INSERT INTO test_uuid_bulk_{count} FORMAT RowBinary",
            binaryData);
        insertResponse.EnsureSuccessStatusCode();

        var selectResponse = await ExecuteQuery($"SELECT uuid_value FROM test_uuid_bulk_{count} FORMAT RowBinary");
        selectResponse.EnsureSuccessStatusCode();
        
        var responseBytes = await selectResponse.Content.ReadAsByteArrayAsync();
        _output.WriteLine($"Received {responseBytes.Length} bytes from ClickHouse");
        
        var sequence = new ReadOnlySequence<byte>(responseBytes);
        var results = new Guid[count];
        var itemsRead = UuidType.Instance.ReadValues(ref sequence, results, out var bytesConsumed);
        
        Assert.Equal(count, itemsRead);
        Assert.Equal(count * 16, bytesConsumed);
        Assert.Equal(0, sequence.Length); // All bytes should be consumed
        
        var originalSet = new HashSet<Guid>(testGuids);
        var resultSet = new HashSet<Guid>(results);
        Assert.Equal(originalSet, resultSet);

        _output.WriteLine($"Successfully round-tripped {count} UUID records through RowBinary");
    }

    private async Task<HttpResponseMessage> ExecuteQuery(string query)
    {
        return await _httpClient.ExecuteQuery(_fixture.Hostname, _fixture.HttpPort, query);
    }
    
    private async Task<HttpResponseMessage> ExecuteBinaryQuery(string query, byte[] binaryData)
    {
        return await _httpClient.ExecuteBinaryQuery(_fixture.Hostname, _fixture.HttpPort, query, binaryData);
    }
    public void Dispose() => _httpClient.Dispose();
}