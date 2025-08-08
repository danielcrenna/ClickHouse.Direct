using System.Buffers;
using System.Net.Http.Headers;
using System.Text;
using ClickHouse.Direct.Types;
using Xunit.Abstractions;

namespace ClickHouse.Direct.IntegrationTests.Types;

[Collection("ClickHouse")]
public class Int32TypeIntegrationTests : IClassFixture<ClickHouseContainerFixture>, IDisposable
{
    private readonly ClickHouseContainerFixture _fixture;
    private readonly ITestOutputHelper _output;
    private readonly HttpClient _httpClient;
    
    public Int32TypeIntegrationTests(ClickHouseContainerFixture fixture, ITestOutputHelper output)
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
    public async Task InsertAndSelect_UsingRowBinary_ShouldRoundTrip()
    {
        await ExecuteQuery("DROP TABLE IF EXISTS test_int32_rowbinary");
        await ExecuteQuery("""
                           CREATE TABLE test_int32_rowbinary (
                               test_value Int32
                           ) ENGINE = Memory
                           """);

        var testValues = new[]
        {
            0,
            1,
            -1,
            42,
            -42,
            int.MaxValue,
            int.MinValue,
            1234567890,
            -1234567890
        };

        var writer = new ArrayBufferWriter<byte>();
        foreach (var value in testValues)
            Int32Type.Instance.WriteValue(writer, value);

        var binaryData = writer.WrittenSpan.ToArray();
        
        _output.WriteLine($"Inserting {testValues.Length} Int32 values using RowBinary format");
        _output.WriteLine($"Binary data length: {binaryData.Length} bytes (expected: {testValues.Length * 4})");
        _output.WriteLine($"Binary data (hex): {Convert.ToHexString(binaryData)}");

        const string insertQuery = "INSERT INTO test_int32_rowbinary FORMAT RowBinary";
        var insertResponse = await ExecuteBinaryQuery(insertQuery, binaryData);
        
        if (!insertResponse.IsSuccessStatusCode)
        {
            var error = await insertResponse.Content.ReadAsStringAsync();
            _output.WriteLine($"Insert failed: {error}");
        }
        insertResponse.EnsureSuccessStatusCode();

        const string selectQuery = "SELECT test_value FROM test_int32_rowbinary ORDER BY test_value FORMAT RowBinary";
        var selectResponse = await ExecuteQuery(selectQuery);
        selectResponse.EnsureSuccessStatusCode();
        
        var responseBytes = await selectResponse.Content.ReadAsByteArrayAsync();
        _output.WriteLine($"Response binary length: {responseBytes.Length} bytes");
        _output.WriteLine($"Response binary (hex): {Convert.ToHexString(responseBytes)}");
        
        // Deserialize using Int32Type
        var sequence = new ReadOnlySequence<byte>(responseBytes);
        var results = new List<int>();
        
        while (sequence.Length >= 4)
        {
            var value = Int32Type.Instance.ReadValue(ref sequence, out _);
            results.Add(value);
            _output.WriteLine($"Read value: {value}");
        }
        
        var expectedSorted = testValues.OrderBy(v => v).ToArray();
        var actualSorted = results.ToArray();
        Assert.Equal(expectedSorted.Length, actualSorted.Length);
        for (var i = 0; i < expectedSorted.Length; i++)
            Assert.Equal(expectedSorted[i], actualSorted[i]);

        _output.WriteLine($"Successfully round-tripped {results.Count} Int32 values through RowBinary format");
    }
    
    [Fact]
    public async Task BulkInsert_UsingRowBinary_LargeDataset()
    {
        await ExecuteQuery("DROP TABLE IF EXISTS test_int32_bulk");
        await ExecuteQuery("""
                           CREATE TABLE test_int32_bulk (
                               value Int32
                           ) ENGINE = Memory
                           """);

        const int count = 10000;
        var values = new int[count];
        var random = new Random(42);
        for (var i = 0; i < count; i++)
        {
            values[i] = random.Next(int.MinValue, int.MaxValue);
        }

        // Serialize using Int32Type.WriteValues (bulk operation)
        var writer = new ArrayBufferWriter<byte>();
        Int32Type.Instance.WriteValues(writer, values);
        var binaryData = writer.WrittenSpan.ToArray();

        _output.WriteLine($"Bulk inserting {count} Int32 values");
        _output.WriteLine($"Binary data length: {binaryData.Length} bytes (expected: {count * 4})");

        const string insertQuery = "INSERT INTO test_int32_bulk FORMAT RowBinary";
        var insertResponse = await ExecuteBinaryQuery(insertQuery, binaryData);
        insertResponse.EnsureSuccessStatusCode();

        // Read back and verify count
        const string countQuery = "SELECT COUNT(*) FROM test_int32_bulk FORMAT RowBinary";
        var countResponse = await ExecuteQuery(countQuery);
        var countBytes = await countResponse.Content.ReadAsByteArrayAsync();
        var countSequence = new ReadOnlySequence<byte>(countBytes);
        
        // COUNT(*) returns UInt64 in RowBinary
        var actualCount = ReadUInt64LittleEndian(ref countSequence);
        Assert.Equal((ulong)count, actualCount);

        // Read back a sample and verify
        const string selectQuery = "SELECT value FROM test_int32_bulk ORDER BY value LIMIT 100 FORMAT RowBinary";
        var selectResponse = await ExecuteQuery(selectQuery);
        var responseBytes = await selectResponse.Content.ReadAsByteArrayAsync();
        
        var sequence = new ReadOnlySequence<byte>(responseBytes);
        var results = new int[100];
        var readCount = Int32Type.Instance.ReadValues(ref sequence, results, out var bytesConsumed);
        
        Assert.Equal(100, readCount);
        Assert.Equal(400, bytesConsumed);
        
        // Verify the values are sorted
        for (var i = 1; i < readCount; i++)
        {
            Assert.True(results[i] >= results[i - 1], $"Values not sorted: {results[i - 1]} > {results[i]}");
        }

        _output.WriteLine($"Successfully bulk inserted and read {count} Int32 values");
    }
    
    [Fact]
    public async Task NullableInt32_RowBinary_HandlesNullsCorrectly()
    {
        await ExecuteQuery("DROP TABLE IF EXISTS test_nullable_int32");
        await ExecuteQuery("""
                           CREATE TABLE test_nullable_int32 (
                               value Nullable(Int32)
                           ) ENGINE = Memory
                           """);

        // For nullable types in RowBinary:
        // - 0x00 byte followed by the value for non-null
        // - 0x01 byte for null
        var writer = new ArrayBufferWriter<byte>();
        
        // Write: 42, NULL, -100, NULL, 0
        writer.GetSpan(1)[0] = 0x00; // not null
        writer.Advance(1);
        Int32Type.Instance.WriteValue(writer, 42);
        
        writer.GetSpan(1)[0] = 0x01; // null
        writer.Advance(1);
        
        writer.GetSpan(1)[0] = 0x00; // not null
        writer.Advance(1);
        Int32Type.Instance.WriteValue(writer, -100);
        
        writer.GetSpan(1)[0] = 0x01; // null
        writer.Advance(1);
        
        writer.GetSpan(1)[0] = 0x00; // not null
        writer.Advance(1);
        Int32Type.Instance.WriteValue(writer, 0);

        var binaryData = writer.WrittenSpan.ToArray();
        _output.WriteLine("Inserting nullable Int32 values");
        _output.WriteLine($"Binary data (hex): {Convert.ToHexString(binaryData)}");

        const string insertQuery = "INSERT INTO test_nullable_int32 FORMAT RowBinary";
        var insertResponse = await ExecuteBinaryQuery(insertQuery, binaryData);
        insertResponse.EnsureSuccessStatusCode();

        // Read back
        const string selectQuery = "SELECT value FROM test_nullable_int32 FORMAT RowBinary";
        var selectResponse = await ExecuteQuery(selectQuery);
        var responseBytes = await selectResponse.Content.ReadAsByteArrayAsync();
        
        _output.WriteLine($"Response binary (hex): {Convert.ToHexString(responseBytes)}");
        
        var sequence = new ReadOnlySequence<byte>(responseBytes);
        var results = new List<int?>();
        
        while (sequence.Length > 0)
        {
            var isNull = sequence.First.Span[0];
            sequence = sequence.Slice(1);
            
            if (isNull == 0x01)
            {
                results.Add(null);
                _output.WriteLine("Read value: NULL");
            }
            else
            {
                var value = Int32Type.Instance.ReadValue(ref sequence, out _);
                results.Add(value);
                _output.WriteLine($"Read value: {value}");
            }
        }
        
        Assert.Equal(5, results.Count);
        Assert.Equal(42, results[0]);
        Assert.Null(results[1]);
        Assert.Equal(-100, results[2]);
        Assert.Null(results[3]);
        Assert.Equal(0, results[4]);

        _output.WriteLine("Successfully handled nullable Int32 values");
    }
    
    [Theory]
    [InlineData(100)]
    [InlineData(1000)]
    [InlineData(10000)]
    public async Task Performance_BulkOperations_DifferentSizes(int count)
    {
        var tableName = $"test_int32_perf_{Guid.NewGuid():N}";
        await ExecuteQuery($"DROP TABLE IF EXISTS {tableName}");
        await ExecuteQuery($"CREATE TABLE {tableName} (value Int32) ENGINE = Memory");

        try
        {
            var values = Enumerable.Range(1, count).ToArray();
            
            var writer = new ArrayBufferWriter<byte>();
            var insertStart = DateTime.UtcNow;
            Int32Type.Instance.WriteValues(writer, values);
            var serializeTime = DateTime.UtcNow - insertStart;
            
            var binaryData = writer.WrittenSpan.ToArray();
            
            insertStart = DateTime.UtcNow;
            var insertQuery = $"INSERT INTO {tableName} FORMAT RowBinary";
            var insertResponse = await ExecuteBinaryQuery(insertQuery, binaryData);
            insertResponse.EnsureSuccessStatusCode();
            var insertTime = DateTime.UtcNow - insertStart;
            
            var selectStart = DateTime.UtcNow;
            var selectQuery = $"SELECT value FROM {tableName} ORDER BY value FORMAT RowBinary";
            var selectResponse = await ExecuteQuery(selectQuery);
            var responseBytes = await selectResponse.Content.ReadAsByteArrayAsync();
            
            var sequence = new ReadOnlySequence<byte>(responseBytes);
            var results = new int[count];
            var itemsRead = Int32Type.Instance.ReadValues(ref sequence, results, out _);
            var selectTime = DateTime.UtcNow - selectStart;
            
            Assert.Equal(count, itemsRead);
            Assert.Equal(values, results);
            
            _output.WriteLine($"Count: {count}");
            _output.WriteLine($"  Serialize: {serializeTime.TotalMilliseconds:F2}ms");
            _output.WriteLine($"  Insert: {insertTime.TotalMilliseconds:F2}ms");
            _output.WriteLine($"  Select+Deserialize: {selectTime.TotalMilliseconds:F2}ms");
        }
        finally
        {
            await ExecuteQuery($"DROP TABLE IF EXISTS {tableName}");
        }
    }
    
    private async Task<HttpResponseMessage> ExecuteQuery(string query)
    {
        return await _httpClient.ExecuteQuery(_fixture.Hostname, _fixture.HttpPort, query);
    }
    
    private async Task<HttpResponseMessage> ExecuteBinaryQuery(string query, byte[] binaryData)
    {
        return await _httpClient.ExecuteBinaryQuery(_fixture.Hostname, _fixture.HttpPort, query, binaryData);
    }
    
    private static ulong ReadUInt64LittleEndian(ref ReadOnlySequence<byte> sequence)
    {
        Span<byte> buffer = stackalloc byte[8];
        sequence.Slice(0, 8).CopyTo(buffer);
        sequence = sequence.Slice(8);
        return BitConverter.ToUInt64(buffer);
    }
    
    private static void WriteUInt64LittleEndian(IBufferWriter<byte> writer, ulong value)
    {
        var span = writer.GetSpan(8);
        BitConverter.TryWriteBytes(span, value);
        writer.Advance(8);
    }
}