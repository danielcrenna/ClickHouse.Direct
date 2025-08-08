using System.Buffers;
using System.Net.Http.Headers;
using System.Text;
using System.Text.Json;
using ClickHouse.Direct.Types;
using Xunit.Abstractions;

namespace ClickHouse.Direct.IntegrationTests.Types;

[Collection("ClickHouse")]
public class StringTypeIntegrationTests : IClassFixture<ClickHouseContainerFixture>, IDisposable
{
    private readonly ClickHouseContainerFixture _fixture;
    private readonly ITestOutputHelper _output;
    private readonly HttpClient _httpClient;

    public StringTypeIntegrationTests(ClickHouseContainerFixture fixture, ITestOutputHelper output)
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
        await ExecuteQuery("DROP TABLE IF EXISTS test_string_rowbinary");
        await ExecuteQuery("""
                           CREATE TABLE test_string_rowbinary (
                               test_string String
                           ) ENGINE = Memory
                           """);

        var testStrings = new[]
        {
            "",
            "Hello",
            "World!",
            "Hello, ClickHouse! 🎉",
            "こんにちは世界",
            "Привет мир",
            "Mixed ASCII + こんにちは + 🚀",
            new string('A', 1000), // Long ASCII
            "Emoji test: 🔥💯⚡🎯🌟"
        };

        var writer = new ArrayBufferWriter<byte>();
        foreach (var str in testStrings)
            StringType.Instance.WriteValue(writer, str);

        var binaryData = writer.WrittenSpan.ToArray();
        
        _output.WriteLine($"Inserting {testStrings.Length} strings using RowBinary format");
        _output.WriteLine($"Binary data length: {binaryData.Length} bytes");
        _output.WriteLine($"Binary data (hex): {Convert.ToHexString(binaryData)}");

        const string insertQuery = "INSERT INTO test_string_rowbinary FORMAT RowBinary";
        var insertResponse = await ExecuteBinaryQuery(insertQuery, binaryData);
        
        if (!insertResponse.IsSuccessStatusCode)
        {
            var error = await insertResponse.Content.ReadAsStringAsync();
            _output.WriteLine($"Insert failed: {error}");
        }
        insertResponse.EnsureSuccessStatusCode();

        const string selectQuery = "SELECT test_string FROM test_string_rowbinary ORDER BY test_string FORMAT RowBinary";
        var selectResponse = await ExecuteQuery(selectQuery);
        selectResponse.EnsureSuccessStatusCode();
        
        var responseBytes = await selectResponse.Content.ReadAsByteArrayAsync();
        _output.WriteLine($"Response binary length: {responseBytes.Length} bytes");
        _output.WriteLine($"Response binary (hex): {Convert.ToHexString(responseBytes)}");
        
        var sequence = new ReadOnlySequence<byte>(responseBytes);
        var results = new List<string>();
        
        while (sequence.Length > 0)
        {
            var str = StringType.Instance.ReadValue(ref sequence, out _);
            results.Add(str);
            _output.WriteLine($"Read string: '{str}' (length: {str.Length})");
        }
        
        var expectedSorted = testStrings.OrderBy(s => s, StringComparer.Ordinal).ToArray();
        var actualSorted = results.ToArray();
        Assert.Equal(expectedSorted.Length, actualSorted.Length);
        for (var i = 0; i < expectedSorted.Length; i++)
            Assert.Equal(expectedSorted[i], actualSorted[i]);

        _output.WriteLine($"Successfully round-tripped {results.Count} strings through RowBinary format");
    }
    
    [Fact]
    public async Task SingleString_RowBinaryRoundTrip_PreservesExactBytes()
    {
        await ExecuteQuery("DROP TABLE IF EXISTS test_single_string_binary");
        await ExecuteQuery("""
                           CREATE TABLE test_single_string_binary (
                               test_string String
                           ) ENGINE = Memory
                           """);

        const string testString = "Hello, ClickHouse! 🎉 こんにちは";
        
        var writer = new ArrayBufferWriter<byte>();
        StringType.Instance.WriteValue(writer, testString);
        var ourBytes = writer.WrittenSpan.ToArray();
        
        _output.WriteLine($"Test string: '{testString}'");
        _output.WriteLine($"Test string length: {testString.Length} chars, {Encoding.UTF8.GetByteCount(testString)} UTF-8 bytes");
        _output.WriteLine($"Our serialization: {Convert.ToHexString(ourBytes)}");
        
        var insertResponse = await ExecuteBinaryQuery(
            "INSERT INTO test_single_string_binary FORMAT RowBinary",
            ourBytes);
        insertResponse.EnsureSuccessStatusCode();
        
        var selectResponse = await ExecuteQuery("SELECT test_string FROM test_single_string_binary FORMAT RowBinary");
        selectResponse.EnsureSuccessStatusCode();
        
        var clickHouseBytes = await selectResponse.Content.ReadAsByteArrayAsync();
        _output.WriteLine($"ClickHouse returned: {Convert.ToHexString(clickHouseBytes)}");
        
        Assert.Equal(ourBytes, clickHouseBytes);
        
        var sequence = new ReadOnlySequence<byte>(clickHouseBytes);
        var resultString = StringType.Instance.ReadValue(ref sequence, out _);
        Assert.Equal(testString, resultString);
    }

    [Fact]
    public async Task MixedFormat_TextInsertBinarySelect_ShouldWork()
    {
        await ExecuteQuery("DROP TABLE IF EXISTS test_mixed_formats");
        await ExecuteQuery("""
                           CREATE TABLE test_mixed_formats (
                               id Int32,
                               string_value String
                           ) ENGINE = Memory
                           """);

        var testData = new[]
        {
            (1, "Hello World"),
            (2, "こんにちは"),
            (3, ""),
            (4, "Emoji 🎉🚀⚡"),
            (5, "Long string: " + new string('X', 500))
        };

        foreach (var (id, str) in testData)
        {
            var escapedString = str.Replace("'", "\\'");
            await ExecuteQuery($"INSERT INTO test_mixed_formats VALUES ({id}, '{escapedString}')");
        }

        var selectResponse = await ExecuteQuery(
            "SELECT string_value FROM test_mixed_formats ORDER BY id FORMAT RowBinary");
        selectResponse.EnsureSuccessStatusCode();

        var binaryData = await selectResponse.Content.ReadAsByteArrayAsync();
        _output.WriteLine($"Selected {binaryData.Length} bytes in RowBinary format");

        var sequence = new ReadOnlySequence<byte>(binaryData);
        var results = new List<string>();

        while (sequence.Length > 0)
        {
            var str = StringType.Instance.ReadValue(ref sequence, out _);
            results.Add(str);
        }

        Assert.Equal(testData.Length, results.Count);
        for (var i = 0; i < testData.Length; i++)
        {
            Assert.Equal(testData[i].Item2, results[i]);
            _output.WriteLine($"Verified string {i + 1}: '{results[i]}'");
        }
    }

    [Fact]
    public async Task MixedFormat_BinaryInsertTextSelect_ShouldWork()
    {
        await ExecuteQuery("DROP TABLE IF EXISTS test_binary_to_text");
        await ExecuteQuery("""
                           CREATE TABLE test_binary_to_text (
                               string_value String
                           ) ENGINE = Memory
                           """);

        var testStrings = new[]
        {
            "ASCII only",
            "Unicode: こんにちは",
            "Mixed: Hello 世界 🌍",
            "",
            "Special chars: \n\t\r\\",
            new string('Z', 100)
        };

        var writer = new ArrayBufferWriter<byte>();
        foreach (var str in testStrings)
            StringType.Instance.WriteValue(writer, str);

        var insertResponse = await ExecuteBinaryQuery("INSERT INTO test_binary_to_text FORMAT RowBinary",
            writer.WrittenSpan.ToArray());
        insertResponse.EnsureSuccessStatusCode();

        var selectResponse = await ExecuteQuery("SELECT string_value FROM test_binary_to_text FORMAT JSON");
        selectResponse.EnsureSuccessStatusCode();

        var jsonResult = await selectResponse.Content.ReadAsStringAsync();
        var jsonDocument = JsonDocument.Parse(jsonResult);
        var rows = jsonDocument.RootElement.GetProperty("data").EnumerateArray().ToArray();

        Assert.Equal(testStrings.Length, rows.Length);

        var returnedStrings = rows.Select(row =>
            row.GetProperty("string_value").GetString()!
        ).ToArray();

        Assert.Equal(testStrings.OrderBy(s => s), returnedStrings.OrderBy(s => s));
        
        foreach (var str in returnedStrings)
            _output.WriteLine($"Retrieved string via JSON: '{str}'");
    }

    [Theory]
    [InlineData(10)]
    [InlineData(100)]
    [InlineData(1000)]
    public async Task BulkStringOperations_UsingRowBinary_ShouldHandleVariousSizes(int count)
    {
        _output.WriteLine($"Testing bulk string operations with {count} records using RowBinary");
        
        await ExecuteQuery($"DROP TABLE IF EXISTS test_string_bulk_{count}");
        await ExecuteQuery($"""
                            CREATE TABLE test_string_bulk_{count} (
                                string_value String
                            ) ENGINE = Memory
                            """);

        var random = new Random(42);
        var testStrings = Enumerable.Range(0, count)
            .Select(i => GenerateTestString(random, i))
            .ToArray();

        var writer = new ArrayBufferWriter<byte>();
        StringType.Instance.WriteValues(writer, testStrings);
        var binaryData = writer.WrittenSpan.ToArray();
        
        _output.WriteLine($"Inserting {count} strings, binary size: {binaryData.Length} bytes");

        var insertResponse = await ExecuteBinaryQuery($"INSERT INTO test_string_bulk_{count} FORMAT RowBinary",
            binaryData);
        insertResponse.EnsureSuccessStatusCode();

        var selectResponse = await ExecuteQuery($"SELECT string_value FROM test_string_bulk_{count} FORMAT RowBinary");
        selectResponse.EnsureSuccessStatusCode();
        
        var responseBytes = await selectResponse.Content.ReadAsByteArrayAsync();
        _output.WriteLine($"Received {responseBytes.Length} bytes from ClickHouse");
        
        var sequence = new ReadOnlySequence<byte>(responseBytes);
        var results = new string[count];
        var itemsRead = StringType.Instance.ReadValues(ref sequence, results, out var bytesConsumed);
        
        Assert.Equal(count, itemsRead);
        Assert.Equal(binaryData.Length, bytesConsumed);
        Assert.Equal(0, sequence.Length); // All bytes should be consumed
        
        var originalSet = new HashSet<string>(testStrings);
        var resultSet = new HashSet<string>(results);
        Assert.Equal(originalSet, resultSet);

        _output.WriteLine($"Successfully round-tripped {count} string records through RowBinary");
    }

    [Fact]
    public async Task BulkInsert_UsingRowBinary_LargeDataset()
    {
        await ExecuteQuery("DROP TABLE IF EXISTS test_string_bulk");
        await ExecuteQuery("""
                           CREATE TABLE test_string_bulk (
                               value String
                           ) ENGINE = Memory
                           """);

        const int count = 5000;
        var strings = new string[count];
        var random = new Random(42);
        for (var i = 0; i < count; i++)
        {
            strings[i] = GenerateTestString(random, i);
        }

        var writer = new ArrayBufferWriter<byte>();
        StringType.Instance.WriteValues(writer, strings);
        var binaryData = writer.WrittenSpan.ToArray();

        _output.WriteLine($"Bulk inserting {count} strings");
        _output.WriteLine($"Binary data length: {binaryData.Length} bytes");

        const string insertQuery = "INSERT INTO test_string_bulk FORMAT RowBinary";
        var insertResponse = await ExecuteBinaryQuery(insertQuery, binaryData);
        insertResponse.EnsureSuccessStatusCode();

        // Read back and verify count
        const string countQuery = "SELECT COUNT(*) FROM test_string_bulk FORMAT RowBinary";
        var countResponse = await ExecuteQuery(countQuery);
        var countBytes = await countResponse.Content.ReadAsByteArrayAsync();
        var countSequence = new ReadOnlySequence<byte>(countBytes);
        
        // COUNT(*) returns UInt64 in RowBinary
        var actualCount = ReadUInt64LittleEndian(ref countSequence);
        Assert.Equal((ulong)count, actualCount);

        const string selectQuery = "SELECT value FROM test_string_bulk ORDER BY value LIMIT 100 FORMAT RowBinary";
        var selectResponse = await ExecuteQuery(selectQuery);
        var responseBytes = await selectResponse.Content.ReadAsByteArrayAsync();
        
        var sequence = new ReadOnlySequence<byte>(responseBytes);
        var results = new string[100];
        var readCount = StringType.Instance.ReadValues(ref sequence, results, out _);
        
        Assert.Equal(100, readCount);
        
        // Verify the values are sorted
        for (var i = 1; i < readCount; i++)
        {
            Assert.True(string.CompareOrdinal(results[i], results[i - 1]) >= 0, 
                $"Values not sorted: '{results[i - 1]}' > '{results[i]}'");
        }

        _output.WriteLine($"Successfully bulk inserted and read {count} string values");
    }
    
    [Fact]
    public async Task NullableString_RowBinary_HandlesNullsCorrectly()
    {
        await ExecuteQuery("DROP TABLE IF EXISTS test_nullable_string");
        await ExecuteQuery("""
                           CREATE TABLE test_nullable_string (
                               value Nullable(String)
                           ) ENGINE = Memory
                           """);

        // For nullable types in RowBinary:
        // - 0x00 byte followed by the value for non-null
        // - 0x01 byte for null
        var writer = new ArrayBufferWriter<byte>();
        
        // Write: "Hello", NULL, "", NULL, "World"
        writer.GetSpan(1)[0] = 0x00; // not null
        writer.Advance(1);
        StringType.Instance.WriteValue(writer, "Hello");
        
        writer.GetSpan(1)[0] = 0x01; // null
        writer.Advance(1);
        
        writer.GetSpan(1)[0] = 0x00; // not null
        writer.Advance(1);
        StringType.Instance.WriteValue(writer, "");
        
        writer.GetSpan(1)[0] = 0x01; // null
        writer.Advance(1);
        
        writer.GetSpan(1)[0] = 0x00; // not null
        writer.Advance(1);
        StringType.Instance.WriteValue(writer, "World");

        var binaryData = writer.WrittenSpan.ToArray();
        _output.WriteLine("Inserting nullable string values");
        _output.WriteLine($"Binary data (hex): {Convert.ToHexString(binaryData)}");

        const string insertQuery = "INSERT INTO test_nullable_string FORMAT RowBinary";
        var insertResponse = await ExecuteBinaryQuery(insertQuery, binaryData);
        insertResponse.EnsureSuccessStatusCode();

        // Read back
        const string selectQuery = "SELECT value FROM test_nullable_string FORMAT RowBinary";
        var selectResponse = await ExecuteQuery(selectQuery);
        var responseBytes = await selectResponse.Content.ReadAsByteArrayAsync();
        
        _output.WriteLine($"Response binary (hex): {Convert.ToHexString(responseBytes)}");
        
        var sequence = new ReadOnlySequence<byte>(responseBytes);
        var results = new List<string?>();
        
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
                var value = StringType.Instance.ReadValue(ref sequence, out _);
                results.Add(value);
                _output.WriteLine($"Read value: '{value}'");
            }
        }
        
        Assert.Equal(5, results.Count);
        Assert.Equal("Hello", results[0]);
        Assert.Null(results[1]);
        Assert.Equal("", results[2]);
        Assert.Null(results[3]);
        Assert.Equal("World", results[4]);

        _output.WriteLine("Successfully handled nullable string values");
    }
    
    [Theory]
    [InlineData(100)]
    [InlineData(1000)]
    [InlineData(5000)]
    public async Task Performance_BulkOperations_DifferentSizes(int count)
    {
        var tableName = $"test_string_perf_{Guid.NewGuid():N}";
        await ExecuteQuery($"DROP TABLE IF EXISTS {tableName}");
        await ExecuteQuery($"CREATE TABLE {tableName} (value String) ENGINE = Memory");

        try
        {
            var strings = Enumerable.Range(1, count)
                .Select(i => $"String_{i:D6}")
                .ToArray();
            
            var writer = new ArrayBufferWriter<byte>();
            var insertStart = DateTime.UtcNow;
            StringType.Instance.WriteValues(writer, strings);
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
            var results = new string[count];
            var itemsRead = StringType.Instance.ReadValues(ref sequence, results, out _);
            var selectTime = DateTime.UtcNow - selectStart;
            
            Assert.Equal(count, itemsRead);
            Assert.Equal(strings, results);
            
            _output.WriteLine($"Count: {count}");
            _output.WriteLine($"  Serialize: {serializeTime.TotalMilliseconds:F2}ms");
            _output.WriteLine($"  Insert: {insertTime.TotalMilliseconds:F2}ms");
            _output.WriteLine($"  Select+Deserialize: {selectTime.TotalMilliseconds:F2}ms");
            _output.WriteLine($"  Total size: {binaryData.Length:N0} bytes");
        }
        finally
        {
            await ExecuteQuery($"DROP TABLE IF EXISTS {tableName}");
        }
    }
    
    private static string GenerateTestString(Random random, int index)
    {
        return random.Next(6) switch
        {
            0 => "", // Empty string
            1 => GenerateAsciiString(random, random.Next(1, 50)), // Short ASCII
            2 => GenerateUnicodeString(random, random.Next(1, 20)), // Unicode
            3 => GenerateLongString(random, random.Next(100, 500)), // Long string
            4 => GenerateMixedString(random, random.Next(10, 100)), // Mixed content
            _ => $"Test string {index}" // Simple indexed string
        };
    }

    private static string GenerateAsciiString(Random random, int length)
    {
        var chars = new char[length];
        for (var i = 0; i < length; i++)
        {
            chars[i] = (char)('a' + random.Next(26));
        }
        return new string(chars);
    }

    private static string GenerateUnicodeString(Random random, int length)
    {
        var chars = new char[length];
        for (var i = 0; i < length; i++)
        {
            chars[i] = (char)('あ' + random.Next(100)); // Japanese characters
        }
        return new string(chars);
    }

    private static string GenerateLongString(Random random, int length)
    {
        var chars = new char[length];
        for (var i = 0; i < length; i++)
        {
            chars[i] = (char)('A' + random.Next(26));
        }
        return new string(chars);
    }

    private static string GenerateMixedString(Random random, int length)
    {
        var chars = new char[length];
        for (var i = 0; i < length; i++)
        {
            chars[i] = random.Next(4) switch
            {
                0 => (char)('a' + random.Next(26)), // ASCII lowercase
                1 => (char)('A' + random.Next(26)), // ASCII uppercase
                2 => (char)('А' + random.Next(32)), // Cyrillic
                _ => (char)('あ' + random.Next(50))  // Japanese
            };
        }
        return new string(chars);
    }
    
    private async Task<HttpResponseMessage> ExecuteQuery(string query)
    {
        return await _httpClient.ExecuteQuery(_fixture.Hostname, _fixture.HttpPort, query);
    }
    
    private async Task<HttpResponseMessage> ExecuteBinaryQuery(string query, byte[] binaryData)
    {
        return await _httpClient.ExecuteBinaryQuery(_fixture.Hostname, _fixture.HttpPort, query, binaryData);
    }
    
    // TODO use specific type when available
    private static ulong ReadUInt64LittleEndian(ref ReadOnlySequence<byte> sequence)
    {
        Span<byte> buffer = stackalloc byte[8];
        sequence.Slice(0, 8).CopyTo(buffer);
        sequence = sequence.Slice(8);
        return BitConverter.ToUInt64(buffer);
    }
    
    public void Dispose() => _httpClient.Dispose();
}