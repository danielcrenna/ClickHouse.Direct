using System.Buffers;
using System.Text.Json;
using ClickHouse.Direct.Types;
using Xunit.Abstractions;

namespace ClickHouse.Direct.IntegrationTests.Types;

[Collection("ClickHouse")]
public class StringTypeIntegrationTests(ClickHouseContainerFixture fixture, ITestOutputHelper output)
    : TypeIntegrationTestBase(fixture, output)
{
    [Fact]
    public async Task InsertAndSelect_UsingRowBinary_ShouldRoundTrip()
    {
        var tableName = GetSanitizedTableName("test_string_rowbinary");
        await Transport.ExecuteNonQueryAsync($"DROP TABLE IF EXISTS {tableName}");
        await Transport.ExecuteNonQueryAsync($"""
            CREATE TABLE {tableName} (
                test_string String
            ) ENGINE = Memory
            """);

        var testStrings = new[]
        {
            "",
            "Hello",
            "World",
            "ClickHouse",
            "Special chars: !@#$%^&*()",
            "Unicode: 你好世界 🚀",
            "Emoji: 😀😁😂🤣😃😄😅",
            "Newline\nand\ttabs",
            "Long string: " + new string('a', 1000),
            "Mixed: ABC123!@#你好🚀"
        };

        var writer = new ArrayBufferWriter<byte>();
        foreach (var str in testStrings)
            StringType.Instance.WriteValue(writer, str);

        Output.WriteLine($"Inserting {testStrings.Length} string values using RowBinary format");
        await SendRowBinaryDataAsync(tableName, writer.WrittenMemory);

        var sequence = await QueryRowBinaryDataAsync($"SELECT test_string FROM {tableName}");
        
        var actualStrings = new List<string>();
        for (var i = 0; i < testStrings.Length; i++)
        {
            var value = StringType.Instance.ReadValue(ref sequence, out _);
            actualStrings.Add(value);
            Output.WriteLine($"Read string {i}: '{(value.Length > 50 ? value[..50] + "..." : value)}'");
            Assert.Equal(testStrings[i], value);
        }

        Assert.Equal(testStrings, actualStrings);
        await Transport.ExecuteNonQueryAsync($"DROP TABLE {tableName}");
    }

    [Fact]
    public async Task BulkInsert_LargeDataset_PerformanceTest()
    {
        var tableName = GetSanitizedTableName("test_string_bulk");
        await Transport.ExecuteNonQueryAsync($"DROP TABLE IF EXISTS {tableName}");
        await Transport.ExecuteNonQueryAsync($"""
            CREATE TABLE {tableName} (
                id Int32,
                data String
            ) ENGINE = Memory
            """);

        const int recordCount = 10000;
        var random = new Random(42);
        var strings = new List<string>(recordCount);
        
        for (var i = 0; i < recordCount; i++)
        {
            var length = random.Next(1, 100);
            var chars = new char[length];
            for (var j = 0; j < length; j++)
                chars[j] = (char)random.Next(32, 127);
            strings.Add(new string(chars));
        }

        var writer = new ArrayBufferWriter<byte>();
        for (var i = 0; i < recordCount; i++)
        {
            Int32Type.Instance.WriteValue(writer, i);
            StringType.Instance.WriteValue(writer, strings[i]);
        }

        var sw = System.Diagnostics.Stopwatch.StartNew();
        await SendRowBinaryDataAsync(tableName, writer.WrittenMemory);
        sw.Stop();
        
        Output.WriteLine($"Inserted {recordCount} records with strings in {sw.Elapsed.TotalMilliseconds:F2}ms");

        var countStr = await GetScalarValueAsync($"SELECT COUNT(*) FROM {tableName}");
        Assert.Equal(recordCount.ToString(), countStr);

        var avgLengthStr = await GetScalarValueAsync($"SELECT AVG(LENGTH(data)) FROM {tableName}");
        var avgLength = double.Parse(avgLengthStr);
        Output.WriteLine($"Average string length: {avgLength:F2}");
        Assert.True(avgLength > 40 && avgLength < 60);

        await Transport.ExecuteNonQueryAsync($"DROP TABLE {tableName}");
    }

    [Fact]
    public async Task JsonStrings_ShouldStoreAndRetrieveCorrectly()
    {
        var tableName = GetSanitizedTableName("test_json_strings");
        await Transport.ExecuteNonQueryAsync($"DROP TABLE IF EXISTS {tableName}");
        await Transport.ExecuteNonQueryAsync($"""
            CREATE TABLE {tableName} (
                id Int32,
                json_data String
            ) ENGINE = Memory
            """);

        var testData = new[]
        {
            new { id = 1, name = "Alice", age = 30, active = true },
            new { id = 2, name = "Bob", age = 25, active = false },
            new { id = 3, name = "Charlie", age = 35, active = true }
        };

        var writer = new ArrayBufferWriter<byte>();
        foreach (var item in testData)
        {
            Int32Type.Instance.WriteValue(writer, item.id);
            var json = JsonSerializer.Serialize(item);
            StringType.Instance.WriteValue(writer, json);
        }

        await SendRowBinaryDataAsync(tableName, writer.WrittenMemory);

        var sequence = await QueryRowBinaryDataAsync($"SELECT id, json_data FROM {tableName} ORDER BY id");
        
        for (var i = 0; i < testData.Length; i++)
        {
            var id = Int32Type.Instance.ReadValue(ref sequence, out _);
            var jsonStr = StringType.Instance.ReadValue(ref sequence, out _);
            
            Assert.Equal(testData[i].id, id);
            
            using var doc = JsonDocument.Parse(jsonStr);
            var root = doc.RootElement;
            Assert.Equal(testData[i].id, root.GetProperty("id").GetInt32());
            Assert.Equal(testData[i].name, root.GetProperty("name").GetString());
            Assert.Equal(testData[i].age, root.GetProperty("age").GetInt32());
            Assert.Equal(testData[i].active, root.GetProperty("active").GetBoolean());
        }

        await Transport.ExecuteNonQueryAsync($"DROP TABLE {tableName}");
    }

    [Fact]
    public async Task NullableStrings_HandlesNullsCorrectly()
    {
        var tableName = GetSanitizedTableName("test_nullable_strings");
        await Transport.ExecuteNonQueryAsync($"DROP TABLE IF EXISTS {tableName}");
        await Transport.ExecuteNonQueryAsync($"""
            CREATE TABLE {tableName} (
                id Int32,
                nullable_string Nullable(String)
            ) ENGINE = Memory
            """);

        var testData = new[]
        {
            (id: 1, str: (string?)"Hello"),
            (id: 2, str: null),
            (id: 3, str: (string?)""),
            (id: 4, str: null),
            (id: 5, str: (string?)"World")
        };

        var writer = new ArrayBufferWriter<byte>();
        foreach (var item in testData)
        {
            Int32Type.Instance.WriteValue(writer, item.id);
            
            var span = writer.GetSpan(1);
            if (item.str != null)
            {
                span[0] = 0;
                writer.Advance(1);
                StringType.Instance.WriteValue(writer, item.str);
            }
            else
            {
                span[0] = 1;
                writer.Advance(1);
            }
        }

        await SendRowBinaryDataAsync(tableName, writer.WrittenMemory);

        var nullCountStr = await GetScalarValueAsync($"SELECT COUNT(*) FROM {tableName} WHERE nullable_string IS NULL");
        Assert.Equal("2", nullCountStr);

        var notNullCountStr = await GetScalarValueAsync($"SELECT COUNT(*) FROM {tableName} WHERE nullable_string IS NOT NULL");
        Assert.Equal("3", notNullCountStr);

        var emptyCountStr = await GetScalarValueAsync($"SELECT COUNT(*) FROM {tableName} WHERE nullable_string = ''");
        Assert.Equal("1", emptyCountStr);

        await Transport.ExecuteNonQueryAsync($"DROP TABLE {tableName}");
    }

    [Fact]
    public async Task SpecialCharacters_AllCharsShouldRoundTrip()
    {
        var tableName = GetSanitizedTableName("test_special_chars");
        await Transport.ExecuteNonQueryAsync($"DROP TABLE IF EXISTS {tableName}");
        await Transport.ExecuteNonQueryAsync($"""
            CREATE TABLE {tableName} (
                char_code Int32,
                char_string String
            ) ENGINE = Memory
            """);

        var writer = new ArrayBufferWriter<byte>();
        
        for (var i = 1; i < 128; i++)
        {
            if (i == 0) continue;
            
            Int32Type.Instance.WriteValue(writer, i);
            StringType.Instance.WriteValue(writer, ((char)i).ToString());
        }

        await SendRowBinaryDataAsync(tableName, writer.WrittenMemory);

        var countStr = await GetScalarValueAsync($"SELECT COUNT(*) FROM {tableName}");
        Assert.Equal("127", countStr);

        var sequence = await QueryRowBinaryDataAsync($"SELECT char_code, char_string FROM {tableName} ORDER BY char_code");
        
        for (var i = 1; i < 128; i++)
        {
            var code = Int32Type.Instance.ReadValue(ref sequence, out _);
            var str = StringType.Instance.ReadValue(ref sequence, out _);
            
            Assert.Equal(i, code);
            Assert.Equal(((char)i).ToString(), str);
        }

        await Transport.ExecuteNonQueryAsync($"DROP TABLE {tableName}");
    }
}