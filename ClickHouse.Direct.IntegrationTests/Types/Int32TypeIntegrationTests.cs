using System.Buffers;
using ClickHouse.Direct.Types;
using Xunit.Abstractions;

namespace ClickHouse.Direct.IntegrationTests.Types;

[Collection("ClickHouse")]
public class Int32TypeIntegrationTests(ClickHouseContainerFixture fixture, ITestOutputHelper output)
    : TypeIntegrationTestBase(fixture, output)
{
    [Fact]
    public async Task InsertAndSelect_UsingRowBinary_ShouldRoundTrip()
    {
        var tableName = GetSanitizedTableName("test_int32_rowbinary");
        await Transport.ExecuteNonQueryAsync($"DROP TABLE IF EXISTS {tableName}");
        await Transport.ExecuteNonQueryAsync($"""
            CREATE TABLE {tableName} (
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

        var binaryData = writer.WrittenMemory;
        
        Output.WriteLine($"Inserting {testValues.Length} Int32 values using RowBinary format");
        await SendRowBinaryDataAsync(tableName, binaryData);

        var sequence = await QueryRowBinaryDataAsync($"SELECT test_value FROM {tableName} ORDER BY test_value");
        
        var expectedSorted = testValues.OrderBy(v => v).ToArray();
        var actualValues = new List<int>();
        
        foreach (var b in expectedSorted)
        {
            var value = Int32Type.Instance.ReadValue(ref sequence, out _);
            actualValues.Add(value);
            Output.WriteLine($"Read value: {value} (expected: {b})");
            Assert.Equal(b, value);
        }
        
        Assert.Equal(expectedSorted, actualValues);
        await Transport.ExecuteNonQueryAsync($"DROP TABLE {tableName}");
    }
    
    [Fact]
    public async Task BulkInsert_UsingRowBinary_LargeDataset()
    {
        var tableName = GetSanitizedTableName("test_int32_bulk");
        await Transport.ExecuteNonQueryAsync($"DROP TABLE IF EXISTS {tableName}");
        await Transport.ExecuteNonQueryAsync($"""
            CREATE TABLE {tableName} (
                value Int32
            ) ENGINE = Memory
            """);

        const int valueCount = 10000;
        var values = new int[valueCount];
        var random = new Random(42);
        
        for (var i = 0; i < valueCount; i++)
            values[i] = random.Next(int.MinValue, int.MaxValue);

        var writer = new ArrayBufferWriter<byte>();
        foreach (var value in values)
            Int32Type.Instance.WriteValue(writer, value);

        Output.WriteLine($"Inserting {valueCount} random Int32 values");
        await SendRowBinaryDataAsync(tableName, writer.WrittenMemory);

        var countStr = await GetScalarValueAsync($"SELECT COUNT(*) FROM {tableName}");
        Assert.Equal(valueCount.ToString(), countStr);

        var minStr = await GetScalarValueAsync($"SELECT MIN(value) FROM {tableName}");
        var maxStr = await GetScalarValueAsync($"SELECT MAX(value) FROM {tableName}");
        
        var actualMin = int.Parse(minStr);
        var actualMax = int.Parse(maxStr);
        var expectedMin = values.Min();
        var expectedMax = values.Max();
        
        Output.WriteLine($"Min value: {actualMin} (expected: {expectedMin})");
        Output.WriteLine($"Max value: {actualMax} (expected: {expectedMax})");
        
        Assert.Equal(expectedMin, actualMin);
        Assert.Equal(expectedMax, actualMax);
        
        await Transport.ExecuteNonQueryAsync($"DROP TABLE {tableName}");
    }
    
    [Fact]
    public async Task NullableInt32_RowBinary_HandlesNullsCorrectly()
    {
        var tableName = GetSanitizedTableName("test_nullable_int32");
        await Transport.ExecuteNonQueryAsync($"DROP TABLE IF EXISTS {tableName}");
        await Transport.ExecuteNonQueryAsync($"""
            CREATE TABLE {tableName} (
                id Int32,
                nullable_value Nullable(Int32)
            ) ENGINE = Memory
            """);

        var testData = new[]
        {
            (id: 1, value: 42),
            (id: 2, value: null),
            (id: 3, value: -123),
            (id: 4, value: null),
            (id: 5, value: (int?)0)
        };

        var writer = new ArrayBufferWriter<byte>();
        foreach (var item in testData)
        {
            Int32Type.Instance.WriteValue(writer, item.id);
            
            var span = writer.GetSpan(1);
            if (item.value.HasValue)
            {
                span[0] = 0;
                writer.Advance(1);
                Int32Type.Instance.WriteValue(writer, item.value.Value);
            }
            else
            {
                span[0] = 1;
                writer.Advance(1);
            }
        }

        await SendRowBinaryDataAsync(tableName, writer.WrittenMemory);

        var countStr = await GetScalarValueAsync($"SELECT COUNT(*) FROM {tableName}");
        Assert.Equal("5", countStr);

        var nullCountStr = await GetScalarValueAsync($"SELECT COUNT(*) FROM {tableName} WHERE nullable_value IS NULL");
        Assert.Equal("2", nullCountStr);

        var notNullCountStr = await GetScalarValueAsync($"SELECT COUNT(*) FROM {tableName} WHERE nullable_value IS NOT NULL");
        Assert.Equal("3", notNullCountStr);

        await Transport.ExecuteNonQueryAsync($"DROP TABLE {tableName}");
    }
    
    [Fact]
    public async Task Performance_BulkOperations_DifferentSizes()
    {
        var testSizes = new[] { 100, 1000, 10000 };
        
        foreach (var size in testSizes)
        {
            var tableName = GetSanitizedTableName($"test_perf_{size}");
            await Transport.ExecuteNonQueryAsync($"DROP TABLE IF EXISTS {tableName}");
            await Transport.ExecuteNonQueryAsync($"""
                CREATE TABLE {tableName} (
                    value Int32
                ) ENGINE = Memory
                """);

            var values = Enumerable.Range(1, size).ToArray();
            var writer = new ArrayBufferWriter<byte>();
            
            var sw = System.Diagnostics.Stopwatch.StartNew();
            foreach (var value in values)
                Int32Type.Instance.WriteValue(writer, value);
            sw.Stop();
            
            Output.WriteLine($"Serialized {size} values in {sw.Elapsed.TotalMilliseconds:F2}ms");
            
            sw.Restart();
            await SendRowBinaryDataAsync(tableName, writer.WrittenMemory);
            sw.Stop();
            
            Output.WriteLine($"Inserted {size} values in {sw.Elapsed.TotalMilliseconds:F2}ms");
            
            sw.Restart();
            var sequence = await QueryRowBinaryDataAsync($"SELECT value FROM {tableName} ORDER BY value");
            sw.Stop();
            
            Output.WriteLine($"Selected {size} values in {sw.Elapsed.TotalMilliseconds:F2}ms");
            
            sw.Restart();
            var readValues = new List<int>(size);
            for (var i = 0; i < size; i++)
            {
                var value = Int32Type.Instance.ReadValue(ref sequence, out _);
                readValues.Add(value);
            }
            sw.Stop();
            
            Output.WriteLine($"Deserialized {size} values in {sw.Elapsed.TotalMilliseconds:F2}ms");
            
            Assert.Equal(values, readValues);
            await Transport.ExecuteNonQueryAsync($"DROP TABLE {tableName}");
        }
    }
}