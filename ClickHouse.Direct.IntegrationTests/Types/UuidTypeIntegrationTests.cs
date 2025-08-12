using System.Buffers;
using ClickHouse.Direct.Types;
using Xunit.Abstractions;

namespace ClickHouse.Direct.IntegrationTests.Types;

[Collection("ClickHouse")]
public class UuidTypeIntegrationTests(ClickHouseContainerFixture fixture, ITestOutputHelper output)
    : TypeIntegrationTestBase(fixture, output)
{
    [Fact]
    public async Task InsertAndSelect_UsingRowBinary_ShouldRoundTrip()
    {
        var tableName = GetSanitizedTableName("test_uuid_rowbinary");
        await Transport.ExecuteNonQueryAsync($"DROP TABLE IF EXISTS {tableName}");
        await Transport.ExecuteNonQueryAsync($"""
            CREATE TABLE {tableName} (
                test_uuid UUID
            ) ENGINE = Memory
            """);

        var testUuids = new[]
        {
            Guid.Empty,
            Guid.Parse("123e4567-e89b-12d3-a456-426614174000"),
            Guid.Parse("550e8400-e29b-41d4-a716-446655440000"),
            Guid.NewGuid(),
            Guid.NewGuid(),
            Guid.Parse("ffffffff-ffff-ffff-ffff-ffffffffffff"),
            Guid.Parse("00000000-0000-0000-0000-000000000001"),
            Guid.Parse("12345678-1234-5678-9abc-def012345678")
        };

        var writer = new ArrayBufferWriter<byte>();
        foreach (var uuid in testUuids)
            UuidType.Instance.WriteValue(writer, uuid);

        Output.WriteLine($"Inserting {testUuids.Length} UUID values using RowBinary format");
        await SendRowBinaryDataAsync(tableName, writer.WrittenMemory);

        var sequence = await QueryRowBinaryDataAsync($"SELECT test_uuid FROM {tableName}");

        var actualUuids = new List<Guid>();
        for (var i = 0; i < testUuids.Length; i++)
        {
            var value = UuidType.Instance.ReadValue(ref sequence, out _);
            actualUuids.Add(value);
            Output.WriteLine($"Read UUID: {value}");
            Assert.Equal(testUuids[i], value);
        }

        Assert.Equal(testUuids, actualUuids);
        await Transport.ExecuteNonQueryAsync($"DROP TABLE {tableName}");
    }

    [Fact]
    public async Task BulkInsert_LargeDataset_PerformanceTest()
    {
        var tableName = GetSanitizedTableName("test_uuid_bulk");
        await Transport.ExecuteNonQueryAsync($"DROP TABLE IF EXISTS {tableName}");
        await Transport.ExecuteNonQueryAsync($"""
            CREATE TABLE {tableName} (
                id Int32,
                uuid_value UUID
            ) ENGINE = Memory
            """);

        const int recordCount = 10000;
        var uuids = new Guid[recordCount];
        
        for (var i = 0; i < recordCount; i++)
            uuids[i] = Guid.NewGuid();

        var writer = new ArrayBufferWriter<byte>();
        for (var i = 0; i < recordCount; i++)
        {
            Int32Type.Instance.WriteValue(writer, i);
            UuidType.Instance.WriteValue(writer, uuids[i]);
        }

        var sw = System.Diagnostics.Stopwatch.StartNew();
        await SendRowBinaryDataAsync(tableName, writer.WrittenMemory);
        sw.Stop();
        
        Output.WriteLine($"Inserted {recordCount} UUID records in {sw.Elapsed.TotalMilliseconds:F2}ms");

        var countStr = await GetScalarValueAsync($"SELECT COUNT(*) FROM {tableName}");
        Assert.Equal(recordCount.ToString(), countStr);

        var distinctCountStr = await GetScalarValueAsync($"SELECT COUNT(DISTINCT uuid_value) FROM {tableName}");
        Assert.Equal(recordCount.ToString(), distinctCountStr);

        await Transport.ExecuteNonQueryAsync($"DROP TABLE {tableName}");
    }

    [Fact]
    public async Task SpecialUuidValues_ShouldHandleCorrectly()
    {
        var tableName = GetSanitizedTableName("test_special_uuids");
        await Transport.ExecuteNonQueryAsync($"DROP TABLE IF EXISTS {tableName}");
        await Transport.ExecuteNonQueryAsync($"""
            CREATE TABLE {tableName} (
                description String,
                uuid_value UUID
            ) ENGINE = Memory
            """);

        var specialCases = new[]
        {
            ("Empty/Nil UUID", Guid.Empty),
            ("All ones", Guid.Parse("ffffffff-ffff-ffff-ffff-ffffffffffff")),
            ("Version 1 example", Guid.Parse("550e8400-e29b-11d4-a716-446655440000")),
            ("Version 4 example", Guid.Parse("936da01f-9abd-4d9d-80c7-02af85c822a8")),
            ("Sequential pattern", Guid.Parse("01234567-89ab-cdef-0123-456789abcdef")),
            ("Reverse sequential", Guid.Parse("fedcba98-7654-3210-fedc-ba9876543210"))
        };

        var writer = new ArrayBufferWriter<byte>();
        foreach (var (desc, uuid) in specialCases)
        {
            StringType.Instance.WriteValue(writer, desc);
            UuidType.Instance.WriteValue(writer, uuid);
        }

        await SendRowBinaryDataAsync(tableName, writer.WrittenMemory);

        var sequence = await QueryRowBinaryDataAsync($"SELECT description, uuid_value FROM {tableName} ORDER BY description");

        var orderedCases = specialCases.OrderBy(x => x.Item1).ToArray();
        for (var i = 0; i < orderedCases.Length; i++)
        {
            var desc = StringType.Instance.ReadValue(ref sequence, out _);
            var uuid = UuidType.Instance.ReadValue(ref sequence, out _);
            
            Assert.Equal(orderedCases[i].Item1, desc);
            Assert.Equal(orderedCases[i].Item2, uuid);
            Output.WriteLine($"{desc}: {uuid}");
        }

        await Transport.ExecuteNonQueryAsync($"DROP TABLE {tableName}");
    }

    [Fact]
    public async Task NullableUuid_HandlesNullsCorrectly()
    {
        var tableName = GetSanitizedTableName("test_nullable_uuid");
        await Transport.ExecuteNonQueryAsync($"DROP TABLE IF EXISTS {tableName}");
        await Transport.ExecuteNonQueryAsync($"""
            CREATE TABLE {tableName} (
                id Int32,
                nullable_uuid Nullable(UUID)
            ) ENGINE = Memory
            """);

        var testData = new[]
        {
            (id: 1, uuid: Guid.NewGuid()),
            (id: 2, uuid: null),
            (id: 3, uuid: Guid.Empty),
            (id: 4, uuid: null),
            (id: 5, uuid: (Guid?)Guid.NewGuid())
        };

        var writer = new ArrayBufferWriter<byte>();
        foreach (var item in testData)
        {
            Int32Type.Instance.WriteValue(writer, item.id);
            
            var span = writer.GetSpan(1);
            if (item.uuid.HasValue)
            {
                span[0] = 0;
                writer.Advance(1);
                UuidType.Instance.WriteValue(writer, item.uuid.Value);
            }
            else
            {
                span[0] = 1;
                writer.Advance(1);
            }
        }

        await SendRowBinaryDataAsync(tableName, writer.WrittenMemory);

        var nullCountStr = await GetScalarValueAsync($"SELECT COUNT(*) FROM {tableName} WHERE nullable_uuid IS NULL");
        Assert.Equal("2", nullCountStr);

        var notNullCountStr = await GetScalarValueAsync($"SELECT COUNT(*) FROM {tableName} WHERE nullable_uuid IS NOT NULL");
        Assert.Equal("3", notNullCountStr);

        var emptyCountStr = await GetScalarValueAsync($"SELECT COUNT(*) FROM {tableName} WHERE nullable_uuid = '00000000-0000-0000-0000-000000000000'");
        Assert.Equal("1", emptyCountStr);

        await Transport.ExecuteNonQueryAsync($"DROP TABLE {tableName}");
    }

    [Fact]
    public async Task UuidWithOtherTypes_MixedTable()
    {
        var tableName = GetSanitizedTableName("test_mixed_with_uuid");
        await Transport.ExecuteNonQueryAsync($"DROP TABLE IF EXISTS {tableName}");
        await Transport.ExecuteNonQueryAsync($"""
            CREATE TABLE {tableName} (
                id Int32,
                name String,
                uuid_value UUID,
                score Int32
            ) ENGINE = Memory
            """);

        var testData = new[]
        {
            (id: 1, name: "Alice", uuid: Guid.NewGuid(), score: 100),
            (id: 2, name: "Bob", uuid: Guid.NewGuid(), score: 85),
            (id: 3, name: "Charlie", uuid: Guid.NewGuid(), score: 92),
            (id: 4, name: "David", uuid: Guid.NewGuid(), score: 78),
            (id: 5, name: "Eve", uuid: Guid.NewGuid(), score: 95)
        };

        var writer = new ArrayBufferWriter<byte>();
        foreach (var item in testData)
        {
            Int32Type.Instance.WriteValue(writer, item.id);
            StringType.Instance.WriteValue(writer, item.name);
            UuidType.Instance.WriteValue(writer, item.uuid);
            Int32Type.Instance.WriteValue(writer, item.score);
        }

        await SendRowBinaryDataAsync(tableName, writer.WrittenMemory);

        var countStr = await GetScalarValueAsync($"SELECT COUNT(*) FROM {tableName}");
        Assert.Equal("5", countStr);

        var sequence = await QueryRowBinaryDataAsync($"SELECT id, name, uuid_value, score FROM {tableName} ORDER BY id");

        for (var i = 0; i < testData.Length; i++)
        {
            var id = Int32Type.Instance.ReadValue(ref sequence, out _);
            var name = StringType.Instance.ReadValue(ref sequence, out _);
            var uuid = UuidType.Instance.ReadValue(ref sequence, out _);
            var score = Int32Type.Instance.ReadValue(ref sequence, out _);
            
            Assert.Equal(testData[i].id, id);
            Assert.Equal(testData[i].name, name);
            Assert.Equal(testData[i].uuid, uuid);
            Assert.Equal(testData[i].score, score);
            
            Output.WriteLine($"Row {id}: {name}, UUID={uuid}, Score={score}");
        }

        await Transport.ExecuteNonQueryAsync($"DROP TABLE {tableName}");
    }
}