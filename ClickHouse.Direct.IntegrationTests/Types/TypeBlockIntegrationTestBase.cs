using System.Buffers;
using ClickHouse.Direct.Abstractions;
using ClickHouse.Direct.Formats;
using ClickHouse.Direct.Transports;
using Xunit.Abstractions;

namespace ClickHouse.Direct.IntegrationTests.Types;

/// <summary>
/// Base class for type integration tests that work with blocks and support multiple format serializers.
/// </summary>
[Collection("ClickHouse")]
public abstract class TypeBlockIntegrationTestBase : IClassFixture<ClickHouseContainerFixture>, IDisposable
{
    protected readonly ClickHouseContainerFixture Fixture;
    protected readonly ITestOutputHelper Output;
    protected readonly IClickHouseTransport Transport;
    
    protected TypeBlockIntegrationTestBase(ClickHouseContainerFixture fixture, ITestOutputHelper output)
    {
        Fixture = fixture;
        Output = output;
        Transport = new HttpTransport(
            Fixture.Hostname,
            Fixture.HttpPort,
            ClickHouseContainerFixture.Username,
            ClickHouseContainerFixture.Password
        );
    }
    
    public void Dispose()
    {
        Transport.Dispose();
    }
    
    /// <summary>
    /// Gets a sanitized table name with TFM suffix to prevent conflicts when running tests on multiple TFMs.
    /// </summary>
    protected string GetSanitizedTableName(string baseTableName)
    {
        return baseTableName.SanitizeForTfm();
    }
    
    protected static IFormatSerializer CreateSerializer(string formatName)
    {
        return formatName switch
        {
            "Native" => new NativeFormatSerializer(),
            "RowBinary" => new RowBinaryFormatSerializer(),
            _ => throw new ArgumentException($"Unknown format: {formatName}", nameof(formatName))
        };
    }
    
    protected async Task SendBlockDataAsync(string tableName, string formatName, Block block)
    {
        var serializer = CreateSerializer(formatName);
        var writer = new ArrayBufferWriter<byte>();
        serializer.WriteBlock(block, writer);
        
        await Transport.SendDataAsync(
            $"INSERT INTO {tableName} FORMAT {formatName}",
            writer.WrittenMemory
        );
    }
    
    protected async Task<Block> QueryBlockDataAsync(
        string query, 
        string formatName, 
        int expectedRows,
        IReadOnlyList<ColumnDescriptor> columns)
    {
        var data = await Transport.QueryDataAsync($"{query} FORMAT {formatName}");
        var sequence = new ReadOnlySequence<byte>(data);
        
        var serializer = CreateSerializer(formatName);
        return serializer.ReadBlock(expectedRows, columns, ref sequence, out _);
    }
    
    protected async Task<string> GetScalarValueAsync(string query)
    {
        var result = await Transport.ExecuteQueryAsync($"{query} FORMAT TabSeparated");
        return System.Text.Encoding.UTF8.GetString(result).Trim();
    }
    
    public static IEnumerable<object[]> FormatNames()
    {
        yield return ["Native"];
        yield return ["RowBinary"];
    }
}