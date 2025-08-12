using System.Buffers;
using System.Text;
using ClickHouse.Direct.Abstractions;
using ClickHouse.Direct.Transports;
using Xunit.Abstractions;

namespace ClickHouse.Direct.IntegrationTests.Types;

/// <summary>
/// Base class for type integration tests that provides common transport setup and utilities.
/// </summary>
public abstract class TypeIntegrationTestBase : IClassFixture<ClickHouseContainerFixture>, IDisposable
{
    protected readonly ClickHouseContainerFixture Fixture;
    protected readonly ITestOutputHelper Output;
    protected readonly IClickHouseTransport Transport;
    
    protected TypeIntegrationTestBase(ClickHouseContainerFixture fixture, ITestOutputHelper output)
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
    
    protected async Task SendRowBinaryDataAsync(string tableName, ReadOnlyMemory<byte> data)
    {
        await Transport.SendDataAsync($"INSERT INTO {tableName} FORMAT RowBinary", data);
    }
    
    protected async Task<ReadOnlySequence<byte>> QueryRowBinaryDataAsync(string query)
    {
        var data = await Transport.QueryDataAsync($"{query} FORMAT RowBinary");
        return new ReadOnlySequence<byte>(data);
    }
    
    protected async Task<string> GetScalarValueAsync(string query)
    {
        var result = await Transport.ExecuteQueryAsync($"{query} FORMAT TabSeparated");
        return Encoding.UTF8.GetString(result).Trim();
    }
}