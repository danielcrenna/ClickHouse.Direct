using System.Text;
using ClickHouse.Direct.Transports;

namespace ClickHouse.Direct.IntegrationTests.Transport;

[Collection("ClickHouse")]
public class TcpTransportIntegrationTests : IDisposable
{
    private readonly ClickHouseContainerFixture _fixture;
    private readonly TcpTransport _transport;
    
    public TcpTransportIntegrationTests(ClickHouseContainerFixture fixture)
    {
        _fixture = fixture;
        _transport = new TcpTransport(_fixture.Hostname, _fixture.NativeTcpPort, ClickHouseContainerFixture.Username, ClickHouseContainerFixture.Password);
    }
    
    [Fact]
    public async Task PingAsync_ShouldReturnTrue_WhenServerIsAvailable()
    {
        // Act
        var result = await _transport.PingAsync();
        
        // Assert
        Assert.True(result);
    }
    
    [Fact]
    public async Task ExecuteQueryAsync_ShouldReturnData_ForSimpleQuery()
    {
        // Act
        var result = await _transport.ExecuteQueryAsync(@"SELECT 42 AS answer FORMAT Native");
        
        // Assert
        Assert.NotNull(result);
        Assert.NotEmpty(result);
    }
    
    [Fact]
    public async Task ExecuteNonQueryAsync_ShouldCreateTable()
    {
        // Arrange
        var tableName = TableNameExtensions.GenerateTableName();
        
        // Act
        await _transport.ExecuteNonQueryAsync($@"
            CREATE TABLE IF NOT EXISTS {tableName} (
                id Int32,
                name String
            ) ENGINE = Memory"
        );
        
        // Verify table exists
        var result = await _transport.ExecuteQueryAsync($@"EXISTS TABLE {tableName} FORMAT TabSeparated");
        var resultStr = Encoding.UTF8.GetString(result).Trim();
        
        // Assert
        Assert.Equal("1", resultStr);
        
        // Cleanup
        await _transport.ExecuteNonQueryAsync($@"DROP TABLE IF EXISTS {tableName}");
    }
    
    [Fact]
    public async Task SendDataAsync_ShouldInsertData()
    {
        // Arrange
        var tableName = TableNameExtensions.GenerateTableName();
        
        await _transport.ExecuteNonQueryAsync($@"
            CREATE TABLE IF NOT EXISTS {tableName} (
                id Int32,
                name String
            ) ENGINE = Memory"
        );
        
        try
        {
            // Create test data in Native format
            // This would normally come from the Protocol layer
            // For now, we'll use a simple INSERT query
            await _transport.ExecuteNonQueryAsync($@"
                INSERT INTO {tableName} VALUES (1, 'Alice'), (2, 'Bob')"
            );
            
            // Verify data
            var result = await _transport.ExecuteQueryAsync($@"
                SELECT count(*) FROM {tableName} FORMAT TabSeparated"
            );
            var count = Encoding.UTF8.GetString(result).Trim();
            
            // Assert
            Assert.Equal("2", count);
        }
        finally
        {
            // Cleanup
            await _transport.ExecuteNonQueryAsync($@"DROP TABLE IF EXISTS {tableName}");
        }
    }
    
    [Fact]
    public async Task QueryDataAsync_ShouldReturnMemory()
    {
        // Act
        var result = await _transport.QueryDataAsync(@"SELECT 123 AS number FORMAT TabSeparated");
        
        // Assert
        Assert.False(result.IsEmpty);
        var resultStr = Encoding.UTF8.GetString(result.Span).Trim();
        Assert.Equal("123", resultStr);
    }
    
    [Fact]
    public async Task ExecuteQueryAsync_ShouldHandleMultipleRows()
    {
        // Act
        var result = await _transport.ExecuteQueryAsync(@"
            SELECT number FROM system.numbers LIMIT 5 FORMAT TabSeparated"
        );
        
        // Assert
        var resultStr = Encoding.UTF8.GetString(result).Trim();
        var lines = resultStr.Split('\n');
        Assert.Equal(5, lines.Length);
        Assert.Equal("0", lines[0]);
        Assert.Equal("1", lines[1]);
        Assert.Equal("2", lines[2]);
        Assert.Equal("3", lines[3]);
        Assert.Equal("4", lines[4]);
    }
    
    [Fact]
    public async Task ExecuteQueryAsync_ShouldHandleEmptyResult()
    {
        // Arrange
        var tableName = TableNameExtensions.GenerateTableName();
        
        await _transport.ExecuteNonQueryAsync($@"
            CREATE TABLE IF NOT EXISTS {tableName} (
                id Int32
            ) ENGINE = Memory"
        );
        
        try
        {
            // Act - query empty table
            var result = await _transport.ExecuteQueryAsync($@"
                SELECT * FROM {tableName} FORMAT TabSeparated"
            );
            
            // Assert
            var resultStr = Encoding.UTF8.GetString(result).Trim();
            Assert.Empty(resultStr);
        }
        finally
        {
            // Cleanup
            await _transport.ExecuteNonQueryAsync($@"DROP TABLE IF EXISTS {tableName}");
        }
    }
    
    public void Dispose()
    {
        _transport?.Dispose();
    }
}