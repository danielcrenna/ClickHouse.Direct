using System.Text;
using BenchmarkDotNet.Attributes;
using BenchmarkDotNet.Jobs;
using ClickHouse.Driver.ADO;
using ClickHouse.Direct.Abstractions;
using ClickHouse.Direct.Transports;

namespace ClickHouse.Direct.Benchmarks;

[SimpleJob(RuntimeMoniker.Net90)]
[MemoryDiagnoser]
public class HttpTransportVsOfficialClientBenchmarks
{
    private IClickHouseTransport? _httpTransport;
    private ClickHouseConnection? _officialConnection;
    private string _connectionString = "";
    
    [GlobalSetup]
    public async Task Setup()
    {
        _connectionString = Environment.GetEnvironmentVariable("CLICKHOUSE_CONNECTION_STRING") 
            ?? "Host=localhost;Port=8123;Database=default;User=default;Password=";
        
        var parameters = ParseConnectionString(_connectionString);
        
        _httpTransport = new HttpTransport(
            parameters.GetValueOrDefault("Host", "localhost"),
            int.Parse(parameters.GetValueOrDefault("Port", "8123")),
            parameters.GetValueOrDefault("User", "default"),
            parameters.GetValueOrDefault("Password", ""),
            parameters.GetValueOrDefault("Database", "default")
        );
        
        var connectionString = $"Host={parameters.GetValueOrDefault("Host", "localhost")};" +
                             $"Port={parameters.GetValueOrDefault("Port", "8123")};" +
                             $"User={parameters.GetValueOrDefault("User", "default")};" +
                             $"Password={parameters.GetValueOrDefault("Password", "")};" +
                             $"Database={parameters.GetValueOrDefault("Database", "default")}";
        
        _officialConnection = new ClickHouseConnection(connectionString);
        await _officialConnection.OpenAsync();
        
        await CreateTestTableAsync();
        await InsertTestDataAsync();
    }
    
    [GlobalCleanup]
    public async Task Cleanup()
    {
        _httpTransport?.Dispose();
        if (_officialConnection != null)
        {
            await _officialConnection.CloseAsync();
            _officialConnection.Dispose();
        }
    }
    
    private Dictionary<string, string> ParseConnectionString(string connectionString)
    {
        var result = new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase);
        var parts = connectionString.Split(';', StringSplitOptions.RemoveEmptyEntries);
        
        foreach (var part in parts)
        {
            var keyValue = part.Split('=', 2);
            if (keyValue.Length == 2)
            {
                result[keyValue[0].Trim()] = keyValue[1].Trim();
            }
        }
        
        return result;
    }
    
    private async Task CreateTestTableAsync()
    {
        // Create table using HttpTransport
        await _httpTransport!.ExecuteNonQueryAsync(@"
            DROP TABLE IF EXISTS transport_benchmark_test");
        
        await _httpTransport!.ExecuteNonQueryAsync(@"
            CREATE TABLE transport_benchmark_test (
                id Int32,
                name String,
                value Float64,
                created_date Date
            ) ENGINE = Memory");
    }
    
    private async Task InsertTestDataAsync()
    {
        await _httpTransport!.ExecuteNonQueryAsync("TRUNCATE TABLE transport_benchmark_test");
        
        var valuesData = new StringBuilder();
        for (var i = 0; i < 10000; i++)
        {
            if (i > 0) valuesData.AppendLine(",");
            valuesData.Append($"({i}, 'Name_{i}', {i * 1.5}, '{DateTime.Today.AddDays(-i % 365):yyyy-MM-dd}')");
        }
        
        var dataBytes = Encoding.UTF8.GetBytes(valuesData.ToString());
        await _httpTransport!.SendDataAsync(
            "INSERT INTO transport_benchmark_test (id, name, value, created_date) FORMAT Values", 
            new ReadOnlyMemory<byte>(dataBytes)
        );
    }
    
    [Benchmark(Baseline = true)]
    public async Task<int> HttpTransport_SimpleSelect()
    {
        var result = await _httpTransport!.ExecuteQueryAsync("SELECT id, name, value FROM transport_benchmark_test WHERE id < 100 FORMAT TabSeparated");

        var lines = Encoding.UTF8.GetString(result).Split('\n', StringSplitOptions.RemoveEmptyEntries);
        return lines.Length;
    }
    
    [Benchmark]
    public async Task<int> OfficialClient_SimpleSelect()
    {
        await using var command = _officialConnection!.CreateCommand();
        command.CommandText = "SELECT id, name, value FROM transport_benchmark_test WHERE id < 100";
        await using var reader = await command.ExecuteReaderAsync();
        
        var count = 0;
        while (await reader.ReadAsync())
        {
            var id = reader.GetInt32(0);
            var name = reader.GetString(1);
            var value = reader.GetDouble(2);
            count++;
        }
        return count;
    }
    
    [Benchmark]
    public async Task<double> HttpTransport_Aggregation()
    {
        var result = await _httpTransport!.ExecuteQueryAsync(@"
            SELECT AVG(value) FROM transport_benchmark_test WHERE id < 1000 FORMAT TabSeparated");
        
        var resultString = Encoding.UTF8.GetString(result).Trim();
        return double.Parse(resultString);
    }
    
    [Benchmark]
    public async Task<double> OfficialClient_Aggregation()
    {
        await using var cmd = _officialConnection!.CreateCommand();
        cmd.CommandText = "SELECT AVG(value) FROM transport_benchmark_test WHERE id < 1000";
        var result = await cmd.ExecuteScalarAsync();
        return Convert.ToDouble(result);
    }
    
    [Benchmark]
    public async Task<int> HttpTransport_ComplexQuery()
    {
        var result = await _httpTransport!.ExecuteQueryAsync(@"
            SELECT 
                COUNT(*) as cnt,
                AVG(value) as avg_value,
                MAX(value) as max_value,
                MIN(value) as min_value
            FROM transport_benchmark_test
            WHERE id < 5000
            GROUP BY created_date
            ORDER BY created_date
            FORMAT TabSeparated");

        var lines = Encoding.UTF8.GetString(result).Split('\n', StringSplitOptions.RemoveEmptyEntries);
        return lines.Length;
    }
    
    [Benchmark]
    public async Task<int> OfficialClient_ComplexQuery()
    {
        await using var cmd = _officialConnection!.CreateCommand();
        cmd.CommandText = """
                          SELECT
                              COUNT(*) as cnt,
                              AVG(value) as avg_value,
                              MAX(value) as max_value,
                              MIN(value) as min_value
                          FROM transport_benchmark_test
                          WHERE id < 5000
                          GROUP BY created_date
                          ORDER BY created_date
                          """;
        await using var reader = await cmd.ExecuteReaderAsync();
        
        var count = 0;
        while (await reader.ReadAsync())
        {
            count++;
        }
        return count;
    }
    
    [Benchmark]
    public async Task<int> HttpTransport_BulkRead()
    {
        var result = await _httpTransport!.ExecuteQueryAsync(@"
            SELECT * FROM transport_benchmark_test FORMAT TabSeparated");

        var lines = Encoding.UTF8.GetString(result).Split('\n', StringSplitOptions.RemoveEmptyEntries);
        return lines.Length;
    }
    
    [Benchmark]
    public async Task<int> OfficialClient_BulkRead()
    {
        await using var cmd = _officialConnection!.CreateCommand();
        cmd.CommandText = "SELECT * FROM transport_benchmark_test";
        await using var reader = await cmd.ExecuteReaderAsync();
        
        var count = 0;
        while (await reader.ReadAsync())
        {
            count++;
        }
        return count;
    }
    
    [Benchmark]
    public async Task HttpTransport_ExecuteNonQuery()
    {
        await _httpTransport!.ExecuteNonQueryAsync(@"
            ALTER TABLE transport_benchmark_test UPDATE value = value + 0 WHERE id = 0");
    }
    
    [Benchmark]
    public async Task OfficialClient_ExecuteNonQuery()
    {
        await using var cmd = _officialConnection!.CreateCommand();
        cmd.CommandText = "ALTER TABLE transport_benchmark_test UPDATE value = value + 0 WHERE id = 0";
        await cmd.ExecuteNonQueryAsync();
    }
}