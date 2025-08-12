using System.Text;
using BenchmarkDotNet.Attributes;
using BenchmarkDotNet.Jobs;
using ClickHouse.Direct.Abstractions;
using ClickHouse.Direct.Transports;
using DuckDB.NET.Data;

namespace ClickHouse.Direct.Benchmarks;

[SimpleJob(RuntimeMoniker.Net90)]
[MemoryDiagnoser]
public class ClickHouseVsDuckDBBenchmarks
{
    private IClickHouseTransport? _clickHouseTransport;
    private DuckDBConnection? _duckDbConnection;
    private string _clickHouseConnectionString = "";
    
    [GlobalSetup]
    public async Task Setup()
    {
        // Get connection string from environment variable or use default
        _clickHouseConnectionString = Environment.GetEnvironmentVariable("CLICKHOUSE_CONNECTION_STRING") 
            ?? "Host=localhost;Port=8123;Database=default;User=default;Password=";
        
        // Parse connection string to get parameters
        var parameters = ParseConnectionString(_clickHouseConnectionString);
        
        // Setup ClickHouse HTTP transport
        _clickHouseTransport = new HttpTransport(
            parameters.GetValueOrDefault("Host", "localhost"),
            int.Parse(parameters.GetValueOrDefault("Port", "8123")),
            parameters.GetValueOrDefault("User", "default"),
            parameters.GetValueOrDefault("Password", ""),
            parameters.GetValueOrDefault("Database", "default")
        );
        
        // Setup DuckDB in-memory connection
        _duckDbConnection = new DuckDBConnection("DataSource=:memory:");
        _duckDbConnection.Open();
        
        // Create test tables in both databases
        await CreateTestTablesAsync();
        await InsertTestDataAsync();
    }
    
    [GlobalCleanup]
    public void Cleanup()
    {
        _clickHouseTransport?.Dispose();
        _duckDbConnection?.Dispose();
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
    
    private async Task CreateTestTablesAsync()
    {
        // Create table in ClickHouse
        await _clickHouseTransport!.ExecuteNonQueryAsync(@"
            CREATE TABLE IF NOT EXISTS benchmark_test (
                id Int32,
                name String,
                value Float64,
                created_date Date
            ) ENGINE = Memory");
        
        // Create table in DuckDB
        await using (var cmd = _duckDbConnection!.CreateCommand())
        {
            cmd.CommandText = @"
                CREATE TABLE IF NOT EXISTS benchmark_test (
                    id INTEGER,
                    name VARCHAR,
                    value DOUBLE,
                    created_date DATE
                )";
            cmd.ExecuteNonQuery();
        }
    }
    
    private async Task InsertTestDataAsync()
    {
        // Clear existing data in ClickHouse
        await _clickHouseTransport!.ExecuteNonQueryAsync("TRUNCATE TABLE benchmark_test");
        
        // Clear existing data in DuckDB
        await using (var cmd = _duckDbConnection!.CreateCommand())
        {
            cmd.CommandText = "DELETE FROM benchmark_test";
            cmd.ExecuteNonQuery();
        }
        
        // Insert all data into ClickHouse in one request using VALUES format
        var clickHouseInsertQuery = new StringBuilder();
        clickHouseInsertQuery.AppendLine("INSERT INTO benchmark_test (id, name, value, created_date) VALUES");
        
        for (var i = 0; i < 10000; i++)
        {
            if (i > 0) clickHouseInsertQuery.Append(',');
            clickHouseInsertQuery.AppendLine($"({i}, 'Name_{i}', {i * 1.5}, '{DateTime.Today.AddDays(-i % 365):yyyy-MM-dd}')");
        }
        
        // Send as POST body to avoid URL length limits
        var dataBytes = Encoding.UTF8.GetBytes(clickHouseInsertQuery.ToString());
        await _clickHouseTransport!.SendDataAsync("INSERT INTO benchmark_test FORMAT Values", new ReadOnlyMemory<byte>(dataBytes));
        
        // Insert data into DuckDB
        await using (var cmd = _duckDbConnection!.CreateCommand())
        {
            cmd.CommandText = @"
                INSERT INTO benchmark_test (id, name, value, created_date) VALUES
                ($1, $2, $3, $4)";
            
            for (var i = 0; i < 10000; i++)
            {
                cmd.Parameters.Clear();
                var idParam = cmd.CreateParameter();
                idParam.Value = i;
                cmd.Parameters.Add(idParam);
                
                var nameParam = cmd.CreateParameter();
                nameParam.Value = $"Name_{i}";
                cmd.Parameters.Add(nameParam);
                
                var valueParam = cmd.CreateParameter();
                valueParam.Value = i * 1.5;
                cmd.Parameters.Add(valueParam);
                
                var dateParam = cmd.CreateParameter();
                dateParam.Value = DateTime.Today.AddDays(-i % 365);
                cmd.Parameters.Add(dateParam);
                
                cmd.ExecuteNonQuery();
            }
        }
    }
    
    [Benchmark(Baseline = true)]
    public async Task<int> ClickHouse_SimpleSelect()
    {
        var result = await _clickHouseTransport!.ExecuteQueryAsync(@"
            SELECT id, name, value FROM benchmark_test WHERE id < 100 FORMAT TabSeparated");
        
        var count = 0;
        var lines = Encoding.UTF8.GetString(result).Split('\n', StringSplitOptions.RemoveEmptyEntries);
        foreach (var line in lines)
        {
            count++;
        }
        return count;
    }
    
    [Benchmark]
    public int DuckDB_SimpleSelect()
    {
        using var cmd = _duckDbConnection!.CreateCommand();
        cmd.CommandText = @"SELECT id, name, value FROM benchmark_test WHERE id < 100";
        using var reader = cmd.ExecuteReader();
        
        var count = 0;
        while (reader.Read())
        {
            var id = reader.GetInt32(0);
            var name = reader.GetString(1);
            var value = reader.GetDouble(2);
            count++;
        }
        return count;
    }
    
    [Benchmark]
    public async Task<double> ClickHouse_Aggregation()
    {
        var result = await _clickHouseTransport!.ExecuteQueryAsync(@"
            SELECT AVG(value) FROM benchmark_test WHERE id < 1000 FORMAT TabSeparated");
        
        var resultString = Encoding.UTF8.GetString(result).Trim();
        return double.Parse(resultString);
    }
    
    [Benchmark]
    public double DuckDB_Aggregation()
    {
        using var cmd = _duckDbConnection!.CreateCommand();
        cmd.CommandText = @"SELECT AVG(value) FROM benchmark_test WHERE id < 1000";
        var result = cmd.ExecuteScalar();
        return Convert.ToDouble(result);
    }
    
    [Benchmark]
    public async Task<int> ClickHouse_ComplexQuery()
    {
        var result = await _clickHouseTransport!.ExecuteQueryAsync(@"
            SELECT 
                COUNT(*) as cnt,
                AVG(value) as avg_value,
                MAX(value) as max_value,
                MIN(value) as min_value
            FROM benchmark_test
            WHERE id < 5000
            GROUP BY created_date
            ORDER BY created_date
            FORMAT TabSeparated");
        
        var count = 0;
        var lines = Encoding.UTF8.GetString(result).Split('\n', StringSplitOptions.RemoveEmptyEntries);
        foreach (var line in lines)
        {
            count++;
        }
        return count;
    }
    
    [Benchmark]
    public int DuckDB_ComplexQuery()
    {
        using var cmd = _duckDbConnection!.CreateCommand();
        cmd.CommandText = @"
            SELECT 
                COUNT(*) as cnt,
                AVG(value) as avg_value,
                MAX(value) as max_value,
                MIN(value) as min_value
            FROM benchmark_test
            WHERE id < 5000
            GROUP BY created_date
            ORDER BY created_date";
        using var reader = cmd.ExecuteReader();
        
        var count = 0;
        while (reader.Read())
        {
            count++;
        }
        return count;
    }
    
    [Benchmark]
    public async Task<int> ClickHouse_BulkRead()
    {
        var result = await _clickHouseTransport!.ExecuteQueryAsync(@"
            SELECT * FROM benchmark_test FORMAT TabSeparated");
        
        var count = 0;
        var lines = Encoding.UTF8.GetString(result).Split('\n', StringSplitOptions.RemoveEmptyEntries);
        foreach (var line in lines)
        {
            count++;
        }
        return count;
    }
    
    [Benchmark]
    public int DuckDB_BulkRead()
    {
        using var cmd = _duckDbConnection!.CreateCommand();
        cmd.CommandText = @"SELECT * FROM benchmark_test";
        using var reader = cmd.ExecuteReader();
        
        var count = 0;
        while (reader.Read())
        {
            count++;
        }
        return count;
    }
}