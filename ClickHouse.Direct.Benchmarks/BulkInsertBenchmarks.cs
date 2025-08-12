using System.Buffers;
using System.Text;
using BenchmarkDotNet.Attributes;
using BenchmarkDotNet.Jobs;
using ClickHouse.Driver.ADO;
using ClickHouse.Driver.Copy;
using ClickHouse.Direct.Abstractions;
using ClickHouse.Direct.Transports;
using ClickHouse.Direct.Types;

namespace ClickHouse.Direct.Benchmarks;

[SimpleJob(RuntimeMoniker.Net90)]
[MemoryDiagnoser]
public class BulkInsertBenchmarks
{
    private IClickHouseTransport? _httpTransport;
    private ClickHouseConnection? _officialConnection;
    private string _connectionString = "";
    private List<object[]> _testData = [];
    
    [Params(1000, 10000, 50000)]
    public int RowCount { get; set; }
    
    [Params(1, 1000, 10000)]
    public int BatchSize { get; set; }
    
    [GlobalSetup]
    public async Task Setup()
    {
        // Get connection string from environment variable or use default
        _connectionString = Environment.GetEnvironmentVariable("CLICKHOUSE_CONNECTION_STRING") 
            ?? "Host=localhost;Port=8123;Database=default;User=default;Password=";
        
        // Parse connection string to get parameters
        var parameters = ParseConnectionString(_connectionString);
        
        // Setup our HttpTransport
        _httpTransport = new HttpTransport(
            parameters.GetValueOrDefault("Host", "localhost"),
            int.Parse(parameters.GetValueOrDefault("Port", "8123")),
            parameters.GetValueOrDefault("User", "default"),
            parameters.GetValueOrDefault("Password", ""),
            parameters.GetValueOrDefault("Database", "default")
        );
        
        // Setup official ClickHouse.Driver connection
        var officialConnStr = $"Host={parameters.GetValueOrDefault("Host", "localhost")};" +
                             $"Port={parameters.GetValueOrDefault("Port", "8123")};" +
                             $"User={parameters.GetValueOrDefault("User", "default")};" +
                             $"Password={parameters.GetValueOrDefault("Password", "")};" +
                             $"Database={parameters.GetValueOrDefault("Database", "default")}";
        
        _officialConnection = new ClickHouseConnection(officialConnStr);
        await _officialConnection.OpenAsync();
        
        // Create test table
        await CreateTestTableAsync();
        
        // Generate test data
        GenerateTestData();
    }
    
    [GlobalCleanup]
    public async Task Cleanup()
    {
        // Drop test table
        await _httpTransport!.ExecuteNonQueryAsync("DROP TABLE IF EXISTS bulk_insert_benchmark");
        
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
        await _httpTransport!.ExecuteNonQueryAsync(@"
            DROP TABLE IF EXISTS bulk_insert_benchmark");
        
        await _httpTransport!.ExecuteNonQueryAsync(@"
            CREATE TABLE bulk_insert_benchmark (
                id Int32,
                name String,
                value Float64,
                created_date Date,
                description String
            ) ENGINE = Memory");
    }
    
    private void GenerateTestData()
    {
        _testData.Clear();
        var random = new Random(42); // Fixed seed for reproducibility
        
        for (var i = 0; i < RowCount; i++)
        {
            _testData.Add([
                i,
                $"Name_{i}_{random.Next(1000)}",
                i * 1.5 + random.NextDouble(),
                DateTime.Today.AddDays(-random.Next(365)),
                $"Description for item {i} with some random text {random.Next(10000)}"
            ]);
        }
    }
    
    [IterationSetup]
    public async Task IterationSetup()
    {
        // Clear table before each iteration
        await _httpTransport!.ExecuteNonQueryAsync("TRUNCATE TABLE bulk_insert_benchmark");
    }
    
    [Benchmark(Baseline = true)]
    public async Task HttpTransport_BulkInsert_SingleBatch()
    {
        // Build VALUES format data for all rows
        var valuesData = new StringBuilder();
        foreach (var row in _testData)
        {
            if (valuesData.Length > 0) valuesData.AppendLine(",");
            valuesData.Append($"({row[0]}, '{row[1]}', {row[2]:F6}, '{row[3]:yyyy-MM-dd}', '{row[4]}')");
        }
        
        // Send as single POST with data in body
        var dataBytes = Encoding.UTF8.GetBytes(valuesData.ToString());
        await _httpTransport!.SendDataAsync(
            "INSERT INTO bulk_insert_benchmark (id, name, value, created_date, description) FORMAT Values",
            new ReadOnlyMemory<byte>(dataBytes)
        );
    }
    
    [Benchmark]
    public async Task HttpTransport_BulkInsert_RowBinary()
    {
        // Use RowBinary format with our type serializers
        var arrayWriter = new ArrayBufferWriter<byte>(1024 * 1024); // 1MB initial capacity
        
        // Create type instances for serialization
        var int32Type = new Int32Type();
        var stringType = new StringType();
        var float64Type = new Float64Type();
        var dateType = new DateType();
        
        // Serialize all rows to RowBinary format
        foreach (var row in _testData)
        {
            int32Type.WriteValue(arrayWriter, (int)row[0]);
            stringType.WriteValue(arrayWriter, (string)row[1]);
            float64Type.WriteValue(arrayWriter, (double)row[2]);
            dateType.WriteValue(arrayWriter, DateOnly.FromDateTime((DateTime)row[3]));
            stringType.WriteValue(arrayWriter, (string)row[4]);
        }
        
        // Send as RowBinary format
        await _httpTransport!.SendDataAsync(
            "INSERT INTO bulk_insert_benchmark FORMAT RowBinary",
            new ReadOnlyMemory<byte>(arrayWriter.WrittenMemory.ToArray())
        );
    }
    
    [Benchmark]
    public async Task HttpTransport_BulkInsert_Batched()
    {
        // Insert in batches if BatchSize is smaller than RowCount
        var actualBatchSize = BatchSize > RowCount ? RowCount : BatchSize;
        
        for (var batch = 0; batch < _testData.Count; batch += actualBatchSize)
        {
            var batchData = _testData.Skip(batch).Take(actualBatchSize);
            var valuesData = new StringBuilder();
            
            foreach (var row in batchData)
            {
                if (valuesData.Length > 0) valuesData.AppendLine(",");
                valuesData.Append($"({row[0]}, '{row[1]}', {row[2]:F6}, '{row[3]:yyyy-MM-dd}', '{row[4]}')");
            }
            
            var dataBytes = Encoding.UTF8.GetBytes(valuesData.ToString());
            await _httpTransport!.SendDataAsync(
                "INSERT INTO bulk_insert_benchmark (id, name, value, created_date, description) FORMAT Values",
                new ReadOnlyMemory<byte>(dataBytes)
            );
        }
    }
    
    [Benchmark]
    public async Task OfficialDriver_BulkCopy()
    {
        using var bulkCopy = new ClickHouseBulkCopy(_officialConnection!)
        {
            DestinationTableName = "bulk_insert_benchmark",
            BatchSize = BatchSize > RowCount ? RowCount : BatchSize,
            MaxDegreeOfParallelism = 1 // Keep it simple for fair comparison
        };
        
        await bulkCopy.InitAsync();
        await bulkCopy.WriteToServerAsync(_testData);
    }
    
    [Benchmark]
    public async Task OfficialDriver_BulkCopy_Parallel()
    {
        using var bulkCopy = new ClickHouseBulkCopy(_officialConnection!)
        {
            DestinationTableName = "bulk_insert_benchmark",
            BatchSize = BatchSize > RowCount ? RowCount : BatchSize,
            MaxDegreeOfParallelism = 4 // Use parallel processing
        };
        
        await bulkCopy.InitAsync();
        await bulkCopy.WriteToServerAsync(_testData);
    }
    
    [Benchmark]
    public async Task OfficialDriver_SimpleInsert()
    {
        // For comparison: traditional VALUES-based inserts
        await using var cmd = _officialConnection!.CreateCommand();
        
        foreach (var row in _testData)
        {
            // Use VALUES format directly without parameters (ClickHouse.Driver doesn't support parameters well)
            cmd.CommandText = $@"INSERT INTO bulk_insert_benchmark 
                (id, name, value, created_date, description) 
                VALUES ({row[0]}, '{row[1]}', {row[2]:F6}, '{row[3]:yyyy-MM-dd}', '{row[4]}')";
            
            await cmd.ExecuteNonQueryAsync();
        }
    }
}