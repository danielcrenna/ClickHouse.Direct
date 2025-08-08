using System.Buffers;
using BenchmarkDotNet.Attributes;
using BenchmarkDotNet.Configs;
using BenchmarkDotNet.Jobs;
using ClickHouse.Direct.Abstractions;
using ClickHouse.Direct.Types;

namespace ClickHouse.Direct.Benchmarks.Types;

[Config(typeof(Config))]
[MemoryDiagnoser]
[MarkdownExporter]
public class Int32TypeComparisonBenchmarks
{
    private class Config : ManualConfig
    {
        public Config()
        {
            AddJob(Job.Default
                .WithId("Comparison")
                .WithWarmupCount(3)
                .WithIterationCount(10));
        }
    }
    
    private const int Count = 10000;
    private byte[] _readBuffer = null!;
    private int[] _writeValues = null!;
    private int[] _readDestination = null!;
    
    private Int32Type _defaultType = null!;
    private Int32Type _optimizedType = null!;
    
    [GlobalSetup]
    public void Setup()
    {
        _readBuffer = new byte[Count * sizeof(int)];
        _writeValues = new int[Count];
        _readDestination = new int[Count];
        
        var random = new Random(42);
        for (var i = 0; i < Count; i++)
        {
            _writeValues[i] = random.Next(int.MinValue, int.MaxValue);
            BitConverter.GetBytes(_writeValues[i]).CopyTo(_readBuffer, i * sizeof(int));
        }
        
        _defaultType = new Int32Type();
        _optimizedType = new Int32Type(DefaultSimdCapabilities.Instance);
    }
    
    [Benchmark]
    public int[] RoundTrip_Default()
    {
        // Write
        var writer = new ArrayBufferWriter<byte>();
        _defaultType.WriteValues(writer, _writeValues);
        
        // Read
        var sequence = new ReadOnlySequence<byte>(writer.WrittenSpan.ToArray());
        var destination = new int[Count];
        _defaultType.ReadValues(ref sequence, destination, out _);
        
        return destination;
    }
    
    [Benchmark]
    public int[] RoundTrip_Optimized()
    {
        // Write
        var writer = new ArrayBufferWriter<byte>();
        _optimizedType.WriteValues(writer, _writeValues);
        
        // Read
        var sequence = new ReadOnlySequence<byte>(writer.WrittenSpan.ToArray());
        var destination = new int[Count];
        _optimizedType.ReadValues(ref sequence, destination, out _);
        
        return destination;
    }
    
    [Benchmark]
    public void BulkWrite_Default()
    {
        var writer = new ArrayBufferWriter<byte>();
        for (var i = 0; i < 10; i++)
        {
            _defaultType.WriteValues(writer, _writeValues);
        }
    }
    
    [Benchmark]
    public void BulkWrite_Optimized()
    {
        var writer = new ArrayBufferWriter<byte>();
        for (var i = 0; i < 10; i++)
        {
            _optimizedType.WriteValues(writer, _writeValues);
        }
    }
    
    [Benchmark]
    public void BulkRead_Default()
    {
        for (var i = 0; i < 10; i++)
        {
            var sequence = new ReadOnlySequence<byte>(_readBuffer);
            _defaultType.ReadValues(ref sequence, _readDestination, out _);
        }
    }
    
    [Benchmark]
    public void BulkRead_Optimized()
    {
        for (var i = 0; i < 10; i++)
        {
            var sequence = new ReadOnlySequence<byte>(_readBuffer);
            _optimizedType.ReadValues(ref sequence, _readDestination, out _);
        }
    }
}