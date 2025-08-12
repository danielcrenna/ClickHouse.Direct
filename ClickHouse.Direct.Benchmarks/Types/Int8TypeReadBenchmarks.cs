using System.Buffers;
using BenchmarkDotNet.Attributes;
using ClickHouse.Direct.Abstractions;
using ClickHouse.Direct.Types;

namespace ClickHouse.Direct.Benchmarks.Types;

[MemoryDiagnoser]
[SimpleJob]
public class Int8TypeReadBenchmarks
{
    private ReadOnlySequence<byte> _sequence;
    private Int8Type _scalarHandler = null!;
    private Int8Type _simdHandler = null!;
    private byte[] _serializedData = null!;

    [Params(100, 1000, 10000)]
    public int Count { get; set; }

    [GlobalSetup]
    public void Setup()
    {
        var values = new sbyte[Count];
        var random = new Random(42);
        for (var i = 0; i < Count; i++)
        {
            values[i] = (sbyte)random.Next(sbyte.MinValue, sbyte.MaxValue + 1);
        }
        
        // Create handlers with different SIMD capabilities
        _scalarHandler = new Int8Type(
            ConstrainedSimdCapabilities.ScalarOnly(DefaultSimdCapabilities.Instance));
        _simdHandler = new Int8Type(DefaultSimdCapabilities.Instance);
        
        // Pre-serialize data for read benchmarks
        var tempWriter = new ArrayBufferWriter<byte>();
        _simdHandler.WriteValues(tempWriter, values);
        _serializedData = tempWriter.WrittenMemory.ToArray();
        _sequence = new ReadOnlySequence<byte>(_serializedData);
    }

    [IterationSetup]
    public void IterationSetup()
    {
        _sequence = new ReadOnlySequence<byte>(_serializedData);
    }

    [Benchmark(Baseline = true)]
    public void Scalar_Batch()
    {
        var destination = new sbyte[Count];
        var seq = _sequence;
        _scalarHandler.ReadValues(ref seq, destination, out _);
    }

    [Benchmark]
    public void SIMD_Batch()
    {
        var destination = new sbyte[Count];
        var seq = _sequence;
        _simdHandler.ReadValues(ref seq, destination, out _);
    }
    
    [Benchmark]
    public void Scalar_Single()
    {
        var seq = _sequence;
        for (var i = 0; i < Count; i++)
        {
            _ = _scalarHandler.ReadValue(ref seq, out _);
        }
    }
    
    [Benchmark]
    public void SIMD_Single()
    {
        var seq = _sequence;
        for (var i = 0; i < Count; i++)
        {
            _ = _simdHandler.ReadValue(ref seq, out _);
        }
    }
}