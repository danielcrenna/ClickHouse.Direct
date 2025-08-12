using System.Buffers;
using BenchmarkDotNet.Attributes;
using ClickHouse.Direct.Abstractions;
using ClickHouse.Direct.Types;

namespace ClickHouse.Direct.Benchmarks.Types;

[MemoryDiagnoser]
[SimpleJob]
public class Int8TypeWriteBenchmarks
{
    private sbyte[] _values = null!;
    private ArrayBufferWriter<byte> _writer = null!;
    private Int8Type _scalarHandler = null!;
    private Int8Type _simdHandler = null!;

    [Params(100, 1000, 10000)]
    public int Count { get; set; }

    [GlobalSetup]
    public void Setup()
    {
        _values = new sbyte[Count];
        var random = new Random(42);
        for (var i = 0; i < Count; i++)
        {
            _values[i] = (sbyte)random.Next(sbyte.MinValue, sbyte.MaxValue + 1);
        }

        _writer = new ArrayBufferWriter<byte>();
        
        // Create handlers with different SIMD capabilities
        _scalarHandler = new Int8Type(
            ConstrainedSimdCapabilities.ScalarOnly(DefaultSimdCapabilities.Instance));
        _simdHandler = new Int8Type(DefaultSimdCapabilities.Instance);
    }

    [IterationSetup]
    public void IterationSetup()
    {
        _writer.Clear();
    }

    [Benchmark(Baseline = true)]
    public void Scalar_Batch()
    {
        _scalarHandler.WriteValues(_writer, _values);
    }

    [Benchmark]
    public void SIMD_Batch()
    {
        _simdHandler.WriteValues(_writer, _values);
    }
    
    [Benchmark]
    public void Scalar_Single()
    {
        foreach (var b in _values)
        {
            _scalarHandler.WriteValue(_writer, b);
        }
    }
    
    [Benchmark]
    public void SIMD_Single()
    {
        foreach (var b in _values)
        {
            _simdHandler.WriteValue(_writer, b);
        }
    }
}