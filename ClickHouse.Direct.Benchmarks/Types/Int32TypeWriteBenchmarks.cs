using System.Buffers;
using BenchmarkDotNet.Attributes;
using BenchmarkDotNet.Configs;
using BenchmarkDotNet.Jobs;
using ClickHouse.Direct.Abstractions;
using ClickHouse.Direct.Types;

namespace ClickHouse.Direct.Benchmarks.Types;

[Config(typeof(Config))]
[MemoryDiagnoser]
[DisassemblyDiagnoser(maxDepth: 3)]
[MarkdownExporter]
public class Int32TypeWriteBenchmarks
{
    private class Config : ManualConfig
    {
        public Config()
        {
            AddJob(Job.Default
                .WithId("Default")
                .WithWarmupCount(3)
                .WithIterationCount(10));
        }
    }
    
    private int[] _writeValues = null!;
    
    private Int32Type _scalarType = null!;
    private Int32Type _sse2Type = null!;
    private Int32Type _avx2Type = null!;
    private Int32Type _avx512Type = null!;
    
    [Params(16, 64, 256, 1024, 4096, 16384)]
    public int Count { get; set; }
    
    [GlobalSetup]
    public void Setup()
    {
        _writeValues = new int[Count];
        
        var random = new Random(42);
        for (var i = 0; i < Count; i++)
        {
            _writeValues[i] = random.Next(int.MinValue, int.MaxValue);
        }
        
        _scalarType = new Int32Type(ConstrainedSimdCapabilities.ScalarOnly(DefaultSimdCapabilities.Instance));
        _sse2Type = new Int32Type(ConstrainedSimdCapabilities.MaxSse2(DefaultSimdCapabilities.Instance));
        _avx2Type = new Int32Type(ConstrainedSimdCapabilities.MaxAvx2(DefaultSimdCapabilities.Instance));
        _avx512Type = new Int32Type(DefaultSimdCapabilities.Instance);
    }
    
    [Benchmark(Baseline = true)]
    public void Scalar()
    {
        var writer = new ArrayBufferWriter<byte>();
        _scalarType.WriteValues(writer, _writeValues);
    }
    
    [Benchmark]
    public void SSE2()
    {
        var writer = new ArrayBufferWriter<byte>();
        _sse2Type.WriteValues(writer, _writeValues);
    }
    
    [Benchmark]
    public void AVX2()
    {
        var writer = new ArrayBufferWriter<byte>();
        _avx2Type.WriteValues(writer, _writeValues);
    }
    
    [Benchmark]
    public void AVX512()
    {
        var writer = new ArrayBufferWriter<byte>();
        _avx512Type.WriteValues(writer, _writeValues);
    }
}