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
public class Int32TypeReadBenchmarks
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
    
    private byte[] _readBuffer = null!;
    private int[] _readDestination = null!;
    
    private Int32Type _scalarType = null!;
    private Int32Type _sse2Type = null!;
    private Int32Type _avx2Type = null!;
    private Int32Type _avx512Type = null!;
    
    [Params(16, 64, 256, 1024, 4096, 16384)]
    public int Count { get; set; }
    
    [GlobalSetup]
    public void Setup()
    {
        _readBuffer = new byte[Count * sizeof(int)];
        _readDestination = new int[Count];
        
        var random = new Random(42);
        for (var i = 0; i < Count; i++)
        {
            var value = random.Next(int.MinValue, int.MaxValue);
            BitConverter.GetBytes(value).CopyTo(_readBuffer, i * sizeof(int));
        }
        
        _scalarType = new Int32Type(ConstrainedSimdCapabilities.ScalarOnly(DefaultSimdCapabilities.Instance));
        _sse2Type = new Int32Type(ConstrainedSimdCapabilities.MaxSse2(DefaultSimdCapabilities.Instance));
        _avx2Type = new Int32Type(ConstrainedSimdCapabilities.MaxAvx2(DefaultSimdCapabilities.Instance));
        _avx512Type = new Int32Type(DefaultSimdCapabilities.Instance);
    }
    
    [Benchmark(Baseline = true)]
    public int Scalar()
    {
        var sequence = new ReadOnlySequence<byte>(_readBuffer);
        return _scalarType.ReadValues(ref sequence, _readDestination, out _);
    }
    
    [Benchmark]
    public int SSE2()
    {
        var sequence = new ReadOnlySequence<byte>(_readBuffer);
        return _sse2Type.ReadValues(ref sequence, _readDestination, out _);
    }
    
    [Benchmark]
    public int AVX2()
    {
        var sequence = new ReadOnlySequence<byte>(_readBuffer);
        return _avx2Type.ReadValues(ref sequence, _readDestination, out _);
    }
    
    [Benchmark]
    public int AVX512()
    {
        var sequence = new ReadOnlySequence<byte>(_readBuffer);
        return _avx512Type.ReadValues(ref sequence, _readDestination, out _);
    }
}