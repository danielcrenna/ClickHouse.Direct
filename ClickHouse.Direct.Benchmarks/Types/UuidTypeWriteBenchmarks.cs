using System.Buffers;
using System.Runtime.Intrinsics.X86;
using BenchmarkDotNet.Attributes;
using ClickHouse.Direct.Abstractions;
using ClickHouse.Direct.Types;

namespace ClickHouse.Direct.Benchmarks.Types;

[SimpleJob]
[MemoryDiagnoser]
[MarkdownExporter]
public class UuidTypeWriteBenchmarks
{
    private readonly Guid[] _guids;
    
    private readonly UuidType _fullSimd;
    private readonly UuidType _maxAvx2;
    private readonly UuidType _maxAvx;
    private readonly UuidType _maxSse2;
    private readonly UuidType _scalarOnly;

    [Params(10_000_000)]
    public int Count { get; set; }

    public UuidTypeWriteBenchmarks()
    {
        var actualCapabilities = DefaultSimdCapabilities.Instance;
        _fullSimd = new UuidType(actualCapabilities);
        _maxAvx2 = new UuidType(ConstrainedSimdCapabilities.MaxAvx2(actualCapabilities));
        _maxAvx = new UuidType(ConstrainedSimdCapabilities.MaxAvx(actualCapabilities));
        _maxSse2 = new UuidType(ConstrainedSimdCapabilities.MaxSse2(actualCapabilities));
        _scalarOnly = new UuidType(ConstrainedSimdCapabilities.ScalarOnly(actualCapabilities));
        
        _guids = new Guid[10_000_000];
    }

    [Benchmark(Baseline = true)]
    public void ScalarOnly()
    {
        var writer = new ArrayBufferWriter<byte>();
        var span = _guids.AsSpan(0, Count);
        _scalarOnly.WriteValues(writer, span);
    }

    [Benchmark]
    public void MaxSse2()
    {
        var writer = new ArrayBufferWriter<byte>();
        var span = _guids.AsSpan(0, Count);
        _maxSse2.WriteValues(writer, span);
    }

    [Benchmark]
    public void MaxAvx()
    {
        var writer = new ArrayBufferWriter<byte>();
        var span = _guids.AsSpan(0, Count);
        _maxAvx.WriteValues(writer, span);
    }

    [Benchmark]
    public void MaxAvx2()
    {
        var writer = new ArrayBufferWriter<byte>();
        var span = _guids.AsSpan(0, Count);
        _maxAvx2.WriteValues(writer, span);
    }

    [Benchmark]
    public void FullSimd()
    {
        var writer = new ArrayBufferWriter<byte>();
        var span = _guids.AsSpan(0, Count);
        _fullSimd.WriteValues(writer, span);
    }

    [GlobalSetup]
    public void GlobalSetup()
    {
        var random = new Random(42);
        for (var i = 0; i < Count; i++)
        {
            var bytes = new byte[16];
            random.NextBytes(bytes);
            _guids[i] = new Guid(bytes);
        }
        
        Console.WriteLine("Hardware SIMD Capabilities:");
        Console.WriteLine($"  AVX512F Support: {Avx512F.IsSupported}");
        Console.WriteLine($"  AVX512BW Support: {Avx512BW.IsSupported}");
        Console.WriteLine($"  AVX2 Support: {Avx2.IsSupported}");
        Console.WriteLine($"  AVX Support: {Avx.IsSupported}");
        Console.WriteLine($"  SSE2 Support: {Sse2.IsSupported}");
        Console.WriteLine();
        
        Console.WriteLine("Write Benchmark Configuration:");
        Console.WriteLine($"  ScalarOnly: AVX512BW={_scalarOnly.SimdCapabilities.IsAvx512BwSupported}, AVX512F={_scalarOnly.SimdCapabilities.IsAvx512FSupported}, AVX2={_scalarOnly.SimdCapabilities.IsAvx2Supported}");
        Console.WriteLine($"  MaxSse2: AVX512BW={_maxSse2.SimdCapabilities.IsAvx512BwSupported}, AVX512F={_maxSse2.SimdCapabilities.IsAvx512FSupported}, AVX2={_maxSse2.SimdCapabilities.IsAvx2Supported}");
        Console.WriteLine($"  MaxAvx: AVX512BW={_maxAvx.SimdCapabilities.IsAvx512BwSupported}, AVX512F={_maxAvx.SimdCapabilities.IsAvx512FSupported}, AVX2={_maxAvx.SimdCapabilities.IsAvx2Supported}");
        Console.WriteLine($"  MaxAvx2: AVX512BW={_maxAvx2.SimdCapabilities.IsAvx512BwSupported}, AVX512F={_maxAvx2.SimdCapabilities.IsAvx512FSupported}, AVX2={_maxAvx2.SimdCapabilities.IsAvx2Supported}");
        Console.WriteLine($"  FullSimd: AVX512BW={_fullSimd.SimdCapabilities.IsAvx512BwSupported}, AVX512F={_fullSimd.SimdCapabilities.IsAvx512FSupported}, AVX2={_fullSimd.SimdCapabilities.IsAvx2Supported}");
        Console.WriteLine();
        Console.WriteLine($"  Data size: {Count:N0} GUIDs ({Count * 16 / 1024.0 / 1024.0:F2} MB)");
        Console.WriteLine();
    }
}