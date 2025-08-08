using System.Buffers;
using System.Runtime.Intrinsics.X86;
using BenchmarkDotNet.Attributes;
using ClickHouse.Direct.Abstractions;
using ClickHouse.Direct.Types;

namespace ClickHouse.Direct.Benchmarks.Types;

[SimpleJob]
[MemoryDiagnoser]
[MarkdownExporter]
public class UuidTypeBenchmarks
{
    private readonly Guid[] _guids;
    private readonly byte[] _guidBytes;
    
    private readonly UuidType _fullSimd;
    private readonly UuidType _maxAvx2;
    private readonly UuidType _maxAvx;
    private readonly UuidType _maxSse2;
    private readonly UuidType _scalarOnly;

    [Params(10, 100, 1000, 10000)]
    public int Count { get; set; }

    public UuidTypeBenchmarks()
    {
        var actualCapabilities = DefaultSimdCapabilities.Instance;
        _fullSimd = new UuidType(actualCapabilities);
        _maxAvx2 = new UuidType(ConstrainedSimdCapabilities.MaxAvx2(actualCapabilities));
        _maxAvx = new UuidType(ConstrainedSimdCapabilities.MaxAvx(actualCapabilities));
        _maxSse2 = new UuidType(ConstrainedSimdCapabilities.MaxSse2(actualCapabilities));
        _scalarOnly = new UuidType(ConstrainedSimdCapabilities.ScalarOnly(actualCapabilities));
        
        _guids = new Guid[10000];
        _guidBytes = new byte[10000 * 16];

        var random = new Random(42);
        for (var i = 0; i < _guids.Length; i++)
        {
            var bytes = new byte[16];
            random.NextBytes(bytes);
            _guids[i] = new Guid(bytes);
        }

        var writer = new ArrayBufferWriter<byte>();
        _fullSimd.WriteValues(writer, _guids.AsSpan(0, _guids.Length));
        writer.WrittenSpan.CopyTo(_guidBytes);
    }

    [Benchmark(Baseline = true)]
    public void WriteGuids_FullSimd()
    {
        var writer = new ArrayBufferWriter<byte>();
        var span = _guids.AsSpan(0, Count);
        _fullSimd.WriteValues(writer, span);
    }

    [Benchmark]
    public void WriteGuids_MaxAvx2()
    {
        var writer = new ArrayBufferWriter<byte>();
        var span = _guids.AsSpan(0, Count);
        _maxAvx2.WriteValues(writer, span);
    }

    [Benchmark]
    public void WriteGuids_MaxAvx()
    {
        var writer = new ArrayBufferWriter<byte>();
        var span = _guids.AsSpan(0, Count);
        _maxAvx.WriteValues(writer, span);
    }

    [Benchmark]
    public void WriteGuids_MaxSse2()
    {
        var writer = new ArrayBufferWriter<byte>();
        var span = _guids.AsSpan(0, Count);
        _maxSse2.WriteValues(writer, span);
    }

    [Benchmark]
    public void WriteGuids_ScalarOnly()
    {
        var writer = new ArrayBufferWriter<byte>();
        var span = _guids.AsSpan(0, Count);
        _scalarOnly.WriteValues(writer, span);
    }

    [Benchmark]
    public void ReadGuids_FullSimd()
    {
        var sequence = new ReadOnlySequence<byte>(_guidBytes.AsMemory(0, Count * 16));
        var destination = new Guid[Count];
        _fullSimd.ReadValues(ref sequence, destination, out _);
    }

    [Benchmark]
    public void ReadGuids_MaxAvx2()
    {
        var sequence = new ReadOnlySequence<byte>(_guidBytes.AsMemory(0, Count * 16));
        var destination = new Guid[Count];
        _maxAvx2.ReadValues(ref sequence, destination, out _);
    }

    [Benchmark]
    public void ReadGuids_MaxAvx()
    {
        var sequence = new ReadOnlySequence<byte>(_guidBytes.AsMemory(0, Count * 16));
        var destination = new Guid[Count];
        _maxAvx.ReadValues(ref sequence, destination, out _);
    }

    [Benchmark]
    public void ReadGuids_MaxSse2()
    {
        var sequence = new ReadOnlySequence<byte>(_guidBytes.AsMemory(0, Count * 16));
        var destination = new Guid[Count];
        _maxSse2.ReadValues(ref sequence, destination, out _);
    }

    [Benchmark]
    public void ReadGuids_ScalarOnly()
    {
        var sequence = new ReadOnlySequence<byte>(_guidBytes.AsMemory(0, Count * 16));
        var destination = new Guid[Count];
        _scalarOnly.ReadValues(ref sequence, destination, out _);
    }

    [Benchmark]
    public void RoundTrip_FullSimd()
    {
        var span = _guids.AsSpan(0, Count);
        
        // Write
        var writer = new ArrayBufferWriter<byte>();
        _fullSimd.WriteValues(writer, span);
        
        // Read
        var sequence = new ReadOnlySequence<byte>(writer.WrittenMemory);
        var destination = new Guid[Count];
        _fullSimd.ReadValues(ref sequence, destination, out _);
    }

    [Benchmark]
    public void RoundTrip_MaxAvx2()
    {
        var span = _guids.AsSpan(0, Count);
        
        // Write
        var writer = new ArrayBufferWriter<byte>();
        _maxAvx2.WriteValues(writer, span);
        
        // Read
        var sequence = new ReadOnlySequence<byte>(writer.WrittenMemory);
        var destination = new Guid[Count];
        _maxAvx2.ReadValues(ref sequence, destination, out _);
    }

    [Benchmark]
    public void RoundTrip_ScalarOnly()
    {
        var span = _guids.AsSpan(0, Count);
        
        // Write
        var writer = new ArrayBufferWriter<byte>();
        _scalarOnly.WriteValues(writer, span);
        
        // Read
        var sequence = new ReadOnlySequence<byte>(writer.WrittenMemory);
        var destination = new Guid[Count];
        _scalarOnly.ReadValues(ref sequence, destination, out _);
    }

    [GlobalSetup]
    public void GlobalSetup()
    {
        Console.WriteLine("Hardware SIMD Capabilities:");
        Console.WriteLine($"  AVX512F Support: {Avx512F.IsSupported}");
        Console.WriteLine($"  AVX2 Support: {Avx2.IsSupported}");
        Console.WriteLine($"  AVX Support: {Avx.IsSupported}");
        Console.WriteLine($"  SSE2 Support: {Sse2.IsSupported}");
        Console.WriteLine();
        
        Console.WriteLine("Benchmark Configuration:");
        Console.WriteLine($"  FullSimd: AVX512F={_fullSimd.SimdCapabilities.IsAvx512FSupported}, AVX2={_fullSimd.SimdCapabilities.IsAvx2Supported}");
        Console.WriteLine($"  MaxAvx2: AVX512F={_maxAvx2.SimdCapabilities.IsAvx512FSupported}, AVX2={_maxAvx2.SimdCapabilities.IsAvx2Supported}");
        Console.WriteLine($"  ScalarOnly: AVX512F={_scalarOnly.SimdCapabilities.IsAvx512FSupported}, AVX2={_scalarOnly.SimdCapabilities.IsAvx2Supported}");
        Console.WriteLine();
    }
}