using System.Buffers;
using BenchmarkDotNet.Attributes;
using BenchmarkDotNet.Configs;
using BenchmarkDotNet.Jobs;
using ClickHouse.Direct.Types;

namespace ClickHouse.Direct.Benchmarks.Types;

[Config(typeof(Config))]
[MemoryDiagnoser]
[MarkdownExporter]
public class Int32TypeAlignmentBenchmarks
{
    private class Config : ManualConfig
    {
        public Config()
        {
            AddJob(Job.Default
                .WithId("Alignment")
                .WithWarmupCount(3)
                .WithIterationCount(10));
        }
    }
    
    private Int32Type _type = null!;
    private Dictionary<string, byte[]> _buffers = null!;
    private Dictionary<string, int[]> _values = null!;
    
    [GlobalSetup]
    public void Setup()
    {
        _type = new Int32Type();
        _buffers = new Dictionary<string, byte[]>();
        _values = new Dictionary<string, int[]>();
        
        var random = new Random(42);
        
        // Create buffers with different alignments
        var sizes = new[] { 3, 4, 5, 7, 8, 9, 15, 16, 17, 31, 32, 33, 63, 64, 65 };
        foreach (var size in sizes)
        {
            var buffer = new byte[size * sizeof(int)];
            var values = new int[size];
            
            for (var i = 0; i < size; i++)
            {
                values[i] = random.Next(int.MinValue, int.MaxValue);
                BitConverter.GetBytes(values[i]).CopyTo(buffer, i * sizeof(int));
            }
            
            _buffers[$"Size_{size}"] = buffer;
            _values[$"Size_{size}"] = values;
        }
    }
    
    [Benchmark]
    [Arguments("Size_3")]   // Not aligned to any SIMD boundary
    [Arguments("Size_4")]   // SSE2 boundary (1 vector)
    [Arguments("Size_5")]   // Just over SSE2
    [Arguments("Size_7")]   // Not aligned
    [Arguments("Size_8")]   // AVX2 boundary (1 vector)
    [Arguments("Size_9")]   // Just over AVX2
    [Arguments("Size_15")]  // Just under AVX512
    [Arguments("Size_16")]  // AVX512 boundary (1 vector)
    [Arguments("Size_17")]  // Just over AVX512
    [Arguments("Size_31")]  // Just under 2x AVX512
    [Arguments("Size_32")]  // 2x AVX512
    [Arguments("Size_33")]  // Just over 2x AVX512
    [Arguments("Size_63")]  // Just under 4x AVX512
    [Arguments("Size_64")]  // 4x AVX512
    [Arguments("Size_65")]  // Just over 4x AVX512
    public int[] ReadWithAlignment(string key)
    {
        var buffer = _buffers[key];
        var sequence = new ReadOnlySequence<byte>(buffer);
        var destination = new int[_values[key].Length];
        _type.ReadValues(ref sequence, destination, out _);
        return destination;
    }
    
    [Benchmark]
    [Arguments("Size_3")]
    [Arguments("Size_4")]
    [Arguments("Size_5")]
    [Arguments("Size_7")]
    [Arguments("Size_8")]
    [Arguments("Size_9")]
    [Arguments("Size_15")]
    [Arguments("Size_16")]
    [Arguments("Size_17")]
    [Arguments("Size_31")]
    [Arguments("Size_32")]
    [Arguments("Size_33")]
    [Arguments("Size_63")]
    [Arguments("Size_64")]
    [Arguments("Size_65")]
    public byte[] WriteWithAlignment(string key)
    {
        var values = _values[key];
        var writer = new ArrayBufferWriter<byte>();
        _type.WriteValues(writer, values);
        return writer.WrittenSpan.ToArray();
    }
}