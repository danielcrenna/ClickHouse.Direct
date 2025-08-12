using System.Buffers;
using BenchmarkDotNet.Attributes;
using BenchmarkDotNet.Jobs;
using ClickHouse.Direct.Abstractions;
using ClickHouse.Direct.Types;

namespace ClickHouse.Direct.Benchmarks.Types;

[SimpleJob(RuntimeMoniker.Net90)]
[MemoryDiagnoser]
public class StringTypeReadBenchmarks
{
    private StringType _stringType = null!;
    private StringType _stringTypeNoSimd = null!;
    private byte[] _encodedData = null!;
    
    [Params(10, 100, 1000)]
    public int StringCount { get; set; }
    
    [Params(10, 50, 200)]
    public int AverageStringLength { get; set; }
    
    [GlobalSetup]
    public void Setup()
    {
        _stringType = new StringType(DefaultSimdCapabilities.Instance);
        _stringTypeNoSimd = new StringType(ConstrainedSimdCapabilities.ScalarOnly(DefaultSimdCapabilities.Instance));
        
        var testStrings = new string[StringCount];
        var random = new Random(42);
        
        for (var i = 0; i < StringCount; i++)
        {
            var length = random.Next(Math.Max(1, AverageStringLength - 10), AverageStringLength + 10);
            var chars = new char[length];
            
            for (var j = 0; j < length; j++)
            {
                // Mix ASCII and non-ASCII characters
                if (random.Next(10) < 8)
                {
                    chars[j] = (char)('a' + random.Next(26));
                }
                else
                {
                    chars[j] = (char)('А' + random.Next(32)); // Cyrillic
                }
            }
            
            testStrings[i] = new string(chars);
        }
        
        // Pre-encode data for read benchmarks
        var bufferWriter = new ArrayBufferWriter<byte>();
        _stringType.WriteValues(bufferWriter, testStrings);
        _encodedData = bufferWriter.WrittenMemory.ToArray();
    }
    
    [Benchmark(Baseline = true)]
    public string[] NoSimd()
    {
        var sequence = new ReadOnlySequence<byte>(_encodedData);
        var results = new string[StringCount];
        _stringTypeNoSimd.ReadValues(ref sequence, results, out _);
        return results;
    }
    
    [Benchmark]
    public string[] WithSimd()
    {
        var sequence = new ReadOnlySequence<byte>(_encodedData);
        var results = new string[StringCount];
        _stringType.ReadValues(ref sequence, results, out _);
        return results;
    }
}