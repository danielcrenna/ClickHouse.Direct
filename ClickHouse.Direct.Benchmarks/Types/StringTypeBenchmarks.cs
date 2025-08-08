using System.Buffers;
using BenchmarkDotNet.Attributes;
using BenchmarkDotNet.Jobs;
using ClickHouse.Direct.Abstractions;
using ClickHouse.Direct.Types;

namespace ClickHouse.Direct.Benchmarks.Types;

[SimpleJob(RuntimeMoniker.Net90)]
[MemoryDiagnoser]
public class StringTypeBenchmarks
{
    private StringType _stringType = null!;
    private StringType _stringTypeNoSimd = null!;
    private string[] _testStrings = null!;
    private byte[] _encodedData = null!;
    private ArrayBufferWriter<byte> _bufferWriter = null!;
    
    [Params(10, 100, 1000)]
    public int StringCount { get; set; }
    
    [Params(10, 50, 200)]
    public int AverageStringLength { get; set; }
    
    [GlobalSetup]
    public void Setup()
    {
        _stringType = new StringType(DefaultSimdCapabilities.Instance);
        _stringTypeNoSimd = new StringType(ConstrainedSimdCapabilities.ScalarOnly(DefaultSimdCapabilities.Instance));
        
        _testStrings = new string[StringCount];
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
            
            _testStrings[i] = new string(chars);
        }
        
        // Pre-encode data for read benchmarks
        _bufferWriter = new ArrayBufferWriter<byte>();
        _stringType.WriteValues(_bufferWriter, _testStrings);
        _encodedData = _bufferWriter.WrittenMemory.ToArray();
    }
    
    [Benchmark(Baseline = true)]
    public void WriteValues_NoSimd()
    {
        _bufferWriter.Clear();
        _stringTypeNoSimd.WriteValues(_bufferWriter, _testStrings);
    }
    
    [Benchmark]
    public void WriteValues_WithSimd()
    {
        _bufferWriter.Clear();
        _stringType.WriteValues(_bufferWriter, _testStrings);
    }
    
    [Benchmark]
    public string[] ReadValues_NoSimd()
    {
        var sequence = new ReadOnlySequence<byte>(_encodedData);
        var results = new string[StringCount];
        _stringTypeNoSimd.ReadValues(ref sequence, results, out _);
        return results;
    }
    
    [Benchmark]
    public string[] ReadValues_WithSimd()
    {
        var sequence = new ReadOnlySequence<byte>(_encodedData);
        var results = new string[StringCount];
        _stringType.ReadValues(ref sequence, results, out _);
        return results;
    }
    
    [Benchmark]
    public void WriteSingleValue_NoSimd()
    {
        _bufferWriter.Clear();
        foreach (var str in _testStrings)
        {
            _stringTypeNoSimd.WriteValue(_bufferWriter, str);
        }
    }
    
    [Benchmark]
    public void WriteSingleValue_WithSimd()
    {
        _bufferWriter.Clear();
        foreach (var str in _testStrings)
        {
            _stringType.WriteValue(_bufferWriter, str);
        }
    }
}