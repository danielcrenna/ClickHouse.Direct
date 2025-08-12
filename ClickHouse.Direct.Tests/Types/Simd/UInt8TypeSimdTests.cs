using System.Buffers;
using ClickHouse.Direct.Abstractions;
using ClickHouse.Direct.Types;
using Xunit.Abstractions;

namespace ClickHouse.Direct.Tests.Types.Simd;

public class UInt8TypeSimdTests(ITestOutputHelper output)
{
    [Theory]
    [MemberData(nameof(GetSimdPathWithBwTestData))]
    public void ReadValues_AllSimdPaths_ProduceSameResults(
        bool sse2, bool ssse3, bool avx, bool avx2, bool avx512F, bool avx512Bw, string description)
    {
        output.WriteLine($"Testing read with {description}");
        
        // Test various sizes to hit different SIMD paths
        var testSizes = SimdPathTestHelper.GetTestSizesForType(typeof(byte));
        
        foreach (var size in testSizes)
        {
            output.WriteLine($"  Size: {size}");
            
            var expectedValues = SimdPathTestHelper.GenerateTestData<byte>(size);
            
            // Serialize the data
            var writer = new ArrayBufferWriter<byte>();
            foreach (var value in expectedValues)
            {
                UInt8Type.Instance.WriteValue(writer, value);
            }
            
            // Create type handler with constrained capabilities
            var capabilities = SimdPathTestHelper.CreateConstrainedCapabilities(
                sse2, ssse3, avx, avx2, avx512F, avx512Bw);
            var typeHandler = new UInt8Type(capabilities);
            
            // Read values back
            var sequence = new ReadOnlySequence<byte>(writer.WrittenMemory);
            var actualValues = new byte[size];
            var itemsRead = typeHandler.ReadValues(ref sequence, actualValues, out var bytesConsumed);
            
            // Verify
            Assert.Equal(size, itemsRead);
            Assert.Equal(size * sizeof(byte), bytesConsumed);
            Assert.Equal(expectedValues, actualValues);
        }
    }
    
    [Theory]
    [MemberData(nameof(GetSimdPathWithBwTestData))]
    public void WriteValues_AllSimdPaths_ProduceSameResults(
        bool sse2, bool ssse3, bool avx, bool avx2, bool avx512F, bool avx512Bw, string description)
    {
        output.WriteLine($"Testing write with {description}");
        
        // Test various sizes to hit different SIMD paths
        var testSizes = SimdPathTestHelper.GetTestSizesForType(typeof(byte));
        
        foreach (var size in testSizes)
        {
            output.WriteLine($"  Size: {size}");
            
            var values = SimdPathTestHelper.GenerateTestData<byte>(size);
            
            // Create type handler with constrained capabilities
            var capabilities = SimdPathTestHelper.CreateConstrainedCapabilities(
                sse2, ssse3, avx, avx2, avx512F, avx512Bw);
            var typeHandler = new UInt8Type(capabilities);
            
            // Write values
            var writer = new ArrayBufferWriter<byte>();
            typeHandler.WriteValues(writer, values);
            
            // Write expected values with scalar method for comparison
            var expectedWriter = new ArrayBufferWriter<byte>();
            var scalarCapabilities = SimdPathTestHelper.CreateConstrainedCapabilities(
                false, false, false, false, false);
            var scalarHandler = new UInt8Type(scalarCapabilities);
            scalarHandler.WriteValues(expectedWriter, values);
            
            // Verify
            SimdPathTestHelper.AssertBytesEqual(
                expectedWriter.WrittenMemory.ToArray(),
                writer.WrittenMemory.ToArray(),
                $"SIMD path {description} size {size}");
        }
    }
    
    [Theory]
    [MemberData(nameof(GetSimdPathWithBwTestData))]
    public void ReadValues_FragmentedSequence_AllSimdPaths(
        bool sse2, bool ssse3, bool avx, bool avx2, bool avx512F, bool avx512Bw, string description)
    {
        output.WriteLine($"Testing fragmented read with {description}");
        
        // Test with fragmented sequences of various sizes
        var testSizes = new[] { 17, 33, 65 }; // Odd sizes to ensure partial vectors
        
        foreach (var size in testSizes)
        {
            output.WriteLine($"  Size: {size}");
            
            var expectedValues = SimdPathTestHelper.GenerateTestData<byte>(size);
            
            // Serialize the data
            var writer = new ArrayBufferWriter<byte>();
            foreach (var value in expectedValues)
            {
                UInt8Type.Instance.WriteValue(writer, value);
            }
            
            // Create fragmented sequence (each byte in separate segment)
            var bytes = writer.WrittenMemory.ToArray();
            ReadOnlySequence<byte> sequence;
            
            if (size == 1)
            {
                sequence = new ReadOnlySequence<byte>(bytes);
            }
            else
            {
                // Create a fragmented sequence with each byte in a separate segment
                var firstSegment = new BufferSegment(new Memory<byte>(bytes, 0, sizeof(byte)));
                var lastSegment = firstSegment;
                
                for (var i = 1; i < size; i++)
                {
                    var nextSegment = new BufferSegment(
                        new Memory<byte>(bytes, i * sizeof(byte), sizeof(byte)));
                    lastSegment.Append(nextSegment);
                    lastSegment = nextSegment;
                }
                
                sequence = new ReadOnlySequence<byte>(firstSegment, 0, lastSegment, sizeof(byte));
            }
            
            // Create type handler with constrained capabilities
            var capabilities = SimdPathTestHelper.CreateConstrainedCapabilities(
                sse2, ssse3, avx, avx2, avx512F, avx512Bw);
            var typeHandler = new UInt8Type(capabilities);
            
            // Read values back
            var actualValues = new byte[size];
            var itemsRead = typeHandler.ReadValues(ref sequence, actualValues, out var bytesConsumed);
            
            // Verify
            Assert.Equal(size, itemsRead);
            Assert.Equal(size * sizeof(byte), bytesConsumed);
            Assert.Equal(expectedValues, actualValues);
        }
    }
    
    [Fact]
    public void ReadValues_VerifySimdPathSelection()
    {
        // This test verifies that the correct SIMD path is selected based on capabilities
        var testData = SimdPathTestHelper.GenerateTestData<byte>(100);
        
        var writer = new ArrayBufferWriter<byte>();
        foreach (var value in testData)
        {
            UInt8Type.Instance.WriteValue(writer, value);
        }
        
        // Test AVX512BW path (processes 64 values at once)
        if (DefaultSimdCapabilities.Instance.IsAvx512BwSupported)
        {
            output.WriteLine("Testing AVX512BW path");
            var avx512BwHandler = new UInt8Type(
                SimdPathTestHelper.CreateConstrainedCapabilities(true, true, true, true, true, true));
            var sequence = new ReadOnlySequence<byte>(writer.WrittenMemory);
            var result = new byte[100];
            avx512BwHandler.ReadValues(ref sequence, result, out _);
            Assert.Equal(testData, result);
        }
        
        // Test AVX2 path (processes 32 values at once)
        if (DefaultSimdCapabilities.Instance.IsAvx2Supported)
        {
            output.WriteLine("Testing AVX2 path");
            var avx2Handler = new UInt8Type(
                SimdPathTestHelper.CreateConstrainedCapabilities(true, true, true, true, false));
            var sequence = new ReadOnlySequence<byte>(writer.WrittenMemory);
            var result = new byte[100];
            avx2Handler.ReadValues(ref sequence, result, out _);
            Assert.Equal(testData, result);
        }
        
        // Test SSE2 path (processes 16 values at once)
        if (DefaultSimdCapabilities.Instance.IsSse2Supported)
        {
            output.WriteLine("Testing SSE2 path");
            var sse2Handler = new UInt8Type(
                SimdPathTestHelper.CreateConstrainedCapabilities(true, false, false, false, false));
            var sequence = new ReadOnlySequence<byte>(writer.WrittenMemory);
            var result = new byte[100];
            sse2Handler.ReadValues(ref sequence, result, out _);
            Assert.Equal(testData, result);
        }
        
        // Test scalar path
        {
            output.WriteLine("Testing scalar path");
            var scalarHandler = new UInt8Type(
                SimdPathTestHelper.CreateConstrainedCapabilities(false, false, false, false, false));
            var sequence = new ReadOnlySequence<byte>(writer.WrittenMemory);
            var result = new byte[100];
            scalarHandler.ReadValues(ref sequence, result, out _);
            Assert.Equal(testData, result);
        }
    }
    
    [Theory]
    [InlineData(15, "Just below SSE2 boundary")]
    [InlineData(16, "Exactly SSE2 boundary")]
    [InlineData(17, "Just above SSE2 boundary")]
    [InlineData(31, "Just below AVX2 boundary")]
    [InlineData(32, "Exactly AVX2 boundary")]
    [InlineData(33, "Just above AVX2 boundary")]
    [InlineData(63, "Just below AVX512BW boundary")]
    [InlineData(64, "Exactly AVX512BW boundary")]
    [InlineData(65, "Just above AVX512BW boundary")]
    public void ReadValues_BoundaryConditions(int size, string description)
    {
        output.WriteLine($"Testing boundary: {description}");
        
        // Generate test data
        var expectedValues = SimdPathTestHelper.GenerateTestData<byte>(size);
        
        // Serialize the data
        var writer = new ArrayBufferWriter<byte>();
        foreach (var value in expectedValues)
        {
            UInt8Type.Instance.WriteValue(writer, value);
        }
        
        // Test with all SIMD enabled
        var allSimdHandler = new UInt8Type();
        var sequence1 = new ReadOnlySequence<byte>(writer.WrittenMemory);
        var result1 = new byte[size];
        allSimdHandler.ReadValues(ref sequence1, result1, out _);
        Assert.Equal(expectedValues, result1);
        
        // Test with scalar only
        var scalarHandler = new UInt8Type(
            SimdPathTestHelper.CreateConstrainedCapabilities(false, false, false, false, false));
        var sequence2 = new ReadOnlySequence<byte>(writer.WrittenMemory);
        var result2 = new byte[size];
        scalarHandler.ReadValues(ref sequence2, result2, out _);
        Assert.Equal(expectedValues, result2);
    }
    
    public static IEnumerable<object[]> GetSimdPathWithBwTestData()
        => SimdPathTestHelper.GetSimdPathWithBwTestData();
}