using System.Buffers;
using ClickHouse.Direct.Types;
using Xunit.Abstractions;

namespace ClickHouse.Direct.Tests.Types.Simd;

public class Float32TypeSimdTests(ITestOutputHelper output)
{
    [Theory]
    [MemberData(nameof(GetSimdPathTestData))]
    public void ReadValues_AllSimdPaths_ProduceSameResults(
        bool sse2, bool ssse3, bool avx, bool avx2, bool avx512F, string description)
    {
        output.WriteLine($"Testing read with {description}");
        
        // Test various sizes to hit different SIMD paths
        var testSizes = SimdPathTestHelper.GetTestSizesForType(typeof(float));
        
        foreach (var size in testSizes)
        {
            output.WriteLine($"  Size: {size}");
            
            var expectedValues = SimdPathTestHelper.GenerateTestData<float>(size);
            
            // Serialize the data
            var writer = new ArrayBufferWriter<byte>();
            foreach (var value in expectedValues)
                Float32Type.Instance.WriteValue(writer, value);

            // Create type handler with constrained capabilities
            var capabilities = SimdPathTestHelper.CreateConstrainedCapabilities(
                sse2, ssse3, avx, avx2, avx512F);
            var typeHandler = new Float32Type(capabilities);
            
            // Read values back
            var sequence = new ReadOnlySequence<byte>(writer.WrittenMemory);
            var actualValues = new float[size];
            var itemsRead = typeHandler.ReadValues(ref sequence, actualValues, out var bytesConsumed);
            
            // Verify
            Assert.Equal(size, itemsRead);
            Assert.Equal(size * sizeof(float), bytesConsumed);
            Assert.Equal(expectedValues, actualValues);
        }
    }
    
    [Theory]
    [MemberData(nameof(GetSimdPathTestData))]
    public void WriteValues_AllSimdPaths_ProduceSameResults(
        bool sse2, bool ssse3, bool avx, bool avx2, bool avx512F, string description)
    {
        output.WriteLine($"Testing write with {description}");
        
        // Test various sizes to hit different SIMD paths
        var testSizes = SimdPathTestHelper.GetTestSizesForType(typeof(float));
        
        foreach (var size in testSizes)
        {
            output.WriteLine($"  Size: {size}");
            
            var values = SimdPathTestHelper.GenerateTestData<float>(size);
            
            // Create type handler with constrained capabilities
            var capabilities = SimdPathTestHelper.CreateConstrainedCapabilities(
                sse2, ssse3, avx, avx2, avx512F);
            var typeHandler = new Float32Type(capabilities);
            
            // Write values
            var writer = new ArrayBufferWriter<byte>();
            typeHandler.WriteValues(writer, values);
            
            // Write expected values with scalar method for comparison
            var expectedWriter = new ArrayBufferWriter<byte>();
            var scalarCapabilities = SimdPathTestHelper.CreateConstrainedCapabilities(
                false, false, false, false, false);
            var scalarHandler = new Float32Type(scalarCapabilities);
            scalarHandler.WriteValues(expectedWriter, values);
            
            // Verify
            SimdPathTestHelper.AssertBytesEqual(
                expectedWriter.WrittenMemory.ToArray(),
                writer.WrittenMemory.ToArray(),
                $"SIMD path {description} size {size}");
        }
    }
    
    [Theory]
    [MemberData(nameof(GetSimdPathTestData))]
    public void ReadValues_FragmentedSequence_AllSimdPaths(
        bool sse2, bool ssse3, bool avx, bool avx2, bool avx512F, string description)
    {
        output.WriteLine($"Testing fragmented read with {description}");
        
        // Test with fragmented sequences of various sizes
        var testSizes = new[] { 17, 33, 65 }; // Odd sizes to ensure partial vectors
        
        foreach (var size in testSizes)
        {
            output.WriteLine($"  Size: {size}");
            
            var expectedValues = SimdPathTestHelper.GenerateTestData<float>(size);
            
            // Serialize the data
            var writer = new ArrayBufferWriter<byte>();
            foreach (var value in expectedValues)
            {
                Float32Type.Instance.WriteValue(writer, value);
            }
            
            // Create fragmented sequence with small segments
            var fullBuffer = writer.WrittenMemory.ToArray();
            var segments = new List<BufferSegment>();
            var segmentSize = 7 * sizeof(float); // Non-aligned segment size
            
            for (var offset = 0; offset < fullBuffer.Length; offset += segmentSize)
            {
                var length = Math.Min(segmentSize, fullBuffer.Length - offset);
                var segment = new BufferSegment(new Memory<byte>(fullBuffer, offset, length));
                if (segments.Count > 0)
                {
                    segments[^1].Append(segment);
                }
                segments.Add(segment);
            }
            
            var sequence = new ReadOnlySequence<byte>(
                segments[0],
                0,
                segments[^1],
                segments[^1].Memory.Length);
            
            // Create type handler with constrained capabilities
            var capabilities = SimdPathTestHelper.CreateConstrainedCapabilities(
                sse2, ssse3, avx, avx2, avx512F);
            var typeHandler = new Float32Type(capabilities);
            
            // Read values back
            var actualValues = new float[size];
            var itemsRead = typeHandler.ReadValues(ref sequence, actualValues, out var bytesConsumed);
            
            // Verify
            Assert.Equal(size, itemsRead);
            Assert.Equal(size * sizeof(float), bytesConsumed);
            Assert.Equal(expectedValues, actualValues);
        }
    }
    
    [Fact]
    public void ReadValues_VerifySimdPathSelection()
    {
        // Test that different sizes actually use different SIMD paths
        var testData = SimdPathTestHelper.GenerateTestData<float>(100);
        var writer = new ArrayBufferWriter<byte>();
        foreach (var value in testData)
        {
            Float32Type.Instance.WriteValue(writer, value);
        }
        
        // Test with all SIMD enabled
        var allSimdHandler = new Float32Type();
        var sequence1 = new ReadOnlySequence<byte>(writer.WrittenMemory);
        var result1 = new float[100];
        allSimdHandler.ReadValues(ref sequence1, result1, out _);
        Assert.Equal(testData, result1);
        
        // Test with scalar only
        var scalarHandler = new Float32Type(
            SimdPathTestHelper.CreateConstrainedCapabilities(false, false, false, false, false));
        var sequence2 = new ReadOnlySequence<byte>(writer.WrittenMemory);
        var result2 = new float[100];
        scalarHandler.ReadValues(ref sequence2, result2, out _);
        Assert.Equal(testData, result2);
    }
    
    [Theory]
    [InlineData(3, "Just below SSE2 boundary")]
    [InlineData(4, "Exactly SSE2 boundary")]
    [InlineData(5, "Just above SSE2 boundary")]
    [InlineData(7, "Just below AVX2 boundary")]
    [InlineData(8, "Exactly AVX2 boundary")]
    [InlineData(9, "Just above AVX2 boundary")]
    [InlineData(15, "Just below AVX512 boundary")]
    [InlineData(16, "Exactly AVX512 boundary")]
    [InlineData(17, "Just above AVX512 boundary")]
    public void ReadValues_BoundaryConditions(int size, string description)
    {
        output.WriteLine($"Testing boundary: {description}");
        
        // Generate test data
        var expectedValues = SimdPathTestHelper.GenerateTestData<float>(size);
        
        // Serialize the data
        var writer = new ArrayBufferWriter<byte>();
        foreach (var value in expectedValues)
        {
            Float32Type.Instance.WriteValue(writer, value);
        }
        
        // Test with all SIMD enabled
        var allSimdHandler = new Float32Type();
        var sequence1 = new ReadOnlySequence<byte>(writer.WrittenMemory);
        var result1 = new float[size];
        allSimdHandler.ReadValues(ref sequence1, result1, out _);
        Assert.Equal(expectedValues, result1);
        
        // Test with scalar only
        var scalarHandler = new Float32Type(
            SimdPathTestHelper.CreateConstrainedCapabilities(false, false, false, false, false));
        var sequence2 = new ReadOnlySequence<byte>(writer.WrittenMemory);
        var result2 = new float[size];
        scalarHandler.ReadValues(ref sequence2, result2, out _);
        Assert.Equal(expectedValues, result2);
    }
    
    public static IEnumerable<object[]> GetSimdPathTestData()
        => SimdPathTestHelper.GetSimdPathTestData();
}