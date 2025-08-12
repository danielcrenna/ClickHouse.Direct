using System.Buffers;
using ClickHouse.Direct.Abstractions;
using ClickHouse.Direct.Types;
using Xunit.Abstractions;

namespace ClickHouse.Direct.Tests.Types.Simd;

public class UInt16TypeSimdTests(ITestOutputHelper output)
{
    [Theory]
    [MemberData(nameof(GetSimdPathWithBwTestData))]
    public void ReadValues_AllSimdPaths_ProduceSameResults(
        bool sse2, bool ssse3, bool avx, bool avx2, bool avx512F, bool avx512Bw, string description)
    {
        output.WriteLine($"Testing read with {description}");
        
        // Test various sizes to hit different SIMD paths
        var testSizes = SimdPathTestHelper.GetTestSizesForType(typeof(ushort));
        
        foreach (var size in testSizes)
        {
            output.WriteLine($"  Size: {size}");
            
            var expectedValues = SimdPathTestHelper.GenerateTestData<ushort>(size);
            
            // Serialize the data
            var writer = new ArrayBufferWriter<byte>();
            foreach (var value in expectedValues)
            {
                UInt16Type.Instance.WriteValue(writer, value);
            }
            
            // Create type handler with constrained capabilities
            var capabilities = SimdPathTestHelper.CreateConstrainedCapabilities(
                sse2, ssse3, avx, avx2, avx512F, avx512Bw);
            var typeHandler = new UInt16Type(capabilities);
            
            // Read values back
            var sequence = new ReadOnlySequence<byte>(writer.WrittenMemory);
            var actualValues = new ushort[size];
            var itemsRead = typeHandler.ReadValues(ref sequence, actualValues, out var bytesConsumed);
            
            // Verify
            Assert.Equal(size, itemsRead);
            Assert.Equal(size * sizeof(ushort), bytesConsumed);
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
        var testSizes = SimdPathTestHelper.GetTestSizesForType(typeof(ushort));
        
        foreach (var size in testSizes)
        {
            output.WriteLine($"  Size: {size}");
            
            var values = SimdPathTestHelper.GenerateTestData<ushort>(size);
            
            // Create type handler with constrained capabilities
            var capabilities = SimdPathTestHelper.CreateConstrainedCapabilities(
                sse2, ssse3, avx, avx2, avx512F, avx512Bw);
            var typeHandler = new UInt16Type(capabilities);
            
            // Write values
            var writer = new ArrayBufferWriter<byte>();
            typeHandler.WriteValues(writer, values);
            
            // Write expected values with scalar method for comparison
            var expectedWriter = new ArrayBufferWriter<byte>();
            var scalarCapabilities = SimdPathTestHelper.CreateConstrainedCapabilities(
                false, false, false, false, false);
            var scalarHandler = new UInt16Type(scalarCapabilities);
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
        var testSizes = new[] { 9, 17, 33 }; // Odd sizes to ensure partial vectors
        
        foreach (var size in testSizes)
        {
            output.WriteLine($"  Size: {size}");
            
            var expectedValues = SimdPathTestHelper.GenerateTestData<ushort>(size);
            
            // Serialize the data
            var writer = new ArrayBufferWriter<byte>();
            foreach (var value in expectedValues)
            {
                UInt16Type.Instance.WriteValue(writer, value);
            }
            
            // Create fragmented sequence (each ushort in separate segment)
            var bytes = writer.WrittenMemory.ToArray();
            ReadOnlySequence<byte> sequence;
            
            if (size == 1)
            {
                sequence = new ReadOnlySequence<byte>(bytes);
            }
            else
            {
                // Create a fragmented sequence with each ushort in a separate segment
                var firstSegment = new BufferSegment(new Memory<byte>(bytes, 0, sizeof(ushort)));
                var lastSegment = firstSegment;
                
                for (var i = 1; i < size; i++)
                {
                    var nextSegment = new BufferSegment(
                        new Memory<byte>(bytes, i * sizeof(ushort), sizeof(ushort)));
                    lastSegment.Append(nextSegment);
                    lastSegment = nextSegment;
                }
                
                sequence = new ReadOnlySequence<byte>(firstSegment, 0, lastSegment, sizeof(ushort));
            }
            
            // Create type handler with constrained capabilities
            var capabilities = SimdPathTestHelper.CreateConstrainedCapabilities(
                sse2, ssse3, avx, avx2, avx512F, avx512Bw);
            var typeHandler = new UInt16Type(capabilities);
            
            // Read values back
            var actualValues = new ushort[size];
            var itemsRead = typeHandler.ReadValues(ref sequence, actualValues, out var bytesConsumed);
            
            // Verify
            Assert.Equal(size, itemsRead);
            Assert.Equal(size * sizeof(ushort), bytesConsumed);
            Assert.Equal(expectedValues, actualValues);
        }
    }
    
    [Fact]
    public void ReadValues_VerifySimdPathSelection()
    {
        // This test verifies that the correct SIMD path is selected based on capabilities
        var testData = SimdPathTestHelper.GenerateTestData<ushort>(100);
        
        var writer = new ArrayBufferWriter<byte>();
        foreach (var value in testData)
        {
            UInt16Type.Instance.WriteValue(writer, value);
        }
        
        // Test AVX512BW path (processes 32 values at once)
        if (DefaultSimdCapabilities.Instance.IsAvx512BwSupported)
        {
            output.WriteLine("Testing AVX512BW path");
            var avx512BwHandler = new UInt16Type(
                SimdPathTestHelper.CreateConstrainedCapabilities(true, true, true, true, true, true));
            var sequence = new ReadOnlySequence<byte>(writer.WrittenMemory);
            var result = new ushort[100];
            avx512BwHandler.ReadValues(ref sequence, result, out _);
            Assert.Equal(testData, result);
        }
        
        // Test AVX2 path (processes 16 values at once)
        if (DefaultSimdCapabilities.Instance.IsAvx2Supported)
        {
            output.WriteLine("Testing AVX2 path");
            var avx2Handler = new UInt16Type(
                SimdPathTestHelper.CreateConstrainedCapabilities(true, true, true, true, false));
            var sequence = new ReadOnlySequence<byte>(writer.WrittenMemory);
            var result = new ushort[100];
            avx2Handler.ReadValues(ref sequence, result, out _);
            Assert.Equal(testData, result);
        }
        
        // Test SSE2 path (processes 8 values at once)
        if (DefaultSimdCapabilities.Instance.IsSse2Supported)
        {
            output.WriteLine("Testing SSE2 path");
            var sse2Handler = new UInt16Type(
                SimdPathTestHelper.CreateConstrainedCapabilities(true, false, false, false, false));
            var sequence = new ReadOnlySequence<byte>(writer.WrittenMemory);
            var result = new ushort[100];
            sse2Handler.ReadValues(ref sequence, result, out _);
            Assert.Equal(testData, result);
        }
        
        // Test scalar path
        {
            output.WriteLine("Testing scalar path");
            var scalarHandler = new UInt16Type(
                SimdPathTestHelper.CreateConstrainedCapabilities(false, false, false, false, false));
            var sequence = new ReadOnlySequence<byte>(writer.WrittenMemory);
            var result = new ushort[100];
            scalarHandler.ReadValues(ref sequence, result, out _);
            Assert.Equal(testData, result);
        }
    }
    
    [Theory]
    [InlineData(7, "Just below SSE2 boundary")]
    [InlineData(8, "Exactly SSE2 boundary")]
    [InlineData(9, "Just above SSE2 boundary")]
    [InlineData(15, "Just below AVX2 boundary")]
    [InlineData(16, "Exactly AVX2 boundary")]
    [InlineData(17, "Just above AVX2 boundary")]
    [InlineData(31, "Just below AVX512BW boundary")]
    [InlineData(32, "Exactly AVX512BW boundary")]
    [InlineData(33, "Just above AVX512BW boundary")]
    public void ReadValues_BoundaryConditions(int size, string description)
    {
        output.WriteLine($"Testing boundary: {description}");
        
        // Generate test data
        var expectedValues = SimdPathTestHelper.GenerateTestData<ushort>(size);
        
        // Serialize the data
        var writer = new ArrayBufferWriter<byte>();
        foreach (var value in expectedValues)
        {
            UInt16Type.Instance.WriteValue(writer, value);
        }
        
        // Test with all SIMD enabled
        var allSimdHandler = new UInt16Type();
        var sequence1 = new ReadOnlySequence<byte>(writer.WrittenMemory);
        var result1 = new ushort[size];
        allSimdHandler.ReadValues(ref sequence1, result1, out _);
        Assert.Equal(expectedValues, result1);
        
        // Test with scalar only
        var scalarHandler = new UInt16Type(
            SimdPathTestHelper.CreateConstrainedCapabilities(false, false, false, false, false));
        var sequence2 = new ReadOnlySequence<byte>(writer.WrittenMemory);
        var result2 = new ushort[size];
        scalarHandler.ReadValues(ref sequence2, result2, out _);
        Assert.Equal(expectedValues, result2);
    }
    
    public static IEnumerable<object[]> GetSimdPathWithBwTestData()
        => SimdPathTestHelper.GetSimdPathWithBwTestData();
}