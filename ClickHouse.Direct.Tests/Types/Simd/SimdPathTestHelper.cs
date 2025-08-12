using ClickHouse.Direct.Abstractions;

namespace ClickHouse.Direct.Tests.Types.Simd;

public static class SimdPathTestHelper
{
    public static IEnumerable<object[]> GetSimdPathTestData()
    {
        yield return [false, false, false, false, false, "Scalar"];
        yield return [true, false, false, false, false, "SSE2"];
        yield return [true, true, false, false, false, "SSE2+SSSE3"];
        yield return [true, true, true, false, false, "SSE2+SSSE3+AVX"];
        yield return [true, true, true, true, false, "SSE2+SSSE3+AVX+AVX2"];
        yield return [true, true, true, true, true, "All SIMD"];
    }
    
    public static IEnumerable<object[]> GetSimdPathWithBwTestData()
    {
        yield return [false, false, false, false, false, false, "Scalar"];
        yield return [true, false, false, false, false, false, "SSE2"];
        yield return [true, true, false, false, false, false, "SSE2+SSSE3"];
        yield return [true, true, true, false, false, false, "SSE2+SSSE3+AVX"];
        yield return [true, true, true, true, false, false, "SSE2+SSSE3+AVX+AVX2"];
        yield return [true, true, true, true, true, false, "All except AVX512BW"];
        yield return [true, true, true, true, true, true, "All SIMD with BW"];
    }
    
    public static ISimdCapabilities CreateConstrainedCapabilities(
        bool allowSse2,
        bool allowSsse3,
        bool allowAvx,
        bool allowAvx2,
        bool allowAvx512F,
        bool allowAvx512Bw = false)
    {
        return new ConstrainedSimdCapabilities(
            DefaultSimdCapabilities.Instance,
            allowSse2: allowSse2,
            allowSsse3: allowSsse3,
            allowAvx: allowAvx,
            allowAvx2: allowAvx2,
            allowAvx512F: allowAvx512F,
            allowAvx512Bw: allowAvx512Bw
        );
    }
    
    public static int[] GetTestSizesForType(Type valueType)
    {
        return valueType.Name switch
        {
            "SByte" or "Byte" => [1, 15, 16, 17, 31, 32, 33, 63, 64, 65, 128, 256],
            "Int16" or "UInt16" => [1, 7, 8, 9, 15, 16, 17, 31, 32, 33, 64, 128],
            "Int32" or "UInt32" or "Single" => [1, 3, 4, 5, 7, 8, 9, 15, 16, 17, 32, 64],
            "Int64" or "UInt64" or "Double" => [1, 2, 3, 4, 5, 7, 8, 9, 16, 32],
            _ => [1, 10, 100]
        };
    }
    
    public static Array GenerateTestData(Type valueType, int count, int seed = 42)
    {
        var random = new Random(seed);
        var array = Array.CreateInstance(valueType, count);
        
        for (var i = 0; i < count; i++)
        {
            object value = valueType.Name switch
            {
                "SByte" => (sbyte)random.Next(sbyte.MinValue, sbyte.MaxValue + 1),
                "Byte" => (byte)random.Next(0, byte.MaxValue + 1),
                "Int16" => (short)random.Next(short.MinValue, short.MaxValue + 1),
                "UInt16" => (ushort)random.Next(0, ushort.MaxValue + 1),
                "Int32" => random.Next(int.MinValue, int.MaxValue),
                "UInt32" => (uint)(random.Next(int.MinValue, int.MaxValue) + (long)int.MaxValue),
                "Int64" => (long)random.Next(int.MinValue, int.MaxValue),
                "UInt64" => (ulong)random.Next(0, int.MaxValue),
                "Guid" => Guid.NewGuid(),
                "Single" => (float)(random.NextDouble() * 200.0 - 100.0),
                "Double" => random.NextDouble() * 200.0 - 100.0,
                _ => throw new NotSupportedException($"Type {valueType.Name} not supported")
            };
            array.SetValue(value, i);
        }
        
        return array;
    }
    
    /// <summary>
    /// Generic version of GenerateTestData for better type safety and easier usage.
    /// </summary>
    public static T[] GenerateTestData<T>(int count, int seed = 42) where T : struct
    {
        var array = GenerateTestData(typeof(T), count, seed);
        return (T[])array;
    }
    
    public static void AssertBytesEqual(byte[] expected, byte[] actual, string? message = null)
    {
        Assert.Equal(expected.Length, actual.Length);
        
        for (var i = 0; i < expected.Length; i++)
        {
            if (expected[i] != actual[i])
            {
                var context = message != null ? $"{message}: " : "";
                Assert.Fail($"{context}Byte arrays differ at index {i}. Expected: 0x{expected[i]:X2}, Actual: 0x{actual[i]:X2}");
            }
        }
    }
}