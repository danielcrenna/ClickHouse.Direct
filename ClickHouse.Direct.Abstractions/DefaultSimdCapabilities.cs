using System.Runtime.Intrinsics.X86;

namespace ClickHouse.Direct.Abstractions;

public sealed class DefaultSimdCapabilities : ISimdCapabilities
{
    public static readonly DefaultSimdCapabilities Instance = new();
    
    private DefaultSimdCapabilities() { }
    
    public bool IsAvx512FSupported => Avx512F.IsSupported;
    public bool IsAvx512BwSupported => Avx512BW.IsSupported;
    public bool IsAvx2Supported => Avx2.IsSupported;
    public bool IsAvxSupported => Avx.IsSupported;
    public bool IsSse2Supported => Sse2.IsSupported;
    public bool IsSsse3Supported => Ssse3.IsSupported;
}