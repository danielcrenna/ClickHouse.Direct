namespace ClickHouse.Direct.Abstractions;

public sealed class ConstrainedSimdCapabilities(
    ISimdCapabilities actualCapabilities,
    bool allowAvx512F = true,
    bool allowAvx2 = true,
    bool allowAvx = true,
    bool allowSse2 = true,
    bool allowSsse3 = true)
    : ISimdCapabilities
{
    public static ConstrainedSimdCapabilities ScalarOnly(ISimdCapabilities actualCapabilities) =>
        new(actualCapabilities, allowAvx512F: false, allowAvx2: false, allowAvx: false, allowSse2: false, allowSsse3: false);
    
    public static ConstrainedSimdCapabilities MaxAvx2(ISimdCapabilities actualCapabilities) =>
        new(actualCapabilities, allowAvx512F: false);
    
    public static ConstrainedSimdCapabilities MaxAvx(ISimdCapabilities actualCapabilities) =>
        new(actualCapabilities, allowAvx512F: false, allowAvx2: false);
    
    public static ConstrainedSimdCapabilities MaxSse2(ISimdCapabilities actualCapabilities) =>
        new(actualCapabilities, allowAvx512F: false, allowAvx2: false, allowAvx: false);
    
    public bool IsAvx512FSupported => actualCapabilities.IsAvx512FSupported && allowAvx512F;
    public bool IsAvx2Supported => actualCapabilities.IsAvx2Supported && allowAvx2;
    public bool IsAvxSupported => actualCapabilities.IsAvxSupported && allowAvx;
    public bool IsSse2Supported => actualCapabilities.IsSse2Supported && allowSse2;
    public bool IsSsse3Supported => actualCapabilities.IsSsse3Supported && allowSsse3;
}