namespace ClickHouse.Direct.Abstractions;

public interface ISimdCapabilities
{
    bool IsAvx512FSupported { get; }
    bool IsAvx512BwSupported { get; }
    bool IsAvx2Supported { get; }
    bool IsAvxSupported { get; }
    bool IsSse2Supported { get; }
    bool IsSsse3Supported { get; }
}