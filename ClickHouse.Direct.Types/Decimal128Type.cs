using System.Buffers;
using System.Buffers.Binary;
using ClickHouse.Direct.Abstractions;

namespace ClickHouse.Direct.Types;

/// <summary>
/// ClickHouse Decimal128 type handler.
///
/// https://clickhouse.com/docs/sql-reference/data-types/decimal
/// 
/// Decimal128 stores fixed-point decimal numbers with up to 38 digits total.
/// The value is stored as a 128-bit signed integer representing the unscaled value.
/// Scale parameter (0-38) determines the number of decimal places.
/// 
/// Note: This implementation uses Int128 which is available in .NET 7+
/// SIMD optimizations are limited for 128-bit values.
/// </summary>
public sealed class Decimal128Type : BaseClickHouseType<decimal>
{
    public const byte MaxPrecision = 38;
    public const byte DefaultScale = 4;
    
    // Pre-calculated scale factors for common scales (limited to decimal range)
    // For larger scales, we use dynamic calculation
    private static readonly decimal[] ScaleFactors =
    [
        1m,                                    // 0
        10m,                                   // 1
        100m,                                  // 2
        1_000m,                                // 3
        10_000m,                               // 4
        100_000m,                              // 5
        1_000_000m,                            // 6
        10_000_000m,                           // 7
        100_000_000m,                          // 8
        1_000_000_000m,                        // 9
        10_000_000_000m,                       // 10
        100_000_000_000m,                      // 11
        1_000_000_000_000m,                    // 12
        10_000_000_000_000m,                   // 13
        100_000_000_000_000m,                  // 14
        1_000_000_000_000_000m,                // 15
        10_000_000_000_000_000m,               // 16
        100_000_000_000_000_000m,              // 17
        1_000_000_000_000_000_000m,            // 18
        10_000_000_000_000_000_000m,           // 19
        100_000_000_000_000_000_000m,          // 20
        1_000_000_000_000_000_000_000m,        // 21
        10_000_000_000_000_000_000_000m,       // 22
        100_000_000_000_000_000_000_000m,      // 23
        1_000_000_000_000_000_000_000_000m,    // 24
        10_000_000_000_000_000_000_000_000m,   // 25
        100_000_000_000_000_000_000_000_000m,  // 26
        1_000_000_000_000_000_000_000_000_000m,// 27
        10_000_000_000_000_000_000_000_000_000m // 28 (max for decimal)
    ];
    
    public ISimdCapabilities SimdCapabilities { get; }
    public byte Precision { get; }
    public byte Scale { get; }
    private readonly decimal? _scaleFactor;
    private readonly Int128? _scaleFactorInt128;

    public Decimal128Type(byte precision = MaxPrecision, byte scale = DefaultScale, ISimdCapabilities? simdCapabilities = null)
    {
        if (precision > MaxPrecision)
            throw new ArgumentOutOfRangeException(nameof(precision), precision, $"Precision must be between 1 and {MaxPrecision}");
        if (scale > precision)
            throw new ArgumentOutOfRangeException(nameof(scale), scale, "Scale cannot be greater than precision");
            
        Precision = precision;
        Scale = scale;
        
        // Use pre-calculated factors for small scales, otherwise calculate
        if (scale <= 28)
        {
            _scaleFactor = ScaleFactors[scale];
        }
        else
        {
            // For scales > 28, we need to use Int128 for calculations
            _scaleFactorInt128 = CalculateScaleFactorInt128(scale);
        }
        
        SimdCapabilities = simdCapabilities ?? DefaultSimdCapabilities.Instance;
    }

    private static Int128 CalculateScaleFactorInt128(byte scale)
    {
        Int128 result = 1;
        for (var i = 0; i < scale; i++)
        {
            result *= 10;
        }
        return result;
    }

    public static Decimal128Type CreateWithScale(byte scale) => new(MaxPrecision, scale);
    public static readonly Decimal128Type Instance = new();

    public override byte ProtocolCode => 0x18;
    public override string TypeName => $"Decimal128({Precision},{Scale})";
    public override bool IsFixedLength => true;
    public override int FixedByteLength => 16; // 128 bits = 16 bytes

    public override decimal ReadValue(ref ReadOnlySequence<byte> sequence, out int bytesConsumed)
    {
        if (sequence.Length < 16)
            throw new InvalidOperationException($"Insufficient data to read Decimal128. Expected 16 bytes, got {sequence.Length}");

        Span<byte> buffer = stackalloc byte[16];
        sequence.Slice(0, 16).CopyTo(buffer);
        sequence = sequence.Slice(16);
        bytesConsumed = 16;

        // Read as two 64-bit values (little-endian)
        var low = BitConverter.IsLittleEndian
            ? BinaryPrimitives.ReadUInt64LittleEndian(buffer)
            : BinaryPrimitives.ReadUInt64BigEndian(buffer[8..]);
            
        var high = BitConverter.IsLittleEndian
            ? BinaryPrimitives.ReadInt64LittleEndian(buffer[8..])
            : BinaryPrimitives.ReadInt64BigEndian(buffer);

        var int128Value = new Int128((ulong)high, low);
        
        // Convert to decimal
        if (_scaleFactor.HasValue)
        {
            // For small scales, we can convert directly
            // Note: This may lose precision for very large values
            return (decimal)int128Value / _scaleFactor.Value;
        }
        else
        {
            // For large scales, we need more complex conversion
            // This is a simplified approach - may need refinement for edge cases
            var result = (decimal)int128Value;
            for (var i = 0; i < Scale; i++)
            {
                result /= 10m;
            }
            return result;
        }
    }

    public override int ReadValues(ref ReadOnlySequence<byte> sequence, Span<decimal> destination, out int bytesConsumed)
    {
        var valuesRead = Math.Min(destination.Length, (int)(sequence.Length / 16));
        bytesConsumed = valuesRead * 16;

        if (valuesRead == 0)
            return valuesRead;

        // Process values one by one (no SIMD for 128-bit values)
        for (var i = 0; i < valuesRead; i++)
        {
            destination[i] = ReadValue(ref sequence, out _);
        }

        return valuesRead;
    }

    public override void WriteValue(IBufferWriter<byte> writer, decimal value)
    {
        var span = writer.GetSpan(16);
        
        // Convert decimal to Int128
        Int128 unscaledValue;
        if (_scaleFactor.HasValue)
        {
            unscaledValue = (Int128)(value * _scaleFactor.Value);
        }
        else
        {
            // For large scales, multiply step by step
            var temp = value;
            for (var i = 0; i < Scale; i++)
            {
                temp *= 10m;
            }
            unscaledValue = (Int128)temp;
        }
        
        // Write as two 64-bit values (little-endian)
        var (high, low) = ((long)(unscaledValue >> 64), (ulong)unscaledValue);
        
        if (BitConverter.IsLittleEndian)
        {
            BinaryPrimitives.WriteUInt64LittleEndian(span, low);
            BinaryPrimitives.WriteInt64LittleEndian(span[8..], high);
        }
        else
        {
            BinaryPrimitives.WriteInt64BigEndian(span, high);
            BinaryPrimitives.WriteUInt64BigEndian(span[8..], low);
        }
        
        writer.Advance(16);
    }

    public override void WriteValues(IBufferWriter<byte> writer, ReadOnlySpan<decimal> values)
    {
        // Process values one by one (no SIMD for 128-bit values)
        for (var i = 0; i < values.Length; i++)
        {
            WriteValue(writer, values[i]);
        }
    }
}