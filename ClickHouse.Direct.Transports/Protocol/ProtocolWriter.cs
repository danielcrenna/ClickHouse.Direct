using System.Buffers.Binary;
using System.IO.Pipelines;
using System.Text;

namespace ClickHouse.Direct.Transports.Protocol;

internal sealed class ProtocolWriter(PipeWriter writer)
{
    private readonly PipeWriter _writer = writer ?? throw new ArgumentNullException(nameof(writer));

    public void WriteByte(byte value)
    {
        var span = _writer.GetSpan(1);
        span[0] = value;
        _writer.Advance(1);
    }
    
    public void WriteVarUInt(ulong value)
    {
        while (value >= 0x80)
        {
            WriteByte((byte)(value | 0x80));
            value >>= 7;
        }
        WriteByte((byte)value);
    }
    
    public void WriteString(string value)
    {
        if (string.IsNullOrEmpty(value))
        {
            WriteVarUInt(0);
            return;
        }
        
        var bytes = Encoding.UTF8.GetBytes(value);
        WriteVarUInt((ulong)bytes.Length);
        WriteBytes(bytes);
    }
    
    public void WriteBytes(ReadOnlySpan<byte> bytes)
    {
        if (bytes.IsEmpty)
        {
            return;
        }
        
        var span = _writer.GetSpan(bytes.Length);
        bytes.CopyTo(span);
        _writer.Advance(bytes.Length);
    }
    
    public void WriteUInt64(ulong value)
    {
        var span = _writer.GetSpan(8);
        BinaryPrimitives.WriteUInt64LittleEndian(span, value);
        _writer.Advance(8);
    }
    
    public void WriteUInt32(uint value)
    {
        var span = _writer.GetSpan(4);
        BinaryPrimitives.WriteUInt32LittleEndian(span, value);
        _writer.Advance(4);
    }
    
    public void WriteBool(bool value)
    {
        WriteByte((byte)(value ? 1 : 0));
    }
    
    public async Task FlushAsync(CancellationToken cancellationToken = default)
    {
        await _writer.FlushAsync(cancellationToken);
    }
}