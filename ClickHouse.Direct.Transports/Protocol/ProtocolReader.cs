using System.Buffers;
using System.Buffers.Binary;
using System.IO.Pipelines;
using System.Text;

namespace ClickHouse.Direct.Transports.Protocol;

internal sealed class ProtocolReader(PipeReader reader)
{
    private readonly PipeReader _reader = reader ?? throw new ArgumentNullException(nameof(reader));

    public async Task<byte> ReadByteAsync(CancellationToken cancellationToken = default)
    {
        var result = await _reader.ReadAsync(cancellationToken);
        var buffer = result.Buffer;
        
        if (buffer.Length < 1)
        {
            if (result.IsCompleted)
            {
                throw new EndOfStreamException("Connection closed unexpectedly");
            }
            
            _reader.AdvanceTo(buffer.Start, buffer.End);
            return await ReadByteAsync(cancellationToken);
        }
        
        var value = buffer.First.Span[0];
        _reader.AdvanceTo(buffer.GetPosition(1));
        return value;
    }
    
    public async Task<ulong> ReadVarUIntAsync(CancellationToken cancellationToken = default)
    {
        ulong value = 0;
        var shift = 0;
        
        while (true)
        {
            var b = await ReadByteAsync(cancellationToken);
            value |= (ulong)(b & 0x7F) << shift;
            
            if ((b & 0x80) == 0)
            {
                return value;
            }
            
            shift += 7;
            if (shift >= 64)
            {
                throw new InvalidOperationException("VarUInt too large");
            }
        }
    }
    
    public async Task<string> ReadStringAsync(CancellationToken cancellationToken = default)
    {
        var length = await ReadVarUIntAsync(cancellationToken);
        if (length == 0)
        {
            return string.Empty;
        }
        
        if (length > int.MaxValue)
        {
            throw new InvalidOperationException($"String length {length} exceeds maximum");
        }
        
        var bytes = await ReadBytesAsync((int)length, cancellationToken);
        return Encoding.UTF8.GetString(bytes);
    }
    
    public async Task<byte[]> ReadBytesAsync(int count, CancellationToken cancellationToken = default)
    {
        var result = new byte[count];
        var totalRead = 0;
        
        while (totalRead < count)
        {
            var readResult = await _reader.ReadAsync(cancellationToken);
            var buffer = readResult.Buffer;
            
            if (buffer.IsEmpty && readResult.IsCompleted)
            {
                throw new EndOfStreamException("Connection closed unexpectedly");
            }
            
            var toRead = Math.Min(count - totalRead, (int)buffer.Length);
            buffer.Slice(0, toRead).CopyTo(result.AsSpan(totalRead));
            totalRead += toRead;
            
            _reader.AdvanceTo(buffer.GetPosition(toRead));
        }
        
        return result;
    }
    
    public async Task<ulong> ReadUInt64Async(CancellationToken cancellationToken = default)
    {
        var bytes = await ReadBytesAsync(8, cancellationToken);
        return BinaryPrimitives.ReadUInt64LittleEndian(bytes);
    }
    
    public async Task<uint> ReadUInt32Async(CancellationToken cancellationToken = default)
    {
        var bytes = await ReadBytesAsync(4, cancellationToken);
        return BinaryPrimitives.ReadUInt32LittleEndian(bytes);
    }
    
    public async Task<bool> ReadBoolAsync(CancellationToken cancellationToken = default)
    {
        var b = await ReadByteAsync(cancellationToken);
        return b != 0;
    }
    
    public async Task<ReadOnlySequence<byte>> ReadBlockDataAsync(CancellationToken cancellationToken = default)
    {
        // Read until we have a complete block
        // This is a simplified version - in production we'd need more sophisticated buffering
        var segments = new List<ReadOnlyMemory<byte>>();
        
        while (true)
        {
            var result = await _reader.ReadAsync(cancellationToken);
            var buffer = result.Buffer;
            
            if (!buffer.IsEmpty)
            {
                var memory = buffer.ToArray();
                segments.Add(memory);
                _reader.AdvanceTo(buffer.End);
            }
            
            if (result.IsCompleted)
            {
                break;
            }
        }
        
        if (segments.Count == 0)
        {
            return ReadOnlySequence<byte>.Empty;
        }
        
        if (segments.Count == 1)
        {
            return new ReadOnlySequence<byte>(segments[0]);
        }
        
        // Build multi-segment sequence
        var first = new BufferSegment(segments[0]);
        var current = first;
        
        for (var i = 1; i < segments.Count; i++)
        {
            current = current.Append(segments[i]);
        }
        
        return new ReadOnlySequence<byte>(first, 0, current, current.Memory.Length);
    }
    
    private class BufferSegment : ReadOnlySequenceSegment<byte>
    {
        public BufferSegment(ReadOnlyMemory<byte> memory)
        {
            Memory = memory;
        }
        
        public BufferSegment Append(ReadOnlyMemory<byte> memory)
        {
            var segment = new BufferSegment(memory)
            {
                RunningIndex = RunningIndex + Memory.Length
            };
            Next = segment;
            return segment;
        }
    }
}