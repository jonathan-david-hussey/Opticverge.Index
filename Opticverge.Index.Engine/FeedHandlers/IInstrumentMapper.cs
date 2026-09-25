using System.Text;
using Opticverge.Index.Contracts;

namespace Opticverge.Index.Engine.FeedHandlers;

public interface IInstrumentMapper
{
    // Used by ITCH (StockLocate), CME (SecurityID), XDP (SymbolIndex).
    bool TryMap(int numericKey, out InstrumentId instrumentId, out int partition);

    // Used by PITCH (6-byte ASCII symbol field).
    bool TryMap(ReadOnlySpan<byte> symbolAscii, out InstrumentId instrumentId, out int partition);
}

public sealed class StaticInstrumentMapper : IInstrumentMapper
{
    private readonly Dictionary<int, (InstrumentId InstrumentId, int Partition)> _byId;
    private readonly Dictionary<string, (InstrumentId InstrumentId, int Partition)> _bySymbol;

    public StaticInstrumentMapper(
        Dictionary<int, (InstrumentId, int)>? byId = null,
        Dictionary<string, (InstrumentId, int)>? bySymbol = null)
    {
        _byId = byId ?? [];
        _bySymbol = bySymbol ?? [];
    }

    public bool TryMap(int numericKey, out InstrumentId instrumentId, out int partition)
    {
        if (_byId.TryGetValue(numericKey, out var e))
        {
            instrumentId = e.InstrumentId;
            partition = e.Partition;
            return true;
        }

        instrumentId = default;
        partition = 0;
        return false;
    }

    public bool TryMap(ReadOnlySpan<byte> symbolAscii, out InstrumentId instrumentId, out int partition)
    {
        var key = Encoding.ASCII.GetString(symbolAscii).TrimEnd();
        if (_bySymbol.TryGetValue(key, out var e))
        {
            instrumentId = e.InstrumentId;
            partition = e.Partition;
            return true;
        }

        instrumentId = default;
        partition = 0;
        return false;
    }
}