using System.Buffers.Binary;
using System.Runtime.CompilerServices;
using Opticverge.Index.Contracts;

namespace Opticverge.Index.Engine.FeedHandlers;

// NASDAQ ITCH 5.0 — Trade (Non-Cross) message type 'P' (0x50)
//
// Wire format: 44 bytes, big-endian (network byte order)
// Caller strips the 2-byte SOUP/BinaryFile length prefix before calling TryParse.
//
//  Offset  Len  Type      Field
//  ------  ---  --------  -----
//       0    1  uint8     MessageType         0x50 = 'P'
//       1    2  uint16-BE StockLocate         exchange symbol index → InstrumentId via mapper
//       3    2  uint16-BE TrackingNumber      (ignored)
//       5    6  uint48-BE Timestamp           nanoseconds since midnight (exchange clock)
//      11    8  uint64-BE OrderReferenceNo    used as sequence number
//      19    1  char      Side                'B' = buy, 'S' = sell
//      20    4  uint32-BE Shares              trade quantity
//      24    8  ASCII     Stock               space-padded symbol (reference only)
//      32    4  uint32-BE Price               price × 10⁻⁴ USD  →  PriceE8 = price × 10_000
//      36    8  uint64-BE MatchNumber         (ignored)
//
// Price conversion: ITCH encodes in 1/10_000 USD; canonical E8 format is 1/100_000_000 USD.
//   PriceE8 = itchPrice × 10_000
//   Example: $150.25 → itchPrice = 1_502_500 → PriceE8 = 15_025_000_000

public sealed class NasdaqItchHandler(ExchangeId exchangeId, IInstrumentMapper mapper) : IMarketFeedHandler
{
    private const int MessageLength = 44;
    private const byte TradeMessageType = 0x50; // 'P'

    public ExchangeId ExchangeId => exchangeId;

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public bool TryParse(ReadOnlySpan<byte> message, long receiveTimestampNanos, out MarketTick tick)
    {
        if (message.Length < MessageLength || message[0] != TradeMessageType)
        {
            tick = default;
            return false;
        }

        var stockLocate = BinaryPrimitives.ReadUInt16BigEndian(message[1..]);
        if (!mapper.TryMap(stockLocate, out var instrumentId, out var partition))
        {
            tick = default;
            return false;
        }

        var exchangeTimestampNanos = ReadUInt48BigEndian(message[5..]);
        var sequence = (long)BinaryPrimitives.ReadUInt64BigEndian(message[11..]);
        var quantity = (int)BinaryPrimitives.ReadUInt32BigEndian(message[20..]);
        var itchPrice = BinaryPrimitives.ReadUInt32BigEndian(message[32..]);
        var priceE8 = itchPrice * 10_000L;

        tick = new MarketTick(
            sequence,
            instrumentId,
            exchangeId,
            priceE8,
            quantity,
            exchangeTimestampNanos,
            receiveTimestampNanos,
            partition,
            TickFlags.Trade);

        return true;
    }

    // ITCH timestamps are 48-bit big-endian integers — no standard BinaryPrimitives overload exists.
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static long ReadUInt48BigEndian(ReadOnlySpan<byte> src)
    {
        return ((long)src[0] << 40) | ((long)src[1] << 32) | ((long)src[2] << 24) |
               ((long)src[3] << 16) | ((long)src[4] << 8) | src[5];
    }
}