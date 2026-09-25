using System.Buffers.Binary;
using System.Runtime.CompilerServices;
using Opticverge.Index.Contracts;

namespace Opticverge.Index.Engine.FeedHandlers;

// CBOE PITCH 2.x — Trade (Long) message (MsgType = 0x2C)
//
// Wire format: big-endian (network byte order), same as ITCH.
// Price uses the same 1/10_000 USD scale as ITCH but is 8 bytes (vs ITCH's 4),
// accommodating higher-priced instruments (index options, futures on premium products).
// Symbol lookup is by ASCII symbol rather than a numeric locate code.
//
// Caller strips any PITCH sequenced-unit framing before calling TryParse.
// Each call receives one message starting with the 1-byte Length field.
//
// ── Trade (Long) Message (0x2C) ──────────────────────────────────────
//  Offset  Len  Type      Field
//  ------  ---  --------  -----
//       0    1  uint8     Length           total message length including this byte
//       1    1  uint8     MsgType          0x2C
//       2    6  uint48-BE TimeOffset       nanoseconds offset from unit timestamp
//       8    8  uint64-BE OrderId          (not used as sequence; PITCH lacks per-symbol seq)
//      16    1  char      SideIndicator    'B' or 'S'
//      17    4  uint32-BE Quantity
//      21    6  ASCII     Symbol           space-padded, right-justified on some venues
//      27    8  uint64-BE Price            price × 10⁻⁴ USD  →  PriceE8 = price × 10_000
//      35    4  uint32-BE ExecutionId      (informational)
//
// Length = 39 bytes (including the Length byte itself).
//
// ── PITCH vs ITCH side-by-side ────────────────────────────────────────
// Both big-endian, both 1/10_000 USD pricing. Key structural differences:
//   • Symbol field: ITCH uses a 2-byte numeric StockLocate; PITCH uses a 6-byte ASCII symbol.
//   • Price width:  ITCH 4 bytes (max ~$429,496); PITCH 8 bytes (max ~$1.8 × 10¹⁵).
//   • Sequence:     ITCH carries an explicit OrderReferenceNumber per message;
//                   PITCH has no per-symbol sequence — the unit sequence number
//                   (in the framing header) is the ordering guarantee.

public sealed class CboePitchHandler(ExchangeId exchangeId, IInstrumentMapper mapper) : IMarketFeedHandler
{
    private const byte TradeMessageType = 0x2C;
    private const int ExpectedLength = 39;

    public ExchangeId ExchangeId => exchangeId;

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public bool TryParse(ReadOnlySpan<byte> message, long receiveTimestampNanos, out MarketTick tick)
    {
        if (message.Length < ExpectedLength || message[1] != TradeMessageType)
        {
            tick = default;
            return false;
        }

        if (!mapper.TryMap(message[21..27], out var instrumentId, out var partition))
        {
            tick = default;
            return false;
        }

        var timeOffset = ReadUInt48BigEndian(message[2..]);
        var quantity = (int)BinaryPrimitives.ReadUInt32BigEndian(message[17..]);
        var pitchPrice = BinaryPrimitives.ReadUInt64BigEndian(message[27..]);
        var priceE8 = (long)pitchPrice * 10_000L;

        tick = new MarketTick(
            0, // PITCH carries no per-symbol sequence; caller uses unit sequence
            instrumentId,
            exchangeId,
            priceE8,
            quantity,
            timeOffset,
            receiveTimestampNanos,
            partition,
            TickFlags.Trade);

        return true;
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static long ReadUInt48BigEndian(ReadOnlySpan<byte> src)
    {
        return ((long)src[0] << 40) | ((long)src[1] << 32) | ((long)src[2] << 24) |
               ((long)src[3] << 16) | ((long)src[4] << 8) | src[5];
    }
}