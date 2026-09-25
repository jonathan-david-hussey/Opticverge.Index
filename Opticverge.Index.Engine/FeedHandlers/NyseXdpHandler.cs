using System.Buffers.Binary;
using System.Runtime.CompilerServices;
using Opticverge.Index.Contracts;

namespace Opticverge.Index.Engine.FeedHandlers;

// NYSE XDP Integrated Feed — Trade Summary message (MsgType = 220 / 0x00DC)
//
// Wire format: little-endian throughout (unlike ITCH which is big-endian).
// Caller strips the 16-byte XDP packet header before calling TryParse.
// Each call receives one complete XDP message starting with its 4-byte message header.
//
// ── XDP Message Header (4 bytes, LE) ──────────────────────────────────
//  Offset  Len  Type       Field
//  ------  ---  ---------  -----
//       0    2  uint16-LE  MsgSize          total message size in bytes
//       2    2  uint16-LE  MsgType          220 = Trade Summary
//
// ── Trade Summary Body (LE) ───────────────────────────────────────────
//       4    4  uint32-LE  SourceTime       nanoseconds within the packet second
//       8    2  uint16-LE  SourceTimeMicros (sub-microsecond refinement, often 0)
//      10    4  uint32-LE  SymbolIndex      → InstrumentId via mapper
//      14    4  uint32-LE  SymbolSeqNum     per-symbol sequence number
//      18    4  uint32-LE  TradeID
//      22    4  uint32-LE  SourceSeqNum     (informational)
//      26    4  uint32-LE  TradeVolume      quantity
//      30    4  uint32-LE  TradePrice       price × 10⁻³ USD  →  PriceE8 = price × 100_000
//      34    1  uint8      TradeCondition1
//      35    1  uint8      TradeCondition2
//      36    1  uint8      TradeCondition3
//      37    1  uint8      TradeCondition4
//      38    1  uint8      SourceSessionID
//      39    1  uint8      TradeReportType  (0 = regular trade)
//      40    2  uint16-LE  SellerDays       (for short sales)
//
// ── Price encoding difference vs ITCH ────────────────────────────────
// XDP encodes in 1/1_000 USD (millicents), ITCH in 1/10_000 USD (tenth-millicents).
// The canonical E8 format is 1/100_000_000 USD.
//
//   XDP:   PriceE8 = xdpPrice × 100_000
//   ITCH:  PriceE8 = itchPrice × 10_000
//
// Example: $150.25
//   XDP:  xdpPrice = 150_250  → PriceE8 = 15_025_000_000
//   ITCH: itchPrice = 1_502_500 → PriceE8 = 15_025_000_000
//
// Same canonical result, different wire representation. The handler absorbs the
// multiplier difference; the index engine sees identical PriceE8 regardless of source.

public sealed class NyseXdpHandler(ExchangeId exchangeId, IInstrumentMapper mapper) : IMarketFeedHandler
{
    private const int MessageHeaderLength = 4;
    private const ushort TradeSummaryMsgType = 220;
    private const int MinMessageLength = 42; // header + body

    public ExchangeId ExchangeId => exchangeId;

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public bool TryParse(ReadOnlySpan<byte> message, long receiveTimestampNanos, out MarketTick tick)
    {
        if (message.Length < MinMessageLength)
        {
            tick = default;
            return false;
        }

        var msgType = BinaryPrimitives.ReadUInt16LittleEndian(message[2..]);
        if (msgType != TradeSummaryMsgType)
        {
            tick = default;
            return false;
        }

        var sourceTimeNanos = (long)BinaryPrimitives.ReadUInt32LittleEndian(message[4..]);
        var symbolIndex = (int)BinaryPrimitives.ReadUInt32LittleEndian(message[10..]);
        var symbolSeqNum = (long)BinaryPrimitives.ReadUInt32LittleEndian(message[14..]);
        var quantity = (int)BinaryPrimitives.ReadUInt32LittleEndian(message[26..]);
        var xdpPrice = BinaryPrimitives.ReadUInt32LittleEndian(message[30..]);

        if (!mapper.TryMap(symbolIndex, out var instrumentId, out var partition))
        {
            tick = default;
            return false;
        }

        // 1/1_000 USD → 1/100_000_000 USD
        var priceE8 = xdpPrice * 100_000L;

        tick = new MarketTick(
            symbolSeqNum,
            instrumentId,
            exchangeId,
            priceE8,
            quantity,
            sourceTimeNanos,
            receiveTimestampNanos,
            partition,
            TickFlags.Trade);

        return true;
    }
}