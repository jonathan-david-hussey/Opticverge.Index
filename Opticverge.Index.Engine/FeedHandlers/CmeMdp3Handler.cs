using System.Buffers.Binary;
using System.Runtime.CompilerServices;
using Opticverge.Index.Contracts;

namespace Opticverge.Index.Engine.FeedHandlers;

// CME MDP 3.0 — MDIncrementalRefreshTrade50 (templateId = 50)
//
// Encoded with SBE (Simple Binary Encoding). Everything is little-endian.
// Caller strips the 12-byte CME packet header before calling TryParse.
//
// ── SBE Message Header (8 bytes, LE) ──────────────────────────────────
//  Offset  Len  Type       Field
//  ------  ---  ---------  -----
//       0    2  uint16-LE  BlockLength      size of root block (bytes, excl. groups)
//       2    2  uint16-LE  TemplateId       50 = MDIncrementalRefreshTrade50
//       4    2  uint16-LE  SchemaId         1
//       6    2  uint16-LE  Version
//
// ── Root Block (BlockLength bytes, LE) ────────────────────────────────
//       8    8  uint64-LE  TransactTime     UTC nanoseconds since epoch
//      16    1  uint8      MatchEventIndicator
//      (remaining pad bytes to fill BlockLength)
//
// ── NoMDEntries Repeating Group Header (3 bytes, LE) ──────────────────
//       *    2  uint16-LE  BlockLength      bytes per repeating entry
//       *    1  uint8      NumInGroup
//
// ── Each MDEntry (BlockLength bytes, LE) ──────────────────────────────
//       *    8  int64-LE   MDEntryPx.mantissa
//       *    1  int8       MDEntryPx.exponent
//       *    4  int32-LE   MDEntrySize      trade quantity
//       *    4  int32-LE   SecurityID       → InstrumentId via mapper
//       *    4  uint32-LE  RptSeq           per-instrument sequence number
//       *    4  int32-LE   NumberOfOrders   (informational)
//       *    1  uint8      MDUpdateAction   0 = New, 1 = Change, 2 = Delete
//       *    1  char       MDEntryType      0x32 = '2' (Trade)
//
// ── SBE Decimal64 vs raw integer ──────────────────────────────────────
// ITCH/PITCH encode price as a plain uint32/uint64 with a fixed implicit exponent.
// SBE Decimal64 makes the exponent explicit: price = mantissa × 10^exponent.
// This allows CME to represent prices across asset classes (equities, futures,
// options, FX) without separate codec logic per product — the same composite type
// works whether the price is $0.0001 or $50,000.
//
// PriceE8 conversion:
//   PriceE8 = mantissa × 10^(exponent + 8)
//   Example: mantissa = 15_025, exponent = -2 → $150.25 → PriceE8 = 15_025_000_000
//   vs ITCH:  itchPrice = 1_502_500 (= $150.25 × 10_000)  → PriceE8 = 15_025_000_000

public sealed class CmeMdp3Handler(ExchangeId exchangeId, IInstrumentMapper mapper) : IMarketFeedHandler
{
    private const int SbeHeaderLength = 8;
    private const ushort TemplateIdTrade50 = 50;
    private const byte MdEntryTypeTrade = 0x32; // '2'

    // Lookup table avoids floating-point in the hot path.
    // Index = shift = exponent + 8, range [0..8].
    private static readonly long[] Pow10 =
        [1L, 10L, 100L, 1_000L, 10_000L, 100_000L, 1_000_000L, 10_000_000L, 100_000_000L];

    public ExchangeId ExchangeId => exchangeId;

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public bool TryParse(ReadOnlySpan<byte> message, long receiveTimestampNanos, out MarketTick tick)
    {
        if (message.Length < SbeHeaderLength + 9 + 3) // header + min root block + group header
        {
            tick = default;
            return false;
        }

        var blockLength = BinaryPrimitives.ReadUInt16LittleEndian(message);
        var templateId = BinaryPrimitives.ReadUInt16LittleEndian(message[2..]);

        if (templateId != TemplateIdTrade50)
        {
            tick = default;
            return false;
        }

        // Root block starts immediately after SBE header.
        var root = message[SbeHeaderLength..];
        var transactTime = (long)BinaryPrimitives.ReadUInt64LittleEndian(root);

        // NoMDEntries group header follows the root block.
        var groupOffset = SbeHeaderLength + blockLength;
        var entryBlockLength = BinaryPrimitives.ReadUInt16LittleEndian(message[groupOffset..]);
        var numEntries = message[groupOffset + 2];

        var entryStart = groupOffset + 3;

        for (var i = 0; i < numEntries; i++)
        {
            var entry = message[(entryStart + i * entryBlockLength)..];

            var mdEntryType = entry[26]; // MDEntryType at fixed offset 26 within each entry
            if (mdEntryType != MdEntryTypeTrade)
                continue;

            var mantissa = BinaryPrimitives.ReadInt64LittleEndian(entry);
            var exponent = (sbyte)entry[8];
            var quantity = BinaryPrimitives.ReadInt32LittleEndian(entry[9..]);
            var securityId = BinaryPrimitives.ReadInt32LittleEndian(entry[13..]);
            var rptSeq = (long)BinaryPrimitives.ReadUInt32LittleEndian(entry[17..]);

            if (!mapper.TryMap(securityId, out var instrumentId, out var partition))
                continue;

            var priceE8 = SbeDecimal64ToPriceE8(mantissa, exponent);

            tick = new MarketTick(
                rptSeq,
                instrumentId,
                exchangeId,
                priceE8,
                quantity,
                transactTime,
                receiveTimestampNanos,
                partition,
                TickFlags.Trade);

            return true; // return first trade entry; caller can loop for multi-entry messages
        }

        tick = default;
        return false;
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static long SbeDecimal64ToPriceE8(long mantissa, sbyte exponent)
    {
        // PriceE8 = mantissa × 10^(exponent + 8)
        var shift = exponent + 8;
        return shift switch
        {
            > 0 and <= 8 => mantissa * Pow10[shift],
            0 => mantissa,
            _ => mantissa / Pow10[-shift] // exponent < -8 (unusual)
        };
    }
}