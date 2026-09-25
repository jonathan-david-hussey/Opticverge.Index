using Opticverge.Index.Contracts;
using Opticverge.Index.Engine.FeedHandlers;

namespace Opticverge.Index.Tests;

// ═══════════════════════════════════════════════════════════════════════════════
// Feed Handler Tests
//
// Each test constructs a raw byte buffer that matches the exchange wire format,
// parses it through the appropriate handler, and asserts that every field of the
// resulting MarketTick matches the value encoded in those bytes.
//
// All four exchanges represent the same trade to make the encoding differences
// obvious by inspection:
//
//   Instrument : AAPL  (canonical InstrumentId = 1)
//   Price      : $150.25  →  PriceE8 = 15_025_000_000
//   Quantity   : 1_000 shares
//   Timestamp  : 1_000_000_000 ns (1 s since midnight / epoch)
//
// Price encoding by exchange:
//   NASDAQ ITCH  uint32-BE  1_502_500       (× 10_000, big-endian, 4 bytes)
//   CME MDP3/SBE Decimal64  mantissa=15_025 exponent=-2  (little-endian, 9 bytes)
//   NYSE XDP     uint32-LE  150_250         (× 1_000,   little-endian, 4 bytes)
//   CBOE PITCH   uint64-BE  150_250_0... wait see below
//
// The canonical PriceE8 = 15_025_000_000 in every case.
// ═══════════════════════════════════════════════════════════════════════════════

public sealed class FeedHandlerTests
{
    private const int AaplPartition = 0;
    private const long ExpectedPriceE8 = 15_025_000_000L; // $150.25 × 10^8
    private const long ExchangeTimestampNanos = 1_000_000_000L; // 1 s

    private const long ReceiveTimestampNanos = 2_000_000_000L; // 2 s (always later)
    // ── Shared test fixtures ─────────────────────────────────────────────────

    private static readonly InstrumentId AaplId = new(1);

    // ITCH StockLocate 1 → AAPL
    private static IInstrumentMapper ItchMapper()
    {
        return new StaticInstrumentMapper(
            new Dictionary<int, (InstrumentId, int)> { [1] = (AaplId, AaplPartition) });
    }

    // CME SecurityID 12_345 → AAPL
    private static IInstrumentMapper CmeMapper()
    {
        return new StaticInstrumentMapper(
            new Dictionary<int, (InstrumentId, int)> { [12_345] = (AaplId, AaplPartition) });
    }

    // XDP SymbolIndex 7 → AAPL
    private static IInstrumentMapper XdpMapper()
    {
        return new StaticInstrumentMapper(
            new Dictionary<int, (InstrumentId, int)> { [7] = (AaplId, AaplPartition) });
    }

    // PITCH ASCII symbol "AAPL  " → AAPL
    private static IInstrumentMapper PitchMapper()
    {
        return new StaticInstrumentMapper(
            bySymbol: new Dictionary<string, (InstrumentId, int)> { ["AAPL"] = (AaplId, AaplPartition) });
    }

    // ── NASDAQ ITCH 5.0 ──────────────────────────────────────────────────────

    [Fact]
    public void NasdaqItch_TradeMessage_ParsesAllFieldsCorrectly()
    {
        // ITCH 5.0 Trade (Non-Cross) 'P' — 44 bytes, big-endian
        //
        // Price encoding:  $150.25 × 10_000 = 1_502_500 = 0x0016ED24
        // Timestamp:       1_000_000_000 ns = 0x000000_3B9ACA00  (6 bytes)
        // OrderRef:        10_000          = 0x0000000000002710  (8 bytes) → sequence
        // StockLocate:     1               = 0x0001                        → mapper key
        ReadOnlySpan<byte> wire =
        [
            0x50, // MessageType  = 'P'
            0x00, 0x01, // StockLocate  = 1
            0x00, 0x00, // TrackingNumber (ignored)
            0x00, 0x00, 0x3B, 0x9A, 0xCA, 0x00, // Timestamp    = 1_000_000_000 ns
            0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x27, 0x10, // OrderRef     = 10_000
            0x42, // Side         = 'B'
            0x00, 0x00, 0x03, 0xE8, // Shares       = 1_000
            0x41, 0x41, 0x50, 0x4C, 0x20, 0x20, 0x20, 0x20, // Stock        = "AAPL    "
            0x00, 0x16, 0xED, 0x24, // Price        = 1_502_500
            0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x01 // MatchNumber  (ignored)
        ];

        var handler = new NasdaqItchHandler(new ExchangeId(1), ItchMapper());

        Assert.True(handler.TryParse(wire, ReceiveTimestampNanos, out var tick));
        Assert.Equal(AaplId, tick.InstrumentId);
        Assert.Equal(ExpectedPriceE8, tick.PriceE8);
        Assert.Equal(1_000, tick.Quantity);
        Assert.Equal(10_000L, tick.Sequence);
        Assert.Equal(ExchangeTimestampNanos, tick.ExchangeTimestampNanos);
        Assert.Equal(ReceiveTimestampNanos, tick.ReceiveTimestampNanos);
        Assert.Equal(new ExchangeId(1), tick.ExchangeId);
        Assert.True((tick.Flags & TickFlags.Trade) != 0);
    }

    [Fact]
    public void NasdaqItch_WrongMessageType_ReturnsFalse()
    {
        // 'S' = System Event, not a trade — should be ignored
        ReadOnlySpan<byte> wire = [0x53, 0x00, 0x01, 0x00, 0x00, .. new byte[39]];
        var handler = new NasdaqItchHandler(new ExchangeId(1), ItchMapper());

        Assert.False(handler.TryParse(wire, ReceiveTimestampNanos, out _));
    }

    [Fact]
    public void NasdaqItch_UnmappedStockLocate_ReturnsFalse()
    {
        // StockLocate = 99 has no mapping in the mapper
        ReadOnlySpan<byte> wire =
        [
            0x50,
            0x00, 0x63, // StockLocate = 99 (unmapped)
            .. new byte[41]
        ];
        var handler = new NasdaqItchHandler(new ExchangeId(1), ItchMapper());

        Assert.False(handler.TryParse(wire, ReceiveTimestampNanos, out _));
    }

    // ── CME MDP 3.0 / SBE ────────────────────────────────────────────────────

    [Fact]
    public void CmeMdp3_SbeDecimal64_ParsesAllFieldsCorrectly()
    {
        // MDIncrementalRefreshTrade50 (templateId=50) — all little-endian
        //
        // Price encoding: Decimal64 composite type
        //   mantissa = 15_025 = 0x3AB1 (LE: 0xB1 0x3A 0x00 ...)
        //   exponent = -2     = 0xFE   (int8)
        //   price    = 15_025 × 10^-2  = $150.25
        //   PriceE8  = 15_025 × 10^(−2+8) = 15_025 × 10^6 = 15_025_000_000
        //
        // Compare to ITCH which encodes the same price as:
        //   plain uint32 1_502_500 with implicit exponent -4
        //   PriceE8 = 1_502_500 × 10_000 = 15_025_000_000  ← same result
        //
        // SecurityID = 12_345 = 0x3039 (LE: 0x39 0x30 0x00 0x00)
        // TransactTime = 1_000_000_000 ns (LE: 0x00 0xCA 0x9A 0x3B 0x00 ...)
        ReadOnlySpan<byte> wire =
        [
            // SBE Message Header (8 bytes, LE):
            0x09, 0x00, // BlockLength = 9 (root block: TransactTime 8B + indicator 1B)
            0x32, 0x00, // TemplateId  = 50
            0x01, 0x00, // SchemaId    = 1
            0x09, 0x00, // Version     = 9

            // Root block (9 bytes, LE):
            0x00, 0xCA, 0x9A, 0x3B, 0x00, 0x00, 0x00, 0x00, // TransactTime = 1_000_000_000 ns
            0x01, // MatchEventIndicator = EndOfEvent

            // NoMDEntries group header (3 bytes, LE):
            0x1B, 0x00, // BlockLength = 27 (bytes per entry)
            0x01, // NumInGroup  = 1

            // MDEntry 0 (27 bytes, LE):
            0xB1, 0x3A, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, // mantissa     = 15_025
            0xFE, // exponent     = -2
            0x0A, 0x00, 0x00, 0x00, // MDEntrySize  = 10
            0x39, 0x30, 0x00, 0x00, // SecurityID   = 12_345
            0x01, 0x00, 0x00, 0x00, // RptSeq       = 1
            0x01, 0x00, 0x00, 0x00, // NumberOfOrders = 1
            0x00, // MDUpdateAction = New
            0x32 // MDEntryType  = '2' (Trade)
        ];

        var handler = new CmeMdp3Handler(new ExchangeId(2), CmeMapper());

        Assert.True(handler.TryParse(wire, ReceiveTimestampNanos, out var tick));
        Assert.Equal(AaplId, tick.InstrumentId);
        Assert.Equal(ExpectedPriceE8, tick.PriceE8);
        Assert.Equal(10, tick.Quantity);
        Assert.Equal(1L, tick.Sequence);
        Assert.Equal(ExchangeTimestampNanos, tick.ExchangeTimestampNanos);
        Assert.Equal(ReceiveTimestampNanos, tick.ReceiveTimestampNanos);
        Assert.Equal(new ExchangeId(2), tick.ExchangeId);
        Assert.True((tick.Flags & TickFlags.Trade) != 0);
    }

    [Fact]
    public void CmeMdp3_QuoteEntry_SkipsNonTradeEntries()
    {
        // MDEntryType = '0' (Bid quote, not a trade) — should return false
        ReadOnlySpan<byte> wire =
        [
            0x09, 0x00, 0x32, 0x00, 0x01, 0x00, 0x09, 0x00, // SBE header
            0x00, 0xCA, 0x9A, 0x3B, 0x00, 0x00, 0x00, 0x00, 0x00, // root block
            0x1B, 0x00, 0x01, // group header
            0xB1, 0x3A, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, // mantissa
            0xFE, // exponent
            0x0A, 0x00, 0x00, 0x00, // MDEntrySize
            0x39, 0x30, 0x00, 0x00, // SecurityID
            0x01, 0x00, 0x00, 0x00, // RptSeq
            0x01, 0x00, 0x00, 0x00, // NumberOfOrders
            0x00, // MDUpdateAction
            0x30 // MDEntryType = '0' (Bid)
        ];

        var handler = new CmeMdp3Handler(new ExchangeId(2), CmeMapper());
        Assert.False(handler.TryParse(wire, ReceiveTimestampNanos, out _));
    }

    [Fact]
    public void CmeMdp3_SbeDecimal64_NegativeExponentBeyondE8_HandledByDivision()
    {
        // exponent = -10 → shift = -10 + 8 = -2 → PriceE8 = mantissa / 100
        // mantissa = 1_502_500_000_000, exponent = -10 → $150.25 → PriceE8 = 15_025_000_000
        ReadOnlySpan<byte> wire =
        [
            0x09, 0x00, 0x32, 0x00, 0x01, 0x00, 0x09, 0x00,
            0x00, 0xCA, 0x9A, 0x3B, 0x00, 0x00, 0x00, 0x00, 0x00,
            0x1B, 0x00, 0x01,
            // mantissa = 1_502_500_000_000 = 0x0000_015D_D3FA_9100 (LE)
            0x00, 0x91, 0xFA, 0xD3, 0x5D, 0x01, 0x00, 0x00,
            0xF6, // exponent = -10 (0xF6)
            0x0A, 0x00, 0x00, 0x00,
            0x39, 0x30, 0x00, 0x00,
            0x01, 0x00, 0x00, 0x00,
            0x01, 0x00, 0x00, 0x00,
            0x00,
            0x32
        ];

        var handler = new CmeMdp3Handler(new ExchangeId(2), CmeMapper());
        Assert.True(handler.TryParse(wire, ReceiveTimestampNanos, out var tick));
        Assert.Equal(ExpectedPriceE8, tick.PriceE8);
    }

    // ── NYSE XDP ─────────────────────────────────────────────────────────────

    [Fact]
    public void NyseXdp_TradeSummary_ParsesAllFieldsCorrectly()
    {
        // XDP Trade Summary (MsgType = 220) — all little-endian
        //
        // Price encoding: 1/1_000 USD (millicents)
        //   $150.25 × 1_000 = 150_250 = 0x000024AEA (LE: 0xEA 0x4A 0x02 0x00)
        //   PriceE8 = 150_250 × 100_000 = 15_025_000_000
        //
        // Compare to ITCH: 1_502_500 × 10_000 = 15_025_000_000  ← same PriceE8
        // Multipliers differ (× 100_000 vs × 10_000) because granularity differs
        // (XDP: 0.001, ITCH: 0.0001).
        //
        // SourceTime: 1_000_000_000 ns = 0x3B9ACA00 (LE: 0x00 0xCA 0x9A 0x3B)
        // SymbolIndex: 7 (LE: 0x07 0x00 0x00 0x00)
        ReadOnlySpan<byte> wire =
        [
            // XDP Message Header (4 bytes, LE):
            0x2A, 0x00, // MsgSize  = 42
            0xDC, 0x00, // MsgType  = 220

            // Trade Summary body (38 bytes, LE):
            0x00, 0xCA, 0x9A, 0x3B, // SourceTime      = 1_000_000_000 ns
            0x00, 0x00, // SourceTimeMicros = 0
            0x07, 0x00, 0x00, 0x00, // SymbolIndex     = 7
            0x01, 0x00, 0x00, 0x00, // SymbolSeqNum    = 1  → sequence
            0x01, 0x00, 0x00, 0x00, // TradeID         = 1
            0x00, 0x00, 0x00, 0x00, // SourceSeqNum    (ignored)
            0xE8, 0x03, 0x00, 0x00, // TradeVolume     = 1_000
            0xEA, 0x4A, 0x02, 0x00, // TradePrice      = 150_250  (= $150.25 × 1_000)
            0x00, // TradeCondition1
            0x00, // TradeCondition2
            0x00, // TradeCondition3
            0x00, // TradeCondition4
            0x01, // SourceSessionID
            0x00, // TradeReportType = regular
            0x00, 0x00 // SellerDays
        ];

        var handler = new NyseXdpHandler(new ExchangeId(3), XdpMapper());

        Assert.True(handler.TryParse(wire, ReceiveTimestampNanos, out var tick));
        Assert.Equal(AaplId, tick.InstrumentId);
        Assert.Equal(ExpectedPriceE8, tick.PriceE8);
        Assert.Equal(1_000, tick.Quantity);
        Assert.Equal(1L, tick.Sequence);
        Assert.Equal(ExchangeTimestampNanos, tick.ExchangeTimestampNanos);
        Assert.Equal(ReceiveTimestampNanos, tick.ReceiveTimestampNanos);
        Assert.Equal(new ExchangeId(3), tick.ExchangeId);
        Assert.True((tick.Flags & TickFlags.Trade) != 0);
    }

    [Fact]
    public void NyseXdp_WrongMsgType_ReturnsFalse()
    {
        ReadOnlySpan<byte> wire = [0x2A, 0x00, 0xAB, 0x00, .. new byte[38]]; // MsgType != 220
        var handler = new NyseXdpHandler(new ExchangeId(3), XdpMapper());

        Assert.False(handler.TryParse(wire, ReceiveTimestampNanos, out _));
    }

    // ── CBOE PITCH ───────────────────────────────────────────────────────────

    [Fact]
    public void CboePitch_TradeLong_ParsesAllFieldsCorrectly()
    {
        // PITCH Trade (Long) 0x2C — big-endian (same as ITCH)
        //
        // Price encoding: 1/10_000 USD, uint64 (8 bytes vs ITCH's 4)
        //   $150.25 × 10_000 = 1_502_500 = 0x0000_0000_0016_ED24
        //   (LE: 0x24 0xED 0x16 0x00 0x00 0x00 0x00 0x00 — but PITCH is big-endian)
        //   PriceE8 = 1_502_500 × 10_000 = 15_025_000_000
        //
        // PITCH symbol field: 6 ASCII bytes "AAPL  " (space-padded)
        // TrimEnd() in the mapper strips the trailing spaces for the lookup key.
        //
        // Note: PITCH carries no per-symbol sequence number.
        // Sequence is set to 0; the index engine treats 0 as unsequenced (no gap detection).
        ReadOnlySpan<byte> wire =
        [
            0x27, // Length       = 39
            0x2C, // MsgType      = 0x2C (Trade Long)
            0x00, 0x00, 0x3B, 0x9A, 0xCA, 0x00, // TimeOffset   = 1_000_000_000 ns
            0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x01, // OrderId      = 1 (ignored)
            0x42, // Side         = 'B'
            0x00, 0x00, 0x03, 0xE8, // Quantity     = 1_000
            0x41, 0x41, 0x50, 0x4C, 0x20, 0x20, // Symbol       = "AAPL  "
            0x00, 0x00, 0x00, 0x00, 0x00, 0x16, 0xED, 0x24, // Price        = 1_502_500
            0x00, 0x00, 0x00, 0x01 // ExecutionId  (ignored)
        ];

        var handler = new CboePitchHandler(new ExchangeId(4), PitchMapper());

        Assert.True(handler.TryParse(wire, ReceiveTimestampNanos, out var tick));
        Assert.Equal(AaplId, tick.InstrumentId);
        Assert.Equal(ExpectedPriceE8, tick.PriceE8);
        Assert.Equal(1_000, tick.Quantity);
        Assert.Equal(0L, tick.Sequence); // PITCH has no per-symbol sequence
        Assert.Equal(ExchangeTimestampNanos, tick.ExchangeTimestampNanos);
        Assert.Equal(ReceiveTimestampNanos, tick.ReceiveTimestampNanos);
        Assert.Equal(new ExchangeId(4), tick.ExchangeId);
        Assert.True((tick.Flags & TickFlags.Trade) != 0);
    }

    [Fact]
    public void CboePitch_WrongMsgType_ReturnsFalse()
    {
        ReadOnlySpan<byte> wire = [0x27, 0x21, .. new byte[37]]; // 0x21 = Add Order, not trade
        var handler = new CboePitchHandler(new ExchangeId(4), PitchMapper());

        Assert.False(handler.TryParse(wire, ReceiveTimestampNanos, out _));
    }

    [Fact]
    public void CboePitch_UnmappedSymbol_ReturnsFalse()
    {
        ReadOnlySpan<byte> wire =
        [
            0x27, 0x2C,
            0x00, 0x00, 0x3B, 0x9A, 0xCA, 0x00,
            0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x01,
            0x42,
            0x00, 0x00, 0x03, 0xE8,
            0x4D, 0x53, 0x46, 0x54, 0x20, 0x20, // Symbol = "MSFT  " (not in mapper)
            0x00, 0x00, 0x00, 0x00, 0x00, 0x16, 0xED, 0x24,
            0x00, 0x00, 0x00, 0x01
        ];
        var handler = new CboePitchHandler(new ExchangeId(4), PitchMapper());

        Assert.False(handler.TryParse(wire, ReceiveTimestampNanos, out _));
    }

    // ── Cross-protocol invariant ──────────────────────────────────────────────

    [Fact]
    public void AllHandlers_SameTrade_ProduceIdenticalPriceE8()
    {
        // The four wire representations of $150.25 are all radically different,
        // but every handler must produce the same canonical PriceE8.

        var itchWire = new byte[]
        {
            0x50, 0x00, 0x01, 0x00, 0x00,
            0x00, 0x00, 0x3B, 0x9A, 0xCA, 0x00,
            0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x27, 0x10,
            0x42, 0x00, 0x00, 0x03, 0xE8,
            0x41, 0x41, 0x50, 0x4C, 0x20, 0x20, 0x20, 0x20,
            0x00, 0x16, 0xED, 0x24,
            0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x01
        };

        var cmeWire = new byte[]
        {
            0x09, 0x00, 0x32, 0x00, 0x01, 0x00, 0x09, 0x00,
            0x00, 0xCA, 0x9A, 0x3B, 0x00, 0x00, 0x00, 0x00, 0x01,
            0x1B, 0x00, 0x01,
            0xB1, 0x3A, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
            0xFE,
            0x0A, 0x00, 0x00, 0x00,
            0x39, 0x30, 0x00, 0x00,
            0x01, 0x00, 0x00, 0x00,
            0x01, 0x00, 0x00, 0x00,
            0x00, 0x32
        };

        var xdpWire = new byte[]
        {
            0x2A, 0x00, 0xDC, 0x00,
            0x00, 0xCA, 0x9A, 0x3B,
            0x00, 0x00,
            0x07, 0x00, 0x00, 0x00,
            0x01, 0x00, 0x00, 0x00,
            0x01, 0x00, 0x00, 0x00,
            0x00, 0x00, 0x00, 0x00,
            0xE8, 0x03, 0x00, 0x00,
            0xEA, 0x4A, 0x02, 0x00,
            0x00, 0x00, 0x00, 0x00, 0x01, 0x00, 0x00, 0x00
        };

        var pitchWire = new byte[]
        {
            0x27, 0x2C,
            0x00, 0x00, 0x3B, 0x9A, 0xCA, 0x00,
            0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x01,
            0x42,
            0x00, 0x00, 0x03, 0xE8,
            0x41, 0x41, 0x50, 0x4C, 0x20, 0x20,
            0x00, 0x00, 0x00, 0x00, 0x00, 0x16, 0xED, 0x24,
            0x00, 0x00, 0x00, 0x01
        };

        var rt = ReceiveTimestampNanos;
        new NasdaqItchHandler(new ExchangeId(1), ItchMapper()).TryParse(itchWire, rt, out var itchTick);
        new CmeMdp3Handler(new ExchangeId(2), CmeMapper()).TryParse(cmeWire, rt, out var cmeTick);
        new NyseXdpHandler(new ExchangeId(3), XdpMapper()).TryParse(xdpWire, rt, out var xdpTick);
        new CboePitchHandler(new ExchangeId(4), PitchMapper()).TryParse(pitchWire, rt, out var pitchTick);

        Assert.Equal(ExpectedPriceE8, itchTick.PriceE8);
        Assert.Equal(ExpectedPriceE8, cmeTick.PriceE8);
        Assert.Equal(ExpectedPriceE8, xdpTick.PriceE8);
        Assert.Equal(ExpectedPriceE8, pitchTick.PriceE8);
    }
}