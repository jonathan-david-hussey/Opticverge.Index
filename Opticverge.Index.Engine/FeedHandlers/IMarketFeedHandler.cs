using Opticverge.Index.Contracts;

namespace Opticverge.Index.Engine.FeedHandlers;

public interface IMarketFeedHandler
{
    ExchangeId ExchangeId { get; }

    // Returns false for non-trade messages or unmapped instruments.
    // message: raw bytes starting at the first byte of the protocol message body
    //          (length/framing prefix already stripped by the caller).
    bool TryParse(ReadOnlySpan<byte> message, long receiveTimestampNanos, out MarketTick tick);
}