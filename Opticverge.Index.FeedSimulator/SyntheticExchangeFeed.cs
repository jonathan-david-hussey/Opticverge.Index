using System.Runtime.CompilerServices;
using Opticverge.Index.Contracts;
using Opticverge.Index.Engine;

namespace Opticverge.Index.FeedSimulator;

public sealed class SyntheticExchangeFeed(SyntheticFeedOptions options)
{
    private readonly SyntheticFeedOptions _options = options;

    public async IAsyncEnumerable<MarketTick> ReadTicksAsync(
        [EnumeratorCancellation] CancellationToken cancellationToken = default)
    {
        // Per-instrument sequences so gap detection isn't fooled by the global ordering.
        var sequences = new long[_options.Instruments + 1];
        var random = new XorShift64(_options.Seed);
        var nextYield = _options.BurstSize;

        while (!cancellationToken.IsCancellationRequested)
        {
            var instrument = new InstrumentId((int)(random.NextUInt32() % (uint)_options.Instruments) + 1);
            var exchange = new ExchangeId((short)(random.NextUInt32() % (uint)_options.Exchanges + 1));
            var price = 50_000_000L + random.NextUInt32() % 20_000_000_000;
            var now = MonotonicClock.TimestampNanos();

            yield return new MarketTick(
                ++sequences[instrument.Value],
                instrument,
                exchange,
                price,
                (int)(random.NextUInt32() % 5_000) + 1,
                now - 1_000,
                now,
                PartitionRouter.ForInstrument(instrument, _options.Partitions),
                TickFlags.Trade | TickFlags.Synthetic);

            nextYield--;
            if (nextYield > 0) continue;

            nextYield = _options.BurstSize;
            if (_options.MessagesPerSecond <= 0)
            {
                await Task.Yield();
                continue;
            }

            var delayMicroseconds = Math.Max(0, _options.BurstSize * 1_000_000 / _options.MessagesPerSecond);
            if (delayMicroseconds >= 1_000)
                await Task.Delay(TimeSpan.FromMicroseconds(delayMicroseconds), cancellationToken);
            else
                await Task.Yield();
        }
    }

    private struct XorShift64(long seed)
    {
        private ulong _state = seed == 0 ? 0x9E3779B97F4A7C15UL : (ulong)seed;

        public uint NextUInt32()
        {
            var x = _state;
            x ^= x << 13;
            x ^= x >> 7;
            x ^= x << 17;
            _state = x;
            return (uint)(x >> 32);
        }
    }
}