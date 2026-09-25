namespace Opticverge.Index.Engine;

public static class PriceNormalizer
{
    public const long Scale = 100_000_000L;

    public static long ToScaledPrice(decimal price)
    {
        return decimal.ToInt64(decimal.Round(price * Scale, 0, MidpointRounding.AwayFromZero));
    }

    public static decimal ToDecimal(long priceE8)
    {
        return priceE8 / (decimal)Scale;
    }

    public static bool IsStale(long lastExchangeTimestampNanos, long exchangeTimestampNanos)
    {
        return exchangeTimestampNanos < lastExchangeTimestampNanos;
    }
}