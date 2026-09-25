using System.Numerics;
using System.Runtime.CompilerServices;

namespace Opticverge.Index.Engine;

public static class IndexMath
{
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static long ContributionE8(long priceE8, long weightE8)
    {
        return priceE8 * weightE8 / PriceNormalizer.Scale;
    }

    public static long RecomputeScalar(ReadOnlySpan<long> pricesE8, ReadOnlySpan<long> weightsE8)
    {
        var length = Math.Min(pricesE8.Length, weightsE8.Length);
        var valueE8 = 0L;

        for (var i = 0; i < length; i++) valueE8 += ContributionE8(pricesE8[i], weightsE8[i]);

        return valueE8;
    }


    public static long RecomputeVectorizedProducts(ReadOnlySpan<long> pricesE8, ReadOnlySpan<long> weightsE8)
    {
        var length = Math.Min(pricesE8.Length, weightsE8.Length);
        var i = 0;
        var productAccumulator = Vector<long>.Zero;
        var width = Vector<long>.Count;

        for (; i <= length - width; i += width) productAccumulator += new Vector<long>(pricesE8[i..]) * new Vector<long>(weightsE8[i..]);

        var valueE8 = 0L;
        for (var lane = 0; lane < width; lane++) valueE8 += productAccumulator[lane] / PriceNormalizer.Scale;

        for (; i < length; i++) valueE8 += ContributionE8(pricesE8[i], weightsE8[i]);

        return valueE8;
    }
}