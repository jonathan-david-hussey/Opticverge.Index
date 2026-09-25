using Opticverge.Index.Contracts;

namespace Opticverge.Index.Engine;

public sealed class ReferenceDataValidationException(IReadOnlyList<string> errors)
    : InvalidOperationException("Reference data is invalid: " + string.Join("; ", errors))
{
    public IReadOnlyList<string> Errors { get; } = errors;
}

public static class ReferenceDataValidator
{
    public static void ThrowIfInvalid(ReferenceDataSnapshot snapshot)
    {
        var errors = Validate(snapshot);
        if (errors.Count != 0) throw new ReferenceDataValidationException(errors);
    }

    public static IReadOnlyList<string> Validate(ReferenceDataSnapshot snapshot)
    {
        var errors = new List<string>();

        if (snapshot.Version <= 0)
            errors.Add("Reference data version must be positive.");
        if (snapshot.Instruments.Count == 0)
            errors.Add("Reference data must contain at least one instrument.");
        if (snapshot.Indexes.Count == 0)
            errors.Add("Reference data must contain at least one index.");

        var maxInstrumentId = MaxInstrumentId(snapshot);
        if (maxInstrumentId > snapshot.Instruments.Count * 4)
            errors.Add(
                $"Instrument ids must be normalized to dense internal ids for array indexing; count={snapshot.Instruments.Count}, maxId={maxInstrumentId}.");

        var instrumentExists = maxInstrumentId >= 0 ? new bool[maxInstrumentId + 1] : [];
        var activeInstrument = maxInstrumentId >= 0 ? new bool[maxInstrumentId + 1] : [];
        var currenciesByInstrumentId = maxInstrumentId >= 0 ? new string?[maxInstrumentId + 1] : [];
        var symbols = new HashSet<string>(StringComparer.OrdinalIgnoreCase);

        foreach (var instrument in snapshot.Instruments)
        {
            var id = instrument.InstrumentId.Value;
            if (id <= 0)
            {
                errors.Add($"Instrument '{instrument.Symbol}' has non-positive instrument id {id}.");
                continue;
            }

            if (instrumentExists[id])
                errors.Add($"Duplicate instrument id {id}.");

            instrumentExists[id] = true;
            activeInstrument[id] = instrument.IsActive;
            currenciesByInstrumentId[id] = instrument.Currency;

            if (string.IsNullOrWhiteSpace(instrument.Symbol))
                errors.Add($"Instrument id {id} is missing symbol.");
            else if (!symbols.Add(instrument.Symbol))
                errors.Add($"Duplicate instrument symbol '{instrument.Symbol}'.");

            if (string.IsNullOrWhiteSpace(instrument.Currency))
                errors.Add($"Instrument '{instrument.Symbol}' is missing currency.");
        }

        var indexIds = new HashSet<short>();
        var indexCodes = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
        foreach (var index in snapshot.Indexes)
            ValidateIndex(
                snapshot,
                index,
                instrumentExists,
                activeInstrument,
                currenciesByInstrumentId,
                indexIds,
                indexCodes,
                errors);

        return errors;
    }

    private static void ValidateIndex(
        ReferenceDataSnapshot snapshot,
        IndexDefinition index,
        ReadOnlySpan<bool> instrumentExists,
        ReadOnlySpan<bool> activeInstrument,
        string?[] currenciesByInstrumentId,
        HashSet<short> indexIds,
        HashSet<string> indexCodes,
        List<string> errors)
    {
        if (index.IndexId.Value <= 0)
            errors.Add($"Index '{index.Code}' has non-positive index id {index.IndexId.Value}.");
        else if (!indexIds.Add(index.IndexId.Value))
            errors.Add($"Duplicate index id {index.IndexId.Value}.");

        if (string.IsNullOrWhiteSpace(index.Code))
            errors.Add($"Index id {index.IndexId.Value} is missing code.");
        else if (!indexCodes.Add(index.Code))
            errors.Add($"Duplicate index code '{index.Code}'.");

        if (string.IsNullOrWhiteSpace(index.Currency))
            errors.Add($"Index '{index.Code}' is missing currency.");

        if (index.ReferenceDataVersion > 0 && index.ReferenceDataVersion != snapshot.Version)
            errors.Add($"Index '{index.Code}' version {index.ReferenceDataVersion} does not match snapshot version {snapshot.Version}.");

        if (index.EffectiveFromNanos > 0 && snapshot.EffectiveFromNanos > 0 &&
            index.EffectiveFromNanos != snapshot.EffectiveFromNanos)
            errors.Add($"Index '{index.Code}' effective timestamp does not match snapshot timestamp.");

        if (index.Constituents.Count == 0)
        {
            errors.Add($"Index '{index.Code}' must contain at least one constituent.");
            return;
        }

        var maxConstituentId = 0;
        foreach (var constituent in index.Constituents)
            maxConstituentId = Math.Max(maxConstituentId, constituent.InstrumentId.Value);
        var seenConstituents = new bool[maxConstituentId + 1];
        var weightTotal = 0L;

        foreach (var constituent in index.Constituents)
        {
            ValidateConstituent(
                snapshot,
                index,
                constituent,
                instrumentExists,
                activeInstrument,
                currenciesByInstrumentId,
                seenConstituents,
                errors);

            if (constituent.WeightE8 > 0)
                weightTotal += constituent.WeightE8;
        }

        if (index.Methodology == IndexMethodology.WeightedPrice && weightTotal != PriceNormalizer.Scale)
            errors.Add($"Index '{index.Code}' weighted-price constituents must sum to {PriceNormalizer.Scale}; actual={weightTotal}.");
    }

    private static void ValidateConstituent(
        ReferenceDataSnapshot snapshot,
        IndexDefinition index,
        IndexConstituentDefinition constituent,
        ReadOnlySpan<bool> instrumentExists,
        ReadOnlySpan<bool> activeInstrument,
        string?[] currenciesByInstrumentId,
        Span<bool> seenConstituents,
        List<string> errors)
    {
        var instrumentId = constituent.InstrumentId.Value;
        if (instrumentId <= 0 || (uint)instrumentId >= (uint)instrumentExists.Length)
        {
            errors.Add($"Index '{index.Code}' references unknown instrument id {instrumentId}.");
            return;
        }

        if (!instrumentExists[instrumentId])
        {
            errors.Add($"Index '{index.Code}' references unknown instrument id {instrumentId}.");
            return;
        }

        if (seenConstituents[instrumentId])
            errors.Add($"Index '{index.Code}' has duplicate constituent instrument id {instrumentId}.");
        seenConstituents[instrumentId] = true;

        if (!activeInstrument[instrumentId] && !constituent.CorporateActionPlaceholder)
            errors.Add($"Index '{index.Code}' references inactive instrument id {instrumentId}.");

        if (constituent.InitialPriceE8 <= 0)
            errors.Add($"Index '{index.Code}' constituent {instrumentId} has non-positive initial price.");

        if (constituent.WeightE8 < 0)
            errors.Add($"Index '{index.Code}' constituent {instrumentId} has negative weight.");

        if (constituent.FreeFloatFactorE8 is < 0 or > PriceNormalizer.Scale)
            errors.Add($"Index '{index.Code}' constituent {instrumentId} has invalid free-float factor.");

        if (constituent.FxRateE8 <= 0)
            errors.Add($"Index '{index.Code}' constituent {instrumentId} has non-positive FX rate.");

        if (index.Methodology == IndexMethodology.FloatAdjustedMarketCap && constituent.Shares <= 0)
            errors.Add($"Index '{index.Code}' market-cap constituent {instrumentId} must have positive shares.");

        var instrumentCurrency = currenciesByInstrumentId[instrumentId];
        if (!string.Equals(instrumentCurrency, index.Currency, StringComparison.OrdinalIgnoreCase) &&
            !HasFxRate(snapshot.FxRates, instrumentCurrency, index.Currency))
            errors.Add($"Index '{index.Code}' constituent {instrumentId} requires FX {instrumentCurrency}->{index.Currency}.");
    }

    private static bool HasFxRate(IReadOnlyList<FxRateDefinition> rates, string? fromCurrency, string toCurrency)
    {
        if (string.IsNullOrWhiteSpace(fromCurrency) || string.IsNullOrWhiteSpace(toCurrency))
            return false;

        foreach (var rate in rates)
            if (rate.RateE8 > 0 &&
                string.Equals(rate.FromCurrency, fromCurrency, StringComparison.OrdinalIgnoreCase) &&
                string.Equals(rate.ToCurrency, toCurrency, StringComparison.OrdinalIgnoreCase))
                return true;

        return false;
    }

    private static int MaxInstrumentId(ReferenceDataSnapshot snapshot)
    {
        var max = -1;
        foreach (var instrument in snapshot.Instruments)
            max = Math.Max(max, instrument.InstrumentId.Value);
        foreach (var index in snapshot.Indexes)
        foreach (var constituent in index.Constituents)
            max = Math.Max(max, constituent.InstrumentId.Value);
        return max;
    }
}