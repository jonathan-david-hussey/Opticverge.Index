using System.Runtime.CompilerServices;
using Opticverge.Index.Contracts;

[module: SkipLocalsInit]

namespace Opticverge.Index.Engine;

public sealed class WeightedIndexCalculator
{
    private const double BaseLevel = 1000.0;

    private readonly int[] _instrumentIds;
    private readonly int[] _instrumentSlotById;
    private readonly ConstituentSlot[] _slots;
    private int _activeConstituentCount;
    private double _divisor;
    private long _lastReceiveTimestampNanos;
    private long _sequence;
    private int _staleCount;
    private long _valueE8;

    public WeightedIndexCalculator(IndexDefinition definition)
    {
        IndexId = definition.IndexId.Value;
        _instrumentIds = new int[definition.Constituents.Count];
        _slots = new ConstituentSlot[definition.Constituents.Count];

        var maxInstrumentId = 0;
        for (var i = 0; i < definition.Constituents.Count; i++) maxInstrumentId = Math.Max(maxInstrumentId, definition.Constituents[i].InstrumentId.Value);

        _instrumentSlotById = new int[maxInstrumentId + 1];
        Array.Fill(_instrumentSlotById, -1);

        for (var i = 0; i < definition.Constituents.Count; i++)
        {
            var constituent = definition.Constituents[i];
            _instrumentIds[i] = constituent.InstrumentId.Value;
            _instrumentSlotById[constituent.InstrumentId.Value] = i;
            _slots[i].WeightE8 = constituent.WeightE8;
            _slots[i].PriceE8 = constituent.InitialPriceE8;
            _valueE8 += IndexMath.ContributionE8(constituent.InitialPriceE8, constituent.WeightE8);
        }

        _divisor = _valueE8 / BaseLevel;
        for (var i = 0; i < _slots.Length; i++)
            if (_slots[i].WeightE8 > 0)
                _activeConstituentCount++;
    }

    public short IndexId { get; }

    public IndexValue Latest => Snapshot(_lastReceiveTimestampNanos);

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public bool Apply(in MarketTick tick, out IndexValue value)
    {
        if (!ApplyCore(tick.InstrumentId.Value, tick.PriceE8, tick.ExchangeTimestampNanos, true))
        {
            value = default;
            return false;
        }

        _lastReceiveTimestampNanos = tick.ReceiveTimestampNanos;
        value = Latest;
        return true;
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public bool Apply(in TickRingEvent tick, out IndexValue value)
    {
        // Receive timestamp is deferred — caller must invoke SetReceiveTimestamp after the batch.
        if (!ApplyCore(tick.InstrumentId, tick.PriceE8, tick.ExchangeTimestampNanos, true))
        {
            value = default;
            return false;
        }

        value = Latest;
        return true;
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public bool Apply(in MarketTick tick)
    {
        var applied = ApplyCore(tick.InstrumentId.Value, tick.PriceE8, tick.ExchangeTimestampNanos, true);
        if (applied) _lastReceiveTimestampNanos = tick.ReceiveTimestampNanos;
        return applied;
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public bool Apply(in TickRingEvent tick)
    {
        // Receive timestamp is deferred — caller must invoke SetReceiveTimestamp after the batch.
        return ApplyCore(tick.InstrumentId, tick.PriceE8, tick.ExchangeTimestampNanos, true);
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public bool ApplyPrevalidated(in MarketTick tick, out IndexValue value)
    {
        if (!ApplyCore(tick.InstrumentId.Value, tick.PriceE8, tick.ExchangeTimestampNanos, false))
        {
            value = default;
            return false;
        }

        _lastReceiveTimestampNanos = tick.ReceiveTimestampNanos;
        value = Latest;
        return true;
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public bool ApplyPrevalidated(in TickRingEvent tick, out IndexValue value)
    {
        // Receive timestamp is deferred — caller must invoke SetReceiveTimestamp after the batch.
        if (!ApplyCore(tick.InstrumentId, tick.PriceE8, tick.ExchangeTimestampNanos, false))
        {
            value = default;
            return false;
        }

        value = Latest;
        return true;
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public bool ApplyPrevalidated(in MarketTick tick)
    {
        var applied = ApplyCore(tick.InstrumentId.Value, tick.PriceE8, tick.ExchangeTimestampNanos, false);
        if (applied) _lastReceiveTimestampNanos = tick.ReceiveTimestampNanos;
        return applied;
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public bool ApplyPrevalidated(in TickRingEvent tick)
    {
        // Receive timestamp is deferred — caller must invoke SetReceiveTimestamp after the batch.
        return ApplyCore(tick.InstrumentId, tick.PriceE8, tick.ExchangeTimestampNanos, false);
    }

    // Called by EngineBatchEventHandler once per batch to set the receive timestamp
    // without paying a store per tick inside ApplyCore.
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public void SetReceiveTimestamp(long nanos)
    {
        _lastReceiveTimestampNanos = nanos;
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private bool ApplyCore(int instrumentId, long priceE8, long exchangeTimestampNanos, bool checkStale)
    {
        var slot = GetSlot(instrumentId);
        if (slot < 0)
            return false;

        ref var s = ref _slots[slot];

        if (s.WeightE8 == 0)
            return false;

        if (checkStale && PriceNormalizer.IsStale(s.TimestampNanos, exchangeTimestampNanos))
        {
            _staleCount++;
            return false;
        }

        _valueE8 += IndexMath.ContributionE8(priceE8 - s.PriceE8, s.WeightE8);
        s.PriceE8 = priceE8;
        s.TimestampNanos = exchangeTimestampNanos;
        _sequence++;

        return true;
    }

    public IndexValue Snapshot(long timestampNanos)
    {
        var levelE8 = (long)(_valueE8 / _divisor * PriceNormalizer.Scale);
        return new IndexValue(new IndexId(IndexId), _sequence, _valueE8, levelE8, timestampNanos, _activeConstituentCount, _staleCount);
    }

    public CalculatorSnapshot GetSnapshot(long timestampNanos, long[] partitionOffsets)
    {
        var pricesE8 = new long[_slots.Length];
        var weightsE8 = new long[_slots.Length];
        var timestampsNanos = new long[_slots.Length];
        var instrumentIds = new int[_slots.Length];
        var lastSequences = new long[_slots.Length];
        Array.Fill(lastSequences, -1L);
        for (var i = 0; i < _slots.Length; i++)
        {
            instrumentIds[i] = _instrumentIds[i];
            pricesE8[i] = _slots[i].PriceE8;
            weightsE8[i] = _slots[i].WeightE8;
            timestampsNanos[i] = _slots[i].TimestampNanos;
        }

        return new CalculatorSnapshot
        {
            IndexId = IndexId,
            Sequence = _sequence,
            ValueE8 = _valueE8,
            Divisor = _divisor,
            InstrumentIds = instrumentIds,
            LatestPricesE8 = pricesE8,
            WeightsE8 = weightsE8,
            LastTimestampNanos = timestampsNanos,
            LastSequences = lastSequences,
            TimestampNanos = timestampNanos,
            PartitionOffsets = (long[])partitionOffsets.Clone()
        };
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public void FillSnapshot(CalculatorSnapshot target, long timestampNanos, long[] partitionOffsets)
    {
        target.IndexId = IndexId;
        target.Sequence = _sequence;
        target.ValueE8 = _valueE8;
        target.Divisor = _divisor;
        for (var i = 0; i < _slots.Length; i++)
        {
            target.InstrumentIds[i] = _instrumentIds[i];
            target.LatestPricesE8[i] = _slots[i].PriceE8;
            target.WeightsE8[i] = _slots[i].WeightE8;
            target.LastTimestampNanos[i] = _slots[i].TimestampNanos;
        }

        target.TimestampNanos = timestampNanos;
        Array.Copy(partitionOffsets, target.PartitionOffsets,
            Math.Min(partitionOffsets.Length, target.PartitionOffsets.Length));
    }

    public void RestoreFromSnapshot(CalculatorSnapshot snapshot)
    {
        _sequence = snapshot.Sequence;
        _valueE8 = snapshot.ValueE8;
        _divisor = snapshot.Divisor;
        _lastReceiveTimestampNanos = snapshot.TimestampNanos;
        var count = Math.Min(
            Math.Min(snapshot.LatestPricesE8.Length, snapshot.WeightsE8.Length),
            Math.Min(snapshot.LastTimestampNanos.Length, _slots.Length));
        for (var i = 0; i < count; i++)
        {
            _slots[i].PriceE8 = snapshot.LatestPricesE8[i];
            _slots[i].WeightE8 = snapshot.WeightsE8[i];
            _slots[i].TimestampNanos = snapshot.LastTimestampNanos[i];
        }

        _activeConstituentCount = 0;
        for (var i = 0; i < _slots.Length; i++)
            if (_slots[i].WeightE8 > 0)
                _activeConstituentCount++;
    }

    public bool ApplyCorporateAction(CorporateAction action)
    {
        if (action.Type == CorporateActionType.WeightChange)
            return ApplyWeightChange(action);
        if (action.Type == CorporateActionType.ConstituentRemoval)
            return ApplyConstituentRemoval(action);
        if (action.Type == CorporateActionType.ConstituentAddition)
            return ApplyConstituentAddition(action);
        return false;
    }

    private bool ApplyWeightChange(CorporateAction action)
    {
        var slot = GetSlot(action.InstrumentId);
        if (slot < 0)
            return false;

        ref var s = ref _slots[slot];
        var oldContribution = IndexMath.ContributionE8(s.PriceE8, s.WeightE8);
        var newContribution = IndexMath.ContributionE8(s.PriceE8, action.NewWeightE8);
        var newValueE8 = _valueE8 - oldContribution + newContribution;

        var newDivisor = _valueE8 > 0 ? newValueE8 * _divisor / _valueE8 : newValueE8 / BaseLevel;
        s.WeightE8 = action.NewWeightE8;
        _valueE8 = newValueE8;
        _divisor = newDivisor;
        _sequence++;
        return true;
    }

    private bool ApplyConstituentAddition(CorporateAction action)
    {
        var slot = GetSlot(action.InstrumentId);
        if (slot < 0)
            return false;

        ref var s = ref _slots[slot];
        if (s.WeightE8 != 0)
            return false;

        var newContribution = IndexMath.ContributionE8(s.PriceE8, action.NewWeightE8);
        var newValueE8 = _valueE8 + newContribution;
        var newDivisor = _valueE8 > 0 ? newValueE8 * _divisor / _valueE8 : newValueE8 / BaseLevel;
        s.WeightE8 = action.NewWeightE8;
        _valueE8 = newValueE8;
        _divisor = newDivisor;
        _activeConstituentCount++;
        _sequence++;
        return true;
    }

    private bool ApplyConstituentRemoval(CorporateAction action)
    {
        var slot = GetSlot(action.InstrumentId);
        if (slot < 0)
            return false;

        ref var s = ref _slots[slot];
        var oldContribution = IndexMath.ContributionE8(s.PriceE8, s.WeightE8);
        var newValueE8 = _valueE8 - oldContribution;

        var newDivisor = _valueE8 > 0 ? newValueE8 * _divisor / _valueE8 : newValueE8 / BaseLevel;
        s.WeightE8 = 0;
        _valueE8 = newValueE8;
        _divisor = newDivisor;
        _activeConstituentCount--;
        _sequence++;
        return true;
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private int GetSlot(int instrumentId)
    {
        return (uint)instrumentId < (uint)_instrumentSlotById.Length
            ? _instrumentSlotById[instrumentId]
            : -1;
    }

    private struct ConstituentSlot
    {
        public long PriceE8;
        public long WeightE8;
        public long TimestampNanos;
    }
}