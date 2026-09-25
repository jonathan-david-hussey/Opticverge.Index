namespace Opticverge.Index.Engine;

public static class OptimizationCatalogue
{
    public static readonly string[] Notes =
    [
        "Preallocated power-of-two Disruptor ring buffers with single-writer shard ownership.",
        "Mutable event slots on the hot path; immutable records only at boundaries.",
        "Fixed-width binary tick codec for broker payloads; JSON is confined to API/UI edges.",
        "readonly struct identifiers and scaled integer prices avoid decimal/string work in the engine.",
        "No LINQ, closures, boxing, expected exceptions, or per-message heap allocation in index calculation.",
        "Struct-of-arrays constituent storage improves cache locality during weighted sums.",
        "Yielding wait strategy is the default local demo profile; busy-spin is available for latency experiments.",
        "Server GC, tiered compilation, dynamic PGO, and warm-up are assumed for measured runs.",
        "Kafka/Redpanda provides durable fan-out and replay; the in-process lane owns lowest latency.",
        "Thread affinity and NUMA isolation are documented as deployment concerns, not portable local guarantees.",
        "Snapshot + replay recovery: calculator state checkpointed to a compacted Kafka topic; restart replays only the post-snapshot gap.",
        "Corporate actions (weight change, constituent removal) adjust the divisor to preserve level continuity; applied lock-free via ConcurrentQueue drained at batch boundary."
    ];
}