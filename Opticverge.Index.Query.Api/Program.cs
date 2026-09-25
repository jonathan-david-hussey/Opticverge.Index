using System.Text.Json;
using Confluent.Kafka;
using Microsoft.Extensions.Options;
using Opticverge.Index.Contracts;
using Opticverge.Index.Engine;
using Opticverge.Index.Query.Api;

var builder = WebApplication.CreateBuilder(args);
builder.AddServiceDefaults();
builder.Services.ConfigureHttpJsonOptions(options => { options.SerializerOptions.TypeInfoResolverChain.Insert(0, IndexJsonContext.Default); });

builder.Services.AddOpenApi();
builder.Services.Configure<KafkaOptions>(builder.Configuration.GetSection("Kafka"));
builder.Services.AddSingleton<LiveState>();
builder.Services.AddSingleton<LiveMetricsState>();
builder.Services.AddHostedService<IndexDeltaConsumerService>();
builder.Services.AddHostedService<EngineMetricsConsumerService>();

var app = builder.Build();

if (app.Environment.IsDevelopment()) app.MapOpenApi();

app.MapDefaultEndpoints();

var catalog = IndexCatalog.CreateDemo();
var staticIndexes = catalog.Indexes
    .Select(idx => new WeightedIndexCalculator(idx).Snapshot(MonotonicClock.TimestampNanos()))
    .ToList();
var staticMetrics = new EngineMetrics().ToDto();

app.MapGet("/", () => Results.Ok(new
{
    name = "Opticverge Real-Time Index Engineering Demo",
    lane = "in-process Disruptor hot path plus Redpanda/Kafka fan-out",
    dashboard = "/dashboard",
    indexes = "/indexes",
    topics = "/topics",
    optimizations = "/optimizations"
}));

app.MapGet("/topics", () => MarketTopics.All);

app.MapGet("/reference-data", () => IndexCatalog.CreateDemoSnapshot());

app.MapGet("/indexes", () => catalog.Indexes);

app.MapGet("/instruments", () => catalog.Instruments);

app.MapGet("/dashboard", (LiveState liveState, LiveMetricsState liveMetrics) =>
{
    var live = liveState.All;
    var indexes = live.Count > 0 ? live : staticIndexes;
    var metrics = liveMetrics.Latest ?? staticMetrics;
    var state = new DashboardStateDto(
        DateTimeOffset.UtcNow,
        indexes,
        metrics,
        MarketTopics.All,
        OptimizationCatalogue.Notes);

    return Results.Ok(state);
});

app.MapGet("/optimizations", () => OptimizationCatalogue.Notes);

app.MapPost("/admin/corporate-action", (CorporateAction action, IOptions<KafkaOptions> kafkaOptions) =>
{
    var bootstrapServers = kafkaOptions.Value.BootstrapServers;
    if (string.IsNullOrWhiteSpace(bootstrapServers))
        return Results.Problem("Kafka not configured");

    using var producer = new ProducerBuilder<Null, string>(new ProducerConfig
    {
        BootstrapServers = bootstrapServers,
        Acks = Acks.Leader
    }).Build();

    var json = JsonSerializer.Serialize(action, IndexJsonContext.Default.CorporateAction);
    producer.Produce(MarketTopics.CorporateActions, new Message<Null, string> { Value = json });
    // Audit record: durable, append-only log for compliance and replay.
    producer.Produce(MarketTopics.IndexAudit, new Message<Null, string> { Value = json });
    producer.Flush(TimeSpan.FromSeconds(2));

    return Results.Accepted();
});

app.Run();

public partial class Program;