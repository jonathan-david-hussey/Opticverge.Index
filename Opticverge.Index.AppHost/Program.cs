using Projects;

var builder = DistributedApplication.CreateBuilder(args);

var messagesPerSecond = builder.Configuration["Ingestion:MessagesPerSecond"] ?? "10000";
var burstSize = builder.Configuration["Ingestion:BurstSize"] ?? "256";
var deltaPublishInterval = builder.Configuration["Engine:DeltaPublishInterval"] ?? "1024";
var latencySampleRate = builder.Configuration["Engine:LatencySampleRate"] ?? "128";

var postgres = builder.AddPostgres("pg")
    .WithPgAdmin();

var indexDb = postgres.AddDatabase("index-db");

var redpanda = builder.AddRedPanda("redpanda")
    .WithConsole()
    .WithKafkaUI()
    .WithEnvironment("REDPANDA_LOG_RETENTION_MS", "60000")
    .WithEnvironment("REDPANDA_LOG_RETENTION_BYTES", "104857600");

var queryApi = builder.AddProject<Opticverge_Index_Query_Api>("query-api")
    .WithEnvironment("Kafka__BootstrapServers", redpanda.Resource.ConnectionStringExpression)
    .WithEnvironment("Engine__Partitions", "4")
    .WithHttpHealthCheck("/health")
    .WaitFor(redpanda);

builder.AddProject<Opticverge_Index_Ops_Web>("ops-web")
    .WithExternalHttpEndpoints()
    .WithEnvironment("QueryApi__BaseUrl", queryApi.GetEndpoint("http"))
    .WithHttpHealthCheck("/health")
    .WaitFor(queryApi);

builder.AddProject<Opticverge_Index_Ingestion_Worker>("ingestion-worker")
    .WithEnvironment("Kafka__BootstrapServers", redpanda.Resource.ConnectionStringExpression)
    .WithEnvironment("Ingestion__Partitions", "4")
    .WithEnvironment("Ingestion__MessagesPerSecond", messagesPerSecond)
    .WithEnvironment("Ingestion__BurstSize", burstSize)
    .WaitFor(redpanda);

builder.AddProject<Opticverge_Index_IndexEngine_Worker>("index-engine-worker")
    .WithEnvironment("Kafka__BootstrapServers", redpanda.Resource.ConnectionStringExpression)
    .WithEnvironment("Engine__Partitions", "4")
    .WithEnvironment("Engine__RingBufferSize", "65536")
    .WithEnvironment("Engine__WaitStrategy", "yielding")
    .WithEnvironment("Engine__DeltaPublishInterval", deltaPublishInterval)
    .WithEnvironment("Engine__LatencySampleRate", latencySampleRate)
    .WaitFor(redpanda);

builder.AddProject<Opticverge_Index_Publisher_Worker>("publisher-worker")
    .WithReference(indexDb)
    .WaitFor(postgres)
    .WithEnvironment("Kafka__BootstrapServers", redpanda.Resource.ConnectionStringExpression)
    .WaitFor(redpanda);

builder.Build().Run();

public partial class Program;