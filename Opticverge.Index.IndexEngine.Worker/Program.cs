using Opticverge.Index.Engine;
using Opticverge.Index.IndexEngine.Worker;

using var gcLatency = RuntimeTuning.EnterSustainedLowLatencyGc();

var builder = Host.CreateApplicationBuilder(args);
builder.AddServiceDefaults();
builder.Services.Configure<IndexEngineOptions>(builder.Configuration.GetSection("Engine"));
builder.Services.Configure<KafkaOptions>(builder.Configuration.GetSection("Kafka"));
builder.Services.AddHostedService<Worker>();

var host = builder.Build();
host.Run();