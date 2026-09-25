using Opticverge.Index.Engine;
using Opticverge.Index.Publisher.Worker;

using var gcLatency = RuntimeTuning.EnterSustainedLowLatencyGc();

var builder = Host.CreateApplicationBuilder(args);
builder.AddServiceDefaults();
builder.Services.Configure<KafkaOptions>(builder.Configuration.GetSection("Kafka"));
builder.Services.AddHostedService<Worker>();

var host = builder.Build();
host.Run();