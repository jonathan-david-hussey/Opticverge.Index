using System.Diagnostics.Metrics;
using Confluent.Kafka;
using Confluent.Kafka.Admin;
using Microsoft.Extensions.Options;
using Opticverge.Index.Contracts;
using Opticverge.Index.FeedSimulator;

namespace Opticverge.Index.Ingestion.Worker;

public sealed class Worker(
    IOptions<IngestionOptions> ingestionOptions,
    IOptions<KafkaOptions> kafkaOptions,
    ILogger<Worker> logger) : BackgroundService
{
    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        var options = ingestionOptions.Value;
        var kafka = kafkaOptions.Value;
        var feed = new SyntheticExchangeFeed(new SyntheticFeedOptions
        {
            Instruments = options.Instruments,
            Exchanges = options.Exchanges,
            Partitions = options.Partitions,
            MessagesPerSecond = options.MessagesPerSecond,
            BurstSize = options.BurstSize,
            Seed = options.Seed
        });

        using var meter = new Meter("Opticverge.Index.Ingestion", "1.0");
        var ticksPublished = meter.CreateCounter<long>(
            "ingestion.ticks.published", "{ticks}", "Raw market ticks published to Kafka");

        if (string.IsNullOrWhiteSpace(kafka.BootstrapServers))
        {
            logger.LogWarning("Kafka bootstrap servers not configured. Ingestion worker will generate and count ticks only.");
            await RunDryAsync(feed, stoppingToken);
            return;
        }

        await EnsureTopicsAsync(kafka.BootstrapServers, options.Partitions, stoppingToken);

        using var producer = new ProducerBuilder<Null, byte[]>(new ProducerConfig
        {
            BootstrapServers = kafka.BootstrapServers,
            LingerMs = kafka.LingerMs,
            BatchNumMessages = kafka.BatchNumMessages,
            Acks = kafka.RequireAcks ? Acks.Leader : Acks.None,
            CompressionType = kafka.Compression
        }).Build();

        // librdkafka copies the payload before Produce() returns, so one pre-allocated buffer
        // and one reused Message<> object covers the entire hot path without per-tick allocation.
        var buffer = new byte[TickBinaryCodec.Size];
        var msg = new Message<Null, byte[]> { Value = buffer };
        var sent = 0L;
        try
        {
            await foreach (var tick in feed.ReadTicksAsync(stoppingToken))
            {
                if (!TickBinaryCodec.TryWrite(in tick, buffer))
                    continue;

                producer.Produce(new TopicPartition(MarketTopics.RawTicks, new Partition(tick.Partition)), msg);
                ticksPublished.Add(1);
                sent++;

                if ((sent & 0xFFFF) == 0)
                {
                    producer.Poll(TimeSpan.Zero);
                    logger.LogInformation("Published {Count:N0} raw ticks", sent);
                }
            }
        }
        finally
        {
            producer.Flush(TimeSpan.FromSeconds(2));
        }
    }

    private static async Task EnsureTopicsAsync(string bootstrapServers, int partitions, CancellationToken cancellationToken)
    {
        using var admin = new AdminClientBuilder(new AdminClientConfig { BootstrapServers = bootstrapServers }).Build();
        try
        {
            await admin.CreateTopicsAsync([
                new TopicSpecification
                {
                    Name = MarketTopics.RawTicks,
                    NumPartitions = partitions,
                    ReplicationFactor = 1
                }
            ]);
        }
        catch (CreateTopicsException ex) when (ex.Results.All(r => r.Error.Code == ErrorCode.TopicAlreadyExists))
        {
            // already exists — acceptable
        }
    }

    private async Task RunDryAsync(SyntheticExchangeFeed feed, CancellationToken stoppingToken)
    {
        var received = 0L;
        while (!stoppingToken.IsCancellationRequested)
            await foreach (var _ in feed.ReadTicksAsync(stoppingToken))
            {
                received++;
                if ((received & 0xFFFF) == 0)
                {
                    logger.LogInformation("Generated {Count:N0} synthetic ticks", received);
                    await Task.Yield();
                }
            }
    }
}

public sealed class IngestionOptions
{
    public int Instruments { get; init; } = 8;
    public int Exchanges { get; init; } = 4;
    public int Partitions { get; init; } = 4;
    public int MessagesPerSecond { get; init; } = 100_000;
    public int BurstSize { get; init; } = 1_024;
    public long Seed { get; init; } = 42;
}

public sealed class KafkaOptions
{
    public string? BootstrapServers { get; init; }
    public int LingerMs { get; init; } = 0;
    public int BatchNumMessages { get; init; } = 10_000;
    public bool RequireAcks { get; init; } = false;
    public CompressionType Compression { get; init; } = CompressionType.None;
}