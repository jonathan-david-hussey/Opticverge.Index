using System.Net.Http.Headers;
using Opticverge.Index.Contracts;
using Opticverge.Index.Engine;

var builder = WebApplication.CreateBuilder(args);
builder.AddServiceDefaults();
builder.Services.Configure<QueryApiOptions>(builder.Configuration.GetSection("QueryApi"));
builder.Services.AddHttpClient("query-api", (services, client) =>
{
    var options = services.GetRequiredService<IConfiguration>().GetSection("QueryApi").Get<QueryApiOptions>();
    if (!string.IsNullOrWhiteSpace(options?.BaseUrl)) client.BaseAddress = new Uri(options.BaseUrl);

    client.DefaultRequestHeaders.Accept.Add(new MediaTypeWithQualityHeaderValue("application/json"));
});

var app = builder.Build();

app.MapDefaultEndpoints();

app.MapGet("/", () => Results.Text(OpsDashboardAssets.Html, "text/html"));

app.MapGet("/api/dashboard", async (IHttpClientFactory factory, CancellationToken cancellationToken) =>
{
    var client = factory.CreateClient("query-api");
    if (client.BaseAddress is not null)
        try
        {
            return Results.Text(
                await client.GetStringAsync("/dashboard", cancellationToken),
                "application/json");
        }
        catch (HttpRequestException)
        {
        }

    var catalog = IndexCatalog.CreateDemo();
    var state = new DashboardStateDto(
        DateTimeOffset.UtcNow,
        [new WeightedIndexCalculator(catalog.Indexes[0]).Snapshot(MonotonicClock.TimestampNanos())],
        new EngineMetrics().ToDto(),
        MarketTopics.All,
        OptimizationCatalogue.Notes);

    return Results.Json(state, IndexJsonContext.Default.DashboardStateDto);
});

app.Run();

public sealed class QueryApiOptions
{
    public string? BaseUrl { get; init; }
}

public partial class Program;

internal static class OpsDashboardAssets
{
    public const string Html = """
                               <!doctype html>
                               <html lang="en">
                               <head>
                                 <meta charset="utf-8">
                                 <meta name="viewport" content="width=device-width, initial-scale=1">
                                 <title>Opticverge Index Ops</title>
                                 <style>
                                   :root {
                                     color-scheme: dark;
                                     font-family: "Segoe UI", Arial, sans-serif;
                                     background: #0c1116;
                                     color: #e8edf2;
                                   }
                                   body { margin: 0; min-height: 100vh; background: #0c1116; }
                                   header, main { max-width: 1280px; margin: 0 auto; padding: 20px; }
                                   header {
                                     display: flex; align-items: flex-end; justify-content: space-between;
                                     border-bottom: 1px solid #24303a; padding-bottom: 16px;
                                   }
                                   h1 { margin: 0; font-size: 24px; font-weight: 650; }
                                   .subtitle { margin-top: 6px; color: #9fb0bf; font-size: 13px; }
                                   .status { font-size: 13px; color: #68d391; }
                                   .grid {
                                     display: grid;
                                     grid-template-columns: repeat(4, minmax(0, 1fr));
                                     gap: 12px;
                                     margin-top: 20px;
                                   }
                                   section {
                                     border: 1px solid #24303a; border-radius: 8px;
                                     background: #111922; padding: 14px; min-width: 0;
                                   }
                                   section.wide { grid-column: span 2; }
                                   section.full { grid-column: span 4; }
                                   h2 {
                                     margin: 0 0 10px; font-size: 11px; color: #7a90a0;
                                     font-weight: 600; text-transform: uppercase; letter-spacing: 0.06em;
                                   }
                                   .metric { font-size: 28px; font-variant-numeric: tabular-nums; white-space: nowrap; }
                                   .sub { color: #8fa0ae; font-size: 13px; margin-top: 4px; }
                                   .stat-table { width: 100%; border-collapse: collapse; }
                                   .stat-table td {
                                     padding: 5px 0; border-bottom: 1px solid #1a2530;
                                     font-size: 13px; font-variant-numeric: tabular-nums;
                                   }
                                   .stat-table td:first-child { color: #7a90a0; }
                                   .stat-table td:last-child { color: #e8edf2; text-align: right; }
                                   .stat-table tr:last-child td { border-bottom: none; }
                                   table.data { width: 100%; border-collapse: collapse; }
                                   table.data th, table.data td {
                                     text-align: left; padding: 7px 0;
                                     border-bottom: 1px solid #1a2530; font-size: 13px;
                                     font-variant-numeric: tabular-nums; color: #b8c5cf;
                                   }
                                   table.data th { color: #7a90a0; font-weight: 600; font-size: 12px; }
                                   table.data tr:last-child td { border-bottom: none; }
                                   ul { margin: 0; padding-left: 18px; }
                                   li { color: #b8c5cf; font-size: 13px; margin-bottom: 6px; }
                                   @media (max-width: 860px) {
                                     header { display: block; }
                                     .grid { grid-template-columns: 1fr; }
                                     section.wide, section.full { grid-column: span 1; }
                                   }
                                 </style>
                               </head>
                               <body>
                                 <header>
                                   <div>
                                     <h1>Real-Time Index Ops</h1>
                                     <div class="subtitle">Disruptor hot path · Redpanda fan-out · Aspire telemetry</div>
                                   </div>
                                   <div class="status" id="status">connecting</div>
                                 </header>
                                 <main>
                                   <div class="grid">

                                     <section>
                                       <h2>Throughput</h2>
                                       <div class="metric" id="mps">0/s</div>
                                       <div class="sub" id="messages">0 in · 0 out</div>
                                     </section>

                                     <section>
                                       <h2>Consumer Lag</h2>
                                       <div class="metric" id="consumer-lag">0</div>
                                       <div class="sub">messages behind head</div>
                                     </section>

                                     <section>
                                       <h2>Calc Backlog</h2>
                                       <div class="metric" id="ring-buffer">0</div>
                                       <div class="sub">in − out (unresolved ticks)</div>
                                     </section>

                                     <section>
                                       <h2>GC Collections</h2>
                                       <div class="metric" id="gc">0/0/0</div>
                                       <div class="sub">Gen0 / Gen1 / Gen2</div>
                                     </section>

                                     <section class="wide">
                                       <h2>Processing Latency (receive → calculation)</h2>
                                       <table class="stat-table">
                                         <tr><td>p50</td><td id="lat-p50">—</td></tr>
                                         <tr><td>p95</td><td id="lat-p95">—</td></tr>
                                         <tr><td>p99</td><td id="lat-p99">—</td></tr>
                                         <tr><td>p999</td><td id="lat-p999">—</td></tr>
                                       </table>
                                     </section>

                                     <section class="wide">
                                       <h2>End-to-End Latency (exchange → calculation)</h2>
                                       <table class="stat-table">
                                         <tr><td>p50</td><td id="e2e-p50">—</td></tr>
                                         <tr><td>p95</td><td id="e2e-p95">—</td></tr>
                                         <tr><td>p99</td><td id="e2e-p99">—</td></tr>
                                       </table>
                                     </section>

                                     <section class="wide">
                                       <h2>Pipeline Timing (p99 per stage)</h2>
                                       <table class="stat-table">
                                         <tr><td>Calculation</td><td id="calc-p99">—</td></tr>
                                         <tr><td>Publication</td><td id="pub-p99">—</td></tr>
                                       </table>
                                     </section>

                                     <section class="wide">
                                       <h2>Event Quality</h2>
                                       <table class="stat-table">
                                         <tr><td>Duplicates</td><td id="q-duplicates">0</td></tr>
                                         <tr><td>Seq gaps</td><td id="q-gaps">0</td></tr>
                                         <tr><td>Calc fail</td><td id="q-failures">0</td></tr>
                                         <tr><td>Dropped</td><td id="q-dropped">0</td></tr>
                                         <tr><td>Stale</td><td id="q-stale">0</td></tr>
                                       </table>
                                     </section>

                                     <section class="full">
                                       <h2>Indexes</h2>
                                       <table class="data">
                                         <thead><tr><th>Index</th><th>Sequence</th><th>Level</th><th>Raw E8</th><th>Constituents</th><th>Stale</th></tr></thead>
                                         <tbody id="indexes"></tbody>
                                       </table>
                                     </section>

                                     <section class="wide">
                                       <h2>Kafka Topics</h2>
                                       <ul id="topics"></ul>
                                     </section>

                                     <section class="wide">
                                       <h2>Engineering Notes</h2>
                                       <ul id="notes"></ul>
                                     </section>

                                   </div>
                                 </main>
                                 <script>
                                   const el = id => document.getElementById(id);
                                   const text = (id, v) => { el(id).textContent = v; };
                                   const fmt = v => Number(v || 0).toLocaleString();

                                   function fmtNs(v) {
                                     v = v || 0;
                                     if (v === 0) return '0 ns';
                                     if (v < 1000) return v.toFixed(0) + ' ns';
                                     if (v < 1000000) return (v / 1000).toFixed(1) + ' µs';
                                     if (v < 1000000000) return (v / 1000000).toFixed(2) + ' ms';
                                     return (v / 1000000000).toFixed(3) + ' s';
                                   }

                                   function fmtRate(count, total) {
                                     count = count || 0;
                                     if (!total) return fmt(count);
                                     return fmt(count) + ' (' + (count / total * 1000).toFixed(2) + '‰)';
                                   }

                                   async function refresh() {
                                     const response = await fetch('/api/dashboard', { cache: 'no-store' });
                                     const data = await response.json();
                                     const m = data.metrics || {};

                                     text('status', 'live ' + new Date(data.timestamp).toLocaleTimeString());

                                     text('mps', fmt(Math.round(m.messagesPerSecond || 0)) + '/s');
                                     text('messages', fmt(m.messagesIn) + ' in · ' + fmt(m.messagesOut) + ' out');
                                     text('consumer-lag', fmt(m.consumerLag));
                                     text('ring-buffer', fmt(m.ringBufferDepth));
                                     text('gc', [m.gen0Collections, m.gen1Collections, m.gen2Collections].map(fmt).join('/'));

                                     text('lat-p50',  fmtNs(m.p50LatencyNanos));
                                     text('lat-p95',  fmtNs(m.p95LatencyNanos));
                                     text('lat-p99',  fmtNs(m.p99LatencyNanos));
                                     text('lat-p999', fmtNs(m.p999LatencyNanos));

                                     text('e2e-p50', fmtNs(m.endToEndP50Nanos));
                                     text('e2e-p95', fmtNs(m.endToEndP95Nanos));
                                     text('e2e-p99', fmtNs(m.endToEndP99Nanos));

                                     text('calc-p99', fmtNs(m.calcDurationP99Nanos));
                                     text('pub-p99',  fmtNs(m.publicationDurationP99Nanos));

                                     const total = m.messagesIn || 0;
                                     text('q-duplicates', fmtRate(m.duplicateEvents,     total));
                                     text('q-gaps',       fmtRate(m.sequenceGaps,        total));
                                     text('q-failures',   fmtRate(m.calculationFailures, total));
                                     text('q-dropped',    fmt(m.droppedMessages));
                                     text('q-stale',      fmt(m.staleTicks));

                                     const SCALE = 100_000_000;
                                     el('indexes').innerHTML = (data.indexes || []).map(idx => {
                                       const level = idx.levelE8 != null ? (idx.levelE8 / SCALE).toFixed(2) : '—';
                                       return '<tr><td>' + idx.indexId.value + '</td><td>' + fmt(idx.sequence) +
                                         '</td><td>' + level + '</td><td>' + fmt(idx.valueE8) +
                                         '</td><td>' + fmt(idx.constituentCount) +
                                         '</td><td>' + fmt(idx.staleConstituentCount) + '</td></tr>';
                                     }).join('');

                                     el('topics').innerHTML = (data.topics || []).map(t => '<li>' + t + '</li>').join('');
                                     el('notes').innerHTML  = (data.optimizationNotes || []).slice(0, 6).map(n => '<li>' + n + '</li>').join('');
                                   }

                                   setInterval(() => refresh().catch(() => text('status', 'query degraded')), 1000);
                                   refresh().catch(() => text('status', 'query degraded'));
                                 </script>
                               </body>
                               </html>
                               """;
}