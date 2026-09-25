# CloudWatch Alarms — Opticverge Real-Time Index Engine
#
# SLO: 99.9% of index values published within 40 ms of the triggering exchange event,
#      measured at the p99.9 percentile over a rolling 5-minute window.
#
# Usage:
#   cd infra
#   terraform init
#   terraform plan -var-file=env/staging.tfvars
#   terraform apply -var-file=env/staging.tfvars
#
# Metrics are emitted by the ADOT collector sidecar (OpenTelemetry → CloudWatch EMF).

terraform {
  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = ">= 5.0"
    }
  }
}

locals {
  # Half-full ring buffer — alarm before back-pressure becomes unavoidable.
  ring_buffer_warning_threshold = floor(var.ring_buffer_size / 2)

  critical_actions = [var.critical_sns_topic_arn]
  ops_actions      = [var.ops_sns_topic_arn]
}

# ── SLO: p99 end-to-end latency > 40 ms ──────────────────────────────────────
# Exchange timestamp → index calculation start (batch sample; the calculator
# step itself is tracked separately via index.calculation.duration).
# Breaching this is a direct SLO violation.
resource "aws_cloudwatch_metric_alarm" "e2e_latency_p99" {
  alarm_name          = "${var.service_name}-e2e-latency-p99-breach"
  alarm_description   = "p99 end-to-end latency exceeded 40 ms — SLO breach. Check ADOT collector, engine thread scheduling, GC pauses, and MSK consumer lag."
  namespace           = var.namespace
  metric_name         = "index.end_to_end.latency.p99"
  statistic           = "Maximum"
  period              = var.evaluation_period_seconds
  evaluation_periods  = 3
  threshold           = 40_000_000 # 40 ms in nanoseconds
  comparison_operator = "GreaterThanThreshold"
  treat_missing_data  = "notBreaching"
  alarm_actions       = local.critical_actions
  ok_actions          = local.critical_actions
  tags                = var.tags
}

# ── p99 ring-buffer processing latency > 5 ms ────────────────────────────────
# Receive timestamp → calculation start (batch sample). Leading indicator for the SLO alarm.
resource "aws_cloudwatch_metric_alarm" "processing_latency_p99" {
  alarm_name          = "${var.service_name}-processing-latency-p99"
  alarm_description   = "p99 ring-buffer processing latency exceeded 5 ms — leading SLO indicator. Investigate wait strategy, CPU affinity, and GC."
  namespace           = var.namespace
  metric_name         = "index.processing.latency.p99"
  statistic           = "Maximum"
  period              = var.evaluation_period_seconds
  evaluation_periods  = 2
  threshold           = 5_000_000 # 5 ms in nanoseconds
  comparison_operator = "GreaterThanThreshold"
  treat_missing_data  = "notBreaching"
  alarm_actions       = local.ops_actions
  tags                = var.tags
}

# ── Kafka consumer lag > 10 000 messages ─────────────────────────────────────
# Engine is falling behind ingestion; tail latency will increase until lag clears.
resource "aws_cloudwatch_metric_alarm" "consumer_lag" {
  alarm_name          = "${var.service_name}-consumer-lag-high"
  alarm_description   = "Kafka consumer lag exceeded 10 000 — engine cannot keep pace with ingestion. Scale ECS tasks or increase ring buffer size."
  namespace           = var.namespace
  metric_name         = "index.consumer.lag"
  statistic           = "Maximum"
  period              = var.evaluation_period_seconds
  evaluation_periods  = 2
  threshold           = 10_000
  comparison_operator = "GreaterThanThreshold"
  treat_missing_data  = "notBreaching"
  alarm_actions       = local.critical_actions
  ok_actions          = local.critical_actions
  tags                = var.tags
}

# ── Ring buffer depth > 50% ───────────────────────────────────────────────────
resource "aws_cloudwatch_metric_alarm" "ring_buffer_depth" {
  alarm_name          = "${var.service_name}-ring-buffer-depth-high"
  alarm_description   = "Ring buffer depth exceeded 50% (${local.ring_buffer_warning_threshold} of ${var.ring_buffer_size} slots). Engine will stall producers if it reaches 100%."
  namespace           = var.namespace
  metric_name         = "index.ring_buffer.depth"
  statistic           = "Maximum"
  period              = var.evaluation_period_seconds
  evaluation_periods  = 3
  threshold           = local.ring_buffer_warning_threshold
  comparison_operator = "GreaterThanThreshold"
  treat_missing_data  = "notBreaching"
  alarm_actions       = local.ops_actions
  tags                = var.tags
}

# ── Any sequence gaps detected in 5-minute window ────────────────────────────
# A gap means at least one market event was not received. Could be a feed drop,
# a network partition, or a Kafka partition leader failover.
resource "aws_cloudwatch_metric_alarm" "sequence_gaps" {
  alarm_name          = "${var.service_name}-sequence-gaps"
  alarm_description   = "Market-event sequence gaps detected — possible data loss on a feed partition. Trigger replay from Kafka offset or contact exchange."
  namespace           = var.namespace
  metric_name         = "index.events.sequence_gap"
  statistic           = "Sum"
  period              = 300 # 5-minute window
  evaluation_periods  = 1
  threshold           = 0
  comparison_operator = "GreaterThanThreshold"
  treat_missing_data  = "notBreaching"
  alarm_actions       = local.ops_actions
  tags                = var.tags
}

# ── Duplicate events > 100 per 5 minutes ─────────────────────────────────────
# Small numbers are normal (Kafka at-least-once delivery on consumer restart).
# A sustained spike indicates a consumer restart loop or deliberate feed replay.
resource "aws_cloudwatch_metric_alarm" "duplicate_events" {
  alarm_name          = "${var.service_name}-duplicate-events-spike"
  alarm_description   = "Duplicate event rate exceeded 100 per 5 min — possible consumer restart loop or feed replay. Duplicates are safely discarded but indicate instability."
  namespace           = var.namespace
  metric_name         = "index.events.duplicate"
  statistic           = "Sum"
  period              = 300
  evaluation_periods  = 1
  threshold           = 100
  comparison_operator = "GreaterThanThreshold"
  treat_missing_data  = "notBreaching"
  alarm_actions       = local.ops_actions
  tags                = var.tags
}

# ── Calculation failures > 0 in 5-minute window ──────────────────────────────
# A tick for a known instrument that was neither applied nor classified as stale.
# Possible causes: unexpected instrument ID, corrupt tick, calculator state corruption.
resource "aws_cloudwatch_metric_alarm" "calculation_failures" {
  alarm_name          = "${var.service_name}-calculation-failures"
  alarm_description   = "Index calculation failures detected — ticks rejected outside normal stale/duplicate paths. Inspect calculator state and tick payload."
  namespace           = var.namespace
  metric_name         = "index.calculation.failures"
  statistic           = "Sum"
  period              = 300
  evaluation_periods  = 1
  threshold           = 0
  comparison_operator = "GreaterThanThreshold"
  treat_missing_data  = "notBreaching"
  alarm_actions       = local.ops_actions
  tags                = var.tags
}

# ── Dropped messages > 0 in 1-minute window ──────────────────────────────────
# InstrumentId == 0 means the binary codec could not decode the payload.
# Any decode failure is a data-quality event requiring immediate investigation.
resource "aws_cloudwatch_metric_alarm" "dropped_messages" {
  alarm_name          = "${var.service_name}-dropped-messages"
  alarm_description   = "Tick decode failures (dropped messages) detected — binary codec mismatch or corrupt Kafka payload. Check ingestion worker codec version."
  namespace           = var.namespace
  metric_name         = "index.messages.dropped"
  statistic           = "Sum"
  period              = 60
  evaluation_periods  = 1
  threshold           = 0
  comparison_operator = "GreaterThanThreshold"
  treat_missing_data  = "notBreaching"
  alarm_actions       = local.critical_actions
  tags                = var.tags
}

# ── Stale tick rate > 1% of messages (metric math alarm) ─────────────────────
resource "aws_cloudwatch_metric_alarm" "stale_tick_rate" {
  alarm_name          = "${var.service_name}-stale-tick-rate-high"
  alarm_description   = "Stale tick rate exceeded 1% — feed sending out-of-order data or engine clock is skewed relative to exchange timestamps."
  comparison_operator = "GreaterThanThreshold"
  evaluation_periods  = 2
  threshold           = 1 # 1%
  treat_missing_data  = "notBreaching"
  alarm_actions       = local.ops_actions
  tags                = var.tags

  metric_query {
    id          = "msg_in"
    return_data = false
    metric {
      namespace   = var.namespace
      metric_name = "index.messages.in"
      period      = 300
      stat        = "Sum"
    }
  }

  metric_query {
    id          = "stale"
    return_data = false
    metric {
      namespace   = var.namespace
      metric_name = "index.ticks.stale"
      period      = 300
      stat        = "Sum"
    }
  }

  metric_query {
    id          = "stale_pct"
    expression  = "IF(msg_in > 0, stale / msg_in * 100, 0)"
    label       = "StaleTickPercent"
    return_data = true
  }
}

# ── Publisher worker consumer lag > 5 000 ────────────────────────────────────
# Publisher is falling behind; Postgres index_values table has stale data.
resource "aws_cloudwatch_metric_alarm" "publisher_lag" {
  alarm_name          = "${var.service_name}-publisher-lag-high"
  alarm_description   = "Publisher worker consumer lag exceeded 5 000 — Postgres index_values table is falling behind the write stream."
  namespace           = "Opticverge/IndexPublisher"
  metric_name         = "publisher.consumer.lag"
  statistic           = "Maximum"
  period              = var.evaluation_period_seconds
  evaluation_periods  = 3
  threshold           = 5_000
  comparison_operator = "GreaterThanThreshold"
  treat_missing_data  = "notBreaching"
  alarm_actions       = local.ops_actions
  tags                = var.tags
}

# ── Gen 2 GC collections ─────────────────────────────────────────────────────
# Server GC + SustainedLowLatency should suppress Gen 2 entirely.
# Any collection that fires indicates allocation leaking onto the hot path.
resource "aws_cloudwatch_metric_alarm" "gen2_gc" {
  alarm_name          = "${var.service_name}-gen2-gc-rate"
  alarm_description   = "Gen 2 GC collections detected — SustainedLowLatency mode is not suppressing collections. Investigate allocation on the hot path."
  namespace           = var.namespace
  metric_name         = "process.runtime.dotnet.gc.collections.count"
  dimensions = {
    generation = "gen2"
  }
  statistic           = "Sum"
  period              = 300
  evaluation_periods  = 1
  threshold           = 0
  comparison_operator = "GreaterThanThreshold"
  treat_missing_data  = "notBreaching"
  alarm_actions       = local.ops_actions
  tags                = var.tags
}
