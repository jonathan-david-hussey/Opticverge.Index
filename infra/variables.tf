variable "ops_sns_topic_arn" {
  type        = string
  description = "SNS topic ARN for operational alerts (on-call rotation)"
}

variable "critical_sns_topic_arn" {
  type        = string
  description = "SNS topic ARN for critical alerts (immediate page)"
}

variable "service_name" {
  type    = string
  default = "index-engine"
}

variable "namespace" {
  type    = string
  default = "Opticverge/IndexEngine"
}

variable "evaluation_period_seconds" {
  type    = number
  default = 60
}

variable "ring_buffer_size" {
  type        = number
  default     = 65536
  description = "Must match Engine__RingBufferSize in the AppHost / ECS task definition"
}

variable "tags" {
  type    = map(string)
  default = {}
}
