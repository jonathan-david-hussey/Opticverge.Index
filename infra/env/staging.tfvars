service_name           = "index-engine-staging"
namespace              = "Opticverge/IndexEngine/Staging"
ops_sns_topic_arn      = "arn:aws:sns:eu-west-1:123456789012:ops-staging"
critical_sns_topic_arn = "arn:aws:sns:eu-west-1:123456789012:critical-staging"
ring_buffer_size       = 65536

tags = {
  Environment = "staging"
  Service     = "index-engine"
  Team        = "platform"
}
