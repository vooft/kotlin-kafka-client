package io.github.vooft.kafka.transport.dtos

import kotlinx.serialization.Serializable

@Serializable
data class KafkaResponseHeader(val correlationId: CorrelationId)
