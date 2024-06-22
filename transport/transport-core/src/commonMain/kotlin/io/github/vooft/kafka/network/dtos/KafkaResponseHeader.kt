package io.github.vooft.kafka.network.dtos

import kotlinx.serialization.Serializable

@Serializable
data class KafkaResponseHeader(val correlationId: CorrelationId)
