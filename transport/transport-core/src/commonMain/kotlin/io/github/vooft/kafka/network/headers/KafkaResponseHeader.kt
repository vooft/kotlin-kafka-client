package io.github.vooft.kafka.network.headers

import io.github.vooft.kafka.network.messages.CorrelationId
import kotlinx.serialization.Serializable

@Serializable
data class KafkaResponseHeader(val correlationId: CorrelationId)
