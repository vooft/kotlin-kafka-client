package io.github.vooft.kafka.network.headers

import io.github.vooft.kafka.network.messages.CorrelationId
import kotlinx.serialization.Serializable

sealed interface KafkaResponseHeader {
    val correlationId: CorrelationId
}

@Serializable
data class KafkaResponseHeaderV0(override val correlationId: CorrelationId) : KafkaResponseHeader
