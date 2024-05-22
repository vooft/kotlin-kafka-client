package io.github.vooft.kafka.network.messages

import kotlinx.serialization.Serializable

sealed interface KafkaResponseHeader : Versioned {
    val correlationId: CorrelationId
}

@Serializable
data class KafkaResponseHeaderV0(override val correlationId: CorrelationId) : KafkaResponseHeader, VersionedV0

@Serializable
data class KafkaResponseHeaderV1(override val correlationId: CorrelationId, val tags: List<RawTaggedField>) : KafkaResponseHeader, VersionedV1 {
    @Serializable
    data class RawTaggedField(val tag: Int, val data: ByteArray)
}
