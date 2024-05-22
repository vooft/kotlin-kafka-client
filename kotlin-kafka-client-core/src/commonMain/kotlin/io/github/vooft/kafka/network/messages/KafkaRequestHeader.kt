package io.github.vooft.kafka.network.messages

import io.github.vooft.kafka.serialization.common.customtypes.Int16String
import kotlinx.serialization.Serializable

sealed interface KafkaRequestHeader: Versioned {
    val apiKey: ApiKey
    override val apiVersion: ApiVersion
    val correlationId: CorrelationId
}

@Serializable
data class KafkaRequestHeaderV1(
    override val apiKey: ApiKey,
    override val apiVersion: ApiVersion,
    override val correlationId: CorrelationId,
    val clientId: Int16String = Int16String.NULL
) : KafkaRequestHeader, VersionedV1

