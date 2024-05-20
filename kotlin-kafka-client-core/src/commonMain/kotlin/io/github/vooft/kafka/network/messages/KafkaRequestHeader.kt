package io.github.vooft.kafka.network.messages

import kotlinx.serialization.Serializable

sealed interface KafkaRequestHeader: Versioned {
    val apiKey: ApiKey
    override val apiVersion: ApiVersion
    val correlationId: CorrelationId
}

@Serializable
data class KafkaRequestHeaderV0(
    override val apiKey: ApiKey,
    override val apiVersion: ApiVersion = ApiVersion.V0,
    override val correlationId: CorrelationId,
) : KafkaRequestHeader, VersionedV0

@Serializable
data class KafkaRequestHeaderV1(
    override val apiKey: ApiKey,
    override val apiVersion: ApiVersion = ApiVersion.V1,
    override val correlationId: CorrelationId,
    val clientId: String? = null
) : KafkaRequestHeader, VersionedV1

