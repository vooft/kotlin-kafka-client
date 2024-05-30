package io.github.vooft.kafka.network.headers

import io.github.vooft.kafka.network.messages.ApiKey
import io.github.vooft.kafka.network.messages.ApiVersion
import io.github.vooft.kafka.network.messages.CorrelationId
import io.github.vooft.kafka.serialization.common.primitives.NullableInt16String
import kotlinx.serialization.Serializable

sealed interface KafkaRequestHeader {
    val apiKey: ApiKey
    val apiVersion: ApiVersion
    val correlationId: CorrelationId
}

@Serializable
data class KafkaRequestHeaderV1(
    override val apiKey: ApiKey,
    override val apiVersion: ApiVersion,
    override val correlationId: CorrelationId,
    val clientId: NullableInt16String = NullableInt16String.NULL
) : KafkaRequestHeader

