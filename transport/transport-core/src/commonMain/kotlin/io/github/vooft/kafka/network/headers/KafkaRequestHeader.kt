package io.github.vooft.kafka.network.headers

import io.github.vooft.kafka.network.messages.ApiKey
import io.github.vooft.kafka.network.messages.ApiVersion
import io.github.vooft.kafka.network.messages.CorrelationId
import io.github.vooft.kafka.serialization.common.primitives.NullableInt16String
import kotlinx.serialization.Serializable

@Serializable
data class KafkaRequestHeader(
    val apiKey: ApiKey,
    val apiVersion: ApiVersion,
    val correlationId: CorrelationId,
    val clientId: NullableInt16String = NullableInt16String.NULL
)

