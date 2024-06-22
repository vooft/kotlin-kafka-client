package io.github.vooft.kafka.network.common

import io.github.vooft.kafka.network.dtos.CorrelationId
import io.github.vooft.kafka.network.dtos.KafkaRequest
import io.github.vooft.kafka.network.dtos.KafkaRequestHeader
import io.github.vooft.kafka.serialization.common.primitives.NullableInt16String

fun KafkaRequest.nextHeader(clientId: String? = null) = KafkaRequestHeader(
    apiKey = apiKey,
    correlationId = CorrelationId.next(),
    apiVersion = apiVersion,
    clientId = NullableInt16String(clientId)
)

