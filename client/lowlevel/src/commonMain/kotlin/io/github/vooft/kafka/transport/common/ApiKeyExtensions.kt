package io.github.vooft.kafka.transport.common

import io.github.vooft.kafka.serialization.common.primitives.NullableInt16String
import io.github.vooft.kafka.transport.dtos.CorrelationId
import io.github.vooft.kafka.transport.dtos.KafkaRequest
import io.github.vooft.kafka.transport.dtos.KafkaRequestHeader

fun KafkaRequest.nextHeader(clientId: String? = null) = KafkaRequestHeader(
    apiKey = apiKey,
    correlationId = CorrelationId.next(),
    apiVersion = apiVersion,
    clientId = NullableInt16String(clientId)
)

