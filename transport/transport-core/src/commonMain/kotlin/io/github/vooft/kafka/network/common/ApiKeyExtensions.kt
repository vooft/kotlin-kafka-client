package io.github.vooft.kafka.network.common

import io.github.vooft.kafka.network.dtos.CorrelationId
import io.github.vooft.kafka.network.dtos.KafkaRequest
import io.github.vooft.kafka.network.dtos.KafkaRequestHeader

fun KafkaRequest.nextHeader(clientId: String? = null) = KafkaRequestHeader(
    apiKey = apiKey,
    correlationId = CorrelationId.next(),
    apiVersion = apiVersion,
    clientId = clientId.toNullableInt16String()
)

