package io.github.vooft.kafka.network.common

import io.github.vooft.kafka.network.headers.KafkaRequestHeader
import io.github.vooft.kafka.network.messages.CorrelationId
import io.github.vooft.kafka.network.messages.KafkaRequest

fun KafkaRequest.nextHeader(clientId: String? = null) = KafkaRequestHeader(
    apiKey = apiKey,
    correlationId = CorrelationId.next(),
    apiVersion = apiVersion,
    clientId = clientId.toNullableInt16String()
)

