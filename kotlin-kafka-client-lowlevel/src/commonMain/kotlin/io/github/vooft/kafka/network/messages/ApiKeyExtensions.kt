package io.github.vooft.kafka.network.messages

import io.github.vooft.kafka.network.common.toInt16String
import io.github.vooft.kafka.network.headers.KafkaRequestHeaderV1

fun KafkaRequest.nextHeader(clientId: String? = null) = KafkaRequestHeaderV1(
    apiKey = apiKey,
    correlationId = CorrelationId.next(),
    apiVersion = apiVersion,
    clientId = clientId.toInt16String()
)
