package io.github.vooft.kafka.network.serialization

import io.github.vooft.kafka.network.messages.KafkaRequestHeader
import io.github.vooft.kafka.network.messages.KafkaRequestHeaderV1
import io.github.vooft.kafka.serialization.encode
import kotlinx.io.Sink

fun Sink.encodeHeader(requestHeader: KafkaRequestHeader) = when (requestHeader) {
    is KafkaRequestHeaderV1 -> encode(KafkaRequestHeaderV1.serializer(), requestHeader)
}
