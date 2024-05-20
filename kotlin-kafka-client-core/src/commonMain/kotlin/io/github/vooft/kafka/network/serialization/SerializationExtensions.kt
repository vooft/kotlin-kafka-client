package io.github.vooft.kafka.network.serialization

import io.github.vooft.kafka.network.messages.ApiKey
import io.github.vooft.kafka.network.messages.ApiVersion
import io.github.vooft.kafka.network.messages.KafkaRequestHeader
import io.github.vooft.kafka.network.messages.KafkaRequestHeaderV0
import io.github.vooft.kafka.network.messages.KafkaRequestHeaderV1
import io.github.vooft.kafka.network.messages.KafkaResponseHeader
import io.github.vooft.kafka.network.messages.KafkaResponseHeaderV0
import io.github.vooft.kafka.network.messages.KafkaResponseHeaderV1
import io.github.vooft.kafka.network.messages.apiVersion
import io.github.vooft.kafka.serialization.encode
import kotlinx.io.Sink
import kotlinx.serialization.DeserializationStrategy

fun ApiVersion.responseHeaderDeserializer(): DeserializationStrategy<KafkaResponseHeader> = when (this) {
    ApiVersion.V0 -> KafkaResponseHeaderV0.serializer()
    ApiVersion.V1 -> KafkaResponseHeaderV1.serializer()
}

fun ApiKey.responseHeaderDeserializer(): DeserializationStrategy<KafkaResponseHeader> = apiVersion().responseHeaderDeserializer()

fun Sink.encodeHeader(requestHeader: KafkaRequestHeader) = when (requestHeader) {
    is KafkaRequestHeaderV0 -> encode(KafkaRequestHeaderV0.serializer(), requestHeader)
    is KafkaRequestHeaderV1 -> encode(KafkaRequestHeaderV1.serializer(), requestHeader)
}
