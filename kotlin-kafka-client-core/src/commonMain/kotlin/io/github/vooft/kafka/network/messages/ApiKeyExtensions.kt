package io.github.vooft.kafka.network.messages

import kotlinx.serialization.DeserializationStrategy

fun ApiKey.requestApiVersion(): ApiVersion = when(this) {
    ApiKey.API_VERSIONS -> ApiVersion.V1
}

fun ApiKey.responseApiVersion(): ApiVersion = when (this) {
    ApiKey.API_VERSIONS -> ApiVersion.V0
}

fun KafkaRequest.nextHeader(clientId: String? = null): KafkaRequestHeader = when (apiVersion) {
    ApiVersion.V0 -> KafkaRequestHeaderV0(apiKey = apiKey, correlationId = CorrelationId.next())
    ApiVersion.V1 -> KafkaRequestHeaderV1(apiKey = apiKey, correlationId = CorrelationId.next(), clientId = clientId)
}

fun ApiKey.responseHeaderDeserializer() = when (responseApiVersion()) {
    ApiVersion.V0 -> KafkaResponseHeaderV0.serializer()
    ApiVersion.V1 -> KafkaResponseHeaderV1.serializer()
}

fun KafkaRequest.responseHeaderDeserializer(): DeserializationStrategy<KafkaResponseHeader> = apiKey.responseHeaderDeserializer()
