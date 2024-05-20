package io.github.vooft.kafka.network.messages

import kotlinx.serialization.DeserializationStrategy

fun ApiKey.apiVersion(): ApiVersion = when(this) {
    ApiKey.API_VERSIONS -> ApiVersion.V1
}

fun KafkaRequest.nextHeader(clientId: String?): KafkaRequestHeader = apiKey.nextRequestHeader(clientId)

fun KafkaRequest.responseHeaderDeserializer(): DeserializationStrategy<KafkaResponseHeader> = when (this) {
    // api version response header has always version 0
    is ApiVersionRequest -> KafkaResponseHeaderV0.serializer()
    is VersionedV0 -> KafkaResponseHeaderV0.serializer()
    is VersionedV1 -> KafkaResponseHeaderV1.serializer()
}
