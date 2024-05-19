package io.github.vooft.kafka.network

import io.github.vooft.kafka.network.dto.ApiVersionRequest
import io.github.vooft.kafka.network.dto.KafkaRequest
import io.github.vooft.kafka.network.dto.KafkaRequestHeader
import kotlinx.io.Sink

fun Sink.write(header: KafkaRequestHeader) {
    writeShort(header.apiKey.value)
    writeShort(header.apiVersion.value)
    writeInt(header.correlationId.value)
    writeShort(-1)
}

fun Sink.write(request: KafkaRequest): Unit = when (request) {
    is ApiVersionRequest -> writeApiVersionRequest(request)
}

fun Sink.writeApiVersionRequest(apiVersionRequest: ApiVersionRequest) {
    // no payload
}
