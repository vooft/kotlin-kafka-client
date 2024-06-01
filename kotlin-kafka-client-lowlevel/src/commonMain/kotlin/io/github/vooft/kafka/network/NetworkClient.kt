package io.github.vooft.kafka.network

import io.github.vooft.kafka.network.messages.KafkaRequest
import io.github.vooft.kafka.network.messages.KafkaResponse
import kotlinx.serialization.DeserializationStrategy
import kotlinx.serialization.SerializationStrategy
import kotlinx.serialization.serializer

interface NetworkClient {
    suspend fun connect(host: String, port: Int): KafkaConnection
}

interface KafkaConnection {
    suspend fun <Rq : KafkaRequest, Rs : KafkaResponse> sendRequest(
        request: Rq,
        requestSerializer: SerializationStrategy<Rq>,
        responseDeserializer: DeserializationStrategy<Rs>
    ): Rs

    suspend fun close()
}

internal suspend inline fun <reified Rq: KafkaRequest, reified Rs: KafkaResponse> KafkaConnection.sendRequest(request: Rq): Rs =
    sendRequest(request, serializer<Rq>(), serializer<Rs>())
