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
    suspend fun <T: KafkaRequest> sendRequest(serializationStrategy: SerializationStrategy<T>, request: T)
    suspend fun <T: KafkaResponse> receiveResponse(deserializationStrategy: DeserializationStrategy<T>): T
    suspend fun close()
}

suspend inline fun <reified T: KafkaRequest> KafkaConnection.sendRequest(request: T) = sendRequest(serializer<T>(), request)
suspend inline fun <reified T: KafkaResponse> KafkaConnection.receiveResponse() = receiveResponse(serializer<T>())
