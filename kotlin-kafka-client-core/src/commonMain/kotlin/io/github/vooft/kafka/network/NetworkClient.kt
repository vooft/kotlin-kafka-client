package io.github.vooft.kafka.network

import io.github.vooft.kafka.network.messages.KafkaRequest
import io.github.vooft.kafka.network.messages.KafkaResponse

interface NetworkClient {
    suspend fun connect(host: String, port: Int): KafkaConnection
}

interface KafkaConnection {
    suspend fun sendRequest(request: KafkaRequest)
    suspend fun receiveResponse(): KafkaResponse
    suspend fun close()
}
