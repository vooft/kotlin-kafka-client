package io.github.vooft.kafka.network

import io.github.vooft.kafka.network.dto.KafkaRequest
import io.github.vooft.kafka.network.dto.KafkaResponse

interface NetworkClient {
    suspend fun connect(host: String, port: Int): KafkaConnection
}

interface KafkaConnection {
    suspend fun sendRequest(request: KafkaRequest)
    suspend fun receiveResponse(): KafkaResponse
}
