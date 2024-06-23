package io.github.vooft.kafka.transport

import kotlinx.io.Source

interface KafkaTransport {
    suspend fun connect(host: String, port: Int): KafkaConnection
    suspend fun close()

    companion object
}

interface KafkaConnection {

    val isClosed: Boolean

    suspend fun writeMessage(source: Source)
    suspend fun readMessage(): Source

    suspend fun close()
}
