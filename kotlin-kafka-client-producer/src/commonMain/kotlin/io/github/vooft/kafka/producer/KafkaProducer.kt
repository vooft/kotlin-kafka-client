package io.github.vooft.kafka.producer

import io.github.vooft.kafka.network.messages.ErrorCode
import kotlinx.io.Buffer
import kotlinx.io.Source
import kotlinx.io.writeString

interface KafkaProducer {
    suspend fun send(topic: String, record: ProducedRecord): RecordMetadata
}

data class RecordMetadata(val topic: String, val partition: Int, val errorCode: ErrorCode)

interface KafkaConnectionProperties {
    val host: String
    val port: Int
}

data class ProducedRecord(val key: Source, val value: Source)

fun ProducedRecord(key: String, value: String) = ProducedRecord(Buffer().apply { writeString(key) }, Buffer().apply { writeString(value) })
