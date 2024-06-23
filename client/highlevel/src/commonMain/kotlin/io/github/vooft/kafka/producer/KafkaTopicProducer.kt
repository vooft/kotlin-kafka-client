package io.github.vooft.kafka.producer

import io.github.vooft.kafka.serialization.common.wrappers.KafkaTopic
import io.github.vooft.kafka.serialization.common.wrappers.PartitionIndex
import io.github.vooft.kafka.transport.common.ErrorCode
import kotlinx.io.Buffer
import kotlinx.io.Source
import kotlinx.io.writeString

interface KafkaTopicProducer {
    val topic: KafkaTopic
    suspend fun send(key: Source, value: Source): RecordMetadata
}

suspend fun KafkaTopicProducer.send(key: String, value: String) = send(
    key = Buffer().apply { writeString(key) },
    value = Buffer().apply { writeString(value) }
)

data class RecordMetadata(val topic: KafkaTopic, val partition: PartitionIndex, val errorCode: ErrorCode)

