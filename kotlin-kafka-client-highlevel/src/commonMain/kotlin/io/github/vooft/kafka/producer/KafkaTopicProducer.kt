package io.github.vooft.kafka.producer

import io.github.vooft.kafka.common.KafkaTopic
import io.github.vooft.kafka.common.PartitionIndex
import io.github.vooft.kafka.network.messages.ErrorCode
import kotlinx.io.Buffer
import kotlinx.io.Source
import kotlinx.io.writeString

interface KafkaTopicProducer {
    val topic: KafkaTopic
    suspend fun send(key: Source, value: Source): RecordMetadata
}

suspend fun KafkaTopicProducer.send(key: String, value: String) = send(Buffer().apply { writeString(key) }, Buffer().apply { writeString(value) })

data class RecordMetadata(val topic: KafkaTopic, val partition: PartitionIndex, val errorCode: ErrorCode)

