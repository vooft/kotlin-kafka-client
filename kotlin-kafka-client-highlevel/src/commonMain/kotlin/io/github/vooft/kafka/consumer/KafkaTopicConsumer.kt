package io.github.vooft.kafka.consumer

import io.github.vooft.kafka.common.PartitionIndex
import kotlinx.io.Source

interface KafkaTopicConsumer {
    val topic: String
    suspend fun consume(): KafkaRecordsBatch
}

data class KafkaRecord(
    val partition: PartitionIndex,
    val offset: Long,
    val key: Source,
    val value: Source,
)

data class KafkaRecordsBatch(
    val topic: String,
    val records: List<KafkaRecord>,
) : List<KafkaRecord> by records
