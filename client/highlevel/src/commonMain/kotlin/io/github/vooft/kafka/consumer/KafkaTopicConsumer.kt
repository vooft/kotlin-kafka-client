package io.github.vooft.kafka.consumer

import io.github.vooft.kafka.serialization.common.wrappers.KafkaTopic
import io.github.vooft.kafka.serialization.common.wrappers.PartitionIndex
import io.github.vooft.kafka.serialization.common.wrappers.PartitionOffset
import kotlinx.io.Source

interface KafkaTopicConsumer {
    val topic: KafkaTopic
    suspend fun consume(): KafkaRecordsBatch
}

data class KafkaRecord(
    val partition: PartitionIndex,
    val offset: PartitionOffset,
    val key: Source,
    val value: Source,
)

data class KafkaRecordsBatch(
    val topic: KafkaTopic,
    val records: List<KafkaRecord>,
) : List<KafkaRecord> by records
