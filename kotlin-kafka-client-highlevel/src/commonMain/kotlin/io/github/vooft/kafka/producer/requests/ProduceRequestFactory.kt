package io.github.vooft.kafka.producer.requests

import io.github.vooft.kafka.common.KafkaTopic
import io.github.vooft.kafka.common.PartitionIndex
import io.github.vooft.kafka.network.common.toVarInt
import io.github.vooft.kafka.network.common.toVarIntByteArray
import io.github.vooft.kafka.network.messages.KafkaRecordBatchContainerV0
import io.github.vooft.kafka.network.messages.KafkaRecordV0
import io.github.vooft.kafka.network.messages.ProduceRequestV3
import io.github.vooft.kafka.serialization.common.primitives.VarIntBytesSizePrefixed
import kotlinx.io.Source

object ProduceRequestFactory {
    fun createProduceRequest(topic: KafkaTopic, partitionIndex: PartitionIndex, records: List<ProducedRecord>) = ProduceRequestV3(
        topicData = listOf(
            ProduceRequestV3.TopicData(
                topic = topic,
                partitionData = listOf(
                    ProduceRequestV3.TopicData.PartitionData(
                        partition = partitionIndex,
                        batchContainer = KafkaRecordBatchContainerV0(
                            batch = KafkaRecordBatchContainerV0.KafkaRecordBatch(
                                body = KafkaRecordBatchContainerV0.KafkaRecordBatch.KafkaRecordBatchBody(
                                    lastOffsetDelta = records.size - 1,
                                    firstTimestamp = 0,
                                    maxTimestamp = 0,
                                    records = records.mapIndexed { index, it ->
                                        KafkaRecordV0(
                                            recordBody = VarIntBytesSizePrefixed(
                                                KafkaRecordV0.KafkaRecordBody(
                                                    offsetDelta = index.toVarInt(),
                                                    recordKey = it.key.toVarIntByteArray(),
                                                    recordValue = it.value.toVarIntByteArray()
                                                )
                                            )
                                        )
                                    }

                                )
                            )
                        )

                    )
                )
            )
        )
    )
}

data class ProducedRecord(val key: Source, val value: Source)
