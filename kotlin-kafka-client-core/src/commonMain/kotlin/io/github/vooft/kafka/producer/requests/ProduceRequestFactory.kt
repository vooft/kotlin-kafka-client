package io.github.vooft.kafka.producer.requests

import io.github.vooft.kafka.network.common.toVarInt
import io.github.vooft.kafka.network.common.toVarIntByteArray
import io.github.vooft.kafka.network.messages.KafkaRecordBatchContainerV0
import io.github.vooft.kafka.network.messages.KafkaRecordV0
import io.github.vooft.kafka.network.messages.ProduceRequestV3
import io.github.vooft.kafka.serialization.common.primitives.Crc32cPrefixed
import io.github.vooft.kafka.serialization.common.primitives.Int32BytesSizePrefixed
import io.github.vooft.kafka.serialization.common.primitives.VarIntBytesSizePrefixed
import io.github.vooft.kafka.serialization.common.primitives.int32ListOf
import io.github.vooft.kafka.serialization.common.primitives.toInt32List
import io.github.vooft.kafka.serialization.common.wrappers.KafkaTopic
import io.github.vooft.kafka.serialization.common.wrappers.PartitionIndex
import kotlinx.io.Source

object ProduceRequestFactory {
    fun createProduceRequest(topic: KafkaTopic, partitionIndex: PartitionIndex, records: List<ProducedRecord>) = ProduceRequestV3(
        timeoutMs = 1000,
        topic = int32ListOf(
            ProduceRequestV3.Topic(
                topic = topic,
                partition = int32ListOf(
                    ProduceRequestV3.Topic.Partition(
                        partition = partitionIndex,
                        batchContainer = Int32BytesSizePrefixed(
                            KafkaRecordBatchContainerV0(
                                batch = Int32BytesSizePrefixed(
                                    KafkaRecordBatchContainerV0.KafkaRecordBatch(
                                        body = Crc32cPrefixed(
                                            KafkaRecordBatchContainerV0.KafkaRecordBatch.KafkaRecordBatchBody(
                                                lastOffsetDelta = records.size - 1,
                                                firstTimestamp = 0,
                                                maxTimestamp = 0,
                                                records = records.mapIndexed { index, record ->
                                                    KafkaRecordV0(
                                                        recordBody = VarIntBytesSizePrefixed(
                                                            KafkaRecordV0.KafkaRecordBody(
                                                                offsetDelta = index.toVarInt(),
                                                                recordKey = record.key.toVarIntByteArray(),
                                                                recordValue = record.value.toVarIntByteArray()
                                                            )
                                                        )
                                                    )
                                                }.toInt32List()

                                            )
                                        )
                                    )
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
