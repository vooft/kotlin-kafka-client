package io.github.vooft.kafka.transport.messages

import io.github.vooft.kafka.serialization.common.primitives.Crc32cPrefixed
import io.github.vooft.kafka.serialization.common.primitives.Int32BytesSizePrefixed
import io.github.vooft.kafka.serialization.common.primitives.Int32List
import io.github.vooft.kafka.serialization.common.primitives.VarInt
import io.github.vooft.kafka.serialization.common.primitives.VarIntByteArray
import io.github.vooft.kafka.serialization.common.primitives.VarIntBytesSizePrefixed
import io.github.vooft.kafka.serialization.common.primitives.VarIntList
import io.github.vooft.kafka.serialization.common.primitives.VarIntString
import io.github.vooft.kafka.serialization.common.primitives.VarLong
import io.github.vooft.kafka.serialization.common.wrappers.PartitionOffset
import io.github.vooft.kafka.transport.common.toVarLong
import kotlinx.serialization.Serializable

@Serializable
data class KafkaRecordV0(
    val recordBody: VarIntBytesSizePrefixed<KafkaRecordBody>
) {
    @Serializable
    data class KafkaRecordBody(
        val attributes: Byte = 0, // not used atm
        val timestampDelta: VarLong = 0.toVarLong(),
        val offsetDelta: VarInt, // index of the current record, starting from 0
        val recordKey: VarIntByteArray,
        val recordValue: VarIntByteArray,
        val headers: VarIntList<KafkaRecordHeader> = VarIntList(emptyList())
    ) {
        @Serializable
        data class KafkaRecordHeader(val headerKey: VarIntString, val headerValue: VarIntByteArray)
    }
}

@Serializable
data class KafkaRecordBatchContainerV0(
    val firstOffset: PartitionOffset = PartitionOffset(0),
    val batch: Int32BytesSizePrefixed<KafkaRecordBatch>
) {
    @Serializable
    data class KafkaRecordBatch(
        val partitionLeaderEpoch: Int = 0,
        val magic: Byte = 2,
        val body: Crc32cPrefixed<KafkaRecordBatchBody>
    ) {
        @Serializable
        data class KafkaRecordBatchBody(
            val attributes: Short = 0, // compression, transaction and timestamp mask?
            val lastOffsetDelta: Int, // records.length - 1

            // timestamps from records
            val firstTimestamp: Long,
            val maxTimestamp: Long,

            val producerId: Long = -1, // for transactions
            val producerEpoch: Short = 0, // ignored
            val firstSequence: Int = 0, // for transactions
            val records: Int32List<KafkaRecordV0> // written with size
        )
    }


}


