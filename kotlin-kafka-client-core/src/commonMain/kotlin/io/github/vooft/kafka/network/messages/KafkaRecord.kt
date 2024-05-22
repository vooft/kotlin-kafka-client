package io.github.vooft.kafka.network.messages

import io.github.vooft.kafka.serialization.common.IntEncoding.INT32
import io.github.vooft.kafka.serialization.common.IntEncoding.VARINT
import io.github.vooft.kafka.serialization.common.KafkaCollectionWithVarIntSize
import io.github.vooft.kafka.serialization.common.KafkaCrc32Prefixed
import io.github.vooft.kafka.serialization.common.KafkaSizeInBytesPrefixed
import io.github.vooft.kafka.serialization.common.customtypes.VarInt
import io.github.vooft.kafka.serialization.common.customtypes.VarIntByteArray
import io.github.vooft.kafka.serialization.common.customtypes.VarIntString
import io.github.vooft.kafka.serialization.common.customtypes.VarLong
import io.github.vooft.kafka.serialization.common.customtypes.toVarLong
import kotlinx.serialization.Serializable

@Serializable
data class KafkaRecordHeader(val headerKey: VarIntString, val headerValue: VarIntByteArray)

@Serializable
data class KafkaRecordBody(
    val attributes: Byte = 0, // not used atm
    val timestampDelta: VarLong = 0.toVarLong(),
    val offsetDelta: VarInt, // index of the current record, starting from 0
    val recordKey: VarIntByteArray,
    val recordValue: VarIntByteArray,
    @KafkaCollectionWithVarIntSize val headers: List<KafkaRecordHeader> = listOf()
)

@Serializable
data class KafkaRecord(
    @KafkaSizeInBytesPrefixed(encoding = VARINT) val recordBody: KafkaRecordBody
)

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
    val records: List<KafkaRecord> // written with size
)

@Serializable
data class KafkaRecordBatch(
    val partitionLeaderEpoch: Int = 0,
    val magic: Byte = 2,
    @KafkaCrc32Prefixed val body: KafkaRecordBatchBody
)

@Serializable
data class KafkaRecordBatchContainer(
    val firstOffset: Long,
    @KafkaSizeInBytesPrefixed(encoding = INT32) val batch: KafkaRecordBatch
)


