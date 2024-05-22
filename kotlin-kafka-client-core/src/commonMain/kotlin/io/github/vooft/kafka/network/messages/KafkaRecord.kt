package io.github.vooft.kafka.network.messages

import io.github.vooft.kafka.serialization.common.IntEncoding
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
data class KafkaRecordHeader(val key: VarIntString, val value: VarIntByteArray)

/*
    .writeInt8(0) // no used record attributes at the moment
    .writeVarLong(timestampDelta)
    .writeVarInt(offsetDelta)
    .writeVarIntBytes(key)
    .writeVarIntBytes(value)
    .writeVarIntArray(headersArray.map(Header))
 */
@Serializable
data class KafkaRecordBody(
    val attributes: Byte = 0,
    val timestampDelta: VarLong = 0.toVarLong(),
    val offsetDelta: VarInt, // index of the current record, starting from 0
    val key: VarIntByteArray,
    val value: VarIntByteArray,
    @KafkaCollectionWithVarIntSize val headers: List<KafkaRecordHeader> = listOf()
)

@Serializable
data class KafkaRecord(
    @KafkaSizeInBytesPrefixed(encoding = IntEncoding.VARINT) val recordBody: KafkaRecordBody
)

/**
 * RecordBatch =>
 *   FirstOffset => int64
 *   Length => int32
 *   PartitionLeaderEpoch => int32
 *   Magic => int8
 *   CRC => int32
 *   Attributes => int16
 *   LastOffsetDelta => int32
 *   FirstTimestamp => int64
 *   MaxTimestamp => int64
 *   ProducerId => int64
 *   ProducerEpoch => int16
 *   FirstSequence => int32
 *   Records => [KafkaRecord]
 */
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
    @KafkaSizeInBytesPrefixed val batch: KafkaRecordBatch
)


/*
    .writeInt32(partitionLeaderEpoch)
    .writeInt8(MAGIC_BYTE)
    .writeUInt32(crc32C(batchBody.buffer))
    .writeEncoder(batchBody)

  const batchBody = new Encoder()
    .writeInt16(attributes)
    .writeInt32(lastOffsetDelta)
    .writeInt64(firstTimestamp)
    .writeInt64(maxTimestamp)
    .writeInt64(producerId)
    .writeInt16(producerEpoch)
    .writeInt32(firstSequence)
    .writeArray(records)
 */

