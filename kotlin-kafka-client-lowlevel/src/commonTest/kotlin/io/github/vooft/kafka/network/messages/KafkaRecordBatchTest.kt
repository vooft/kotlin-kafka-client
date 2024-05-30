package io.github.vooft.kafka.network.messages

import io.github.vooft.kafka.network.common.toVarInt
import io.github.vooft.kafka.network.messages.KafkaRecordBatchContainerV0.KafkaRecordBatch
import io.github.vooft.kafka.network.messages.KafkaRecordBatchContainerV0.KafkaRecordBatch.KafkaRecordBatchBody
import io.github.vooft.kafka.serialization.KafkaSerde
import io.github.vooft.kafka.serialization.common.primitives.Crc32cPrefixed
import io.github.vooft.kafka.serialization.common.primitives.Int32BytesSizePrefixed
import io.github.vooft.kafka.serialization.common.primitives.Int32List
import io.github.vooft.kafka.serialization.common.primitives.VarIntByteArray
import io.github.vooft.kafka.serialization.common.primitives.VarIntBytesSizePrefixed
import io.github.vooft.kafka.serialization.decode
import io.github.vooft.kafka.serialization.encode
import io.kotest.matchers.shouldBe
import kotlinx.io.readByteArray
import kotlin.test.Test

class KafkaRecordBatchTest {
    private val record = KafkaRecordV0(
        VarIntBytesSizePrefixed(
            KafkaRecordV0.KafkaRecordBody(
                offsetDelta = 0.toVarInt(),
                recordKey = VarIntByteArray(byteArrayOf()),
                recordValue = VarIntByteArray(byteArrayOf())
            )
        )
    )
    private val encodedRecord = KafkaSerde.encode(record).readByteArray()

    private val batchBody = KafkaRecordBatchBody(
        attributes = 1,
        lastOffsetDelta = 2,
        firstTimestamp = 3,
        maxTimestamp = 4,
        firstSequence = 5,
        records = Int32List(record)
    )
    private val encodedBatchBody = byteArrayOf(
        0x00, 0x01, // attributes
        0x00, 0x00, 0x00, 0x02, // lastOffsetDelta
        0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x03, // firstTimestamp
        0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x04, // maxTimestamp
        FF, FF, FF, FF, FF, FF, FF, FF, // producerId
        0x00, 0x00, // producerEpoch
        0x00, 0x00, 0x00, 0x05, // firstSequence

        0x00, 0x00, 0x00, 0x01, // records count
        *encodedRecord
    )

    private val batch = KafkaRecordBatch(
        partitionLeaderEpoch = 1,
        magic = 2,
        body = Crc32cPrefixed(batchBody)
    )
    private val encodedBatch = byteArrayOf(
        0x00, 0x00, 0x00, 0x01, // partitionLeaderEpoch
        0x02, // magic
        0x5A, 0x32, 0x41, 0xDF.toByte(), // crc32c
        *encodedBatchBody
    )

    private val batchContainer = KafkaRecordBatchContainerV0(
        firstOffset = 1,
        batch = Int32BytesSizePrefixed(batch)
    )
    private val batchContainerEncoded = byteArrayOf(
        0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x01, // firstOffset
        0x00, 0x00, 0x00, 0x38, // batch bytes size
        *encodedBatch
    )

    @Test
    fun should_encode_batch_body() {
        val actual = KafkaSerde.encode(batchBody)
        actual.readByteArray() shouldBe encodedBatchBody
    }

    @Test
    fun should_decode_batch_body() {
        val actual = KafkaSerde.decode<KafkaRecordBatchBody>(encodedBatchBody)
        actual shouldBe batchBody
    }

    @Test
    fun should_encode_batch() {
        val actual = KafkaSerde.encode(batch)
        actual.readByteArray() shouldBe encodedBatch
    }

    @Test
    fun should_decode_batch() {
        val actual = KafkaSerde.decode<KafkaRecordBatch>(encodedBatch)
        actual shouldBe batch
    }

    @Test
    fun should_encode_batch_container() {
        val actual = KafkaSerde.encode(batchContainer)
        actual.readByteArray() shouldBe batchContainerEncoded
    }

    @Test
    fun should_decode_batch_container() {
        val actual = KafkaSerde.decode<KafkaRecordBatchContainerV0>(batchContainerEncoded)
        actual shouldBe batchContainer
    }
}

private const val FF = 0xFF.toByte()
