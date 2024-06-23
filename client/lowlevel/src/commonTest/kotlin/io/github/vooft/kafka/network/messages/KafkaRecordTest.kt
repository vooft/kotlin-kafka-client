package io.github.vooft.kafka.network.messages

import io.github.vooft.kafka.serialization.KafkaSerde
import io.github.vooft.kafka.serialization.common.primitives.VarInt
import io.github.vooft.kafka.serialization.common.primitives.VarIntByteArray
import io.github.vooft.kafka.serialization.common.primitives.VarIntBytesSizePrefixed
import io.github.vooft.kafka.serialization.common.primitives.VarIntList
import io.github.vooft.kafka.serialization.common.primitives.VarIntString
import io.github.vooft.kafka.serialization.common.primitives.VarLong
import io.github.vooft.kafka.serialization.decode
import io.github.vooft.kafka.serialization.encode
import io.github.vooft.kafka.transport.messages.KafkaRecordV0
import io.github.vooft.kafka.transport.messages.KafkaRecordV0.KafkaRecordBody
import io.github.vooft.kafka.transport.messages.KafkaRecordV0.KafkaRecordBody.KafkaRecordHeader
import io.kotest.matchers.shouldBe
import kotlinx.io.readByteArray
import kotlin.test.Test

class KafkaRecordTest {

    private val header = KafkaRecordHeader(VarIntString("test-key"), VarIntByteArray("test-value"))
    private val encodedHeader = byteArrayOf(
        0x10, // varint length test-key
        0x74, 0x65, 0x73, 0x74, 0x2D, 0x6B, 0x65, 0x79, // test-key

        0x14, // varint length test-value
        0x74, 0x65, 0x73, 0x74, 0x2D, 0x76, 0x61, 0x6C, 0x75, 0x65 // test-value
    )

    private val recordBody = KafkaRecordBody(
        attributes = 5,
        timestampDelta = VarLong.fromDecoded(10),
        offsetDelta = VarInt.fromDecoded(1),
        recordKey = VarIntByteArray("key"),
        recordValue = VarIntByteArray("value"),
        headers = VarIntList(listOf(header))
    )
    private val encodedRecordBody = byteArrayOf(
        0x05, // attributes
        0x14, // timestampDelta varlong

        0x02, // offsetDelta

        0x06, // varint length 3
        0x6B, 0x65, 0x79, // key

        0x0A, // varint length 4
        0x76, 0x61, 0x6C, 0x75, 0x65, // value

        0x02, // varint headers count
        *encodedHeader
    )

    private val record = KafkaRecordV0(
        recordBody = VarIntBytesSizePrefixed(recordBody)
    )
    private val encodedRecord = byteArrayOf(
        0x44, // recordBody size
        *encodedRecordBody
    )

    @Test
    fun should_encode_kafka_record_header() {
        val actual = KafkaSerde.encode(header)
        actual.readByteArray() shouldBe encodedHeader
    }

    @Test
    fun should_decode_kafka_record_header() {
        val actual = KafkaSerde.decode<KafkaRecordHeader>(encodedHeader)
        actual shouldBe header
    }

    @Test
    fun should_encode_kafka_record_body() {
        val actual = KafkaSerde.encode(recordBody)
        actual.readByteArray() shouldBe encodedRecordBody
    }

    @Test
    fun should_decode_kafka_record_body() {
        val actual = KafkaSerde.decode<KafkaRecordBody>(encodedRecordBody)
        actual shouldBe recordBody
    }

    @Test
    fun should_encode_kafka_record() {
        val actual = KafkaSerde.encode(record)
        actual.readByteArray() shouldBe encodedRecord
    }

    @Test
    fun should_decode_kafka_record() {
        val actual = KafkaSerde.decode<KafkaRecordV0>(encodedRecord)
        actual shouldBe record
    }
}
