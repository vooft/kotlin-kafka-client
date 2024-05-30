package io.github.vooft.kafka.network.messages

import io.github.vooft.kafka.network.common.ErrorCode
import io.github.vooft.kafka.serialization.KafkaSerde
import io.github.vooft.kafka.serialization.common.primitives.Crc32cPrefixed
import io.github.vooft.kafka.serialization.common.primitives.Int32BytesSizePrefixed
import io.github.vooft.kafka.serialization.common.primitives.VarInt
import io.github.vooft.kafka.serialization.common.primitives.VarIntByteArray
import io.github.vooft.kafka.serialization.common.primitives.VarIntBytesSizePrefixed
import io.github.vooft.kafka.serialization.common.primitives.int32ListOf
import io.github.vooft.kafka.serialization.common.wrappers.KafkaTopic
import io.github.vooft.kafka.serialization.common.wrappers.PartitionIndex
import io.github.vooft.kafka.serialization.decode
import io.github.vooft.kafka.serialization.encode
import io.kotest.matchers.shouldBe
import kotlinx.io.readByteArray
import kotlin.test.Test

class ProduceTest {
    private val batchContainer = KafkaRecordBatchContainerV0(
        batch = Int32BytesSizePrefixed(
            KafkaRecordBatchContainerV0.KafkaRecordBatch(
                body = Crc32cPrefixed(
                    KafkaRecordBatchContainerV0.KafkaRecordBatch.KafkaRecordBatchBody(
                        lastOffsetDelta = 10,
                        firstTimestamp = 123L,
                        maxTimestamp = 456L,
                        records = int32ListOf(
                            KafkaRecordV0(
                                VarIntBytesSizePrefixed(
                                    KafkaRecordV0.KafkaRecordBody(
                                        offsetDelta = VarInt.fromDecoded(10),
                                        recordKey = VarIntByteArray("key".encodeToByteArray()),
                                        recordValue = VarIntByteArray("value".encodeToByteArray())
                                    )
                                )
                            )

                        )
                    )
                )
            ))
    )
    private val encodedBatchContainer = KafkaSerde.encode(batchContainer).readByteArray()


    private val requestPartition = ProduceRequestV3.Topic.Partition(
        partition = PartitionIndex(10),
        batchContainer = Int32BytesSizePrefixed(batchContainer)
    )
    private val encodedRequestPartitionData = byteArrayOf(
        0x00, 0x00, 0x00, 0x0A, // partition
        0x00, 0x00, 0x00, 0x4C, // int32 bytes size prefixed
        *encodedBatchContainer
    )

    private val requestTopic = ProduceRequestV3.Topic(
        topic = KafkaTopic("topic"),
        partition = int32ListOf(requestPartition)
    )
    private val encodedRequestTopic = byteArrayOf(
        0x00, 0x05, 0x74, 0x6F, 0x70, 0x69, 0x63, // topic
        0x00, 0x00, 0x00, 0x01, // int32 partitions size prefixed
        *encodedRequestPartitionData
    )

    private val request = ProduceRequestV3(
        timeoutMs = 1000,
        topic = int32ListOf(requestTopic)
    )
    private val encodedRequest = byteArrayOf(
        0xFF.toByte(), 0xFF.toByte(), // transactional id length
        0xFF.toByte(), 0xFF.toByte(), // acks
        0x00, 0x00, 0x03, 0xE8.toByte(), // timeout ms
        0x00, 0x00, 0x00, 0x01, // int32 topics size prefixed
        *encodedRequestTopic
    )

    private val responsePartition = ProduceResponseV3.Topic.Partition(
        index = PartitionIndex(10),
        errorCode = ErrorCode.NO_ERROR,
        baseOffset = 123L,
        logAppendTimeMs = 456L
    )
    private val encodedResponsePartition = byteArrayOf(
        0x00, 0x00, 0x00, 0x0A, // partition
        0x00, 0x00, // error code
        0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x7B, // baseOffset
        0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x01, 0xC8.toByte() // logAppendTimeMs
    )

    private val responseTopic = ProduceResponseV3.Topic(
        topic = KafkaTopic("topic"),
        partitions = int32ListOf(responsePartition)
    )
    private val encodedResponseTopic = byteArrayOf(
        0x00, 0x05, 0x74, 0x6F, 0x70, 0x69, 0x63, // topic
        0x00, 0x00, 0x00, 0x01, // int32 partitions size prefixed
        *encodedResponsePartition
    )

    private val response = ProduceResponseV3(
        topics = int32ListOf(responseTopic),
        throttleTimeMs = 1000
    )
    private val encodedResponse = byteArrayOf(
        0x00, 0x00, 0x00, 0x01, // int32 topics size prefixed
        *encodedResponseTopic,
        0x00, 0x00, 0x03, 0xE8.toByte() // throttle time ms
    )

    @Test
    fun should_encode_request_partition_data() {
        val actual = KafkaSerde.encode(requestPartition)
        actual.readByteArray() shouldBe encodedRequestPartitionData
    }

    @Test
    fun should_decode_request_partition_data() {
        val actual = KafkaSerde.decode<ProduceRequestV3.Topic.Partition>(encodedRequestPartitionData)
        actual shouldBe requestPartition
    }

    @Test
    fun should_encode_request_topic() {
        val actual = KafkaSerde.encode(requestTopic)
        actual.readByteArray() shouldBe encodedRequestTopic
    }

    @Test
    fun should_decode_request_topic() {
        val actual = KafkaSerde.decode<ProduceRequestV3.Topic>(encodedRequestTopic)
        actual shouldBe requestTopic
    }

    @Test
    fun should_encode_request() {
        val actual = KafkaSerde.encode(request)
        actual.readByteArray() shouldBe encodedRequest
    }

    @Test
    fun should_decode_request() {
        val actual = KafkaSerde.decode<ProduceRequestV3>(encodedRequest)
        actual shouldBe request
    }

    @Test
    fun should_encode_response_partition() {
        val actual = KafkaSerde.encode(responsePartition)
        actual.readByteArray() shouldBe encodedResponsePartition
    }

    @Test
    fun should_decode_response_partition() {
        val actual = KafkaSerde.decode<ProduceResponseV3.Topic.Partition>(encodedResponsePartition)
        actual shouldBe responsePartition
    }

    @Test
    fun should_encode_response_topic() {
        val actual = KafkaSerde.encode(responseTopic)
        actual.readByteArray() shouldBe encodedResponseTopic
    }

    @Test
    fun should_decode_response_topic() {
        val actual = KafkaSerde.decode<ProduceResponseV3.Topic>(encodedResponseTopic)
        actual shouldBe responseTopic
    }

    @Test
    fun should_encode_response() {
        val actual = KafkaSerde.encode(response)
        actual.readByteArray() shouldBe encodedResponse
    }

    @Test
    fun should_decode_response() {
        val actual = KafkaSerde.decode<ProduceResponseV3>(encodedResponse)
        actual shouldBe response
    }
}
