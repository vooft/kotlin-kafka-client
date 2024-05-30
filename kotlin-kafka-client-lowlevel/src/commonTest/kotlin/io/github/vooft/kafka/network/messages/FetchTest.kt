package io.github.vooft.kafka.network.messages

import io.github.vooft.kafka.common.KafkaTopic
import io.github.vooft.kafka.common.PartitionIndex
import io.github.vooft.kafka.network.common.ErrorCode
import io.github.vooft.kafka.serialization.KafkaSerde
import io.github.vooft.kafka.serialization.common.primitives.Crc32cPrefixed
import io.github.vooft.kafka.serialization.common.primitives.Int32BytesSizePrefixed
import io.github.vooft.kafka.serialization.common.primitives.Int32List
import io.github.vooft.kafka.serialization.common.primitives.VarInt
import io.github.vooft.kafka.serialization.common.primitives.VarIntByteArray
import io.github.vooft.kafka.serialization.common.primitives.VarIntBytesSizePrefixed
import io.github.vooft.kafka.serialization.common.primitives.int32ListOf
import io.github.vooft.kafka.serialization.decode
import io.github.vooft.kafka.serialization.encode
import io.github.vooft.kafka.utils.toHexString
import io.kotest.matchers.shouldBe
import kotlinx.io.readByteArray
import kotlin.test.Test

class FetchTest {
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

    private val requestPartition = FetchRequestV4.Topic.Partition(
        partition = PartitionIndex(10),
        fetchOffset = 123L,
        maxBytes = 1024
    )
    private val requestPartitionEncoded = byteArrayOf(
        0x00, 0x00, 0x00, 0x0A, // partition
        0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x7B, // fetchOffset
        0x00, 0x00, 0x04, 0x00 // maxBytes
    )

    private val requestTopic = FetchRequestV4.Topic(
        topic = KafkaTopic("topic"),
        partitions = Int32List(requestPartition)
    )
    private val encodedRequestTopic = byteArrayOf(
        0x00, 0x05, // topic length
        0x74, 0x6F, 0x70, 0x69, 0x63, // "topic"
        0x00, 0x00, 0x00, 0x01, // partitions size
        *requestPartitionEncoded
    )

    private val request = FetchRequestV4(
        maxWaitTime = 100,
        minBytes = 1024,
        maxBytes = 2048,
        topics = Int32List(requestTopic)
    )
    private val requestEncoded = byteArrayOf(
        0xFF.toByte(), 0xFF.toByte(), 0xFF.toByte(), 0xFF.toByte(), // replicaId
        0x00, 0x00, 0x00, 0x64, // maxWaitTime
        0x00, 0x00, 0x04, 0x00, // minBytes
        0x00, 0x00, 0x08, 0x00, // maxBytes
        0x01, // isolationLevel
        0x00, 0x00, 0x00, 0x01, // topics size
        *encodedRequestTopic
    )

    private val responseAbortedTransaction = FetchResponseV4.Topic.Partition.AbortedTransaction(
        producerId = 123L,
        firstOffset = 456L
    )
    private val encodedResponseAbortedTransaction = byteArrayOf(
        0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x7B, // producerId
        0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x01, 0xC8.toByte() // firstOffset
    )

    private val responsePartition = FetchResponseV4.Topic.Partition(
        partition = PartitionIndex(10),
        errorCode = ErrorCode.NO_ERROR,
        highwaterMarkOffset = 123L,
        lastStableOffset = 456L,
        abortedTransactions = Int32List(responseAbortedTransaction),
        batchContainer = Int32BytesSizePrefixed(batchContainer)
    )
    private val encodedResponsePartition = byteArrayOf(
        0x00, 0x00, 0x00, 0x0A, // partition
        0x00, 0x00, // errorCode
        0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x7B, // highwaterMarkOffset
        0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x01, 0xC8.toByte(), // lastStableOffset,

        0x00, 0x00, 0x00, 0x01, // abortedTransactions size
        *encodedResponseAbortedTransaction,

        0x00, 0x00, 0x00, 0x4C,
        *encodedBatchContainer
    )

    private val responseTopic = FetchResponseV4.Topic(
        topic = KafkaTopic("topic"),
        partitions = Int32List(responsePartition)
    )
    private val encodedResponseTopic = byteArrayOf(
        0x00, 0x05, // topic length
        0x74, 0x6F, 0x70, 0x69, 0x63, // "topic"
        0x00, 0x00, 0x00, 0x01, // partitions size
        *encodedResponsePartition
    )

    private val response = FetchResponseV4(
        throttleTimeMs = 100,
        topics = Int32List(responseTopic)
    )
    private val encodedResponse = byteArrayOf(
        0x00, 0x00, 0x00, 0x64, // throttleTimeMs
        0x00, 0x00, 0x00, 0x01, // topics size
        *encodedResponseTopic
    )

    @Test
    fun should_encode_request_partition() {
        val actual = KafkaSerde.encode(requestPartition)
        actual.readByteArray() shouldBe requestPartitionEncoded
    }

    @Test
    fun should_decode_request_partition() {
        val actual = KafkaSerde.decode<FetchRequestV4.Topic.Partition>(requestPartitionEncoded)
        actual shouldBe requestPartition
    }

    @Test
    fun should_encode_request_topic() {
        val actual = KafkaSerde.encode(requestTopic)
        actual.readByteArray() shouldBe encodedRequestTopic
    }

    @Test
    fun should_decode_request_topic() {
        val actual = KafkaSerde.decode<FetchRequestV4.Topic>(encodedRequestTopic)
        actual shouldBe requestTopic
    }

    @Test
    fun should_encode_request() {
        val actual = KafkaSerde.encode(request)
        actual.readByteArray() shouldBe requestEncoded
    }

    @Test
    fun should_decode_request() {
        val actual = KafkaSerde.decode<FetchRequestV4>(requestEncoded)
        actual shouldBe request
    }

    @Test
    fun should_encode_response_aborted_transaction() {
        val actual = KafkaSerde.encode(responseAbortedTransaction)
        actual.readByteArray() shouldBe encodedResponseAbortedTransaction
    }

    @Test
    fun should_decode_response_aborted_transaction() {
        val actual = KafkaSerde.decode<FetchResponseV4.Topic.Partition.AbortedTransaction>(encodedResponseAbortedTransaction)
        actual shouldBe responseAbortedTransaction
    }

    @Test
    fun should_encode_response_partition() {
        val actual = KafkaSerde.encode(responsePartition)
        println(actual.peek().readByteArray().toHexString())
        println(encodedResponsePartition.toHexString())
        actual.readByteArray() shouldBe encodedResponsePartition
    }

    @Test
    fun should_decode_response_partition() {
        val actual = KafkaSerde.decode<FetchResponseV4.Topic.Partition>(encodedResponsePartition)
        actual shouldBe responsePartition
    }

    @Test
    fun should_decode_response_partition_with_missing_batch_container() {
        val encoded = byteArrayOf(
            0x00, 0x00, 0x00, 0x0A, // partition
            0x00, 0x00, // errorCode
            0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x7B, // highwaterMarkOffset
            0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x01, 0xC8.toByte(), // lastStableOffset,

            0x00, 0x00, 0x00, 0x01, // abortedTransactions size
            *encodedResponseAbortedTransaction,

            0x00, 0x00, 0x00, 0x00, // 0 length for batch container
        )

        val actual = KafkaSerde.decode<FetchResponseV4.Topic.Partition>(encoded)
        actual.batchContainer.value shouldBe null
    }

    @Test
    fun should_encode_response_topic() {
        val actual = KafkaSerde.encode(responseTopic)
        actual.readByteArray() shouldBe encodedResponseTopic
    }

    @Test
    fun should_decode_response_topic() {
        val actual = KafkaSerde.decode<FetchResponseV4.Topic>(encodedResponseTopic)
        actual shouldBe responseTopic
    }

    @Test
    fun should_encode_response() {
        val actual = KafkaSerde.encode(response)
        actual.readByteArray() shouldBe encodedResponse
    }

    @Test
    fun should_decode_response() {
        val actual = KafkaSerde.decode<FetchResponseV4>(encodedResponse)
        actual shouldBe response
    }
}
