package io.github.vooft.kafka.network.messages

import io.github.vooft.kafka.serialization.KafkaSerde
import io.github.vooft.kafka.serialization.common.primitives.NullableInt16String
import io.github.vooft.kafka.serialization.common.primitives.int32ListOf
import io.github.vooft.kafka.serialization.common.wrappers.GroupId
import io.github.vooft.kafka.serialization.common.wrappers.KafkaTopic
import io.github.vooft.kafka.serialization.common.wrappers.PartitionIndex
import io.github.vooft.kafka.serialization.common.wrappers.PartitionOffset
import io.github.vooft.kafka.serialization.decode
import io.github.vooft.kafka.serialization.encode
import io.github.vooft.kafka.transport.common.ErrorCode
import io.github.vooft.kafka.transport.messages.OffsetFetchRequestV1
import io.github.vooft.kafka.transport.messages.OffsetFetchResponseV1
import io.kotest.matchers.shouldBe
import kotlinx.io.readByteArray
import kotlin.test.Test

class OffsetFetchTest {
    private val requestTopic = OffsetFetchRequestV1.Topic(
        topic = KafkaTopic("topic"),
        partitions = int32ListOf(PartitionIndex(0), PartitionIndex(1))
    )
    private val encodedRequestTopic = byteArrayOf(
        0x00, 0x05, // topic length
        0x74, 0x6f, 0x70, 0x69, 0x63, // topic
        0x00, 0x00, 0x00, 0x02, // partitions count
        0x00, 0x00, 0x00, 0x00, // partition 0
        0x00, 0x00, 0x00, 0x01 // partition 1
    )

    private val request = OffsetFetchRequestV1(
        groupId = GroupId("group"),
        topics = int32ListOf(requestTopic)
    )
    private val encodedRequest = byteArrayOf(
        0x00, 0x05, // group length
        0x67, 0x72, 0x6f, 0x75, 0x70, // group
        0x00, 0x00, 0x00, 0x01, // topics count
        *encodedRequestTopic
    )

    private val responsePartition = OffsetFetchResponseV1.Topic.Partition(
        partition = PartitionIndex(0),
        offset = PartitionOffset(1),
        metadata = NullableInt16String.NULL,
        errorCode = ErrorCode.NO_ERROR
    )
    private val encodedResponsePartition = byteArrayOf(
        0x00, 0x00, 0x00, 0x00, // partition
        0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x01, // offset
        0xff.toByte(), 0xff.toByte(), // metadata
        0x00, 0x00 // error code
    )

    private val responseTopic = OffsetFetchResponseV1.Topic(
        topic = KafkaTopic("topic"),
        partitions = int32ListOf(responsePartition)
    )
    private val encodedResponseTopic = byteArrayOf(
        0x00, 0x05, // topic length
        0x74, 0x6f, 0x70, 0x69, 0x63, // topic
        0x00, 0x00, 0x00, 0x01, // partitions count
        *encodedResponsePartition
    )

    private val response = OffsetFetchResponseV1(
        topics = int32ListOf(responseTopic)
    )
    private val encodedResponse = byteArrayOf(
        0x00, 0x00, 0x00, 0x01, // topics count
        *encodedResponseTopic
    )

    @Test
    fun should_encode_request_topic() {
        val actual = KafkaSerde.encode(requestTopic)
        actual.readByteArray() shouldBe encodedRequestTopic
    }

    @Test
    fun should_decode_request_topic() {
        val actual = KafkaSerde.decode<OffsetFetchRequestV1.Topic>(encodedRequestTopic)
        actual shouldBe requestTopic
    }

    @Test
    fun should_encode_request() {
        val actual = KafkaSerde.encode(request)
        actual.readByteArray() shouldBe encodedRequest
    }

    @Test
    fun should_decode_request() {
        val actual = KafkaSerde.decode<OffsetFetchRequestV1>(encodedRequest)
        actual shouldBe request
    }

    @Test
    fun should_encode_response_partition() {
        val actual = KafkaSerde.encode(responsePartition)
        actual.readByteArray() shouldBe encodedResponsePartition
    }

    @Test
    fun should_decode_response_partition() {
        val actual = KafkaSerde.decode<OffsetFetchResponseV1.Topic.Partition>(encodedResponsePartition)
        actual shouldBe responsePartition
    }

    @Test
    fun should_encode_response_topic() {
        val actual = KafkaSerde.encode(responseTopic)
        actual.readByteArray() shouldBe encodedResponseTopic
    }

    @Test
    fun should_decode_response_topic() {
        val actual = KafkaSerde.decode<OffsetFetchResponseV1.Topic>(encodedResponseTopic)
        actual shouldBe responseTopic
    }

    @Test
    fun should_encode_response() {
        val actual = KafkaSerde.encode(response)
        actual.readByteArray() shouldBe encodedResponse
    }

    @Test
    fun should_decode_response() {
        val actual = KafkaSerde.decode<OffsetFetchResponseV1>(encodedResponse)
        actual shouldBe response
    }
}
