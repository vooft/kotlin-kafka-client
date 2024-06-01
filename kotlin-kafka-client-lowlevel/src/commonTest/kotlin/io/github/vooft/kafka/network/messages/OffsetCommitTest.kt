package io.github.vooft.kafka.network.messages

import io.github.vooft.kafka.network.common.ErrorCode
import io.github.vooft.kafka.serialization.KafkaSerde
import io.github.vooft.kafka.serialization.common.primitives.NullableInt16String
import io.github.vooft.kafka.serialization.common.primitives.int32ListOf
import io.github.vooft.kafka.serialization.common.wrappers.GroupId
import io.github.vooft.kafka.serialization.common.wrappers.KafkaTopic
import io.github.vooft.kafka.serialization.common.wrappers.MemberId
import io.github.vooft.kafka.serialization.common.wrappers.PartitionIndex
import io.github.vooft.kafka.serialization.common.wrappers.PartitionOffset
import io.github.vooft.kafka.serialization.decode
import io.github.vooft.kafka.serialization.encode
import io.kotest.matchers.shouldBe
import kotlinx.io.readByteArray
import kotlin.test.Test

class OffsetCommitTest {
    private val requestPartition = OffsetCommitRequestV1.Topic.Partition(
        partitionIndex = PartitionIndex(5),
        committedOffset = PartitionOffset(10),
        commitTimestamp = 0,
        committedMetadata = NullableInt16String(null)
    )
    private val encodedRequestPartition = byteArrayOf(
        0x00, 0x00, 0x00, 0x05, // partition index
        0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x0A, // committed offset
        0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, // commit timestamp
        0xFF.toByte(), 0xFF.toByte() // committed metadata
    )

    private val requestTopic = OffsetCommitRequestV1.Topic(
        topic = KafkaTopic("topic"),
        partitions = int32ListOf(requestPartition)
    )
    private val encodedRequestTopic = byteArrayOf(
        0x00, 0x05, // topic length
        0x74, 0x6F, 0x70, 0x69, 0x63, // topic
        0x00, 0x00, 0x00, 0x01, // partitions count
        *encodedRequestPartition // partition
    )

    private val request = OffsetCommitRequestV1(
        groupId = GroupId("group"),
        generationIdOrMemberEpoch = 0,
        memberId = MemberId("member"),
        topics = int32ListOf(requestTopic)
    )
    private val encodedRequest = byteArrayOf(
        0x00, 0x05, // group length
        0x67, 0x72, 0x6F, 0x75, 0x70, // group
        0x00, 0x00, 0x00, 0x00, // generation id
        0x00, 0x06, // member length
        0x6D, 0x65, 0x6D, 0x62, 0x65, 0x72, // member
        0x00, 0x00, 0x00, 0x01, // topics count
        *encodedRequestTopic // topic
    )

    private val responsePartition = OffsetCommitResponseV1.Topic.Partition(
        partitionIndex = PartitionIndex(5),
        errorCode = ErrorCode.NO_ERROR
    )
    private val encodedResponsePartition = byteArrayOf(
        0x00, 0x00, 0x00, 0x05, // partition index
        0x00, 0x00 // error code
    )

    private val responseTopic = OffsetCommitResponseV1.Topic(
        topic = KafkaTopic("topic"),
        partitions = int32ListOf(responsePartition)
    )
    private val encodedResponseTopic = byteArrayOf(
        0x00, 0x05, // topic length
        0x74, 0x6F, 0x70, 0x69, 0x63, // topic
        0x00, 0x00, 0x00, 0x01, // partitions count
        *encodedResponsePartition // partition
    )

    private val response = OffsetCommitResponseV1(
        topics = int32ListOf(responseTopic)
    )
    private val encodedResponse = byteArrayOf(
        0x00, 0x00, 0x00, 0x01, // topics count
        *encodedResponseTopic // topic
    )

    @Test
    fun should_encode_request_partition() {
        val actual = KafkaSerde.encode(requestPartition)
        actual.readByteArray() shouldBe encodedRequestPartition
    }

    @Test
    fun should_decode_request_partition() {
        val actual = KafkaSerde.decode<OffsetCommitRequestV1.Topic.Partition>(encodedRequestPartition)
        actual shouldBe requestPartition
    }

    @Test
    fun should_encode_request_topic() {
        val actual = KafkaSerde.encode(requestTopic)
        actual.readByteArray() shouldBe encodedRequestTopic
    }

    @Test
    fun should_decode_request_topic() {
        val actual = KafkaSerde.decode<OffsetCommitRequestV1.Topic>(encodedRequestTopic)
        actual shouldBe requestTopic
    }

    @Test
    fun should_encode_request() {
        val actual = KafkaSerde.encode(request)
        actual.readByteArray() shouldBe encodedRequest
    }

    @Test
    fun should_decode_request() {
        val actual = KafkaSerde.decode<OffsetCommitRequestV1>(encodedRequest)
        actual shouldBe request
    }

    @Test
    fun should_encode_response_partition() {
        val actual = KafkaSerde.encode(responsePartition)
        actual.readByteArray() shouldBe encodedResponsePartition
    }

    @Test
    fun should_decode_response_partition() {
        val actual = KafkaSerde.decode<OffsetCommitResponseV1.Topic.Partition>(encodedResponsePartition)
        actual shouldBe responsePartition
    }

    @Test
    fun should_encode_response_topic() {
        val actual = KafkaSerde.encode(responseTopic)
        actual.readByteArray() shouldBe encodedResponseTopic
    }

    @Test
    fun should_decode_response_topic() {
        val actual = KafkaSerde.decode<OffsetCommitResponseV1.Topic>(encodedResponseTopic)
        actual shouldBe responseTopic
    }

    @Test
    fun should_encode_response() {
        val actual = KafkaSerde.encode(response)
        actual.readByteArray() shouldBe encodedResponse
    }

    @Test
    fun should_decode_response() {
        val actual = KafkaSerde.decode<OffsetCommitResponseV1>(encodedResponse)
        actual shouldBe response
    }
}
