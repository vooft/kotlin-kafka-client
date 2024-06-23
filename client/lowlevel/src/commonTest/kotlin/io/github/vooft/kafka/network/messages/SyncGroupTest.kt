package io.github.vooft.kafka.network.messages

import io.github.vooft.kafka.serialization.KafkaSerde
import io.github.vooft.kafka.serialization.common.primitives.Int32BytesSizePrefixed
import io.github.vooft.kafka.serialization.common.primitives.int32ListOf
import io.github.vooft.kafka.serialization.common.wrappers.GroupId
import io.github.vooft.kafka.serialization.common.wrappers.KafkaTopic
import io.github.vooft.kafka.serialization.common.wrappers.MemberId
import io.github.vooft.kafka.serialization.common.wrappers.PartitionIndex
import io.github.vooft.kafka.serialization.decode
import io.github.vooft.kafka.serialization.encode
import io.github.vooft.kafka.transport.common.ErrorCode
import io.github.vooft.kafka.transport.messages.MemberAssignment
import io.github.vooft.kafka.transport.messages.SyncGroupRequestV1
import io.github.vooft.kafka.transport.messages.SyncGroupResponseV1
import io.kotest.matchers.shouldBe
import kotlinx.io.readByteArray
import kotlin.test.Test

class SyncGroupTest {
    private val partitionAssignment = MemberAssignment.PartitionAssignment(
        topic = KafkaTopic("topic"),
        partitions = int32ListOf(PartitionIndex(1), PartitionIndex(2))
    )
    private val encodedPartitionsAssignment = byteArrayOf(
        0x00, 0x05, // topic length
        0x74, 0x6f, 0x70, 0x69, 0x63, // "topic"

        0x00, 0x00, 0x00, 0x02, // partitions count
        0x00, 0x00, 0x00, 0x01, // partition 1
        0x00, 0x00, 0x00, 0x02 // partition 2
    )

    private val memberAssignment = MemberAssignment(
        partitionAssignments = int32ListOf(partitionAssignment),
    )
    private val encodedMembershipAssignment = byteArrayOf(
        0x00, 0x00, // version

        0x00, 0x00, 0x00, 0x01, // partition assignments count
        *encodedPartitionsAssignment,

        0x00, 0x00, 0x00, 0x00 // user data length
    )

    private val requestAssignment = SyncGroupRequestV1.Assignment(
        memberId = MemberId("member"),
        assignment = Int32BytesSizePrefixed(memberAssignment)
    )
    private val encodedRequestAssignment = byteArrayOf(
        0x00, 0x06, // member length
        0x6d, 0x65, 0x6d, 0x62, 0x65, 0x72, // "member"

        0x00, 0x00, 0x00, 0x1D, // assignment length
        *encodedMembershipAssignment
    )

    private val request = SyncGroupRequestV1(
        groupId = GroupId("group"),
        generationId = 1,
        memberId = MemberId("member"),
        assignments = int32ListOf(requestAssignment)
    )
    private val encodedRequest = byteArrayOf(
        0x00, 0x05, // group length
        0x67, 0x72, 0x6f, 0x75, 0x70, // "group"

        0x00, 0x00, 0x00, 0x01, // generation id
        0x00, 0x06, // member length
        0x6d, 0x65, 0x6d, 0x62, 0x65, 0x72, // "member"

        0x00, 0x00, 0x00, 0x01, // assignments length
        *encodedRequestAssignment
    )

    private val response = SyncGroupResponseV1(
        throttleTimeMs = 1,
        errorCode = ErrorCode.NO_ERROR,
        assignment = Int32BytesSizePrefixed(memberAssignment)
    )
    private val encodedResponse = byteArrayOf(
        0x00, 0x00, 0x00, 0x01, // throttle time ms
        0x00, 0x00, // error code

        0x00, 0x00, 0x00, 0x1D, // assignment length
        *encodedMembershipAssignment
    )

    @Test
    fun should_encode_partition_assignment() {
        val actual = KafkaSerde.encode(partitionAssignment)
        actual.readByteArray() shouldBe encodedPartitionsAssignment
    }

    @Test
    fun should_decode_partition_assignment() {
        val actual = KafkaSerde.decode<MemberAssignment.PartitionAssignment>(encodedPartitionsAssignment)
        actual shouldBe partitionAssignment
    }

    @Test
    fun should_encode_member_assignment() {
        val actual = KafkaSerde.encode(memberAssignment)
        actual.readByteArray() shouldBe encodedMembershipAssignment
    }

    @Test
    fun should_decode_member_assignment() {
        val actual = KafkaSerde.decode<MemberAssignment>(encodedMembershipAssignment)
        actual shouldBe memberAssignment
    }

    @Test
    fun should_encode_request_assignment() {
        val actual = KafkaSerde.encode(requestAssignment)
        actual.readByteArray() shouldBe encodedRequestAssignment
    }

    @Test
    fun should_decode_request_assignment() {
        val actual = KafkaSerde.decode<SyncGroupRequestV1.Assignment>(encodedRequestAssignment)
        actual shouldBe requestAssignment
    }

    @Test
    fun should_encode_request() {
        val actual = KafkaSerde.encode(request)
        actual.readByteArray() shouldBe encodedRequest
    }

    @Test
    fun should_decode_request() {
        val actual = KafkaSerde.decode<SyncGroupRequestV1>(encodedRequest)
        actual shouldBe request
    }

    @Test
    fun should_encode_response() {
        val actual = KafkaSerde.encode(response)
        actual.readByteArray() shouldBe encodedResponse
    }

    @Test
    fun should_decode_response() {
        val actual = KafkaSerde.decode<SyncGroupResponseV1>(encodedResponse)
        actual shouldBe response
    }

    @Test
    fun should_decode_response_when_assignment_is_missing_with_length() {
        val encoded = byteArrayOf(
            0x00, 0x00, 0x00, 0x01, // throttle time ms
            0x00, 0x00, // error code
            // both assignment and length are missing
        )

        val actual = KafkaSerde.decode<SyncGroupResponseV1>(encoded)
        actual.assignment.value shouldBe null
    }
}
