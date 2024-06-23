package io.github.vooft.kafka.network.messages

import io.github.vooft.kafka.common.types.GroupId
import io.github.vooft.kafka.common.types.KafkaTopic
import io.github.vooft.kafka.common.types.MemberId
import io.github.vooft.kafka.serialization.KafkaSerde
import io.github.vooft.kafka.serialization.common.primitives.Int16String
import io.github.vooft.kafka.serialization.common.primitives.Int32ByteArray
import io.github.vooft.kafka.serialization.common.primitives.Int32BytesSizePrefixed
import io.github.vooft.kafka.serialization.common.primitives.Int32List
import io.github.vooft.kafka.serialization.decode
import io.github.vooft.kafka.serialization.encode
import io.github.vooft.kafka.transport.common.ErrorCode
import io.github.vooft.kafka.transport.messages.JoinGroupRequestV1
import io.github.vooft.kafka.transport.messages.JoinGroupRequestV1.GroupProtocol
import io.github.vooft.kafka.transport.messages.JoinGroupRequestV1.GroupProtocol.Metadata
import io.github.vooft.kafka.transport.messages.JoinGroupResponseV1
import io.github.vooft.kafka.transport.messages.JoinGroupResponseV1.Member
import io.kotest.matchers.shouldBe
import kotlinx.io.readByteArray
import kotlin.test.Test

class JoinGroupTest {
    private val requestMetadata = Metadata(
        version = 1,
        topics = Int32List(KafkaTopic("topic1"), KafkaTopic("topic2")),
        userData = Int32List()
    )
    private val encodedRequestMetadata = byteArrayOf(
        0x0, 0x1, // version

        0x0, 0x0, 0x0, 0x2, // topics size

        0x0, 0x6, // topic1 length
        0x74, 0x6f, 0x70, 0x69, 0x63, 0x31, // topic1

        0x0, 0x6, // topic2 length
        0x74, 0x6f, 0x70, 0x69, 0x63, 0x32, // topic2

        0x0, 0x0, 0x0, 0x0 // userData size
    )

    private val requestGroupProtocol = GroupProtocol(
        protocol = Int16String("protocol"),
        metadata = Int32BytesSizePrefixed(requestMetadata)
    )
    private val encodedGroupProtocol = byteArrayOf(
        0x0, 0x8, // protocol length
        0x70, 0x72, 0x6f, 0x74, 0x6f, 0x63, 0x6f, 0x6c, // protocol

        0x0, 0x0, 0x0, 0x1A, // metadata size
        *encodedRequestMetadata
    )

    private val request = JoinGroupRequestV1(
        groupId = GroupId("group"),
        sessionTimeoutMs = 1000,
        rebalanceTimeoutMs = 1000,
        memberId = MemberId("member"),
        protocolType = Int16String("type"),
        groupProtocols = Int32List(requestGroupProtocol)
    )
    private val encodedRequest = byteArrayOf(
        0x0, 0x5, // groupId length
        0x67, 0x72, 0x6f, 0x75, 0x70, // "group"

        0x0, 0x0, 0x3, 0xE8.toByte(), // sessionTimeoutMs

        0x0, 0x0, 0x3, 0xE8.toByte(), // rebalanceTimeoutMs

        0x0, 0x6, // memberOd length
        0x6d, 0x65, 0x6d, 0x62, 0x65, 0x72, // "member"

        0x0, 0x4, // protocol type length
        0x74, 0x79, 0x70, 0x65, // "type"

        0x0, 0x0, 0x0, 0x1, // groupProtocols size
        *encodedGroupProtocol
    )

    private val responseMember = Member(
        memberId = MemberId("member"),
        metadata = Int32ByteArray(byteArrayOf(0x1, 0x2, 0x3))
    )
    private val responseMemberEncoded = byteArrayOf(
        0x0, 0x6, // memberId length
        0x6d, 0x65, 0x6d, 0x62, 0x65, 0x72, // "member"

        0x0, 0x0, 0x0, 0x3, // metadata size
        0x1, 0x2, 0x3 // metadata
    )

    private val response = JoinGroupResponseV1(
        errorCode = ErrorCode.NO_ERROR,
        generationId = 1,
        groupProtocol = Int16String("protocol"),
        leaderId = MemberId("leader"),
        memberId = MemberId("member"),
        members = Int32List(responseMember)
    )
    private val responseEncoded = byteArrayOf(
        0x0, 0x0, // error code

        0x0, 0x0, 0x0, 0x1, // generationId

        0x0, 0x8, // groupProtocol length
        0x70, 0x72, 0x6f, 0x74, 0x6f, 0x63, 0x6f, 0x6c, // "protocol"

        0x0, 0x6, // leaderId length
        0x6c, 0x65, 0x61, 0x64, 0x65, 0x72, // "leader"

        0x0, 0x6, // memberId length
        0x6d, 0x65, 0x6d, 0x62, 0x65, 0x72, // "member"

        0x0, 0x0, 0x0, 0x1, // members size
        *responseMemberEncoded
    )

    @Test
    fun should_encode_request_metadata() {
        val actual = KafkaSerde.encode(requestMetadata)
        actual.readByteArray() shouldBe encodedRequestMetadata
    }

    @Test
    fun should_decode_request_metadata() {
        val actual = KafkaSerde.decode<Metadata>(encodedRequestMetadata)
        actual shouldBe requestMetadata
    }

    @Test
    fun should_encode_request_group_protocol() {
        val actual = KafkaSerde.encode(requestGroupProtocol)
        actual.readByteArray() shouldBe encodedGroupProtocol
    }

    @Test
    fun should_decode_request_group_protocol() {
        val actual = KafkaSerde.decode<GroupProtocol>(encodedGroupProtocol)
        actual shouldBe requestGroupProtocol
    }

    @Test
    fun should_encode_request() {
        val actual = KafkaSerde.encode(request)
        actual.readByteArray() shouldBe encodedRequest
    }

    @Test
    fun should_decode_request() {
        val actual = KafkaSerde.decode<JoinGroupRequestV1>(encodedRequest)
        actual shouldBe request
    }

    @Test
    fun should_encode_response_member() {
        val actual = KafkaSerde.encode(responseMember)
        actual.readByteArray() shouldBe responseMemberEncoded
    }

    @Test
    fun should_decode_response_member() {
        val actual = KafkaSerde.decode<Member>(responseMemberEncoded)
        actual shouldBe responseMember
    }

    @Test
    fun should_encode_response() {
        val actual = KafkaSerde.encode(response)
        actual.readByteArray() shouldBe responseEncoded
    }

    @Test
    fun should_decode_response() {
        val actual = KafkaSerde.decode<JoinGroupResponseV1>(responseEncoded)
        actual shouldBe response
    }
}
