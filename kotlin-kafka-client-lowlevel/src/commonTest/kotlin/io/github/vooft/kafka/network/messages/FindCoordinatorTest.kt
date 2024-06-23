package io.github.vooft.kafka.network.messages

import io.github.vooft.kafka.common.types.NodeId
import io.github.vooft.kafka.serialization.KafkaSerde
import io.github.vooft.kafka.serialization.common.primitives.Int16String
import io.github.vooft.kafka.serialization.common.primitives.NullableInt16String
import io.github.vooft.kafka.serialization.decode
import io.github.vooft.kafka.serialization.encode
import io.github.vooft.kafka.transport.common.ErrorCode
import io.github.vooft.kafka.transport.messages.FindCoordinatorRequestV1
import io.github.vooft.kafka.transport.messages.FindCoordinatorResponseV1
import io.kotest.matchers.shouldBe
import kotlinx.io.readByteArray
import kotlin.test.Test

class FindCoordinatorTest {
    private val request = FindCoordinatorRequestV1(Int16String("my-group"))
    private val encodedRequest = byteArrayOf(
        0x0, 0x8, // "my-group" length
        0x6d, 0x79, 0x2d, 0x67, 0x72, 0x6f, 0x75, 0x70, // "my-group"
        0x0 // key type GROUP
    )

    private val response = FindCoordinatorResponseV1(
        throttleTimeMs = 0,
        errorCode = ErrorCode.NO_ERROR,
        errorMessage = NullableInt16String.NULL,
        nodeId = NodeId(1),
        host = Int16String("localhost"),
        port = 9092
    )
    private val encodedResponse = byteArrayOf(
        0x0, 0x0, 0x0, 0x0, // throttle time
        0x0, 0x0, // error code
        0xff.toByte(), 0xff.toByte(), // error message
        0x0, 0x0, 0x0, 0x1, // node id
        0x0, 0x9, // "localhost" length
        0x6c, 0x6f, 0x63, 0x61, 0x6c, 0x68, 0x6f, 0x73, 0x74, // "localhost"
        0x0, 0x0, 0x23, 0x84.toByte() // port
    )

    @Test
    fun should_encode_request() {
        val actual = KafkaSerde.encode(request)
        actual.readByteArray() shouldBe encodedRequest
    }

    @Test
    fun should_decode_request() {
        val actual = KafkaSerde.decode<FindCoordinatorRequestV1>(encodedRequest)
        actual shouldBe request
    }

    @Test
    fun should_encode_response() {
        val actual = KafkaSerde.encode(response)
        actual.readByteArray() shouldBe encodedResponse
    }

    @Test
    fun should_decode_response() {
        val actual = KafkaSerde.decode<FindCoordinatorResponseV1>(encodedResponse)
        actual shouldBe response
    }
}
