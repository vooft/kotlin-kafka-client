package io.github.vooft.kafka.network.messages

import io.github.vooft.kafka.common.KafkaTopic
import io.github.vooft.kafka.serialization.KafkaSerde
import io.github.vooft.kafka.serialization.decode
import io.github.vooft.kafka.serialization.encode
import io.kotest.matchers.shouldBe
import kotlinx.io.Buffer
import kotlinx.io.readByteArray
import kotlin.test.Test

class MetadataTest {

    private val topics = listOf("topic1", "topic2")
    private val request = MetadataRequestV1(topics.map { KafkaTopic(it) })
    private val encoded = byteArrayOf(
        0x0, 0x0, 0x0, 0x2, // int32 collection topics.size

        0x0, 0x6, // int16 string length
        0x74, 0x6f, 0x70, 0x69, 0x63, 0x31, // topic1

        0x0, 0x6, // int16 string length
        0x74, 0x6f, 0x70, 0x69, 0x63, 0x32 // topic1
    )

    @Test
    fun should_serialize_metadata_request() {
        val actual = KafkaSerde.encode(request)
        actual.readByteArray() shouldBe encoded
    }

    @Test
    fun should_deserialize_metadata_request() {
        val actual = KafkaSerde.decode<MetadataRequestV1>(Buffer().apply { write(encoded) })
        actual shouldBe request
    }
}
