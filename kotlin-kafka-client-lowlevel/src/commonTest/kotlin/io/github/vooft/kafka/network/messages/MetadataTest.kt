package io.github.vooft.kafka.network.messages

import io.github.vooft.kafka.serialization.KafkaSerde
import io.github.vooft.kafka.serialization.encode
import io.kotest.matchers.shouldBe
import kotlinx.io.readByteArray
import kotlin.test.Test

class MetadataTest {
    @Test
    fun should_serialize_metadata_request() {
        val value = MetadataRequestV1("topic1")

        val encoded = KafkaSerde.encode(value)

        encoded.readByteArray() shouldBe byteArrayOf(
            0x0, 0x0, 0x0, 0x1, // int32 collection topics.size

            0x0, 0x6, // int16 string length
            0x74, 0x6f, 0x70, 0x69, 0x63, 0x31 // topic1
        )
    }
}
