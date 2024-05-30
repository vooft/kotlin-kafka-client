package io.github.vooft.kafka.serialization.encoder

import io.github.vooft.kafka.serialization.Int16StringClass
import io.github.vooft.kafka.serialization.KafkaSerde
import io.github.vooft.kafka.serialization.NumbersClass
import io.github.vooft.kafka.serialization.VarNumberClass
import io.github.vooft.kafka.serialization.common.customtypes.Int16String
import io.github.vooft.kafka.serialization.common.customtypes.NullableInt16String
import io.github.vooft.kafka.serialization.common.primitives.VarInt
import io.github.vooft.kafka.serialization.common.primitives.VarLong
import io.github.vooft.kafka.serialization.encode
import io.kotest.matchers.shouldBe
import kotlinx.io.readByteArray
import kotlin.test.Test

class SimpleEncoderTest {
    @Test
    fun should_encode_simple_numbers() {
        val value = NumbersClass(
            int8 = 1,
            int16 = 2,
            int32 = 3,
            int64 = 4
        )

        val encoded = KafkaSerde.encode(value)
        encoded.readByteArray() shouldBe byteArrayOf(
            0x1,
            0x0, 0x2,
            0x0, 0x0, 0x0, 0x3,
            0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x4
        )
    }

    @Test
    fun should_encode_simple_numbers_max_values() {
        val value = NumbersClass(
            int8 = Byte.MAX_VALUE,
            int16 = Short.MAX_VALUE,
            int32 = Int.MAX_VALUE,
            int64 = Long.MAX_VALUE
        )

        val xFFByte = 0xFF.toByte()
        val encoded = KafkaSerde.encode(value)
        encoded.readByteArray() shouldBe byteArrayOf(
            0x7F,
            0x7F, xFFByte,
            0x7F, xFFByte, xFFByte, xFFByte,
            0x7F, xFFByte, xFFByte, xFFByte, xFFByte, xFFByte, xFFByte, xFFByte
        )
    }

    @Test
    fun should_encode_non_null_int16_string() {
        val value = Int16StringClass(
            nonNullString = Int16String("test1"),
            nullString = NullableInt16String("test23")
        )

        val encoded = KafkaSerde.encode(value)
        encoded.readByteArray() shouldBe byteArrayOf(
            0x0, 0x5, 0x74, 0x65, 0x73, 0x74, 0x31,
            0x0, 0x6, 0x74, 0x65, 0x73, 0x74, 0x32, 0x33
        )
    }

    @Test
    fun should_encode_null_int16_string() {
        val value = Int16StringClass(
            nonNullString = Int16String("test1"),
            nullString = NullableInt16String(null)
        )

        val encoded = KafkaSerde.encode(value)
        encoded.readByteArray() shouldBe byteArrayOf(
            0x0, 0x5, 0x74, 0x65, 0x73, 0x74, 0x31,
            0xFF.toByte(), 0xFF.toByte()
        )
    }

    @Test
    fun should_encode_varnumber_short() {
        val value = VarNumberClass(
            varInt = VarInt.fromDecoded(1),
            varLong = VarLong.fromDecoded(1)
        )

        val encoded = KafkaSerde.encode(value)
        encoded.readByteArray() shouldBe byteArrayOf(
            0x1,
            0x1
        )
    }

    @Test
    fun should_encode_varnumber_long() {
        val value = VarNumberClass(
            varInt = VarInt.fromDecoded(Int.MAX_VALUE),
            varLong = VarLong.fromDecoded(Long.MAX_VALUE)
        )

        val xFFByte = 0xFF.toByte()
        val xFEByte = 0xFE.toByte()
        val encoded = KafkaSerde.encode(value)
        encoded.readByteArray() shouldBe byteArrayOf(
            xFEByte, xFFByte, xFFByte, xFFByte, 0x0F, // varint
            xFEByte, xFFByte, xFFByte, xFFByte, xFFByte, xFFByte, xFFByte, xFFByte, xFFByte, 0x01 // varlong
        )
    }


}

private fun ByteArray.toHexString() = joinToString(", ", "[", "]") { "0x" + it.toUByte().toString(16).padStart(2, '0').uppercase() }
