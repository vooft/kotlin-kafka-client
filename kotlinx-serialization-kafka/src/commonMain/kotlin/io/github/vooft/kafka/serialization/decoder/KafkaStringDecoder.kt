package io.github.vooft.kafka.serialization.decoder

import io.github.vooft.kafka.serialization.common.IntEncoding
import kotlinx.io.Source
import kotlinx.io.readString
import kotlinx.serialization.DeserializationStrategy
import kotlinx.serialization.ExperimentalSerializationApi
import kotlinx.serialization.encoding.Decoder
import kotlinx.serialization.modules.EmptySerializersModule
import kotlinx.serialization.modules.SerializersModule

class KafkaStringDecoder(
    private val source: Source,
    private val lengthEncoding: IntEncoding,
    override val serializersModule: SerializersModule = EmptySerializersModule(),
    valueDecoder: KafkaValueDecoder = KafkaValueDecoder(source, serializersModule)
) : Decoder by valueDecoder {
    override fun decodeString(): String {
        val string = decodeNullableString()
        return string ?: error("String can not be null")
    }

    @ExperimentalSerializationApi
    override fun <T : Any> decodeNullableSerializableValue(deserializer: DeserializationStrategy<T?>): T? {
        return decodeNullableString() as T?
    }

    private fun decodeNullableString(): String? {
        val length: Long = when (lengthEncoding) {
            IntEncoding.INT16 -> source.readShort().toLong()
            IntEncoding.VARINT -> decodeVarInt().toDecoded().toLong()
            else -> error("Only INT32 and VARINT are supported, but got $lengthEncoding")
        }

        if (length < 0) {
            return null
        }

        return source.readString(length)
    }
}
