package io.github.vooft.kafka.serialization.decoder

import io.github.vooft.kafka.serialization.common.IntEncoding
import io.github.vooft.kafka.serialization.common.decodeVarInt
import kotlinx.io.Buffer
import kotlinx.io.Source
import kotlinx.serialization.modules.EmptySerializersModule
import kotlinx.serialization.modules.SerializersModule

class KafkaBytesSizePrefixedDecoder(
    source: Source,
    sizeEncoding: IntEncoding,
    override val serializersModule: SerializersModule = EmptySerializersModule(),
) : KafkaValueDecoder(source.readPrefixed(sizeEncoding), serializersModule)

private fun Source.readPrefixed(sizeEncoding: IntEncoding): Source {
    val length = when (sizeEncoding) {
        IntEncoding.INT16 -> readShort()
        IntEncoding.INT32 -> readInt()
        IntEncoding.VARINT -> {
            val objectDecoder = KafkaObjectDecoder(this)
            objectDecoder.decodeVarInt().toDecoded()
        }
    }

    val result = Buffer()
    readTo(result, length.toLong())
    return result
}

