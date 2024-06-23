package io.github.vooft.kafka.serialization.decoder

import io.github.vooft.kafka.common.annotations.IntEncoding
import io.github.vooft.kafka.serialization.common.decodeVarInt
import kotlinx.io.Buffer
import kotlinx.io.Source
import kotlinx.serialization.ExperimentalSerializationApi
import kotlinx.serialization.modules.EmptySerializersModule
import kotlinx.serialization.modules.SerializersModule

class KafkaBytesSizePrefixedDecoder(
    source: Source,
    sizeEncoding: IntEncoding,
    private val length: Int = source.readPrefix(sizeEncoding),
    override val serializersModule: SerializersModule = EmptySerializersModule(),
) : KafkaValueDecoder(source.readPrefixed(length), serializersModule) {
    @ExperimentalSerializationApi
    override fun decodeNotNullMark(): Boolean = length != 0
}

private fun Source.readPrefix(sizeEncoding: IntEncoding): Int {
    if (exhausted()) {
        return 0
    }

    return when (sizeEncoding) {
        IntEncoding.INT16 -> readShort().toInt()
        IntEncoding.INT32 -> readInt()
        IntEncoding.VARINT -> {
            val objectDecoder = KafkaObjectDecoder(this)
            objectDecoder.decodeVarInt().toDecoded()
        }
    }
}

private fun Source.readPrefixed(length: Int): Source {
    val result = Buffer()
    readTo(result, length.toLong())
    return result
}

