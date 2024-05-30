package io.github.vooft.kafka.serialization.encoder

import io.github.vooft.kafka.serialization.common.IntEncoding
import io.github.vooft.kafka.serialization.common.encodeVarInt
import io.github.vooft.kafka.serialization.common.primitives.VarInt
import io.github.vooft.kafka.serialization.encoder.ListEncodingMode.SIZE_IN_BYTES
import io.github.vooft.kafka.serialization.encoder.ListEncodingMode.SIZE_IN_ITEMS
import kotlinx.io.Buffer
import kotlinx.io.Sink
import kotlinx.serialization.descriptors.SerialDescriptor
import kotlinx.serialization.encoding.CompositeEncoder
import kotlinx.serialization.modules.EmptySerializersModule
import kotlinx.serialization.modules.SerializersModule

class KafkaListEncoder(
    private val targetSink: Sink,
    private val tempBuffer: Buffer = Buffer(),
    private val encodingMode: ListEncodingMode,
    private val sizeEncoding: IntEncoding,
    override val serializersModule: SerializersModule = EmptySerializersModule(),
) : KafkaValueEncoder(tempBuffer, serializersModule), AbstractKafkaCompositeEncoder {

    override fun beginCollection(descriptor: SerialDescriptor, collectionSize: Int): CompositeEncoder {
        if (encodingMode == SIZE_IN_ITEMS) {
            when (sizeEncoding) {
                IntEncoding.INT16 -> encodeShort(collectionSize.toShort())
                IntEncoding.INT32 -> encodeInt(collectionSize)
                IntEncoding.VARINT -> encodeVarInt(VarInt.fromDecoded(collectionSize))
            }
        }

        return this
    }

    override fun endStructure(descriptor: SerialDescriptor) {
        if (encodingMode == SIZE_IN_BYTES) {
            val size = tempBuffer.size.toInt()
            when (sizeEncoding) {
                IntEncoding.INT16 -> encodeShort(size.toShort())
                IntEncoding.INT32 -> encodeInt(size)
                IntEncoding.VARINT -> encodeVarInt(VarInt.fromDecoded(size))
            }
        }

        targetSink.write(tempBuffer, tempBuffer.size)
    }
}

enum class ListEncodingMode {
    SIZE_IN_ITEMS,
    SIZE_IN_BYTES
}
