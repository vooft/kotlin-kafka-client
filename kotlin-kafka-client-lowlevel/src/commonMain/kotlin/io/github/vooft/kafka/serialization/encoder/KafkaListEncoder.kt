package io.github.vooft.kafka.serialization.encoder

import io.github.vooft.kafka.common.annotations.IntEncoding
import io.github.vooft.kafka.serialization.common.encodeVarInt
import io.github.vooft.kafka.serialization.common.primitives.VarInt
import kotlinx.io.Sink
import kotlinx.serialization.descriptors.SerialDescriptor
import kotlinx.serialization.encoding.CompositeEncoder
import kotlinx.serialization.modules.SerializersModule

class KafkaListEncoder(
    sink: Sink,
    private val sizeEncoding: IntEncoding,
    override val serializersModule: SerializersModule,
) : KafkaValueEncoder(sink, serializersModule), AbstractKafkaCompositeEncoder {

    override fun beginCollection(descriptor: SerialDescriptor, collectionSize: Int): CompositeEncoder {
        when (sizeEncoding) {
            IntEncoding.INT16 -> encodeShort(collectionSize.toShort())
            IntEncoding.INT32 -> encodeInt(collectionSize)
            IntEncoding.VARINT -> encodeVarInt(VarInt.fromDecoded(collectionSize))
        }

        return this
    }

    override fun endStructure(descriptor: SerialDescriptor) {
        // do nothing
    }
}
