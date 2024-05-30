package io.github.vooft.kafka.serialization.decoder

import io.github.vooft.kafka.serialization.common.IntEncoding
import io.github.vooft.kafka.serialization.common.decodeVarInt
import kotlinx.io.Source
import kotlinx.serialization.ExperimentalSerializationApi
import kotlinx.serialization.descriptors.SerialDescriptor
import kotlinx.serialization.descriptors.StructureKind
import kotlinx.serialization.encoding.CompositeDecoder
import kotlinx.serialization.modules.SerializersModule

@OptIn(ExperimentalSerializationApi::class)
internal class KafkaListDecoder(
    source: Source,
    sizeEncoding: IntEncoding,
    override val serializersModule: SerializersModule,
) : KafkaValueDecoder(source, serializersModule), AbstractKafkaCompositeDecoder {

    private var currentListElementIndex = 0
    private val size: Int = when (sizeEncoding) {
        IntEncoding.INT16 -> decodeShort().toInt()
        IntEncoding.INT32 -> decodeInt()
        IntEncoding.VARINT -> decodeVarInt().toDecoded()
    }

    override fun beginStructure(descriptor: SerialDescriptor): CompositeDecoder {
        if (currentListElementIndex == 0) {
            require(descriptor.kind == StructureKind.LIST) { "Only list deserialization is supported" }
            return this
        } else {
            return super.beginStructure(descriptor)
        }
    }

    override fun decodeElementIndex(descriptor: SerialDescriptor): Int {
        require(descriptor.kind == StructureKind.LIST) { "Only list deserialization is supported" }

        if (currentListElementIndex >= size) {
            return CompositeDecoder.DECODE_DONE
        } else {
            return currentListElementIndex++
        }
    }
}
