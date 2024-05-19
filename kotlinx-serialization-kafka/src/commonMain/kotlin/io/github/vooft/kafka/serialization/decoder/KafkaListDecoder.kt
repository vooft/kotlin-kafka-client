package io.github.vooft.kafka.serialization.decoder

import kotlinx.io.Source
import kotlinx.serialization.ExperimentalSerializationApi
import kotlinx.serialization.descriptors.SerialDescriptor
import kotlinx.serialization.descriptors.StructureKind
import kotlinx.serialization.encoding.CompositeDecoder
import kotlinx.serialization.encoding.Decoder
import kotlinx.serialization.modules.SerializersModule

@OptIn(ExperimentalSerializationApi::class)
internal class KafkaListDecoder(
    private val size: Int,
    private val source: Source,
    override val serializersModule: SerializersModule,
    valueDecoder: KafkaValueDecoder = KafkaValueDecoder(source, serializersModule)
) : AbstractKafkaCompositeDecoder(valueDecoder), Decoder by valueDecoder {

    private var currentListElementIndex = 0

    override fun decodeElementIndex(descriptor: SerialDescriptor): Int {
        require(descriptor.kind == StructureKind.LIST) { "Only list deserialization is supported" }

        if (currentListElementIndex >= size) {
            return CompositeDecoder.DECODE_DONE
        } else {
            return currentListElementIndex++
        }
    }
}
