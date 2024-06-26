package io.github.vooft.kafka.serialization.decoder

import kotlinx.io.Source
import kotlinx.serialization.ExperimentalSerializationApi
import kotlinx.serialization.descriptors.SerialDescriptor
import kotlinx.serialization.encoding.CompositeDecoder
import kotlinx.serialization.modules.EmptySerializersModule
import kotlinx.serialization.modules.SerializersModule

@OptIn(ExperimentalSerializationApi::class)
internal class KafkaObjectDecoder(
    source: Source,
    override val serializersModule: SerializersModule = EmptySerializersModule(),
) : KafkaValueDecoder(source, serializersModule), AbstractKafkaCompositeDecoder {

    private var elementIndex = 0

    override fun decodeElementIndex(descriptor: SerialDescriptor): Int {
        if (elementIndex >= descriptor.elementsCount) {
            return CompositeDecoder.DECODE_DONE
        }

        return elementIndex++
    }
}
