package io.github.vooft.kafka.serialization.decoder

import io.github.vooft.kafka.serialization.common.CRC32
import io.github.vooft.kafka.serialization.common.IntEncoding
import io.github.vooft.kafka.serialization.common.KafkaCollectionWithVarIntSize
import io.github.vooft.kafka.serialization.common.KafkaCrc32Prefixed
import io.github.vooft.kafka.serialization.common.KafkaSizeInBytesPrefixed
import io.github.vooft.kafka.serialization.common.customtypes.KafkaCustomTypeSerializer
import kotlinx.io.Buffer
import kotlinx.io.Source
import kotlinx.serialization.DeserializationStrategy
import kotlinx.serialization.ExperimentalSerializationApi
import kotlinx.serialization.descriptors.SerialDescriptor
import kotlinx.serialization.descriptors.StructureKind
import kotlinx.serialization.encoding.CompositeDecoder
import kotlinx.serialization.encoding.Decoder
import kotlinx.serialization.modules.EmptySerializersModule
import kotlinx.serialization.modules.SerializersModule

@OptIn(ExperimentalSerializationApi::class)
internal class KafkaObjectDecoder(
    private val source: Source,
    override val serializersModule: SerializersModule = EmptySerializersModule(),
    valueDecoder: KafkaValueDecoder = KafkaValueDecoder(source, serializersModule)
) : AbstractKafkaCompositeDecoder(valueDecoder), Decoder by valueDecoder {

    private var elementIndex = 0

    override fun decodeElementIndex(descriptor: SerialDescriptor): Int {
        if (elementIndex >= descriptor.elementsCount) {
            return CompositeDecoder.DECODE_DONE
        }

        return elementIndex++
        //.also { println("Decoding ${descriptor.getElementName(it)}") }
    }

    override fun <T> decodeSerializableElement(
        descriptor: SerialDescriptor,
        index: Int,
        deserializer: DeserializationStrategy<T>,
        previousValue: T?
    ): T {
        val annotations = descriptor.getElementAnnotations(index)
        val elementDescriptor = descriptor.getElementDescriptor(index)

        return when {
            annotations.any { it is KafkaSizeInBytesPrefixed } -> {
                val annotation = annotations.filterIsInstance<KafkaSizeInBytesPrefixed>().single()
                val length = when (annotation.encoding) {
                    IntEncoding.INT32 -> decodeInt()
                    IntEncoding.VARINT -> decodeVarInt().toDecoded()
                    IntEncoding.INT16 -> error("Unsupported encoding: ${IntEncoding.INT16}")
                }

                val buffer = Buffer()
                source.readTo(buffer, length.toLong())

                val nestedDecoder = KafkaObjectDecoder(buffer, serializersModule)
                nestedDecoder.decodeSerializableValue(deserializer)
            }

            annotations.any { it is KafkaCrc32Prefixed } -> {
                val crc32 = decodeInt()
                val calculated = CRC32.crc32c(source.peek())

                require(crc32 == calculated) { "CRC32 mismatch: expected $crc32, but calculated $calculated" }
                decodeSerializableValue(deserializer)
            }

            elementDescriptor.kind == StructureKind.LIST -> {
                if (deserializer !is KafkaCustomTypeSerializer) {
                    // if this is just an annotated collection, then use special decoder
                    val size = when {
                        annotations.any { it is KafkaCollectionWithVarIntSize } -> decodeVarInt().toDecoded()
                        else -> decodeInt()
                    }

                    deserializer.deserialize(KafkaListDecoder(size, source, serializersModule))
                } else {
                    // if it is a custom type, then use the defined serializer
                    decodeSerializableValue(deserializer)
                }
            }

            else -> decodeSerializableValue(deserializer)
        }
    }
}
