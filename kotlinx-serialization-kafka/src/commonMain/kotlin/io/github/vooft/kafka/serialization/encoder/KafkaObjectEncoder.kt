package io.github.vooft.kafka.serialization.encoder

import io.github.vooft.kafka.serialization.common.CRC32
import io.github.vooft.kafka.serialization.common.IntEncoding
import io.github.vooft.kafka.serialization.common.KafkaCollectionWithVarIntSize
import io.github.vooft.kafka.serialization.common.KafkaCrc32Prefixed
import io.github.vooft.kafka.serialization.common.KafkaSizeInBytesPrefixed
import io.github.vooft.kafka.serialization.common.customtypes.VarIntByteArray
import io.github.vooft.kafka.serialization.common.encodeVarInt
import io.github.vooft.kafka.serialization.common.primitives.VarInt
import kotlinx.io.Buffer
import kotlinx.io.Sink
import kotlinx.serialization.ExperimentalSerializationApi
import kotlinx.serialization.SerializationStrategy
import kotlinx.serialization.descriptors.SerialDescriptor
import kotlinx.serialization.descriptors.StructureKind
import kotlinx.serialization.encoding.Encoder
import kotlinx.serialization.modules.EmptySerializersModule
import kotlinx.serialization.modules.SerializersModule

@OptIn(ExperimentalSerializationApi::class)
class KafkaObjectEncoder(
    private val sink: Sink,
    override val serializersModule: SerializersModule = EmptySerializersModule(),
    valueEncoder: KafkaValueEncoder = KafkaValueEncoder(sink, serializersModule)
) : AbstractKafkaCompositeEncoder(valueEncoder), Encoder by valueEncoder {

    override fun <T> encodeSerializableElement(descriptor: SerialDescriptor, index: Int, serializer: SerializationStrategy<T>, value: T) {
        val annotations = descriptor.getElementAnnotations(index)
        val elementDescriptor = descriptor.getElementDescriptor(index)
        when {
            // TODO: move to a separate class?
            annotations.any { it is KafkaCrc32Prefixed } -> {
                val buffer = Buffer()
                val nestedEncoder = KafkaObjectEncoder(buffer, serializersModule)
                nestedEncoder.encodeSerializableValue(serializer, value)

                sink.writeInt(CRC32.crc32c(buffer.peek()))
                sink.write(buffer, buffer.size)
            }
            annotations.any { it  is KafkaSizeInBytesPrefixed } -> {
                val buffer = Buffer()
                val nestedEncoder = KafkaObjectEncoder(buffer, serializersModule)
                nestedEncoder.encodeSerializableValue(serializer, value)

                // TODO: add custom class wrapping collection
                val annotation = annotations.filterIsInstance<KafkaSizeInBytesPrefixed>().single()

                when (annotation.encoding) {
                    IntEncoding.INT32 -> encodeInt(buffer.size.toInt())
                    IntEncoding.VARINT -> encodeVarInt(VarInt.fromDecoded(buffer.size.toInt()))
                    IntEncoding.INT16 -> error("Unsupported encoding: ${IntEncoding.INT16}")
                }

                sink.write(buffer, buffer.size)
            }
            elementDescriptor.kind == StructureKind.LIST -> {
                val size = when (value) {
                    is Collection<*> -> value.size
                    is ByteArray -> value.size
                    is VarIntByteArray -> null
                    null -> -1
                    else -> error("Unsupported collection type: ${value!!::class}")
                }

                if (size != null) {
                    when {
                        annotations.any { it is KafkaCollectionWithVarIntSize } -> encodeVarInt(VarInt.fromDecoded(size))
                        else -> encodeInt(size)
                    }
                }

                encodeSerializableValue(serializer, value)
            }
            else -> encodeSerializableValue(serializer, value)
        }
    }

    override fun endStructure(descriptor: SerialDescriptor) = Unit

}
