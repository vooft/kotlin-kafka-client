package io.github.vooft.kafka.serialization.encoder

import io.github.vooft.kafka.serialization.common.IntEncoding
import io.github.vooft.kafka.serialization.common.KafkaSizeInBytesPrefixed
import io.github.vooft.kafka.serialization.common.encodeVarInt
import io.github.vooft.kafka.serialization.common.primitives.VarInt
import kotlinx.io.Buffer
import kotlinx.io.Sink
import kotlinx.serialization.ExperimentalSerializationApi
import kotlinx.serialization.SerializationStrategy
import kotlinx.serialization.descriptors.SerialDescriptor
import kotlinx.serialization.modules.EmptySerializersModule
import kotlinx.serialization.modules.SerializersModule

@OptIn(ExperimentalSerializationApi::class)
class KafkaObjectEncoder(
    private val sink: Sink,
    override val serializersModule: SerializersModule = EmptySerializersModule(),
) : KafkaValueEncoder(sink, serializersModule), AbstractKafkaCompositeEncoder {

    override fun <T> encodeSerializableElement(descriptor: SerialDescriptor, index: Int, serializer: SerializationStrategy<T>, value: T) {
        val annotations = descriptor.getElementAnnotations(index)
        val elementDescriptor = descriptor.getElementDescriptor(index)
        when {
            // TODO: move to a separate class?
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
//            elementDescriptor.kind == StructureKind.LIST -> {
//                val size = when (value) {
//                    is Collection<*> -> value.size
//                    is ByteArray -> value.size
//                    is VarIntByteArray -> null
//                    null -> -1
//                    else -> error("Unsupported collection type: ${value!!::class}")
//                }
//
//                if (size != null) {
//                    when {
//                        annotations.any { it is KafkaCollectionWithVarIntSize } -> encodeVarInt(VarInt.fromDecoded(size))
//                        else -> encodeInt(size)
//                    }
//                }
//
//                encodeSerializableValue(serializer, value)
//            }
            else -> encodeSerializableValue(serializer, value)
        }
    }

    override fun endStructure(descriptor: SerialDescriptor) = Unit

}
