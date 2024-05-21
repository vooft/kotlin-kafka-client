package io.github.vooft.kafka.serialization.encoder

import io.github.vooft.kafka.serialization.common.CRC32
import io.github.vooft.kafka.serialization.common.IntEncoding
import io.github.vooft.kafka.serialization.common.KafkaCrc32Prefixed
import io.github.vooft.kafka.serialization.common.KafkaSizeInBytesPrefixed
import io.github.vooft.kafka.serialization.common.primitives.VarInt
import kotlinx.io.Buffer
import kotlinx.io.Sink
import kotlinx.serialization.ExperimentalSerializationApi
import kotlinx.serialization.SerializationStrategy
import kotlinx.serialization.descriptors.SerialDescriptor
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

                val annotation = annotations.filterIsInstance<KafkaSizeInBytesPrefixed>().single()

                when (annotation.encoding) {
                    IntEncoding.INT32 -> encodeInt(buffer.size.toInt())
                    IntEncoding.VARINT -> encodeVarInt(VarInt(buffer.size.toInt()))
                }

                sink.write(buffer, buffer.size)
            }
            else -> encodeSerializableValue(serializer, value)
        }
    }

    override fun endStructure(descriptor: SerialDescriptor) = Unit

}
