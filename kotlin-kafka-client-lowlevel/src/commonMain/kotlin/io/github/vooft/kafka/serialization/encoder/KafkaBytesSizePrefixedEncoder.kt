package io.github.vooft.kafka.serialization.encoder

import io.github.vooft.kafka.common.annotations.IntEncoding
import io.github.vooft.kafka.serialization.common.primitives.VarInt
import kotlinx.io.Buffer
import kotlinx.io.Sink
import kotlinx.serialization.descriptors.SerialDescriptor
import kotlinx.serialization.modules.SerializersModule

class KafkaBytesSizePrefixedEncoder(
    private val sink: Sink,
    private val buffer: Buffer = Buffer(),
    private val sizeEncoding: IntEncoding,
    override val serializersModule: SerializersModule,
) : KafkaValueEncoder(buffer, serializersModule), AbstractKafkaCompositeEncoder {

    override fun beginStructure(descriptor: SerialDescriptor) = when (buffer.size) {
        0L -> this
        else -> super.beginStructure(descriptor)
    }

    override fun endStructure(descriptor: SerialDescriptor) {
        val size = buffer.size.toInt()
        when (sizeEncoding) {
            IntEncoding.INT16 -> sink.writeShort(size.toShort())
            IntEncoding.INT32 -> sink.writeInt(size)
            IntEncoding.VARINT -> {
                val objectEncoder = KafkaObjectEncoder(sink, serializersModule)
                objectEncoder.encodeSerializableValue(VarInt.serializer(), VarInt.fromDecoded(size))
            }
        }

        sink.write(buffer, buffer.size)
    }
}
