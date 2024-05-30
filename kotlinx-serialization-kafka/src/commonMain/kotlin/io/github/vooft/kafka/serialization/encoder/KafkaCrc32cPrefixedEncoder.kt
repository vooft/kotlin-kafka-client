package io.github.vooft.kafka.serialization.encoder

import io.github.vooft.kafka.serialization.common.CRC32
import kotlinx.io.Buffer
import kotlinx.io.Sink
import kotlinx.serialization.descriptors.SerialDescriptor
import kotlinx.serialization.modules.SerializersModule

class KafkaCrc32cPrefixedEncoder(
    private val sink: Sink,
    private val buffer: Buffer = Buffer(),
    override val serializersModule: SerializersModule,
) : KafkaValueEncoder(buffer, serializersModule), AbstractKafkaCompositeEncoder {

    override fun beginStructure(descriptor: SerialDescriptor) = when (buffer.size) {
        0L -> this
        else -> super.beginStructure(descriptor)
    }

    override fun endStructure(descriptor: SerialDescriptor) {
        val crc32 = CRC32.crc32c(buffer.peek())
        sink.writeInt(crc32)

        sink.write(buffer, buffer.size)
    }
}
