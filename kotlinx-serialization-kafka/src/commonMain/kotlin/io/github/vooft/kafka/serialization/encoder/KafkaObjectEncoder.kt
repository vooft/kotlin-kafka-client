package io.github.vooft.kafka.serialization.encoder

import kotlinx.io.Sink
import kotlinx.serialization.descriptors.SerialDescriptor
import kotlinx.serialization.encoding.Encoder
import kotlinx.serialization.modules.SerializersModule

class KafkaObjectEncoder(
    private val sink: Sink,
    override val serializersModule: SerializersModule,
    valueEncoder: KafkaValueEncoder = KafkaValueEncoder(sink, serializersModule)
) : AbstractKafkaCompositeEncoder(valueEncoder), Encoder by valueEncoder {

    override fun endStructure(descriptor: SerialDescriptor) = Unit

}
