package io.github.vooft.kafka.serialization.encoder

import kotlinx.io.Sink
import kotlinx.serialization.descriptors.SerialDescriptor
import kotlinx.serialization.modules.EmptySerializersModule
import kotlinx.serialization.modules.SerializersModule

class KafkaObjectEncoder(
    sink: Sink,
    override val serializersModule: SerializersModule = EmptySerializersModule(),
) : KafkaValueEncoder(sink, serializersModule), AbstractKafkaCompositeEncoder {

    override fun endStructure(descriptor: SerialDescriptor) = Unit

}
