package io.github.vooft.kafka.serialization

import kotlinx.io.Sink
import kotlinx.io.writeString
import kotlinx.serialization.ExperimentalSerializationApi
import kotlinx.serialization.SerializationStrategy
import kotlinx.serialization.descriptors.SerialDescriptor
import kotlinx.serialization.encoding.AbstractEncoder
import kotlinx.serialization.modules.EmptySerializersModule
import kotlinx.serialization.modules.SerializersModule

@OptIn(ExperimentalSerializationApi::class)
class KotlinxSerializationKafkaEncoder(
    private val sink: Sink,
    override val serializersModule: SerializersModule = EmptySerializersModule()
) : AbstractEncoder() {

    override fun encodeValue(value: Any) {
        error("Class ${value::class} is not allowed in Kafka: $value")
    }

    override fun encodeByte(value: Byte) = sink.writeByte(value)
    override fun encodeInt(value: Int) = sink.writeInt(value)
    override fun encodeLong(value: Long) = sink.writeLong(value)
    override fun encodeShort(value: Short) = sink.writeShort(value)

    override fun encodeString(value: String) {
        require(value.length <= Short.MAX_VALUE) { "String is too long: ${value.length}" }
        sink.writeShort(value.length.toShort())
        sink.writeString(value)
    }

    override fun <T : Any> encodeNullableSerializableElement(
        descriptor: SerialDescriptor,
        index: Int,
        serializer: SerializationStrategy<T>,
        value: T?
    ) {
        val elementDescriptor = descriptor.getElementDescriptor(index)
        if (value == null) {
            when (elementDescriptor.serialName) {
                "kotlin.String?" -> sink.writeShort(-1)
                else -> error("Unsupported nullable type: ${descriptor.serialName}")
            }
        } else {
            encodeSerializableElement(descriptor, index, serializer, value)
        }
    }
}
