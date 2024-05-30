package io.github.vooft.kafka.serialization.encoder

import kotlinx.serialization.ExperimentalSerializationApi
import kotlinx.serialization.SerializationStrategy
import kotlinx.serialization.descriptors.SerialDescriptor
import kotlinx.serialization.encoding.CompositeEncoder
import kotlinx.serialization.encoding.Encoder

@OptIn(ExperimentalSerializationApi::class)
interface AbstractKafkaCompositeEncoder2 : CompositeEncoder, Encoder {

    fun encodeElement(descriptor: SerialDescriptor, index: Int): Boolean = true

    override fun encodeBooleanElement(descriptor: SerialDescriptor, index: Int, value: Boolean) {
        if (encodeElement(descriptor, index)) encodeBoolean(value)
    }

    override fun encodeByteElement(descriptor: SerialDescriptor, index: Int, value: Byte) {
        if (encodeElement(descriptor, index)) encodeByte(value)
    }

    override fun encodeShortElement(descriptor: SerialDescriptor, index: Int, value: Short) {
        if (encodeElement(descriptor, index)) encodeShort(value)
    }

    override fun encodeIntElement(descriptor: SerialDescriptor, index: Int, value: Int) {
        if (encodeElement(descriptor, index)) encodeInt(value)
    }

    override fun encodeLongElement(descriptor: SerialDescriptor, index: Int, value: Long) {
        if (encodeElement(descriptor, index)) encodeLong(value)
    }

    override fun encodeFloatElement(descriptor: SerialDescriptor, index: Int, value: Float) {
        if (encodeElement(descriptor, index)) encodeFloat(value)
    }

    override fun encodeDoubleElement(descriptor: SerialDescriptor, index: Int, value: Double) {
        if (encodeElement(descriptor, index)) encodeDouble(value)
    }

    override fun encodeCharElement(descriptor: SerialDescriptor, index: Int, value: Char) {
        if (encodeElement(descriptor, index)) encodeChar(value)
    }

    override fun encodeStringElement(descriptor: SerialDescriptor, index: Int, value: String) {
        if (encodeElement(descriptor, index)) encodeString(value)
    }

    override fun encodeInlineElement(
        descriptor: SerialDescriptor,
        index: Int
    ): Encoder =
        if (encodeElement(descriptor, index)) encodeInline(descriptor.getElementDescriptor(index)) else this

    override fun <T : Any?> encodeSerializableElement(
        descriptor: SerialDescriptor,
        index: Int,
        serializer: SerializationStrategy<T>,
        value: T
    ) {
        if (encodeElement(descriptor, index))
            encodeSerializableValue(serializer, value)
    }

    override fun <T : Any> encodeNullableSerializableElement(
        descriptor: SerialDescriptor,
        index: Int,
        serializer: SerializationStrategy<T>,
        value: T?
    ) {
        if (encodeElement(descriptor, index))
            encodeNullableSerializableValue(serializer, value)
    }
}
