package io.github.vooft.kafka.serialization.decoder

import kotlinx.serialization.DeserializationStrategy
import kotlinx.serialization.ExperimentalSerializationApi
import kotlinx.serialization.descriptors.SerialDescriptor
import kotlinx.serialization.encoding.CompositeDecoder
import kotlinx.serialization.encoding.Decoder

@OptIn(ExperimentalSerializationApi::class)
abstract class AbstractKafkaCompositeDecoder(private val delegate: Decoder) : CompositeDecoder {

    override fun decodeBooleanElement(descriptor: SerialDescriptor, index: Int) = delegate.decodeBoolean()

    override fun decodeByteElement(descriptor: SerialDescriptor, index: Int) = delegate.decodeByte()

    override fun decodeCharElement(descriptor: SerialDescriptor, index: Int) = delegate.decodeChar()

    override fun decodeDoubleElement(descriptor: SerialDescriptor, index: Int) = delegate.decodeDouble()

    override fun decodeElementIndex(descriptor: SerialDescriptor) = delegate.decodeInt()

    override fun decodeFloatElement(descriptor: SerialDescriptor, index: Int) = delegate.decodeFloat()

    override fun decodeInlineElement(descriptor: SerialDescriptor, index: Int) = delegate.decodeInline(descriptor)

    override fun decodeIntElement(descriptor: SerialDescriptor, index: Int) = delegate.decodeInt()

    override fun decodeLongElement(descriptor: SerialDescriptor, index: Int) = delegate.decodeLong()

    override fun decodeShortElement(descriptor: SerialDescriptor, index: Int) = delegate.decodeShort()

    override fun decodeStringElement(descriptor: SerialDescriptor, index: Int) = delegate.decodeString()

    override fun endStructure(descriptor: SerialDescriptor) = Unit

    override fun <T : Any> decodeNullableSerializableElement(
        descriptor: SerialDescriptor,
        index: Int,
        deserializer: DeserializationStrategy<T?>,
        previousValue: T?
    ): T? = delegate.decodeNullableSerializableValue(deserializer)

    override fun <T> decodeSerializableElement(
        descriptor: SerialDescriptor,
        index: Int,
        deserializer: DeserializationStrategy<T>,
        previousValue: T?
    ) = delegate.decodeSerializableValue(deserializer)
}

