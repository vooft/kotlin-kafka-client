package io.github.vooft.kafka.serialization.decoder

import kotlinx.serialization.DeserializationStrategy
import kotlinx.serialization.ExperimentalSerializationApi
import kotlinx.serialization.descriptors.SerialDescriptor
import kotlinx.serialization.encoding.CompositeDecoder
import kotlinx.serialization.encoding.Decoder

@OptIn(ExperimentalSerializationApi::class)
interface AbstractKafkaCompositeDecoder : CompositeDecoder, Decoder {

    override fun decodeBooleanElement(descriptor: SerialDescriptor, index: Int) = decodeBoolean()

    override fun decodeByteElement(descriptor: SerialDescriptor, index: Int) = decodeByte()

    override fun decodeCharElement(descriptor: SerialDescriptor, index: Int) = decodeChar()

    override fun decodeDoubleElement(descriptor: SerialDescriptor, index: Int) = decodeDouble()

    override fun decodeElementIndex(descriptor: SerialDescriptor) = decodeInt()

    override fun decodeFloatElement(descriptor: SerialDescriptor, index: Int) = decodeFloat()

    override fun decodeInlineElement(descriptor: SerialDescriptor, index: Int) = decodeInline(descriptor)

    override fun decodeIntElement(descriptor: SerialDescriptor, index: Int) = decodeInt()

    override fun decodeLongElement(descriptor: SerialDescriptor, index: Int) = decodeLong()

    override fun decodeShortElement(descriptor: SerialDescriptor, index: Int) = decodeShort()

    override fun decodeStringElement(descriptor: SerialDescriptor, index: Int) = decodeString()

    override fun endStructure(descriptor: SerialDescriptor) = Unit

    override fun <T : Any> decodeNullableSerializableElement(
        descriptor: SerialDescriptor,
        index: Int,
        deserializer: DeserializationStrategy<T?>,
        previousValue: T?
    ): T? = decodeNullableSerializableValue(deserializer)

    override fun <T> decodeSerializableElement(
        descriptor: SerialDescriptor,
        index: Int,
        deserializer: DeserializationStrategy<T>,
        previousValue: T?
    ) = decodeSerializableValue(deserializer)
}

