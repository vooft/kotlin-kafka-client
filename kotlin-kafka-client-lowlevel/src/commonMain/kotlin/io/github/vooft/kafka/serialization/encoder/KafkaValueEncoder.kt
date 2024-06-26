package io.github.vooft.kafka.serialization.encoder

import io.github.vooft.kafka.common.annotations.KafkaBytesSizePrefixed
import io.github.vooft.kafka.common.annotations.KafkaCollection
import io.github.vooft.kafka.common.annotations.KafkaCrc32cPrefixed
import io.github.vooft.kafka.common.annotations.KafkaString
import kotlinx.io.Sink
import kotlinx.serialization.ExperimentalSerializationApi
import kotlinx.serialization.SerializationStrategy
import kotlinx.serialization.descriptors.SerialDescriptor
import kotlinx.serialization.encoding.CompositeEncoder
import kotlinx.serialization.encoding.Encoder
import kotlinx.serialization.modules.EmptySerializersModule
import kotlinx.serialization.modules.SerializersModule

@OptIn(ExperimentalSerializationApi::class)
open class KafkaValueEncoder(
    private val sink: Sink,
    override val serializersModule: SerializersModule = EmptySerializersModule()
) : Encoder {

    override fun beginStructure(descriptor: SerialDescriptor): CompositeEncoder {
        return KafkaObjectEncoder(sink, serializersModule)
    }

    override fun encodeBoolean(value: Boolean) = sink.writeByte(if (value) 1 else 0)
    override fun encodeByte(value: Byte) = sink.writeByte(value)
    override fun encodeInt(value: Int) = sink.writeInt(value)
    override fun encodeLong(value: Long) = sink.writeLong(value)
    override fun encodeShort(value: Short) = sink.writeShort(value)

    override fun encodeString(value: String): Unit = error("Strings should not be encoded directly")

    override fun encodeInline(descriptor: SerialDescriptor): Encoder {
        val kafkaString = descriptor.annotations.filterIsInstance<KafkaString>().singleOrNull()
        if (kafkaString != null) {
            return KafkaStringEncoder(sink = sink, lengthEncoding = kafkaString.lengthEncoding, serializersModule = serializersModule)
        }

        val kafkaCollection = descriptor.annotations.filterIsInstance<KafkaCollection>().singleOrNull()
        if (kafkaCollection != null) {
            return KafkaListEncoder(sink = sink, sizeEncoding = kafkaCollection.sizeEncoding, serializersModule = serializersModule)
        }

        val kafkaBytesSizePrefixed = descriptor.annotations.filterIsInstance<KafkaBytesSizePrefixed>().singleOrNull()
        if (kafkaBytesSizePrefixed != null) {
            return KafkaBytesSizePrefixedEncoder(
                sink = sink,
                sizeEncoding = kafkaBytesSizePrefixed.sizeEncoding,
                serializersModule = serializersModule
            )
        }

        val crc32cPrefixed = descriptor.annotations.filterIsInstance<KafkaCrc32cPrefixed>().singleOrNull()
        if (crc32cPrefixed != null) {
            return KafkaCrc32cPrefixedEncoder(
                sink = sink,
                serializersModule = serializersModule
            )
        }

        return this
    }

    override fun encodeChar(value: Char) {
        TODO("Not yet implemented")
    }

    override fun encodeDouble(value: Double) {
        TODO("Not yet implemented")
    }

    override fun encodeEnum(enumDescriptor: SerialDescriptor, index: Int) {
        TODO("Not yet implemented")
    }

    override fun encodeFloat(value: Float) {
        TODO("Not yet implemented")
    }

    @ExperimentalSerializationApi
    override fun encodeNull() = error("Nulls are not supported")

    @ExperimentalSerializationApi
    override fun <T : Any> encodeNullableSerializableValue(serializer: SerializationStrategy<T>, value: T?) {
        if (value == null) {
            val elementDescriptor = serializer.descriptor
            error("Nullable fields are not allowed: ${elementDescriptor.serialName}")
        } else {
            encodeSerializableValue(serializer, value)
        }
    }
}
