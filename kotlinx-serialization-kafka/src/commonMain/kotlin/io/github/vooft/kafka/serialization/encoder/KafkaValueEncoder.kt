package io.github.vooft.kafka.serialization.encoder

import io.github.vooft.kafka.serialization.common.KafkaString
import io.github.vooft.kafka.serialization.common.primitives.KafkaCollection
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
            return KafkaStringEncoder(sink, kafkaString.encoding, serializersModule)
        }

        val kafkaCollection = descriptor.annotations.filterIsInstance<KafkaCollection>().singleOrNull()
        if (kafkaCollection != null) {
            return KafkaListEncoder(sink = sink, sizeEncoding = kafkaCollection.sizeEncoding, serializersModule = serializersModule)
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

    override fun <T> encodeSerializableValue(serializer: SerializationStrategy<T>, value: T) {
        // add crc32 prefixed composite encoder?
        // add size prefixed composite encoder?
        super.encodeSerializableValue(serializer, value)
    }

    @ExperimentalSerializationApi
    override fun <T : Any> encodeNullableSerializableValue(serializer: SerializationStrategy<T>, value: T?) {
        if (value == null) {
            val elementDescriptor = serializer.descriptor
            error("Nullable fields are not allowed: ${elementDescriptor.serialName}")
        } else {
            encodeSerializableValue(serializer, value)
        }

//        if (value == null) {
//            when (elementDescriptor.serialName) {
//                Constants.NULLABLE_STRING, Constants.REGULAR_STRING -> sink.writeShort(-1)
//                else -> error("Unsupported nullable type: ${elementDescriptor.serialName}")
//            }
//        } else {
//            encodeSerializableValue(serializer, value)
//        }
    }
}
