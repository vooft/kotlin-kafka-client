package io.github.vooft.kafka.serialization.decoder

import kotlinx.io.Source
import kotlinx.io.readString
import kotlinx.serialization.DeserializationStrategy
import kotlinx.serialization.ExperimentalSerializationApi
import kotlinx.serialization.descriptors.SerialDescriptor
import kotlinx.serialization.descriptors.StructureKind
import kotlinx.serialization.encoding.CompositeDecoder
import kotlinx.serialization.encoding.Decoder
import kotlinx.serialization.modules.SerializersModule

@OptIn(ExperimentalSerializationApi::class)
class KafkaValueDecoder(
    private val source: Source,
    override val serializersModule: SerializersModule
) : Decoder {
    override fun beginStructure(descriptor: SerialDescriptor): CompositeDecoder = when (descriptor.kind) {
        StructureKind.OBJECT, StructureKind.CLASS -> KafkaObjectDecoder(source, serializersModule)
        StructureKind.LIST -> KafkaListDecoder(source.readInt(), source, serializersModule)
        else -> error("Not supported ${descriptor.kind}")
    }

    override fun decodeBoolean(): Boolean = error("Boolean is not supported")
    override fun decodeChar(): Char = error("Char is not supported")
    override fun decodeDouble(): Double = error("Double is not supported")
    override fun decodeFloat(): Float = error("Float is not supported")

    override fun decodeByte(): Byte {
        println("decodeByte")
        return source.readByte()
    }

    override fun decodeEnum(enumDescriptor: SerialDescriptor): Int {
        TODO("Not yet implemented")
    }

    override fun decodeInline(descriptor: SerialDescriptor): Decoder = this

    override fun decodeInt(): Int = source.readInt()

    override fun decodeLong(): Long = source.readLong()

    @ExperimentalSerializationApi
    override fun decodeNotNullMark(): Boolean = error("Separate null mark decoding is not supported")

    @ExperimentalSerializationApi
    override fun decodeNull(): Nothing? = null

    override fun decodeShort(): Short = source.readShort()

    override fun decodeString(): String {
        val length = source.readShort()
        return source.readString(length.toLong())
    }

    @ExperimentalSerializationApi
    override fun <T : Any> decodeNullableSerializableValue(deserializer: DeserializationStrategy<T?>): T? {
        when (deserializer.descriptor.serialName) {
            "kotlin.String?" -> {
                val length = source.readShort()
                return if (length == NULL_STRING_LENGTH) {
                    null
                } else {
                    source.readString(length.toLong()) as T
                }
            }
            else -> error("Unsupported nullable type: ${deserializer.descriptor.serialName}")
        }
    }
}

private const val NULL_STRING_LENGTH: Short = -1
