package io.github.vooft.kafka.serialization.common.customtypes

import io.github.vooft.kafka.serialization.common.IntEncoding
import io.github.vooft.kafka.serialization.common.KafkaString
import kotlinx.serialization.KSerializer
import kotlinx.serialization.Serializable
import kotlinx.serialization.descriptors.PrimitiveKind
import kotlinx.serialization.descriptors.PrimitiveSerialDescriptor
import kotlinx.serialization.descriptors.SerialDescriptor
import kotlinx.serialization.encoding.Decoder
import kotlinx.serialization.encoding.Encoder
import kotlin.jvm.JvmInline

//@Serializable(with = Int16StringSerializer::class)
@KafkaString(encoding = IntEncoding.INT16)
@Serializable
@JvmInline
value class Int16String(val value: String?) {
    init {
        if (value != null) {
            require(value.length <= Short.MAX_VALUE) { "String is too long: ${value.length}" }
        }
    }

    companion object {
        val NULL = Int16String(null)
    }
}

fun String?.toInt16String() = Int16String(this)
fun List<String>.toInt16String() = map { it.toInt16String() }

object Int16StringSerializer : KSerializer<Int16String> {
    override val descriptor: SerialDescriptor = PrimitiveSerialDescriptor("Int16String", PrimitiveKind.STRING)

    override fun deserialize(decoder: Decoder): Int16String {
        val length = decoder.decodeShort()
        return if (length == NULL_INT16STRING_LENGTH) {
            Int16String.NULL
        } else {
            // TODO: improve
            val array = ByteArray(length.toInt()) { decoder.decodeByte() }
            array.decodeToString().toInt16String()
        }
    }

    override fun serialize(encoder: Encoder, value: Int16String) {
        if (value.value == null) {
            encoder.encodeShort(NULL_INT16STRING_LENGTH)
        } else {
            encoder.encodeShort(value.value.length.toShort())
            encoder.encodeString(value.value)
        }
    }
}

private const val NULL_INT16STRING_LENGTH: Short = -1
