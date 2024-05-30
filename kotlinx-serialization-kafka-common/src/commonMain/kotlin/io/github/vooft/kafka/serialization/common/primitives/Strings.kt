package io.github.vooft.kafka.serialization.common.primitives

import io.github.vooft.kafka.serialization.common.KafkaString
import kotlinx.serialization.Serializable
import kotlin.jvm.JvmInline

@KafkaString(lengthEncoding = IntEncoding.INT16)
@Serializable
@JvmInline
value class Int16String(val value: String) {
    init {
        require(value.length <= Short.MAX_VALUE) { "String is too long: ${value.length}" }
    }
}

@KafkaString(lengthEncoding = IntEncoding.INT16)
@Serializable
@JvmInline
value class NullableInt16String(val value: String?) {
    init {
        if (value != null) {
            require(value.length <= Short.MAX_VALUE) { "String is too long: ${value.length}" }
        }
    }

    companion object {
        val NULL = NullableInt16String(null)
    }
}

@KafkaString(lengthEncoding = IntEncoding.VARINT)
@Serializable
@JvmInline
value class VarIntString(val value: String?)
