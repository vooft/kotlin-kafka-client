package io.github.vooft.kafka.serialization.common.customtypes

import io.github.vooft.kafka.serialization.common.IntEncoding
import io.github.vooft.kafka.serialization.common.KafkaString
import kotlinx.serialization.Serializable
import kotlin.jvm.JvmInline

@KafkaString(encoding = IntEncoding.INT16)
@Serializable
@JvmInline
value class NullableInt16String(val value: String?) : KafkaCustomType {
    init {
        if (value != null) {
            require(value.length <= Short.MAX_VALUE) { "String is too long: ${value.length}" }
        }
    }

    val nonNullValue: String get() = requireNotNull(value)

    companion object {
        val NULL = NullableInt16String(null)
    }
}


@KafkaString(encoding = IntEncoding.INT16)
@Serializable
@JvmInline
value class Int16String(val value: String) : KafkaCustomType {
    init {
        require(value.length <= Short.MAX_VALUE) { "String is too long: ${value.length}" }
    }
}
