package io.github.vooft.kafka.serialization.common.customtypes

import io.github.vooft.kafka.serialization.common.IntEncoding
import io.github.vooft.kafka.serialization.common.KafkaString
import kotlinx.serialization.Serializable
import kotlin.jvm.JvmInline

//@Serializable(with = Int16StringSerializer::class)
@KafkaString(encoding = IntEncoding.INT16)
@Serializable
@JvmInline
value class Int16String(val value: String?) : KafkaCustomType {
    init {
        if (value != null) {
            require(value.length <= Short.MAX_VALUE) { "String is too long: ${value.length}" }
        }
    }

    companion object {
        val NULL = Int16String(null)
    }
}

