package io.github.vooft.kafka.transport.dtos

import io.github.vooft.kafka.serialization.common.IntValue
import kotlinx.serialization.Serializable
import kotlin.jvm.JvmInline

@Serializable
@JvmInline
value class CorrelationId(override val value: Int) : IntValue {
    companion object {
        private var counter = 666 // TODO: use atomic
        fun next() = CorrelationId(counter++)
    }
}
