package io.github.vooft.kafka.transport.dtos

import kotlinx.serialization.Serializable
import kotlin.jvm.JvmInline

@Serializable
@JvmInline
value class CorrelationId(val value: Int) {
    companion object {
        private var counter = 666 // TODO: use atomic
        fun next() = CorrelationId(counter++)
    }
}
