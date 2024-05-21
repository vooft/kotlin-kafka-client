package io.github.vooft.kafka.serialization.common

import kotlinx.io.Sink
import kotlin.jvm.JvmInline

@JvmInline
value class VarInt(val value: Int)

fun Sink.writeVarInt(value: VarInt) {
    var varInt = value.value
    while ((varInt and -0x80).toLong() != 0L) {
        writeByte(((varInt and 0x7F) or 0x80).toByte())
        varInt = varInt ushr 7
    }
}
