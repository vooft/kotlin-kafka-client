package io.github.vooft.kafka.serialization.common.primitives

import io.github.vooft.kafka.serialization.common.ZigzagInteger
import kotlinx.serialization.KSerializer
import kotlinx.serialization.Serializable
import kotlinx.serialization.descriptors.PrimitiveKind
import kotlinx.serialization.descriptors.PrimitiveSerialDescriptor
import kotlinx.serialization.descriptors.SerialDescriptor
import kotlinx.serialization.encoding.Decoder
import kotlinx.serialization.encoding.Encoder
import kotlin.jvm.JvmInline

@Serializable(with = VarLongSerializer::class)
@JvmInline
value class VarLong(val value: Long)

fun Int.toVarLong() = VarLong(this.toLong())

// adapted from https://github.com/addthis/stream-lib
internal object VarLongSerializer : KSerializer<VarLong> {
    override val descriptor: SerialDescriptor = PrimitiveSerialDescriptor("VarLong", PrimitiveKind.LONG)

    override fun deserialize(decoder: Decoder): VarLong {
        var value = 0L
        var index = 0
        var currentByte: Long
        while (true) {
            currentByte = decoder.decodeByte().toLong()
            if (currentByte and 0x80L == 0L) {
                break
            }

            value = value or ((currentByte and 0x7FL) shl index)
            index += 7
            require(index <= 63) { "Variable length quantity is too long" }
        }
        return VarLong(value or (currentByte shl index))
    }

    override fun serialize(encoder: Encoder, value: VarLong) {
        var varLong = ZigzagInteger.encode(value.value)
        while (varLong and -0x80L != 0L) {
            encoder.encodeByte(((varLong and 0x7F) or 0x80).toByte())
            varLong = varLong ushr 7
        }

        encoder.encodeByte((varLong and 0x7F).toByte())
    }

}
