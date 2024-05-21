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

@Serializable(with = VarIntSerializer::class)
@JvmInline
value class VarInt(val value: Int)

fun Int.toVarInt() = VarInt(this)

// adapted from https://github.com/addthis/stream-lib
internal object VarIntSerializer : KSerializer<VarInt> {
    override val descriptor: SerialDescriptor = PrimitiveSerialDescriptor("VarInt", PrimitiveKind.INT)

    override fun deserialize(decoder: Decoder): VarInt {
        var value = 0
        var index = 0
        var currentByte: Int
        while (true) {
            currentByte = decoder.decodeByte().toInt()
            if (currentByte and 0x80 == 0) {
                break
            }

            value = value or ((currentByte and 0x7F) shl index)
            index += 7
            require(index <= 35) { "Variable length quantity is too long" }
        }

        return VarInt(value or (currentByte shl index))
    }

    override fun serialize(encoder: Encoder, value: VarInt) {
        var varInt = ZigzagInteger.encode(value.value)
        while (varInt and -0x80 != 0) {
            encoder.encodeByte(((varInt and 0x7F) or 0x80).toByte())
            varInt = varInt ushr 7
        }

        encoder.encodeByte((varInt and 0x7F).toByte())
    }

}
