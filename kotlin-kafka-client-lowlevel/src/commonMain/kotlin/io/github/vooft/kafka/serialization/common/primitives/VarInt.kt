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
value class VarInt private constructor(private val zigzagEncoded: Int) {

    fun toEncoded() = zigzagEncoded
    fun toDecoded() = ZigzagInteger.decode(zigzagEncoded)

    companion object {
        val MINUS_ONE = VarInt(ZigzagInteger.encode(-1))
        fun fromDecoded(value: Int) = VarInt(ZigzagInteger.encode(value))
        fun fromEncoded(value: Int) = VarInt(value)
    }
}

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

        return VarInt.fromEncoded(value or (currentByte shl index))
    }

    override fun serialize(encoder: Encoder, value: VarInt) {
        var varInt = value.toEncoded()
        while (varInt and -0x80 != 0) {
            encoder.encodeByte(((varInt and 0x7F) or 0x80).toByte())
            varInt = varInt ushr 7
        }

        encoder.encodeByte((varInt and 0x7F).toByte())
    }

}
