package io.github.vooft.kafka.serialization.common.customtypes

import io.github.vooft.kafka.serialization.common.ZigzagInteger
import io.github.vooft.kafka.serialization.encoder.encodeVarInt
import kotlinx.serialization.KSerializer
import kotlinx.serialization.Serializable
import kotlinx.serialization.descriptors.PrimitiveKind
import kotlinx.serialization.descriptors.PrimitiveSerialDescriptor
import kotlinx.serialization.descriptors.SerialDescriptor
import kotlinx.serialization.encoding.Decoder
import kotlinx.serialization.encoding.Encoder
import kotlin.jvm.JvmInline

@Serializable(with = VarIntStringSerializer::class)
@JvmInline
value class VarIntString(val value: String?)

object VarIntStringSerializer : KSerializer<VarIntString> {
    override val descriptor: SerialDescriptor = PrimitiveSerialDescriptor("VarIntString", PrimitiveKind.STRING)

    override fun deserialize(decoder: Decoder): VarIntString {
        TODO("Not yet implemented")
    }

    override fun serialize(encoder: Encoder, value: VarIntString) {
        if (value.value == null) {
            encoder.encodeVarInt(NULL_STRING_VARINT)
        } else {
            encoder.encodeVarInt(ZigzagInteger.encode(value.value.length).toVarInt())
            encoder.encodeString(value.value)
        }
    }
}

private val NULL_STRING_VARINT = ZigzagInteger.encode(-1).toVarInt()
