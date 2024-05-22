package io.github.vooft.kafka.serialization.common.customtypes

import io.github.vooft.kafka.serialization.encoder.encodeVarInt
import kotlinx.serialization.ExperimentalSerializationApi
import kotlinx.serialization.KSerializer
import kotlinx.serialization.Serializable
import kotlinx.serialization.descriptors.SerialDescriptor
import kotlinx.serialization.descriptors.listSerialDescriptor
import kotlinx.serialization.encoding.Decoder
import kotlinx.serialization.encoding.Encoder
import kotlin.jvm.JvmInline

@Serializable(with = VarIntByteArraySerializer::class)
@JvmInline
value class VarIntByteArray(val data: ByteArray)

fun ByteArray.toVarIntByteArray() = VarIntByteArray(this)
fun String.toVarIntByteArray() = VarIntByteArray(this.encodeToByteArray())

@OptIn(ExperimentalSerializationApi::class)
object VarIntByteArraySerializer : KSerializer<VarIntByteArray> {
    override val descriptor: SerialDescriptor = listSerialDescriptor<Byte>()

    override fun deserialize(decoder: Decoder): VarIntByteArray {
        TODO("Not yet implemented")
    }

    override fun serialize(encoder: Encoder, value: VarIntByteArray) {
        encoder.encodeVarInt(value.data.size.toVarInt())
        value.data.forEach { encoder.encodeByte(it) }
    }

}
