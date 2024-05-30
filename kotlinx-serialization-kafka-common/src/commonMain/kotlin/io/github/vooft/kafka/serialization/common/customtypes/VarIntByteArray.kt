package io.github.vooft.kafka.serialization.common.customtypes

import io.github.vooft.kafka.serialization.common.decodeVarInt
import io.github.vooft.kafka.serialization.common.encodeVarInt
import io.github.vooft.kafka.serialization.common.primitives.VarInt
import kotlinx.io.Buffer
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
// TODO: replace with ByteArray once figure out how to make comparision work
// https://youtrack.jetbrains.com/issue/KT-24874/Support-custom-equals-and-hashCode-for-value-classes
value class VarIntByteArray(val data: List<Byte>): KafkaCustomType {
    constructor(data: ByteArray): this(data.toList())
}

fun VarIntByteArray(value: String) = VarIntByteArray(value.encodeToByteArray())

fun VarIntByteArray.toBuffer() = Buffer().apply { write(data.toByteArray()) }

@OptIn(ExperimentalSerializationApi::class)
object VarIntByteArraySerializer : KSerializer<VarIntByteArray>, KafkaCustomTypeSerializer {
    override val descriptor: SerialDescriptor = listSerialDescriptor<Byte>()

    override fun deserialize(decoder: Decoder): VarIntByteArray {
        val varInt = decoder.decodeVarInt()
        val length = varInt.toDecoded()
        val data = ByteArray(length) { decoder.decodeByte() }
        return VarIntByteArray(data)
    }

    override fun serialize(encoder: Encoder, value: VarIntByteArray) {
        encoder.encodeVarInt(VarInt.fromDecoded(value.data.size))
        value.data.forEach { encoder.encodeByte(it) }
    }

}
