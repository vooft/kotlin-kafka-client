package io.github.vooft.kafka.serialization.common.customtypes

import io.github.vooft.kafka.serialization.common.ZigzagInteger
import io.github.vooft.kafka.serialization.decoder.decodeVarInt
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
value class VarIntByteArray(val data: ByteArray): KafkaCustomType

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
        val zigzag = ZigzagInteger.encode(value.data.size)
        encoder.encodeVarInt(VarInt(zigzag))
        value.data.forEach { encoder.encodeByte(it) }
    }

}
