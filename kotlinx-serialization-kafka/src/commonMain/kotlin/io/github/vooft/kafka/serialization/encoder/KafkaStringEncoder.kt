package io.github.vooft.kafka.serialization.encoder

import io.github.vooft.kafka.serialization.common.IntEncoding
import io.github.vooft.kafka.serialization.common.customtypes.VarInt
import io.github.vooft.kafka.serialization.common.customtypes.toVarInt
import kotlinx.io.Sink
import kotlinx.io.writeString
import kotlinx.serialization.ExperimentalSerializationApi
import kotlinx.serialization.SerializationStrategy
import kotlinx.serialization.encoding.Encoder
import kotlinx.serialization.modules.EmptySerializersModule
import kotlinx.serialization.modules.SerializersModule

class KafkaStringEncoder(
    private val sink: Sink,
    private val lengthEncoding: IntEncoding,
    override val serializersModule: SerializersModule = EmptySerializersModule(),
    valueEncoder: KafkaValueEncoder = KafkaValueEncoder(sink, serializersModule)
) : Encoder by valueEncoder {
    override fun encodeString(value: String) {
        when (lengthEncoding) {
            IntEncoding.INT16 -> sink.writeShort(value.length.toShort())
            IntEncoding.INT32 -> sink.writeInt(value.length)
            IntEncoding.VARINT -> encodeVarInt(value.length.toVarInt())
        }

        sink.writeString(value)
    }

    @ExperimentalSerializationApi
    override fun <T : Any> encodeNullableSerializableValue(serializer: SerializationStrategy<T>, value: T?) {
        when (value) {
            null -> when (lengthEncoding) {
                IntEncoding.INT16 -> sink.writeShort(-1)
                IntEncoding.INT32 -> sink.writeInt(-1)
                IntEncoding.VARINT -> encodeVarInt(VarInt.MINUS_ONE)
            }
            is String -> encodeString(value)
            else -> error("Unsupported type: ${value::class}")
        }
    }
}

