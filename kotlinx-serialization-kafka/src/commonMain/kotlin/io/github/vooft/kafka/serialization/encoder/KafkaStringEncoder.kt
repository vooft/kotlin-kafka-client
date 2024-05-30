package io.github.vooft.kafka.serialization.encoder

import io.github.vooft.kafka.serialization.common.IntEncoding
import io.github.vooft.kafka.serialization.common.encodeVarInt
import io.github.vooft.kafka.serialization.common.primitives.VarInt
import kotlinx.io.Sink
import kotlinx.io.writeString
import kotlinx.serialization.ExperimentalSerializationApi
import kotlinx.serialization.SerializationStrategy
import kotlinx.serialization.modules.EmptySerializersModule
import kotlinx.serialization.modules.SerializersModule

class KafkaStringEncoder(
    private val sink: Sink,
    private val lengthEncoding: IntEncoding,
    override val serializersModule: SerializersModule = EmptySerializersModule(),
) : KafkaValueEncoder(sink, serializersModule) {
    override fun encodeString(value: String) {
        when (lengthEncoding) {
            IntEncoding.INT16 -> sink.writeShort(value.length.toShort())
            IntEncoding.INT32 -> sink.writeInt(value.length)
            IntEncoding.VARINT -> encodeVarInt(VarInt.fromDecoded(value.length))
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

