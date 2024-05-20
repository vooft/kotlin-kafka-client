package io.github.vooft.kafka.serialization

import io.github.vooft.kafka.serialization.decoder.KafkaObjectDecoder
import kotlinx.io.Buffer
import kotlinx.serialization.DeserializationStrategy
import kotlinx.serialization.SerializationStrategy
import kotlinx.serialization.serializer

object KafkaSerde {
    fun <T> encode(serializer: SerializationStrategy<T>, value: T, buffer: Buffer = Buffer()): Buffer {
        val encoder = KotlinxSerializationKafkaEncoder(buffer)
        encoder.encodeSerializableValue(serializer, value)
        return buffer
    }

    fun <T> decode(serializer: DeserializationStrategy<T>, buffer: Buffer): T {
        val decoder = KafkaObjectDecoder(buffer)
        return decoder.decodeSerializableValue(serializer)
    }
}

inline fun <reified T> KafkaSerde.encode(value: T, buffer: Buffer = Buffer()) = encode(serializer(), value, buffer)
inline fun <reified T> Buffer.encode(value: T) = encode(serializer(), value)
fun <T> Buffer.encode(serializer: SerializationStrategy<T>, value: T) = KafkaSerde.encode(serializer, value, this)

inline fun <reified T> KafkaSerde.decode(buffer: Buffer): T = decode(serializer(), buffer)
inline fun <reified T> Buffer.decode(): T = KafkaSerde.decode(serializer(), this)
fun <T> Buffer.decode(deserializer: DeserializationStrategy<T>): T = KafkaSerde.decode(deserializer, this)
