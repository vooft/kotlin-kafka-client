package io.github.vooft.kafka.serialization

import io.github.vooft.kafka.serialization.decoder.KafkaObjectDecoder
import io.github.vooft.kafka.serialization.encoder.KafkaObjectEncoder
import kotlinx.io.Buffer
import kotlinx.io.Sink
import kotlinx.io.Source
import kotlinx.serialization.DeserializationStrategy
import kotlinx.serialization.SerializationStrategy
import kotlinx.serialization.serializer

object KafkaSerde {
    fun <T> encode(serializer: SerializationStrategy<T>, value: T, sink: Sink) {
        val encoder = KafkaObjectEncoder(sink)
        encoder.encodeSerializableValue(serializer, value)
    }

    fun <T> decode(serializer: DeserializationStrategy<T>, source: Source): T {
        val decoder = KafkaObjectDecoder(source)
        return decoder.decodeSerializableValue(serializer)
    }
}

inline fun <reified T> KafkaSerde.encode(value: T) = Buffer().apply { encode(serializer(), value, this) }
inline fun <reified T> Sink.encode(value: T) = encode(serializer(), value)
fun <T> Sink.encode(serializer: SerializationStrategy<T>, value: T) = KafkaSerde.encode(serializer, value, this)

inline fun <reified T> KafkaSerde.decode(source: Source): T = decode(serializer(), source)
inline fun <reified T> Source.decode(): T = KafkaSerde.decode(serializer(), this)
fun <T> Source.decode(deserializer: DeserializationStrategy<T>): T = KafkaSerde.decode(deserializer, this)
