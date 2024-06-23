package io.github.vooft.kafka.transport.serialization

import io.github.vooft.kafka.serialization.KafkaSerde
import kotlinx.io.Sink
import kotlinx.io.Source
import kotlinx.serialization.DeserializationStrategy
import kotlinx.serialization.SerializationStrategy
import kotlinx.serialization.serializer

inline fun <reified T> Source.decode(): T = KafkaSerde.decode(serializer(), this)
fun <T> Source.decode(deserializer: DeserializationStrategy<T>): T = KafkaSerde.decode(deserializer, this)

inline fun <reified T> Sink.encode(value: T) = encode(serializer(), value)
fun <T> Sink.encode(serializer: SerializationStrategy<T>, value: T) = KafkaSerde.encode(serializer, value, this)
