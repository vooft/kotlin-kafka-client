package io.github.vooft.kafka.serialization

import kotlinx.serialization.KSerializer
import kotlinx.serialization.descriptors.PrimitiveKind
import kotlinx.serialization.descriptors.PrimitiveSerialDescriptor
import kotlinx.serialization.encoding.Decoder
import kotlinx.serialization.encoding.Encoder

interface ShortValue {
    val value: Short
}

interface IntValue {
    val value: Int
}

abstract class ShortValueSerializer<T: ShortValue>(
    private val factory: (Short) -> T
) : KSerializer<T> {

    constructor(entries: Collection<T>) : this({ value -> entries.first { it.value == value } })

    override val descriptor = PrimitiveSerialDescriptor("ShortValue", PrimitiveKind.SHORT)

    override fun deserialize(decoder: Decoder): T {
        val value = decoder.decodeShort()
        return factory(value)
    }

    override fun serialize(encoder: Encoder, value: T) {
        encoder.encodeShort(value.value)
    }
}

abstract class IntValueSerializer<T: IntValue>(
    private val factory: (Int) -> T
) : KSerializer<T> {
    override val descriptor = PrimitiveSerialDescriptor("IntValue", PrimitiveKind.INT)

    override fun deserialize(decoder: Decoder): T {
        val value = decoder.decodeInt()
        return factory(value)
    }

    override fun serialize(encoder: Encoder, value: T) {
        encoder.encodeInt(value.value)
    }
}
