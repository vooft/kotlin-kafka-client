package io.github.vooft.kafka.serialization.decoder

import io.github.vooft.kafka.serialization.common.CRC32
import kotlinx.io.Source
import kotlinx.serialization.modules.EmptySerializersModule
import kotlinx.serialization.modules.SerializersModule

class KafkaCrc32PrefixedDecoder(
    source: Source,
    override val serializersModule: SerializersModule = EmptySerializersModule(),
) : KafkaValueDecoder(source, serializersModule) {
    init {
        val crc32 = source.readInt()
        val calculated = CRC32.crc32c(source.peek())
        require(crc32 == calculated) { "Calculated CRC32c is not the same as in the source: expected: $calculated, actual: $crc32" }
    }
}
