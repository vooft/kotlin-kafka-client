package io.github.vooft.kafka.serialization.encoder

import io.github.vooft.kafka.serialization.common.customtypes.VarInt
import kotlinx.serialization.encoding.Encoder

fun Encoder.encodeVarInt(value: VarInt) = encodeSerializableValue(VarInt.serializer(), value)
