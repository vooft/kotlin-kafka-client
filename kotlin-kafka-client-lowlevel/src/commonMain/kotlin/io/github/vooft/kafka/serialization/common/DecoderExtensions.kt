package io.github.vooft.kafka.serialization.common

import io.github.vooft.kafka.serialization.common.primitives.VarInt
import kotlinx.serialization.encoding.Decoder

fun Decoder.decodeVarInt(): VarInt = decodeSerializableValue(VarInt.serializer())
