package io.github.vooft.kafka.transport.common

import io.github.vooft.kafka.serialization.common.primitives.Int16String
import io.github.vooft.kafka.serialization.common.primitives.NullableInt16String
import io.github.vooft.kafka.serialization.common.primitives.VarInt
import io.github.vooft.kafka.serialization.common.primitives.VarIntByteArray
import io.github.vooft.kafka.serialization.common.primitives.VarIntString
import io.github.vooft.kafka.serialization.common.primitives.VarLong
import kotlinx.io.Source
import kotlinx.io.readByteArray

fun String?.toNullableInt16String() = this?.let { NullableInt16String(it) } ?: NullableInt16String.NULL
fun String.toInt16String() = Int16String(this)
fun List<String>.toInt16String() = map { it.toNullableInt16String() }

fun Int.toVarInt() = VarInt.fromDecoded(this)

fun ByteArray.toVarIntByteArray() = VarIntByteArray(this)
fun String.toVarIntByteArray() = VarIntByteArray(encodeToByteArray())
fun Source.toVarIntByteArray() = VarIntByteArray(peek().readByteArray())

fun String?.toVarIntString() = VarIntString(this)

fun Int.toVarLong() = VarLong.fromDecoded(toLong())
