package io.github.vooft.kafka.network.common

import io.github.vooft.kafka.serialization.common.ZigzagInteger
import io.github.vooft.kafka.serialization.common.customtypes.Int16String
import io.github.vooft.kafka.serialization.common.customtypes.NullableInt16String
import io.github.vooft.kafka.serialization.common.customtypes.VarInt
import io.github.vooft.kafka.serialization.common.customtypes.VarIntByteArray
import io.github.vooft.kafka.serialization.common.customtypes.VarIntString
import io.github.vooft.kafka.serialization.common.customtypes.VarLong
import kotlinx.io.Source
import kotlinx.io.readByteArray

fun String?.toNullableInt16String() = this?.let { NullableInt16String(it) } ?: NullableInt16String.NULL
fun String.toInt16String() = Int16String(this)
fun List<String>.toInt16String() = map { it.toNullableInt16String() }

fun Int.toVarInt() = VarInt(ZigzagInteger.encode(this))

fun ByteArray.toVarIntByteArray() = VarIntByteArray(this)
fun String.toVarIntByteArray() = VarIntByteArray(this.encodeToByteArray())
fun Source.toVarIntByteArray() = VarIntByteArray(readByteArray())

fun String?.toVarIntString() = VarIntString(this)

fun Int.toVarLong() = VarLong(this.toLong())
