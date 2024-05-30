package io.github.vooft.kafka.serialization

import io.github.vooft.kafka.serialization.common.primitives.Int16String
import io.github.vooft.kafka.serialization.common.primitives.Int32List
import io.github.vooft.kafka.serialization.common.primitives.NullableInt16String
import io.github.vooft.kafka.serialization.common.primitives.VarInt
import io.github.vooft.kafka.serialization.common.primitives.VarIntList
import io.github.vooft.kafka.serialization.common.primitives.VarLong
import kotlinx.serialization.Serializable

private fun ByteArray.toHexString() = joinToString(", ", "[", "]") { "0x" + it.toUByte().toString(16).padStart(2, '0').uppercase() }

@Serializable
data class NumbersClass(
    val int8: Byte,
    val int16: Short,
    val int32: Int,
    val int64: Long
)

@Serializable
data class Int16StringClass(
    val nonNullString: Int16String,
    val nullString: NullableInt16String
)

@Serializable
data class VarNumberClass(
    val varInt: VarInt,
    val varLong: VarLong
)

@Serializable
data class CollectionsClass(
    val int32List: Int32List<Int16String>,
    val varIntList: VarIntList<Int16String>
)
