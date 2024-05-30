package io.github.vooft.kafka.serialization

import io.github.vooft.kafka.serialization.common.customtypes.Int16String
import io.github.vooft.kafka.serialization.common.customtypes.NullableInt16String
import io.github.vooft.kafka.serialization.common.primitives.VarInt
import io.github.vooft.kafka.serialization.common.primitives.VarLong
import kotlinx.serialization.Serializable

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
