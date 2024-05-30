@file:OptIn(ExperimentalSerializationApi::class)

package io.github.vooft.kafka.serialization.common.primitives

import kotlinx.serialization.ExperimentalSerializationApi

enum class IntEncoding {
    INT16,
    INT32,
    VARINT
}
