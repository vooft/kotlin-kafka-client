package io.github.vooft.kafka.network.dtos

import kotlinx.serialization.Serializable
import kotlin.jvm.JvmInline

@Serializable
@JvmInline
value class ApiVersion private constructor(val value: Short) {
    companion object {
        val V0 = ApiVersion(0)
        val V1 = ApiVersion(1)
        val V2 = ApiVersion(2)
        val V3 = ApiVersion(3)
        val V4 = ApiVersion(4)
    }
}
