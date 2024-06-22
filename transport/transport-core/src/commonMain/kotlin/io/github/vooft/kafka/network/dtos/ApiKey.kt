package io.github.vooft.kafka.network.dtos

import kotlinx.serialization.Serializable
import kotlin.jvm.JvmInline

// Even though it is called ApiKey, it is more like a command
@Serializable
@JvmInline
value class ApiKey private constructor(val value: Short) {
    companion object {
        val PRODUCE = ApiKey(0)
        val FETCH = ApiKey(1)
        val METADATA = ApiKey(3)
        val OFFSET_COMMIT = ApiKey(8)
        val OFFSET_FETCH = ApiKey(9)
        val FIND_COORDINATOR = ApiKey(10)
        val JOIN_GROUP = ApiKey(11)
        val HEARTBEAT = ApiKey(12)
        val SYNC_GROUP = ApiKey(14)
        val API_VERSIONS = ApiKey(18)
    }
}
