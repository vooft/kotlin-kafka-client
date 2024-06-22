package io.github.vooft.kafka.network.messages

import io.github.vooft.kafka.serialization.common.IntValue
import io.github.vooft.kafka.serialization.common.ShortValue
import io.github.vooft.kafka.serialization.common.ShortValueSerializer
import kotlinx.serialization.Serializable
import kotlin.jvm.JvmInline

sealed interface Versioned {
    val apiVersion: ApiVersion
}

sealed interface VersionedV0 : Versioned {
    override val apiVersion: ApiVersion get() = ApiVersion.V0
}

sealed interface VersionedV1 : Versioned {
    override val apiVersion: ApiVersion get() = ApiVersion.V1
}

sealed interface VersionedV3: Versioned {
    override val apiVersion: ApiVersion get() = ApiVersion.V3
}

sealed interface VersionedV4 : Versioned {
    override val apiVersion: ApiVersion get() = ApiVersion.V4
}

@Serializable(with = ApiVersionSerializer::class)
enum class ApiVersion(override val value: Short) : ShortValue {
    V0(0),
    V1(1),
    V2(2),
    V3(3),
    V4(4),
}

// TODO: move to module
object ApiVersionSerializer : ShortValueSerializer<ApiVersion>(ApiVersion.entries)

@Serializable
@JvmInline
value class CorrelationId(override val value: Int) : IntValue {
    companion object {
        private var counter = 666 // TODO: use atomic
        fun next() = CorrelationId(counter++)
    }
}

// TODO: add test checking that no raw strings are present
@Serializable
sealed interface KafkaRequest : Versioned {
    val apiKey: ApiKey
}

interface KafkaResponse : Versioned

// Even though it is called ApiKey, it is more like a command
@Serializable(with = ApiKeySerializer::class)
enum class ApiKey(override val value: Short): ShortValue {
    PRODUCE(0),
    FETCH(1),
    METADATA(3),
    OFFSET_COMMIT(8),
    OFFSET_FETCH(9),
    FIND_COORDINATOR(10),
    JOIN_GROUP(11),
    HEARTBEAT(12),
    SYNC_GROUP(14),
    API_VERSIONS(18)
}

object ApiKeySerializer : ShortValueSerializer<ApiKey>(ApiKey.entries)
