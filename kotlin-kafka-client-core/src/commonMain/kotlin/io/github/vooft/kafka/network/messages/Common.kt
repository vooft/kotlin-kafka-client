package io.github.vooft.kafka.network.messages

import io.github.vooft.kafka.serialization.IntValue
import io.github.vooft.kafka.serialization.IntValueSerializer
import io.github.vooft.kafka.serialization.ShortValue
import io.github.vooft.kafka.serialization.ShortValueSerializer
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

@Serializable(with = ApiVersionSerializer::class)
enum class ApiVersion(override val value: Short) : ShortValue {
    V0(0),
    V1(1)
}

// TODO: move to module
object ApiVersionSerializer : ShortValueSerializer<ApiVersion>({ ApiVersion.entries.first { version -> version.value == it } })

@Serializable(with = CorrelationIdSerializer::class)
@JvmInline
value class CorrelationId(override val value: Int) : IntValue {
    companion object {
        private var counter = 0
        fun next() = CorrelationId(counter++)
    }
}

object CorrelationIdSerializer : IntValueSerializer<CorrelationId>({ CorrelationId(it) })

sealed interface KafkaResponseHeader : Versioned {
    val correlationId: CorrelationId
}

@Serializable
data class KafkaResponseHeaderV0(override val correlationId: CorrelationId) : KafkaResponseHeader, VersionedV0

@Serializable
sealed interface KafkaRequest : Versioned {
    val apiKey: ApiKey
}

interface KafkaResponse : Versioned

// Even though it is called ApiKey, it is more like a command
@Serializable(with = ApiKeySerializer::class)
enum class ApiKey(override val value: Short): ShortValue {
    API_VERSIONS(0x12)
}

object ApiKeySerializer : ShortValueSerializer<ApiKey>({ ApiKey.entries.first { key -> key.value == it } })
