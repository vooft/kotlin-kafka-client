package io.github.vooft.kafka.network.messages

import kotlinx.serialization.Serializable

@Serializable
sealed interface KafkaRequest : Versioned {
    val apiKey: Short
}

interface KafkaResponse : Versioned

sealed interface Versioned {
    val version: Short
}

sealed interface VersionV0: Versioned {
    override val version: Short get() = ApiVersion.V0
}

sealed interface VersionV1: Versioned {
    override val version: Short get() = ApiVersion.V1
}

sealed interface VersionV2: Versioned {
    override val version: Short get() = ApiVersion.V2
}

/**
 * Request Header => api_key api_version correlation_id client_id
 *   api_key => INT16
 *   api_version => INT16
 *   correlation_id => INT32
 *   client_id => NULLABLE_STRING
 */
@Serializable
data class KafkaRequestHeader(
    val apiKey: Short,
    val apiVersion: Short,
    val correlationId: Int,
    val clientId: String? = null
)

@Serializable
data class KafkaResponseHeader(val correlationId: Int)

object ApiVersion {
    const val V0: Short = 0
    const val V1: Short = 1
    const val V2: Short = 2
}

/**
 * Even though it is called ApiKey, it is more like a command
 */
object ApiKey {
    const val API_VERSIONS: Short = 18
}
