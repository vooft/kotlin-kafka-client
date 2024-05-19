package io.github.vooft.kafka.network.dto

import kotlinx.serialization.Serializable

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
    const val V1: Short = 1
    const val V2: Short = 2
}

/**
 * Even though it is called ApiKey, it is more like a command
 */
object ApiKey {
    const val API_VERSIONS: Short = 18
}
