package io.github.vooft.kafka.network.dto

import kotlin.jvm.JvmInline

/**
 * Request Header => api_key api_version correlation_id client_id
 *   api_key => INT16
 *   api_version => INT16
 *   correlation_id => INT32
 *   client_id => NULLABLE_STRING
 */
data class KafkaRequestHeader(
    val apiKey: ApiKey,
    val apiVersion: ApiVersion,
    val correlationId: CorrelationId,
    val clientId: String? = null
)

data class KafkaResponseHeader(val correlationId: CorrelationId)

enum class ApiVersion(val value: Short) {
    V1(1),
    V2(2)
}

@JvmInline
value class CorrelationId(val value: Int) {
    companion object {
        fun next(): CorrelationId = CorrelationId(correlationIdCounter++)
    }
}

private var correlationIdCounter = 666

/**
 * Even though it is called ApiKey, it is more like a command
 */
enum class ApiKey(val value: Short) {
    API_VERSIONS(18),
}
