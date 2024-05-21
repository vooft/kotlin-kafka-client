package io.github.vooft.kafka.network.messages

import io.github.vooft.kafka.serialization.common.customtypes.Int16String
import kotlinx.serialization.Serializable

sealed interface MetadataRequest: KafkaRequest {
    override val apiKey: ApiKey get() = ApiKey.METADATA
}

@Serializable
data class MetadataRequestV1(val topics: List<Int16String>) : MetadataRequest, VersionedV1

sealed interface MetadataResponse: KafkaResponse

@Serializable
data class MetadataResponseV1(
    val brokers: List<Broker>,
    val controllerId: Int,
    val topics: List<Topic>,
) : MetadataResponse, VersionedV1 {
    @Serializable
    data class Broker(
        val nodeId: Int,
        val host: Int16String,
        val port: Int,
        val rack: Int16String
    )

    @Serializable
    data class Topic(
        val errorCode: ErrorCode,
        val name: Int16String,
        val isInternal: Boolean,
        val partitions: List<Partition>
    ) {
        @Serializable
        data class Partition(
            val errorCode: ErrorCode,
            val partition: Int,
            val leader: Int,
            val replicas: List<Int>,
            val isr: List<Int>
        )
    }
}
