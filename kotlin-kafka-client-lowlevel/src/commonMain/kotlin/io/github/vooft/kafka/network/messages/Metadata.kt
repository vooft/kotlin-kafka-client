package io.github.vooft.kafka.network.messages

import io.github.vooft.kafka.network.common.ErrorCode
import io.github.vooft.kafka.network.dtos.ApiKey
import io.github.vooft.kafka.network.dtos.KafkaRequest
import io.github.vooft.kafka.network.dtos.KafkaResponse
import io.github.vooft.kafka.serialization.common.primitives.Int16String
import io.github.vooft.kafka.serialization.common.primitives.Int32List
import io.github.vooft.kafka.serialization.common.primitives.NullableInt16String
import io.github.vooft.kafka.serialization.common.wrappers.KafkaTopic
import io.github.vooft.kafka.serialization.common.wrappers.NodeId
import io.github.vooft.kafka.serialization.common.wrappers.PartitionIndex
import kotlinx.serialization.Serializable

sealed interface MetadataRequest: KafkaRequest {
    override val apiKey: ApiKey get() = ApiKey.METADATA
}

/**
 * Metadata Request (Version: 1) => [topics]
 *   topics => name
 *     name => STRING
 */
@Serializable
data class MetadataRequestV1(val topics: Int32List<KafkaTopic>) : MetadataRequest, VersionedV1

fun MetadataRequestV1(topic: String) = MetadataRequestV1(listOf(KafkaTopic(topic)))
fun MetadataRequestV1(topics: Collection<KafkaTopic>) = MetadataRequestV1(Int32List(topics.toList()))

sealed interface MetadataResponse: KafkaResponse

/**
 * Metadata Response (Version: 1) => [brokers] controller_id [topics]
 *   brokers => node_id host port rack
 *     node_id => INT32
 *     host => STRING
 *     port => INT32
 *     rack => NULLABLE_STRING
 *   controller_id => INT32
 *   topics => error_code name is_internal [partitions]
 *     error_code => INT16
 *     name => STRING
 *     is_internal => BOOLEAN
 *     partitions => error_code partition_index leader_id [replica_nodes] [isr_nodes]
 *       error_code => INT16
 *       partition_index => INT32
 *       leader_id => INT32
 *       replica_nodes => INT32
 *       isr_nodes => INT32
 */
@Serializable
data class MetadataResponseV1(
    val brokers: Int32List<Broker>,
    val controllerId: NodeId,
    val topics: Int32List<Topic>,
) : MetadataResponse, VersionedV1 {
    @Serializable
    data class Broker(
        val nodeId: NodeId,
        val host: Int16String,
        val port: Int,
        val rack: NullableInt16String
    )

    @Serializable
    data class Topic(
        val errorCode: ErrorCode,
        val topic: KafkaTopic,
        val isInternal: Boolean,
        val partitions: Int32List<Partition>
    ) {
        @Serializable
        data class Partition(
            val errorCode: ErrorCode,
            val partition: PartitionIndex,
            val leader: NodeId,
            val replicas: Int32List<Int>,
            val isr: Int32List<Int>
        )
    }
}
