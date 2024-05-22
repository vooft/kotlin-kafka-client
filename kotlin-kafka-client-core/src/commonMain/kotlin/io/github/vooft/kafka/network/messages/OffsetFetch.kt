package io.github.vooft.kafka.network.messages

import io.github.vooft.kafka.serialization.common.customtypes.Int16String
import kotlinx.serialization.Serializable

interface OffsetFetchRequest : KafkaRequest {
    override val apiKey: ApiKey get() = ApiKey.OFFSET_FETCH
}

@Serializable
data class OffsetFetchRequestV1(
    val groupId: Int16String,
    val topics: List<Topic>
) : OffsetFetchRequest, VersionedV1 {
    @Serializable
    data class Topic(
        val topic: Int16String,
        val partitions: List<Partition>
    ) {
        @Serializable
        data class Partition(
            val partition: Int
        )
    }
}

interface OffsetFetchResponse : KafkaResponse

@Serializable
data class OffsetFetchResponseV1(
    val topics: List<Topic>
) : OffsetFetchResponse, VersionedV1 {
    @Serializable
    data class Topic(
        val topic: String,
        val partitions: List<Partition>
    ) {
        @Serializable
        data class Partition(
            val partition: Int,
            val offset: Long,
            val metadata: Int16String,
            val errorCode: ErrorCode
        )
    }
}
