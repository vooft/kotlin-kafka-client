package io.github.vooft.kafka.network.messages

import io.github.vooft.kafka.serialization.common.IntEncoding.INT32
import io.github.vooft.kafka.serialization.common.KafkaSizeInBytesPrefixed
import io.github.vooft.kafka.serialization.common.customtypes.Int16String
import kotlinx.serialization.Serializable

interface FetchRequest : KafkaRequest {
    override val apiKey: ApiKey get() = ApiKey.FETCH
}

// up until V4 kafka returns MessageSet instead of RecordBatch
@Serializable
data class FetchRequestV4(
    val replicaId: Int = -1, // always -1 for consumers
    val maxWaitTime: Int,
    val minBytes: Int,
    val maxBytes: Int,
    val isolationLevel: Byte = 1, // 0=READ_UNCOMMITED, 1=READ_COMMITTED
    val topics: List<Topic>
) : FetchRequest, VersionedV4 {
    @Serializable
    data class Topic(
        val topic: Int16String,
        val partitions: List<Partition>
    ) {
        @Serializable
        data class Partition(
            val partition: Int,
            val fetchOffset: Long,
            val maxBytes: Int
        )
    }
}

interface FetchResponse : KafkaResponse

@Serializable
data class FetchResponseV4(
    val throttleTimeMs: Int,
    val topics: List<Topic>
) : FetchResponse, VersionedV4 {
    @Serializable
    data class Topic(
        val topic: String,
        val partitions: List<Partition>
    ) {
        @Serializable
        data class Partition(
            val partitionNumber: Int,
            val errorCode: ErrorCode,
            val highwaterMarkOffset: Long,
            val lastStableOffset: Long,
            val abortedTransaction: List<AbortedTransaction>,
            @KafkaSizeInBytesPrefixed(encoding = INT32) val batchContainer: KafkaRecordBatchContainer
        ) {
            @Serializable
            data class AbortedTransaction(
                val producerId: Long,
                val firstOffset: Long
            )
        }
    }
}

