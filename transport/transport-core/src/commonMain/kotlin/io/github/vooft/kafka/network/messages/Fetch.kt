package io.github.vooft.kafka.network.messages

import io.github.vooft.kafka.network.common.ErrorCode
import io.github.vooft.kafka.serialization.common.primitives.Int32BytesSizePrefixed
import io.github.vooft.kafka.serialization.common.primitives.Int32List
import io.github.vooft.kafka.serialization.common.wrappers.KafkaTopic
import io.github.vooft.kafka.serialization.common.wrappers.PartitionIndex
import io.github.vooft.kafka.serialization.common.wrappers.PartitionOffset
import kotlinx.serialization.Serializable

interface FetchRequest : KafkaRequest {
    override val apiKey: ApiKey get() = ApiKey.FETCH
}

// up until V4 kafka returns MessageSet instead of RecordBatch
/**
 * Fetch Request (Version: 4) => replica_id max_wait_ms min_bytes max_bytes isolation_level [topics]
 *   replica_id => INT32
 *   max_wait_ms => INT32
 *   min_bytes => INT32
 *   max_bytes => INT32
 *   isolation_level => INT8
 *   topics => topic [partitions]
 *     topic => STRING
 *     partitions => partition fetch_offset partition_max_bytes
 *       partition => INT32
 *       fetch_offset => INT64
 *       partition_max_bytes => INT32
 */
@Serializable
data class FetchRequestV4(
    val replicaId: Int = -1, // always -1 for consumers
    val maxWaitTime: Int,
    val minBytes: Int,
    val maxBytes: Int,
    val isolationLevel: Byte = 1, // 0=READ_UNCOMMITED, 1=READ_COMMITTED
    val topics: Int32List<Topic>
) : FetchRequest, VersionedV4 {
    @Serializable
    data class Topic(
        val topic: KafkaTopic,
        val partitions: Int32List<Partition>
    ) {
        @Serializable
        data class Partition(
            val partition: PartitionIndex,
            val fetchOffset: PartitionOffset,
            val maxBytes: Int
        )
    }
}

interface FetchResponse : KafkaResponse

/**
 * Fetch Response (Version: 4) => throttle_time_ms [responses]
 *   throttle_time_ms => INT32
 *   responses => topic [partitions]
 *     topic => STRING
 *     partitions => partition_index error_code high_watermark last_stable_offset [aborted_transactions] records
 *       partition_index => INT32
 *       error_code => INT16
 *       high_watermark => INT64
 *       last_stable_offset => INT64
 *       aborted_transactions => producer_id first_offset
 *         producer_id => INT64
 *         first_offset => INT64
 *       records => RECORDS
 */
@Serializable
data class FetchResponseV4(
    val throttleTimeMs: Int,
    val topics: Int32List<Topic>
) : FetchResponse, VersionedV4 {
    @Serializable
    data class Topic(
        val topic: KafkaTopic,
        val partitions: Int32List<Partition>
    ) {
        @Serializable
        data class Partition(
            val partition: PartitionIndex,
            val errorCode: ErrorCode,
            val highwaterMarkOffset: PartitionOffset,
            val lastStableOffset: PartitionOffset,
            val abortedTransactions: Int32List<AbortedTransaction>,
            val batchContainer: Int32BytesSizePrefixed<KafkaRecordBatchContainerV0?>
        ) {
            @Serializable
            data class AbortedTransaction(
                val producerId: Long,
                val firstOffset: PartitionOffset
            )
        }
    }
}

