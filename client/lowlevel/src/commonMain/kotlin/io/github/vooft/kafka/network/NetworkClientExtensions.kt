package io.github.vooft.kafka.network

import io.github.vooft.kafka.network.common.toInt16String
import io.github.vooft.kafka.network.common.toVarInt
import io.github.vooft.kafka.network.common.toVarIntByteArray
import io.github.vooft.kafka.network.messages.CoordinatorType
import io.github.vooft.kafka.network.messages.FetchRequestV4
import io.github.vooft.kafka.network.messages.FetchResponseV4
import io.github.vooft.kafka.network.messages.FindCoordinatorRequestV1
import io.github.vooft.kafka.network.messages.FindCoordinatorResponseV1
import io.github.vooft.kafka.network.messages.HeartbeatRequestV0
import io.github.vooft.kafka.network.messages.HeartbeatResponseV0
import io.github.vooft.kafka.network.messages.JoinGroupRequestV1
import io.github.vooft.kafka.network.messages.JoinGroupResponseV1
import io.github.vooft.kafka.network.messages.KafkaRecordBatchContainerV0
import io.github.vooft.kafka.network.messages.KafkaRecordV0
import io.github.vooft.kafka.network.messages.MemberAssignment
import io.github.vooft.kafka.network.messages.MetadataRequestV1
import io.github.vooft.kafka.network.messages.MetadataResponseV1
import io.github.vooft.kafka.network.messages.OffsetCommitRequestV1
import io.github.vooft.kafka.network.messages.OffsetCommitResponseV1
import io.github.vooft.kafka.network.messages.OffsetFetchRequestV1
import io.github.vooft.kafka.network.messages.OffsetFetchResponseV1
import io.github.vooft.kafka.network.messages.ProduceRequestV3
import io.github.vooft.kafka.network.messages.ProduceResponseV3
import io.github.vooft.kafka.network.messages.SyncGroupRequestV1
import io.github.vooft.kafka.network.messages.SyncGroupResponseV1
import io.github.vooft.kafka.serialization.common.primitives.Crc32cPrefixed
import io.github.vooft.kafka.serialization.common.primitives.Int16String
import io.github.vooft.kafka.serialization.common.primitives.Int32BytesSizePrefixed
import io.github.vooft.kafka.serialization.common.primitives.VarIntByteArray
import io.github.vooft.kafka.serialization.common.primitives.VarIntBytesSizePrefixed
import io.github.vooft.kafka.serialization.common.primitives.int32ListOf
import io.github.vooft.kafka.serialization.common.primitives.toInt32List
import io.github.vooft.kafka.serialization.common.wrappers.GroupId
import io.github.vooft.kafka.serialization.common.wrappers.KafkaTopic
import io.github.vooft.kafka.serialization.common.wrappers.MemberId
import io.github.vooft.kafka.serialization.common.wrappers.PartitionIndex
import io.github.vooft.kafka.serialization.common.wrappers.PartitionOffset
import kotlinx.io.Source

suspend fun KafkaConnection.metadata(topics: Collection<KafkaTopic>) =
    sendRequest<MetadataRequestV1, MetadataResponseV1>(MetadataRequestV1(topics))

suspend fun KafkaConnection.metadata(topic: String) =
    sendRequest<MetadataRequestV1, MetadataResponseV1>(MetadataRequestV1(topic))

suspend fun KafkaConnection.heartbeat(groupId: GroupId, generationId: Int, memberId: MemberId) =
    sendRequest<HeartbeatRequestV0, HeartbeatResponseV0>(
        HeartbeatRequestV0(
            groupId = groupId,
            generationId = generationId,
            memberId = memberId
        )
    )

suspend fun KafkaConnection.findGroupCoordinator(groupId: GroupId) =
    sendRequest<FindCoordinatorRequestV1, FindCoordinatorResponseV1>(
        FindCoordinatorRequestV1(
            key = groupId.value.toInt16String(),
            keyType = CoordinatorType.GROUP
        )
    )

suspend fun KafkaConnection.joinGroup(groupId: GroupId, memberId: MemberId, topic: KafkaTopic) =
    sendRequest<JoinGroupRequestV1, JoinGroupResponseV1>(
        JoinGroupRequestV1(
            groupId = groupId,
            sessionTimeoutMs = 30000,
            rebalanceTimeoutMs = 60000,
            memberId = memberId,
            protocolType = CONSUMER_PROTOCOL_TYPE,
            groupProtocols = int32ListOf(
                JoinGroupRequestV1.GroupProtocol(
                    protocol = "mybla".toInt16String(), // TODO: change to proper assigner
                    metadata = Int32BytesSizePrefixed(
                        JoinGroupRequestV1.GroupProtocol.Metadata(
                            topics = int32ListOf(topic)
                        )
                    )
                )
            )
        )
    )

suspend fun KafkaConnection.fetchOffset(groupId: GroupId, topic: KafkaTopic, partitions: Collection<PartitionIndex>) =
    sendRequest<OffsetFetchRequestV1, OffsetFetchResponseV1>(
        OffsetFetchRequestV1(
            groupId = groupId,
            topics = int32ListOf(
                OffsetFetchRequestV1.Topic(
                    topic = topic,
                    partitions = partitions.toInt32List()
                )
            )
        )
    )

suspend fun KafkaConnection.commitOffset(
    groupId: GroupId,
    generationId: Int,
    memberId: MemberId,
    topic: KafkaTopic,
    offsets: Map<PartitionIndex, PartitionOffset>
) =
    sendRequest<OffsetCommitRequestV1, OffsetCommitResponseV1>(
        OffsetCommitRequestV1(
            groupId = groupId,
            generationIdOrMemberEpoch = generationId,
            memberId = memberId,
            topics = int32ListOf(
                OffsetCommitRequestV1.Topic(
                    topic = topic,
                    partitions = offsets.map { (partition, offset) ->
                        OffsetCommitRequestV1.Topic.Partition(
                            partition = partition,
                            committedOffset = offset,
                            commitTimestamp = 0
                        )
                    }.toInt32List()
                )
            )
        )
    )

suspend fun KafkaConnection.fetch(topic: KafkaTopic, partitionOffsets: Map<PartitionIndex, PartitionOffset>) =
    sendRequest<FetchRequestV4, FetchResponseV4>(
        FetchRequestV4(
            maxWaitTime = 500,
            minBytes = 1,
            maxBytes = 1024 * 1024,
            topics = int32ListOf(
                FetchRequestV4.Topic(
                    topic = topic,
                    partitions = partitionOffsets.map { (index, offset) ->
                        FetchRequestV4.Topic.Partition(
                            partition = index,
                            fetchOffset = offset,
                            maxBytes = 1024 * 1024
                        )
                    }.toInt32List()
                )
            )
        )
    )

data class ProduceRecord(val key: Source?, val value: Source)

suspend fun KafkaConnection.produce(topic: KafkaTopic, records: Map<PartitionIndex, List<ProduceRecord>>) =
    sendRequest<ProduceRequestV3, ProduceResponseV3>(
        ProduceRequestV3(
            timeoutMs = 1000,
            topic = int32ListOf(
                ProduceRequestV3.Topic(
                    topic = topic,
                    partition = records.map { (partition, records) ->
                        ProduceRequestV3.Topic.Partition(
                            partition = partition,
                            batchContainer = Int32BytesSizePrefixed(
                                KafkaRecordBatchContainerV0(
                                    batch = Int32BytesSizePrefixed(
                                        KafkaRecordBatchContainerV0.KafkaRecordBatch(
                                            body = Crc32cPrefixed(
                                                KafkaRecordBatchContainerV0.KafkaRecordBatch.KafkaRecordBatchBody(
                                                    lastOffsetDelta = records.size - 1,
                                                    firstTimestamp = 0,
                                                    maxTimestamp = 0,
                                                    records = records.mapIndexed { index, record ->
                                                        KafkaRecordV0(
                                                            recordBody = VarIntBytesSizePrefixed(
                                                                KafkaRecordV0.KafkaRecordBody(
                                                                    offsetDelta = index.toVarInt(),
                                                                    recordKey = record.key?.toVarIntByteArray() ?: VarIntByteArray.EMPTY,
                                                                    recordValue = record.value.toVarIntByteArray()
                                                                )
                                                            )
                                                        )
                                                    }.toInt32List()

                                                )
                                            )
                                        )
                                    )
                                )
                            )

                        )
                    }.toInt32List()
                )
            )
        )
    )

suspend fun KafkaConnection.syncGroup(
    groupId: GroupId,
    topic: KafkaTopic,
    generationId: Int,
    currentMemberId: MemberId,
    assignments: Map<MemberId, List<PartitionIndex>>
) =
    sendRequest<SyncGroupRequestV1, SyncGroupResponseV1>(
        SyncGroupRequestV1(
            groupId = groupId,
            generationId = generationId,
            memberId = currentMemberId,
            assignments = assignments.map { (memberId, partitions) ->
                SyncGroupRequestV1.Assignment(
                    memberId = memberId,
                    assignment = Int32BytesSizePrefixed(
                        MemberAssignment(
                            partitionAssignments = int32ListOf(
                                MemberAssignment.PartitionAssignment(
                                    topic = topic,
                                    partitions = partitions.toInt32List()
                                )
                            )

                        )
                    )
                )
            }.toInt32List()
        )
    )

// looks like in kafka itself they use "consumer" for this use case
private val CONSUMER_PROTOCOL_TYPE = Int16String("consumer")
