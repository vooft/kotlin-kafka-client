package io.github.vooft.kafka.network

import io.github.vooft.kafka.network.common.toInt16String
import io.github.vooft.kafka.network.messages.CoordinatorType
import io.github.vooft.kafka.network.messages.FindCoordinatorRequestV1
import io.github.vooft.kafka.network.messages.FindCoordinatorResponseV1
import io.github.vooft.kafka.network.messages.HeartbeatRequestV0
import io.github.vooft.kafka.network.messages.HeartbeatResponseV0
import io.github.vooft.kafka.network.messages.JoinGroupRequestV1
import io.github.vooft.kafka.network.messages.JoinGroupResponseV1
import io.github.vooft.kafka.network.messages.MetadataRequestV1
import io.github.vooft.kafka.network.messages.MetadataResponseV1
import io.github.vooft.kafka.serialization.common.primitives.Int16String
import io.github.vooft.kafka.serialization.common.primitives.Int32BytesSizePrefixed
import io.github.vooft.kafka.serialization.common.primitives.int32ListOf
import io.github.vooft.kafka.serialization.common.wrappers.GroupId
import io.github.vooft.kafka.serialization.common.wrappers.KafkaTopic
import io.github.vooft.kafka.serialization.common.wrappers.MemberId

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

// looks like in kafka itself they use "consumer" for this use case
private val CONSUMER_PROTOCOL_TYPE = Int16String("consumer")
