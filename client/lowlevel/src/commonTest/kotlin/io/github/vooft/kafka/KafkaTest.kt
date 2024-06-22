package io.github.vooft.kafka

import io.github.vooft.kafka.network.KafkaTransport
import io.github.vooft.kafka.network.createDefaultClient
import io.github.vooft.kafka.network.findGroupCoordinator
import io.github.vooft.kafka.network.joinGroup
import io.github.vooft.kafka.serialization.common.wrappers.GroupId
import io.github.vooft.kafka.serialization.common.wrappers.KafkaTopic
import io.github.vooft.kafka.serialization.common.wrappers.MemberId
import io.kotest.common.runBlocking

class KafkaTest {
//    @Test
    fun test(): Unit = runBlocking {
        val ktorClient = KafkaTransport.createDefaultClient()

        val connection = ktorClient.connect("localhost", 9093)

        val group = GroupId("test-group")
        val findCoordinatorResponse = connection.findGroupCoordinator(group)
        println(findCoordinatorResponse)

        val joinGroupResponse = connection.joinGroup(
            groupId = group,
            memberId = MemberId.EMPTY,
            topic = KafkaTopic("test")
        )

        println(joinGroupResponse)

        connection.close()
    }
}
