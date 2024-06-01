package io.github.vooft.kafka

import io.github.vooft.kafka.network.findGroupCoordinator
import io.github.vooft.kafka.network.joinGroup
import io.github.vooft.kafka.network.ktor.KtorNetworkClient
import io.github.vooft.kafka.serialization.common.wrappers.GroupId
import io.github.vooft.kafka.serialization.common.wrappers.KafkaTopic
import io.github.vooft.kafka.serialization.common.wrappers.MemberId
import kotlinx.coroutines.test.runTest

class KafkaTest {
//    @Test
    fun test() = runTest {
        val ktorClient = KtorNetworkClient()

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
