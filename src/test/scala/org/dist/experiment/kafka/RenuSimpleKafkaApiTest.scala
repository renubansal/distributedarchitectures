package org.dist.experiment.kafka

import org.dist.kvstore.JsonSerDes
import org.dist.queue.api.{RequestKeys, RequestOrResponse}
import org.dist.queue.common.TopicAndPartition
import org.dist.queue.server.Config
import org.dist.queue.utils.ZkUtils.Broker
import org.dist.queue.{TestUtils, ZookeeperTestHarness}
import org.dist.simplekafka.{LeaderAndReplicaRequest, LeaderAndReplicas, PartitionInfo, ReplicaManager}
import org.dist.util.Networks

class RenuSimpleKafkaApiTest extends ZookeeperTestHarness {

  test("create leader and follower replicas") {
    val config = Config(1, new Networks().hostname(), TestUtils.choosePort(), "", List(TestUtils.tempDir().getAbsolutePath))
    val replicaManager = new ReplicaManager(config)
    val api = new RenuSimpleKafkaApi(config,replicaManager)
    val broker1 = new Broker(1, "10.0.0.1", 8080)
    val broker2 = new Broker(2, "10.0.0.2", 8081)
    val partitionInfo = PartitionInfo(broker1, List(broker2))
    val otherPartitionInfo = PartitionInfo(broker2, List(broker1))
    val leaderAndReplicas = List(
      LeaderAndReplicas(TopicAndPartition("topic1", 1), partitionInfo),
      LeaderAndReplicas(TopicAndPartition("topic2", 2), otherPartitionInfo)
    )
    val leaderReplicaRequest = LeaderAndReplicaRequest(leaderAndReplicas)
    val correlationId = 12

    val response = api.handle(RequestOrResponse(RequestKeys.LeaderAndIsrKey, JsonSerDes.serialize(leaderReplicaRequest),correlationId))

    assert(response.correlationId == correlationId)
    assert(replicaManager.allPartitions.size == 2)

  }

}
