package org.dist.experiment.kafka

import org.dist.kvstore.JsonSerDes
import org.dist.queue.api.{RequestKeys, RequestOrResponse}
import org.dist.queue.common.TopicAndPartition
import org.dist.queue.server.Config
import org.dist.queue.utils.ZkUtils.Broker
import org.dist.queue.{TestUtils, ZookeeperTestHarness}
import org.dist.simplekafka.{LeaderAndReplicaRequest, LeaderAndReplicas, PartitionInfo, ReplicaManager, TopicMetadataRequest, TopicMetadataResponse, UpdateMetadataRequest}
import org.dist.util.Networks

class RenuSimpleKafkaApiTest extends ZookeeperTestHarness {

  test("create leader and follower replicas") {
    val config = createConfig
    val replicaManager = new ReplicaManager(config)
    val api =  new RenuSimpleKafkaApi(config, replicaManager)

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

  test("should update metadata key"){
    val config = createConfig
    val replicaManager = new ReplicaManager(config)
    val api = new RenuSimpleKafkaApi(config, replicaManager)
    val correlationId = 11
    val broker1 = new Broker(1, "10.0.0.1", 8080)
    val broker2 = new Broker(2, "10.0.0.2", 8081)
    val partitionInfo = PartitionInfo(broker1, List(broker2))
    val otherPartitionInfo = PartitionInfo(broker2, List(broker1))
    val leaderAndReplicas = List(
      LeaderAndReplicas(TopicAndPartition("topic1", 1), partitionInfo),
      LeaderAndReplicas(TopicAndPartition("topic2", 2), otherPartitionInfo)
    )
    val updateMetadataRequest = UpdateMetadataRequest(List(broker1, broker2), leaderAndReplicas)

    val response = api.handle(RequestOrResponse(RequestKeys.UpdateMetadataKey, JsonSerDes.serialize(updateMetadataRequest),correlationId))

    assert(response.correlationId == correlationId)
    assert(api.aliveBrokers.size == 2)
    assert(api.leaderCache.get(TopicAndPartition("topic1", 1)) == partitionInfo)
  }

  test("should get metadata based on the given topic"){
    val config = createConfig
    val replicaManager = new ReplicaManager(config)
    val api = new RenuSimpleKafkaApi(config, replicaManager)
    val correlationId = 11
    val broker1 = new Broker(1, "10.0.0.1", 8080)
    val broker2 = new Broker(2, "10.0.0.2", 8081)
    val partitionInfo = PartitionInfo(broker1, List(broker2))
    val otherPartitionInfo = PartitionInfo(broker2, List(broker1))
    val topicMetadataRequest = TopicMetadataRequest("topic1")
    api.leaderCache.put(TopicAndPartition("topic1", 1), partitionInfo)
    api.leaderCache.put(TopicAndPartition("topic2", 2), otherPartitionInfo)

    val request = RequestOrResponse(RequestKeys.GetMetadataKey, JsonSerDes.serialize(topicMetadataRequest), correlationId)
    val response = api.handle(request)

    assert(response.correlationId == correlationId)
    val topicMetadataResponse = JsonSerDes.deserialize(response.messageBodyJson.getBytes(), classOf[TopicMetadataResponse])
    assert(topicMetadataResponse.topicPartitions.size == 1)

  }

  private def createConfig = {
    Config(1, new Networks().hostname(), TestUtils.choosePort(), "", List(TestUtils.tempDir().getAbsolutePath))
  }

}
