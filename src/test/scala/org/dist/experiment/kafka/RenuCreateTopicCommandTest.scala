package org.dist.experiment.kafka

import org.dist.queue.ZookeeperTestHarness
import org.dist.simplekafka.PartitionReplicas
import org.mockito.Mockito

class RenuCreateTopicCommandTest extends ZookeeperTestHarness {

  test("test Create Topic") {
    val client = Mockito.mock(classOf[ZooClientImpl])
    val partitionStrategy = Mockito.mock(classOf[PartitionStrategy])
    val topicCommand = new RenuCreateTopicCommand(client,partitionStrategy)
    val brokerIds = List(1, 2, 3)
    val replicationFactor = 2
    val noOfPartition = 3
    val partitionReplicas = Set(PartitionReplicas(1, List(1, 2)),
      PartitionReplicas(2, List(2, 3)),
      PartitionReplicas(3, List(3, 1))
    )

    Mockito.when(partitionStrategy.assignReplicasToBrokers(brokerIds,noOfPartition,replicationFactor)).
      thenReturn(partitionReplicas)
    Mockito.when(client.getAllBrokerIds()).thenReturn(brokerIds)

    topicCommand.createTopic("topic1",noOfPartition,replicationFactor)

    Mockito.verify(client, Mockito.times(1)).setPartitionReplicasForTopic("topic1", partitionReplicas)
  }

}


