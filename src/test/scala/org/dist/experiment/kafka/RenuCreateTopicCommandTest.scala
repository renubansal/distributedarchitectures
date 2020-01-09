package org.dist.experiment.kafka

import org.dist.queue.ZookeeperTestHarness
import org.dist.simplekafka.PartitionReplicas
import org.mockito.{ArgumentCaptor, ArgumentMatchers, Mockito}

class RenuCreateTopicCommandTest extends ZookeeperTestHarness {

  test("test Create Topic") {
    val client = Mockito.mock(classOf[ZooClientImpl])
    val topicCommand = new RenuCreateTopicCommand(client,new PartitionStrategy())
    Mockito.when(client.getAllBrokerIds()).thenReturn(List(1,2,3));
    val noOfPartition = 3
    topicCommand.createTopic("topic1",noOfPartition,2)

    val argumentCaptor = ArgumentCaptor.forClass(classOf[Set[PartitionReplicas]])
    Mockito.verify(client).setPartitionReplicasForTopic(ArgumentMatchers.eq("topic1"), argumentCaptor.capture())

    val actualPartitionReplicas = argumentCaptor.getValue.asInstanceOf[Set]
    assert(actualPartitionReplicas.size == noOfPartition)

  }

}


