package org.dist.experiment.kafka

import org.dist.queue.ZookeeperTestHarness
import org.mockito.Mockito

class RenuCreateTopicCommandTest extends ZookeeperTestHarness {

  test("test Create Topic") {
    val client = Mockito.mock(classOf[ZooClientImpl])
    val topicCommand = new RenuCreateTopicCommand(client,new PartitionStrategy())
    Mockito.when(client.getAllBrokerIds()).thenReturn(List(1,2,3));
    topicCommand.createTopic("topic1",3,2)

  }

}
