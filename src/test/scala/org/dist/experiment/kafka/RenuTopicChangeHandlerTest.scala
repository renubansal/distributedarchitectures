package org.dist.experiment.kafka

import org.dist.queue.{TestUtils, ZookeeperTestHarness}
import org.dist.queue.utils.ZkUtils.Broker

class RenuTopicChangeHandlerTest extends ZookeeperTestHarness {

  test("test Handle Child Change") {
    val client = new ZooClientImpl(zkClient)
    val onTopicChange: () => Unit = () => {
      print("topic changed")
    }

    val topicChangeHandler = new RenuTopicChangeHandler(client,onTopicChange)

    client.subscribeTopicChangeListener(topicChangeHandler)
    client.registerBroker(Broker(1,"10.0.0.1",8080))
    client.registerBroker(Broker(2,"10.0.0.2",8081))

    TestUtils.waitUntilTrue(() => {
      client.getAllBrokerIds().size == 2
    },"waiting to complete",1000)

    val topicCommand = new RenuCreateTopicCommand(client,new PartitionStrategy())
    topicCommand.createTopic("topic1",2,2)
    topicCommand.createTopic("topic2",2,2)
  }

}
