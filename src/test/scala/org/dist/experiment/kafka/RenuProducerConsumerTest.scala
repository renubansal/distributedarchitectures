package org.dist.experiment.kafka

import org.dist.queue.{TestUtils, ZookeeperTestHarness}
import org.dist.queue.server.Config
import org.dist.queue.utils.ZkUtils.Broker
import org.dist.util.Networks

class RenuProducerConsumerTest extends ZookeeperTestHarness{

  test("end to end test"){

    val client  = new ZooClientImpl(zkClient)
    client.registerBroker(new Broker(1,"10.0.0.1",8080))
    client.registerBroker(new Broker(2,"10.0.0.2",8081))
    client.registerBroker(new Broker(3,"10.0.0.3",8082))
    client.registerBroker(new Broker(4,"10.0.0.4",8083))



  }
  private def createConfig = {
    Config(1, new Networks().hostname(), TestUtils.choosePort(), "", List(TestUtils.tempDir().getAbsolutePath))
  }

}
