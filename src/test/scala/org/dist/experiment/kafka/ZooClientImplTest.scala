package org.dist.experiment.kafka

import org.dist.queue.utils.ZkUtils.Broker

class ZooClientImplTest extends org.dist.queue.ZookeeperTestHarness {

  test("should register broker"){
    val zooClientImpl = new ZooClientImpl(zkClient)
    val broker = new Broker(1, "10.0.0.1", 8080)
    zooClientImpl.registerBroker(broker)
    val actual = zkClient.getChildren("/brokers/ids")

    assert(actual.size() == 1)
    assert(actual.contains("1"))
  }

}
