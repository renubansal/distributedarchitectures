package org.dist.experiment.kafka

import org.dist.queue.{TestUtils, ZookeeperTestHarness}
import org.dist.queue.utils.ZkUtils.Broker

class RenuBrokerChangeListenerTest extends ZookeeperTestHarness {

  test("should update the live brokers") {
    val brokerState = BrokerState(0)
    val brokerChangeListener = new RenuBrokerChangeListener(brokerState)

    val client = new ZooClientImpl(zkClient)
    client.subscribeBrokerChangeListener(brokerChangeListener)
    client.registerBroker(Broker(1,"10.0.0.1",8080))
    client.registerBroker(Broker(2,"10.0.0.2",8081))
    client.registerBroker(Broker(3,"10.0.0.3",8082))
    TestUtils.waitUntilTrue(() => {
      brokerState.liveBrokers == 3
    },"waiting to complete",1000)

    assert(brokerState.liveBrokers == 3)
  }

}
