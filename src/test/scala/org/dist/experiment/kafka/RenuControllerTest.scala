package org.dist.experiment.kafka

import org.dist.queue.ZookeeperTestHarness

class RenuControllerTest extends ZookeeperTestHarness {

  test("should elect itself as leader if no leader exists") {
    val client = new ZooClientImpl(zkClient)
    val controller = new RenuController(client, 1)
    controller.startup()
  }

  test("should raise exception if  leader exists") {
    val client = new ZooClientImpl(zkClient)
    val controller = new RenuController(client, 1)
    controller.startup()

    val otherController = new RenuController(client, 2)
    otherController.startup()

  }



}
