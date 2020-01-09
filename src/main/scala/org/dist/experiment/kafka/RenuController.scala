package org.dist.experiment.kafka

import org.dist.simplekafka.ControllerExistsException

class RenuController(client: ZooClientImpl, id: Int) {
  var currentLeader = 0
  def startup(): Unit = {
    elect()
  }

  def elect(): Unit = {
    try {
      client.tryCreatingControllerPath(s"$id")
      print("I am leader")
    }
    catch {
      case e: ControllerExistsException =>
        currentLeader = e.controllerId.toInt
        print("someone else is leader")
    }
  }

}
