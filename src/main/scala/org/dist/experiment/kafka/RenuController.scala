package org.dist.experiment.kafka

import java.util

import org.dist.kvstore.{InetAddressAndPort, JsonSerDes}
import org.dist.queue.api.RequestOrResponse
import org.dist.queue.utils.ZkUtils.Broker
import org.dist.simplekafka.{ControllerExistsException, LeaderAndReplicas, PartitionReplicas, SimpleSocketServer}

class RenuController(client: ZooClientImpl, id: Int, socketServer: SimpleSocketServer) {
  var currentLeader = 0

  def startup(): Unit = {
    elect()
  }

  def onBecomingLeader() = {
    client.subscribeTopicChangeListener(new RenuTopicChangeHandler(client, onTopicChange))
  }

  def elect(): Unit = {
    try {
      client.tryCreatingControllerPath(s"$id")
      onBecomingLeader()
      print("I am leader")
    }
    catch {
      case e: ControllerExistsException =>
        currentLeader = e.controllerId.toInt
        print("someone else is leader")
    }
  }

  def onTopicChange(): Unit = {
//    sendLeaderAndFollowerInfoForGivenPartition()
  }

  import scala.jdk.CollectionConverters._

  def sendLeaderAndFollowerInfoForGivenPartition(leaderAndReplicas: Seq[LeaderAndReplicas], partitionReplicas: Seq[PartitionReplicas]) = {
    val brokerToLeaderIsrRequest = new util.HashMap[Broker, java.util.List[LeaderAndReplicas]]()
    leaderAndReplicas.foreach(lr ⇒ {
      lr.partitionStateInfo.allReplicas.foreach(broker ⇒ {
        var leaderReplicas = brokerToLeaderIsrRequest.get(broker)
        if (leaderReplicas == null) {
          leaderReplicas = new util.ArrayList[LeaderAndReplicas]()
          brokerToLeaderIsrRequest.put(broker, leaderReplicas)
        }
        leaderReplicas.add(lr)
      })
    })

    val brokers = brokerToLeaderIsrRequest.keySet().asScala
    for (broker ← brokers) {
      val request = RequestOrResponse(101, JsonSerDes.serialize("sending leader request"), 12)
      socketServer.sendReceiveTcp(request, InetAddressAndPort.create(broker.host, broker.port))
    }
  }

}
