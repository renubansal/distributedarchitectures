package org.dist.experiment.kafka

import java.util

import org.dist.kvstore.{InetAddressAndPort, JsonSerDes}
import org.dist.queue.api.{RequestKeys, RequestOrResponse}
import org.dist.queue.common.TopicAndPartition
import org.dist.queue.utils.ZkUtils.Broker
import org.dist.simplekafka.{ControllerExistsException, LeaderAndReplicaRequest, LeaderAndReplicas, PartitionInfo, PartitionReplicas, SimpleSocketServer}

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

  def onTopicChange(topicName: String, partitionReplicas: Seq[PartitionReplicas]): Unit = {
    val leaderAndReplicas: Seq[LeaderAndReplicas] = selectLeaderAndFollowerForPartition(topicName, partitionReplicas)
    sendLeaderAndFollowerInfoForGivenPartition(leaderAndReplicas, partitionReplicas)
  }

  def selectLeaderAndFollowerForPartition(topicName: String, partitionReplicas: Seq[PartitionReplicas]) ={
    val leaderAndReplicas: Seq[LeaderAndReplicas] = partitionReplicas.map(partitionReplica => {
      val leader = client.getBrokerInfo(partitionReplica.brokerIds.head)
      val followers = partitionReplica.brokerIds.tail.map(id => client.getBrokerInfo(id))
      LeaderAndReplicas(new TopicAndPartition(topicName, partitionReplica.partitionId), PartitionInfo(leader, followers))
    })
    leaderAndReplicas
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
      val leaderAndReplicaRequest = LeaderAndReplicaRequest(leaderAndReplicas.toList)
      val request = RequestOrResponse(RequestKeys.LeaderAndIsrKey, JsonSerDes.serialize(leaderAndReplicaRequest), 12)
      socketServer.sendReceiveTcp(request, InetAddressAndPort.create(broker.host, broker.port))
    }
  }

}
