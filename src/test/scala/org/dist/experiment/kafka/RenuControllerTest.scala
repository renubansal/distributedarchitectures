package org.dist.experiment.kafka

import java.util

import org.dist.kvstore.InetAddressAndPort
import org.dist.queue.api.RequestOrResponse
import org.dist.queue.common.TopicAndPartition
import org.dist.queue.server.Config
import org.dist.queue.utils.ZkUtils.Broker
import org.dist.queue.{TestUtils, ZookeeperTestHarness}
import org.dist.simplekafka.{LeaderAndReplicas, PartitionInfo, PartitionReplicas, SimpleSocketServer}
import org.dist.util.Networks
import org.mockito.Mockito


class TestSocketServer(config: Config) extends SimpleSocketServer(config.brokerId, config.hostName, config.port, null) {
  var messages = new util.ArrayList[RequestOrResponse]()
  var toAddresses = new util.ArrayList[InetAddressAndPort]()

  override def sendReceiveTcp(message: RequestOrResponse, to: InetAddressAndPort): RequestOrResponse = {
    this.messages.add(message)
    this.toAddresses.add(to)
    RequestOrResponse(message.requestId, "", message.correlationId)
  }
}

class RenuControllerTest extends ZookeeperTestHarness {

  test("should elect itself as leader if no leader exists") {
    val client = new ZooClientImpl(zkClient)
    val sockerServer = Mockito.mock(classOf[SimpleSocketServer])
    val controller = new RenuController(client, 1, sockerServer)
    controller.startup()
  }

  test("should raise exception if  leader exists") {
    val client = new ZooClientImpl(zkClient)
    val sockerServer = Mockito.mock(classOf[SimpleSocketServer])
    val controller = new RenuController(client, 1, sockerServer)
    controller.startup()

    val otherController = new RenuController(client, 2, sockerServer)
    otherController.startup()

  }


  test("should send leader and follower info to given brokers") {
    val config = Config(1, new Networks().hostname(), TestUtils.choosePort(), zkConnect, List(TestUtils.tempDir().getAbsolutePath))
    val client = new ZooClientImpl(zkClient)
    val socketServer = new TestSocketServer(config)
    val controller = new RenuController(client, 1, socketServer)
    val broker1 = new Broker(1, "10.0.0.1", 8080)
    val broker2 = new Broker(2, "10.0.0.2", 8081)
    val broker3 = new Broker(3, "10.0.0.3", 8083)
    val partitionInfo = PartitionInfo(broker1, List(broker2))
    val leaderAndReplicas = Seq(
      LeaderAndReplicas(TopicAndPartition("topic1", 1), partitionInfo),
      LeaderAndReplicas(TopicAndPartition("topic1", 2), PartitionInfo(broker2, List(broker3))),
      LeaderAndReplicas(TopicAndPartition("topic1", 3), PartitionInfo(broker3, List(broker1)))
    )
    val partitionReplicas = Seq(PartitionReplicas(1, List(1, 2)),
      PartitionReplicas(2, List(2, 3)),
      PartitionReplicas(3, List(3, 1))
    )

    controller.sendLeaderAndFollowerInfoForGivenPartition(leaderAndReplicas, partitionReplicas)

    assert(socketServer.messages.size == 3)
    assert(socketServer.toAddresses.size == 3)
  }

}
