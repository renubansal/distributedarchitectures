package org.dist.experiment.kafka

import java.util

import org.I0Itec.zkclient.exception.{ZkNoNodeException, ZkNodeExistsException}
import org.I0Itec.zkclient.{IZkChildListener, ZkClient}
import org.dist.kvstore.JsonSerDes
import org.dist.queue.utils.ZkUtils.Broker
import org.dist.simplekafka.{ControllerExistsException, PartitionReplicas}

import scala.jdk.CollectionConverters._

class ZooClientImpl(zkClient: ZkClient) {


  private val BROKER_ROOT_PATH = "/brokers/ids"
  private val TOPIC_PATH = "/brokers/topics"
  private val CONTROLLER_PATH = "/controller"


  def getBrokerPath(id: Int): String = {
    BROKER_ROOT_PATH + "/" + id
  }

  def getAllBrokerIds() = {
    zkClient.getChildren(BROKER_ROOT_PATH).asScala.map(_.toInt).toList
  }

  def createEphemeralPath(zkClient: ZkClient, path: String, data: String): Unit = {
    try {
      zkClient.createEphemeral(path, data)
    }
    catch {
      case _: ZkNoNodeException =>
        zkClient.createPersistent(BROKER_ROOT_PATH, true)
        zkClient.createEphemeral(path, data)
    }
  }

  def registerBroker(broker: Broker): Unit = {
    val path = getBrokerPath(broker.id)
    createEphemeralPath(zkClient, path, "")
  }

  def subscribeBrokerChangeListener(listener: IZkChildListener): util.List[String] = {
    zkClient.subscribeChildChanges(BROKER_ROOT_PATH, listener)
  }


  def tryCreatingControllerPath(data: String): Unit = {
    try{
      createEphemeralPath(zkClient, CONTROLLER_PATH, data)
    }
    catch {
      case _: ZkNodeExistsException =>
        throw ControllerExistsException(zkClient.readData(CONTROLLER_PATH))
    }

  }



  def getTopicPath(topicName: String) = {
    TOPIC_PATH + "/" + topicName
  }

  def createPersistentTopicPath(zkClient: ZkClient, path: String, data: String) = {
    try {
      zkClient.createPersistent(path, data)
    }
    catch {
      case _: ZkNoNodeException =>
        zkClient.createPersistent(TOPIC_PATH, true)
        zkClient.createPersistent(path, data)
    }
  }

  def setPartitionReplicasForTopic(topicName: String, partitionReplicas: Set[PartitionReplicas]) = {
    val topicPath = getTopicPath(topicName)
    val topicData = JsonSerDes.serialize(partitionReplicas)
    createPersistentTopicPath(zkClient,topicPath,topicData)

  }


}
