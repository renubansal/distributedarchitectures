package org.dist.experiment.kafka

import java.util

import org.dist.kvstore.JsonSerDes
import org.dist.queue.api.{RequestKeys, RequestOrResponse}
import org.dist.queue.common.TopicAndPartition
import org.dist.queue.server.Config
import org.dist.queue.utils.ZkUtils.Broker
import org.dist.simplekafka.{LeaderAndReplicaRequest, PartitionInfo, ReplicaManager, TopicMetadataRequest, TopicMetadataResponse, UpdateMetadataRequest}

import scala.jdk.CollectionConverters._

class RenuSimpleKafkaApi(config: Config, replicaManager: ReplicaManager) {
  var aliveBrokers = List[Broker]()
  var leaderCache = new util.HashMap[TopicAndPartition, PartitionInfo]

  def handle(request: RequestOrResponse): RequestOrResponse ={
    request.requestId match {
      case RequestKeys.LeaderAndIsrKey => {
        val leaderAndReplicaRequest = JsonSerDes.deserialize(request.messageBodyJson.getBytes(), classOf[LeaderAndReplicaRequest])
        leaderAndReplicaRequest.leaderReplicas.foreach(leaderReplica => {
          val leader = leaderReplica.partitionStateInfo.leader
          val topicPartition = leaderReplica.topicPartition
          if (leader.id == config.brokerId) {
            replicaManager.makeLeader(topicPartition)
          }
          else {
            replicaManager.makeFollower(topicPartition, leader.id)
          }
        })
        RequestOrResponse(RequestKeys.LeaderAndIsrKey, "", request.correlationId)
      }
      case RequestKeys.UpdateMetadataKey => {
        val updateMetadataRequest = JsonSerDes.deserialize(request.messageBodyJson.getBytes(), classOf[UpdateMetadataRequest])
        aliveBrokers = updateMetadataRequest.aliveBrokers
        updateMetadataRequest.leaderReplicas.foreach(leaderReplica => {
          leaderCache.put(leaderReplica.topicPartition, leaderReplica.partitionStateInfo)
        })
        RequestOrResponse(RequestKeys.UpdateMetadataKey, "", request.correlationId)
      }
      case RequestKeys.GetMetadataKey => {
        val topicMetadataRequest = JsonSerDes.deserialize(request.messageBodyJson.getBytes(), classOf[TopicMetadataRequest])
        val matchedTopics = leaderCache.keySet().asScala.toList.filter(key => key.topic == topicMetadataRequest.topicName)
        val topicMetadata = matchedTopics.map(topic => (topic, leaderCache.get(topic))).toMap

        RequestOrResponse(RequestKeys.GetMetadataKey, JsonSerDes.serialize(TopicMetadataResponse(topicMetadata)),
          request.correlationId)
      }
    }
  }

}
