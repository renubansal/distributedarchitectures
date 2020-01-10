package org.dist.experiment.kafka

import org.dist.kvstore.JsonSerDes
import org.dist.queue.api.{RequestKeys, RequestOrResponse}
import org.dist.queue.server.Config
import org.dist.simplekafka.{LeaderAndReplicaRequest, ReplicaManager}

class RenuSimpleKafkaApi(config: Config, replicaManager: ReplicaManager) {

  def handle(request: RequestOrResponse): RequestOrResponse ={
    request.requestId match {
      case RequestKeys.LeaderAndIsrKey =>
        val leaderAndReplicaRequest = JsonSerDes.deserialize(request.messageBodyJson.getBytes(), classOf[LeaderAndReplicaRequest])
        leaderAndReplicaRequest.leaderReplicas.foreach(leaderReplica => {
          val leader = leaderReplica.partitionStateInfo.leader
          val topicPartition = leaderReplica.topicPartition
          if (leader.id == config.brokerId){
            replicaManager.makeLeader(topicPartition)
          }
          else{
            replicaManager.makeFollower(topicPartition,leader.id)
          }
        })
    }
    RequestOrResponse(RequestKeys.LeaderAndIsrKey,"",request.correlationId)
  }

}
