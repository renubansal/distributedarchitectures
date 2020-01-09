package org.dist.experiment.kafka

class RenuCreateTopicCommand(client: ZooClientImpl, partitionStrategy: PartitionStrategy) {

  def createTopic(topicName: String, noOfPartitions: Int, replicationFactor: Int) = {
    val brokerIds = client.getAllBrokerIds()
    val partitionReplicas = partitionStrategy.assignReplicasToBrokers(
      brokerIds, noOfPartitions, replicationFactor)

    client.setPartitionReplicasForTopic(topicName, partitionReplicas)

  }


}
