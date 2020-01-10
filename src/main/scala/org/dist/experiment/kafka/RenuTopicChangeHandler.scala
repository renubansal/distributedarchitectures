package org.dist.experiment.kafka

import java.util

import org.I0Itec.zkclient.IZkChildListener
import org.dist.simplekafka.PartitionReplicas

class RenuTopicChangeHandler(client: ZooClientImpl,
                             onTopicChange:(String, Seq[PartitionReplicas]) => Unit) extends IZkChildListener {
  override def handleChildChange(parentPath: String,
                                 currentTopics: util.List[String]): Unit = {
    currentTopics.forEach(topicName => onTopicChange(topicName, Seq.empty))

  }
}
