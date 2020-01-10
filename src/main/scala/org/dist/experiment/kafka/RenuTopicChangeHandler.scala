package org.dist.experiment.kafka

import java.util

import org.I0Itec.zkclient.IZkChildListener

class RenuTopicChangeHandler(client: ZooClientImpl,
                             onTopicChange:() => Unit) extends IZkChildListener {
  override def handleChildChange(parentPath: String,
                                 currentTopics: util.List[String]): Unit = {
    currentTopics.forEach(_ => onTopicChange())

  }
}
