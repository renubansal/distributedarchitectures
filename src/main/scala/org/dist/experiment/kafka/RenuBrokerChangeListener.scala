package org.dist.experiment.kafka

import java.util

import org.I0Itec.zkclient.IZkChildListener

case class BrokerState(var liveBrokers: Int)

class RenuBrokerChangeListener(brokerState: BrokerState) extends IZkChildListener{
  override def handleChildChange(parentPath: String, currentBrokers: util.List[String]): Unit = {
    brokerState.liveBrokers = currentBrokers.size()
  }
}
