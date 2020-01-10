package org.dist.experiment.kafka

import org.dist.queue.TestUtils
import org.dist.queue.common.TopicAndPartition
import org.dist.queue.server.Config
import org.dist.simplekafka.Partition
import org.dist.util.Networks
import org.scalatest.FunSuite

class RenuPartitionTest extends FunSuite {
  test("should write and read in partition log"){
    val config1 = Config(1, new Networks().hostname(), TestUtils.choosePort(), "", List(TestUtils.tempDir().getAbsolutePath))
    val partition = new RenuPartition(config1, TopicAndPartition("topic1", 0))
    val offset1 = partition.append("key1","message1")
    val offset2 = partition.append("key2","message2")

    val readResult: Seq[partition.Row] = partition.read()

    assert(readResult.size == 2)
    assert(readResult(0).key=="key1")
    assert(readResult(1).key=="key2")
    assert(readResult(0).value == "message1")
    assert(readResult(1).value == "message2")

    val readResultWithOffset: Seq[partition.Row] = partition.read(offset2)

    assert(readResultWithOffset.size == 1)
    assert(readResultWithOffset(0).key == "key2")
    assert(readResultWithOffset(0).value == "message2")
  }
}
