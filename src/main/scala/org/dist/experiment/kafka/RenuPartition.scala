package org.dist.experiment.kafka

import java.io._

import org.dist.queue.common.TopicAndPartition
import org.dist.queue.server.Config
import org.dist.simplekafka.SequenceFile
import scala.jdk.CollectionConverters._
class RenuPartition(config: Config, topicAndPartition: TopicAndPartition) {

  val logFile = new File(config.logDirs(0), topicAndPartition.topic + "-" + topicAndPartition.partition + ".log")

  val sequenceFile = new SequenceFile()
  val reader = new sequenceFile.Reader(logFile.getAbsolutePath)
  val writer = new sequenceFile.Writer(logFile.getAbsolutePath)

  def append(key: String, message: String) = {
    val currentPosition = writer.getCurrentPosition
    writer.append(key, message)
  }

  def read(offset: Long = 0) = {
    val result = new java.util.ArrayList[Row]()

    val offsetsFromThisOffset = sequenceFile.getAllOffSetsFrom(offset)
    offsetsFromThisOffset.foreach(offset => {
      val filePosition = sequenceFile.offsetIndexes.get(offset)
      val outputStream = new ByteArrayOutputStream()
      val dataOutputStream = new DataOutputStream(outputStream)

      reader.seekToOffset(filePosition)
      reader.next(dataOutputStream)

      val inputStream = new ByteArrayInputStream(outputStream.toByteArray)
      val dataInputStream = new DataInputStream(inputStream)
      result.add(Row.deserialize(dataInputStream))
    })
    result.asScala.toList
  }

  object Row {
    def serialize(row: Row, dos: DataOutputStream): Unit = {
      dos.writeUTF(row.key)
      dos.writeInt(row.value.getBytes().size)
      dos.write(row.value.getBytes) //TODO: as of now only supporting string writes.
    }

    def deserialize(dis: DataInputStream): Row = {
      val key = dis.readUTF()
      val dataSize = dis.readInt()
      val bytes = new Array[Byte](dataSize)
      dis.read(bytes)
      val value = new String(bytes) //TODO:As of now supporting only string values
      Row(key, value)
    }
  }

  case class Row(key: String, value: String)

}
