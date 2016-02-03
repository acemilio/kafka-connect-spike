package com.stratio.kafka.connect.parquet

import java.util

import org.apache.kafka.connect.connector.Task
import org.apache.kafka.connect.source.SourceConnector

class ParquetSourceConnector extends SourceConnector {
  private var filename: String = _
  private var topic: String = _
  val FILE_CONFIG: String = "file"
  val TOPIC_CONFIG: String = "topic"

  override def stop: Unit = {}


  override def start(props: util.Map[String, String]): Unit = {
    filename = props.get(FILE_CONFIG)
    topic = props.get(TOPIC_CONFIG)
  }


  override def version: String = "0.1"

  override def taskClass: Class[_ <: Task] = classOf[ParquetSourceTask]

  override def taskConfigs(maxTasks: Int): util.List[util.Map[String, String]] = {
    import scala.collection.JavaConversions._
    import scala.collection.mutable.ListBuffer

    val config = Map(
      FILE_CONFIG -> filename,
      TOPIC_CONFIG -> topic)
    bufferAsJavaList(ListBuffer(config))

  }
}