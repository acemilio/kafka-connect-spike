package com.stratio.kafka.connect.parquet

import java.util

import org.apache.avro.generic.GenericRecord
import org.apache.hadoop.fs.Path
import org.apache.kafka.connect.data.Schema
import org.apache.kafka.connect.source.{SourceRecord, SourceTask}
import org.apache.parquet.avro.AvroParquetReader


class ParquetSourceTask extends SourceTask {
  var parquetPath: Path = _
  var topic: String = _

  def offsetKey(parquetPath: Path): Map[String, Path] = Map("parquetPath" -> parquetPath)

  def offsetValue(offset: Long): Map[String, Long] = Map("position" -> offset)

  override def poll(): util.List[SourceRecord] = {
    val avroParquetReader: AvroParquetReader[GenericRecord] = new AvroParquetReader(parquetPath)

    import scala.collection.JavaConversions._
    import scala.collection.mutable.ListBuffer

    val listBuffer = ListBuffer[SourceRecord]()
    while(
      Option(avroParquetReader.read).map { entry =>
        listBuffer += new SourceRecord(Map("parquetPath" -> parquetPath), Map("position" -> 0L), topic, Schema.STRING_SCHEMA, entry)
      }.isDefined) ()

    bufferAsJavaList(listBuffer)

    /*for {
    record <- Option(avroParquetReader.read)
    } yield new SourceRecord(Map("parquetPath" -> parquetPath), Map("position" -> 0L), topic, Schema.STRING_SCHEMA, record)*/
  }

  override def start(props: util.Map[String, String]): Unit = {
    parquetPath = new Path(props.get("file"))
    topic = props.get("topic")
  }


  override def version(): String = "0.1"

  override def stop(): Unit = {}


}