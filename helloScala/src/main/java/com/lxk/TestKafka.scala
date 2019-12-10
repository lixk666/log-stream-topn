package com.lxk

import java.util.Properties

import org.apache.flink.api.common.functions.MapFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.{CheckpointingMode, TimeCharacteristic}
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer


object TestKafka {
  private val ZOOKEEPER_HOST = "192.168.40.119:2181,192.168.40.122:2181,192.168.40.123:2181"
  private val KAFKA_BROKER = "192.168.40.119:9092,192.168.40.122:9092,192.168.40.123:9092"
  private val TRANSACTION_GROUP = "com.lxk.flink"

  def main(args: Array[String]): Unit = {


    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val properties = new Properties()
    // 指定Kafka的连接位置
    properties.setProperty("bootstrap.servers", KAFKA_BROKER)
    properties.setProperty("zookeeper.connect", ZOOKEEPER_HOST)
    properties.setProperty("group.id", TRANSACTION_GROUP)
    properties.setProperty("auto.offset.reset", "latest")


    // 指定监听的主题，并定义Kafka字节消息到Flink对象之间的转换规则
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    env.enableCheckpointing(1000)
    env.getCheckpointConfig.setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE)
    val mapper = new MapFunction[String, String] {
      override def map(value: String): String = {
        value + "aaaaa"
      }
    }

    env.addSource(new FlinkKafkaConsumer[String]("test-flink", new SimpleStringSchema(), properties))
      .setParallelism(1)
//      .map(value => (value.split(",")(0),value.split(",")(1)))
      .flatMap { w => w.split(",") }
      .map { w => WordCount(w, 1) }
      .filter(value => value.单词.equals("a"))
      .keyBy("单词")
      .reduce((v1,v2)=>WordCount(v1.单词,(v1.数量+v2.数量)))
      .print().setParallelism(1)

    env.execute("Flink Streaming")
//    println(WordCount.apply("a", 1))
  }

case class WordCount(单词:String,数量:Int)
}
