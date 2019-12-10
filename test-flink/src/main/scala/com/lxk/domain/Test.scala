package com.lxk.domain

import org.apache.flink.api.scala.ExecutionEnvironment
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time

object Test {

  def main(args: Array[String]): Unit = {

    /*
    如果本地跑就是local的模式
    如果提交到yarn上，就是yarn模式
    如果提交到standalone形式，就是standalone形式
    不需要指定，它会获取当前的环境
     */
    val senv = StreamExecutionEnvironment.getExecutionEnvironment

    val data = senv.socketTextStream("192.168.40.151",9999)

    val wordCounts = data
      //      .flatMap{w=>w.split("\\s")}
      .map(w=>WordCount(w.split(",")(0),w.split(",")(1).toLong))
      .keyBy("word")
      .max("count")
    //      .countWindowAll(5)
    //      .countWindow(5,2)
    //      .timeWindow(Time.seconds(3), Time.seconds(1))
    //      .sum("count")

    wordCounts.print().setParallelism(1)
    senv.execute("Stream")

    /*val benv = ExecutionEnvironment.getExecutionEnvironment
    val data = benv.readTextFile("")*/
  }

  case class WordCount(word:String,count:Long)

}