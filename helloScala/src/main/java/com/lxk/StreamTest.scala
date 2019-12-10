package com.lxk


import java.util.Comparator

import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.function.ProcessAllWindowFunction
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector
import org.omg.CORBA.ORBPackage.InconsistentTypeCode

import scala.collection.immutable.TreeMap
object StreamTest {
/*
  def main(args: Array[String]) : Unit = {

    // get the execution environment
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment

    // get input data by connecting to the socket
    val text = env.socketTextStream("192.168.40.119", 9999, '\n')

    // parse the data, group it, window it, and aggregate the counts
    val windowCounts = text
      .flatMap { w => w.split("\\s") }
      .map { w => WordWithCount(w, 1) }
      .keyBy("word")
      .timeWindow(Time.seconds(5), Time.seconds(1))
      .sum("count")

    // print the results with a single thread, rather than in parallel
    windowCounts.print().setParallelism(1)
    println(windowCounts)
    env.execute("Socket Window WordCount")
  }

  // Data type for words with count
  case class WordWithCount(word: String, count: Long)*/


  def main(args: Array[String]): Unit = {



    val env = StreamExecutionEnvironment.getExecutionEnvironment
    // 接收socket上的数据输入
    var streamSource: DataStream[String] = env.socketTextStream("bigdata-119", 9999)
   streamSource.flatMap(value => {
      value.split(" ")
    })
      .map(value => (value, 1))
      .keyBy(0)
//      .timeWindowAll(Time.seconds(3)).reduce((v1,v2)=>(v1._1,(v1._2+v2._2)))
//      .window(ProcessingTimeSessionWindows.withGap(Time.seconds(10))).sum(1)
//    .window(EventTimeSessionWindows.withGap(Time.seconds(10))).sum(1)
      .windowAll(TumblingProcessingTimeWindows.of(Time.milliseconds(10)))
//      .process()
//      .print().setParallelism(1)



    env.execute("Flink Streaming")

  }





}
