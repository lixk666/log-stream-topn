package com.lxk


import org.apache.flink.api.common.state.ListState
import org.apache.flink.api.common.time.Time
import org.apache.flink.streaming.api.scala._
import org.apache.flink.util.Collector


object ThresholdWarning {


  def main(args: Array[String]): Unit = {

    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment

    val tuple2DataStreamSource: DataStream[(String, Long)] = env.fromElements(
      ("a", 50L), ("a", 80L), ("a", 400L),
      ("a", 100L), ("a", 200L), ("a", 200L),
      ("b", 100L), ("b", 200L), ("b", 200L),
      ("b", 500L), ("b", 600L), ("b", 700L))

    var listState: ListState[Long] = null


    tuple2DataStreamSource
      .keyBy(0)
      .flatMap((value: (String, Long), count: Collector[String]) => {
        if (value._2 > 100L) {
          count.collect("dsadsad")
        }
      })

      .print()
      .setParallelism(1)

    //tuple2DataStreamSource.print()
    env.execute("Managed Keyed State")

  }


}















