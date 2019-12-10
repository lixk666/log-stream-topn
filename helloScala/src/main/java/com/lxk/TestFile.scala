package com.lxk

import java.{lang, util}

import org.apache.flink.api.common.functions.RichFlatMapFunction
import org.apache.flink.api.common.state.{ListState, ListStateDescriptor}
import org.apache.flink.api.scala._
import org.apache.flink.util.Collector


object TestFile {
  def main(args: Array[String]): Unit = {
//    val filePath = "D:\\idea\\projects\\scalaTest\\helloScala\\src\\main\\resources\\flink.txt"
//    val env = ExecutionEnvironment.getExecutionEnvironment
//    env.readTextFile(filePath).print()

    class TestFile extends RichFlatMapFunction{
      override def flatMap(value: Nothing, out: Collector[Nothing]): Unit = {

      }

      var listState: ListState[Long] = getRuntimeContext.getListState(new ListStateDescriptor[Long]("sdsad",classOf[Long]))
    }

    var testFile:TestFile = new TestFile
    testFile.listState.add(100L)



    println(testFile.listState)
  }
}
