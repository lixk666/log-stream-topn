package domain

import java.lang

import com.alibaba.fastjson.{JSON, JSONObject}
import org.apache.flink.api.common.functions.AggregateFunction
import org.apache.flink.api.java.tuple.Tuple
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.function.WindowFunction
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

import scala.util.parsing.json.JSONObject


object WordCountTopNScala {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    var text: DataStream[String] = env.socketTextStream("192.168.40.119", 9999)

    text
    //      .setParallelism(1)
    ////      .flatMap(value=>{value.split(",")})
    //      .map(value=>{
    ////      case Student=>JSON.parseObject(value,classOf[Student])
    ////      case _ => value
    //      val student: Student = JSON.parseObject(value,classOf[Student])
    //      student
    //
    //    }).keyBy("name")
    ////      .map(value=>(value,1))
    ////      .keyBy(0)
    //////      .sum(1)
    ////      .timeWindow(Time.minutes(5),Time.seconds(3))
    ////      .sum(1)
    //      .timeWindow(Time.seconds(5))
    //      .sum("age")
    //      .print()
    //      .setParallelism(1)


    text
      .setParallelism(1)
      //      .flatMap(value=>{value.split(",")})
      .map(value => {
      //      case Student=>JSON.parseObject(value,classOf[Student])
      //      case _ => value
      val student: Student = JSON.parseObject(value, classOf[Student])
      student

    }).keyBy(0)
      .timeWindow(Time.seconds(10))
      .aggregate(new CountAgg,new WindowResultFunction)
      .print()
      .setParallelism(1)


    env.execute("Job")
  }

  class CountAgg extends AggregateFunction[Student,Int,Int]{
    override def createAccumulator(): Int = 0;

    override def add(value: Student, accumulator: Int): Int = accumulator +1;

    override def getResult(accumulator: Int): Int = accumulator;

    override def merge(a: Int, b: Int): Int = a + b;
  }

  class WindowResultFunction extends WindowFunction[Int,Student,Tuple,TimeWindow]{
    @throws[Exception]
    override def apply(key: Tuple, window: TimeWindow, input: Iterable[Int], out: Collector[Student]): Unit = {
      var itemId = key.asInstanceOf[Tuple1[Int]]._1

      var count = input.iterator.next
      out.collect(Student("tom",18))
    }
  }

  case class Student(name: String, age: Int)

}
