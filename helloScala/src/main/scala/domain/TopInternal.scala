package domain

import java.sql.Timestamp
import java.util.{ArrayList, Comparator, List, Properties}

import bean.{InternalLog, TopInternalHostLog}
import com.alibaba.fastjson.JSON
import org.apache.flink.api.common.functions.AggregateFunction
import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.api.common.state.{ListState, ListStateDescriptor}
import org.apache.flink.api.java.tuple.{Tuple, Tuple1}
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.function.WindowFunction
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

object TopInternal {

  private val ZOOKEEPER_HOST = "192.168.40.119:2181,192.168.40.122:2181,192.168.40.123:2181"
  private val KAFKA_BROKER = "192.168.40.119:9092,192.168.40.122:9092,192.168.40.123:9092"
  private val TRANSACTION_GROUP = "com.lxk.flink2"

  def main(args: Array[String]): Unit = {
    //获取上下文环境变量
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    //设置kafka的配置参数
    var properties = new Properties()
    // 指定Kafka的连接位置
    properties.setProperty("bootstrap.servers", KAFKA_BROKER)
    properties.setProperty("zookeeper.connect", ZOOKEEPER_HOST)
    properties.setProperty("group.id", TRANSACTION_GROUP)
    properties.setProperty("auto.offset.reset", "latest")


    env.setParallelism(1)
    var KafkaSource: DataStream[String] = env.addSource(new FlinkKafkaConsumer[String]("test-flink", new SimpleStringSchema(), properties)).setParallelism(1)

    KafkaSource.
      map(value => {
          JSON.parseObject(value, classOf[InternalLog])
      })
      //引入时间
      .assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor[InternalLog](Time.seconds(5)) {
      override def extractTimestamp(element: InternalLog): Long = {
        element.get__time()
      }
    })
      .keyBy(value=>{
        value.getSource
      })
      .timeWindow(Time.minutes(1),Time.seconds(5))
//      //      .sum("session_num")
      .aggregate(new CountAgg,new WindowResultFunction)
      .keyBy(1)
      .process(new TopNInter(5))
      .print()

    env.execute("JOB")

  }

  /**
    * 自定义输出格式
    */
  class TopNInter extends KeyedProcessFunction[Tuple,(String,Long,Int,Int,Int),String]{
    private var topSize = 0

    def this(topSize: Int) {
      this()
      this.topSize = topSize
    }

    private var hotState : ListState[(String,Long,Int,Int,Int)] = null
    @throws[Exception]
    override def open(parameters: Configuration): Unit ={
      // 命名状态变量的名字和状态变量的类型
      val itemsStateDesc = new ListStateDescriptor[(String,Long,Int,Int,Int)]("itemState-state", classOf[(String,Long,Int,Int,Int)])
      // 从运行时上下文中获取状态并赋值
      hotState = getRuntimeContext.getListState(itemsStateDesc)
    }


    @throws[Exception]
    override def onTimer(timestamp: Long, ctx: KeyedProcessFunction[Tuple, (String, Long,Int,Int,Int), String]#OnTimerContext, out: Collector[String]): Unit = {
      var allCats: List[(String, Long,Int,Int,Int)] = new ArrayList[(String, Long,Int,Int,Int)]
      import scala.collection.JavaConversions._
      for (item <- hotState.get) {
        allCats.add(item)
      }
      // 提前清除状态中的数据，释放空间
      hotState.clear()
      // 按照点击量从大到小排序
      allCats.sort(new Comparator[(String, Long,Int,Int,Int)]() {
        override def compare(o1: (String, Long,Int,Int,Int), o2: (String, Long,Int,Int,Int)): Int = (o2._3 - o1._3).toInt
      })
      // 将排名信息格式化成 String, 便于打印
      var result: StringBuilder = new StringBuilder
      result.append("====================================\n")
      result.append("时间: ").append(new Timestamp(timestamp - 1)).append("\n")
      var i: Int = 0
      while (i < allCats.size && i < topSize) {
        var currentItem: (String,Long,Int,Int,Int) = allCats.get(i)
        // No1:  商品ID=12224  浏览量=2413
        result.append("No").append(i).append(":").append(" 客户端IP=").append(currentItem._1).append("  会话次数=").append(currentItem._3).append(" 包数=").append(currentItem._4).append(" 字节数=").append(currentItem._5).append("\n")
        i += 1
      }
      result.append("====================================\n\n")
      // 控制输出频率，模拟实时滚动结果
//      Thread.sleep(1000)
      out.collect(result.toString)
    }


    override def processElement(value: (String, Long,Int,Int,Int), ctx: KeyedProcessFunction[Tuple, (String, Long, Int,Int,Int), String]#Context, out: Collector[String]): Unit = {
      hotState.add(value)
      // 注册 windowEnd+1 的 EventTime Timer, 当触发时，说明收齐了属于windowEnd窗口的所有商品数据
      // 也就是当程序看到windowend + 1的水位线watermark时，触发onTimer回调函数
      ctx.timerService.registerEventTimeTimer(value._2 + 1)
    }


  }

  /**
    * 自定义累加器
    */
  class CountAgg extends AggregateFunction[InternalLog, (Int,Int,Int), (Int,Int,Int)] {
    override def createAccumulator(): (Int, Int, Int) = (0,0,0)

    override def add(value: InternalLog, accumulator: (Int, Int, Int)): (Int, Int, Int) = (accumulator._1+1,accumulator._2+value.getC2s_byte_num + value.getS2c_byte_num,accumulator._3+value.getC2s_pkt_num+value.getS2c_pkt_num)

    override def getResult(accumulator: (Int, Int, Int)): (Int, Int, Int) =accumulator

    override def merge(a: (Int, Int, Int), b: (Int, Int, Int)): (Int, Int, Int) = (a._1+b._1,a._2+b._2,a._3+b._3)
  }

  /**
    * 自定义窗口累加
    */
  class WindowResultFunction extends WindowFunction[(Int,Int,Int), (String, Long, Int,Int,Int), String, TimeWindow] {

    override def apply(key: String, window: TimeWindow, input: Iterable[(Int, Int, Int)], out: Collector[(String, Long, Int, Int, Int)]): Unit = {
        val session_num: (Int, Int, Int) = input.iterator.next
        out.collect((key,window.getEnd,session_num._1,session_num._2,session_num._3))

}
  }

  case class Student(name: String, age: Int)

}
