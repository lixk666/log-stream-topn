package com.lxk;

import org.apache.flink.api.common.functions.FlatMapFunction;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.TimeCharacteristic;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import org.apache.flink.streaming.api.windowing.assigners.SlidingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;

public class WordCountTopN {

    public static void main(String[] args) throws Exception {


        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

//        env.setStreamTimeCharacteristic(TimeCharacteristic.ProcessingTime); //以processtime作为时间语义

        DataStream<String> text = env.socketTextStream("bigdata-119", 9999).setParallelism(1); //监听指定socket端口作为输入

        DataStream<Tuple2<String, Integer>> ds = text
                .flatMap(new LineSplitter()); //将输入语句split成一个一个单词并初始化count值为1的Tuple2<String, Integer>类型
        DataStream<Tuple2<String, Integer>> wcount = ds
                .keyBy(0) //按照Tuple2<String, Integer>的第一个元素为key，也就是单词
                .window(SlidingProcessingTimeWindows.of(Time.seconds(10),Time.seconds(5)))
                //key之后的元素进入一个总时间长度为600s,每20s向后滑动一次的滑动窗口
                .sum(1);
        // 将相同的key的元素第二个count值相加
        DataStream<Tuple2<String, Integer>> ret = wcount
                .windowAll(TumblingProcessingTimeWindows.of(Time.seconds(3)))
                //所有key元素进入一个20s长的窗口（选20秒是因为上游窗口每20s计算一轮数据，topN窗口一次计算只统计一个窗口时间内的变化）
                .process(new TopNAllFunction(5));//计算该窗口TopN

        ret.print().setParallelism(1);
        env.execute("sadsad");
    }



}
class LineSplitter implements
        FlatMapFunction<String, Tuple2<String, Integer>> {

    @Override
    public void flatMap(String value, Collector<Tuple2<String, Integer>> out) {
        // normalize and split the line
        String[] tokens = value.toLowerCase().split(",");

        // emit the pairs
        for (String token : tokens) {
            if (token.length() > 0) {
                out.collect(new Tuple2<String, Integer>(token, 1));
            }
        }
    }
}