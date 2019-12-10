package com.lxk;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.functions.windowing.ProcessAllWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.util.Comparator;
import java.util.Map;
import java.util.TreeMap;

public class TopNAllFunction
        extends
        ProcessAllWindowFunction<Tuple2<String, Integer>, Tuple2<String, Integer>, TimeWindow> {

    private int topSize = 10;

    public TopNAllFunction(int topSize) {
        // TODO Auto-generated constructor stub

        this.topSize = topSize;
    }

    @Override
    public void process(
            ProcessAllWindowFunction<Tuple2<String, Integer>, Tuple2<String, Integer>, TimeWindow>.Context arg0,
            Iterable<Tuple2<String, Integer>> input,
            Collector<Tuple2<String, Integer>> out) throws Exception {
        // TODO Auto-generated method stub

        TreeMap<Integer, Tuple2<String, Integer>> treemap = new TreeMap<Integer, Tuple2<String, Integer>>(
                new Comparator<Integer>() {

                    @Override
                    public int compare(Integer y, Integer x) {
                        // TODO Auto-generated method stub
                        return (x < y) ? -1 : 1;
                    }

                }); //treemap按照key降序排列，相同count值不覆盖

        for (Tuple2<String, Integer> element : input) {
            treemap.put(element.f1, element);
            if (treemap.size() > topSize) { //只保留前面TopN个元素
                treemap.pollLastEntry();
            }
        }

        for (Map.Entry<Integer, Tuple2<String, Integer>> entry : treemap
                .entrySet()) {
            out.collect(entry.getValue());
        }

    }

}