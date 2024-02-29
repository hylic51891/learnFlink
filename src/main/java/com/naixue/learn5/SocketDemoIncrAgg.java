package com.naixue.learn5;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.functions.RichAggregateFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;

public class SocketDemoIncrAgg {
    public static void main(String[] args) throws  Exception{
        StreamExecutionEnvironment env =
                StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env.setStreamTimeCharacteristic(TimeCharacteristic.ProcessingTime);
        env.getConfig().setAutoWatermarkInterval(1000);
        DataStreamSource<String> dataStream = env.socketTextStream("localhost",
                9999);
        SingleOutputStreamOperator<Integer> intDStream = dataStream.map(number -> Integer.valueOf(number));
        intDStream.windowAll(TumblingEventTimeWindows.of(Time.seconds(10))) 
//                .reduce(new ReduceFunction<Integer>() {
//            @Override
//            public Integer reduce(Integer last, Integer cur) throws Exception {
//                return last +cur;
//            }
//        })
                .aggregate(new AggregateFunction<Integer, Tuple2<Integer,Integer>, Double>() {
                    @Override
                    public Tuple2<Integer, Integer> createAccumulator() {
                        return null;
                    }

                    @Override
                    public Tuple2<Integer, Integer> add(Integer integer, Tuple2<Integer, Integer> integerIntegerTuple2) {
                        return null;
                    }

                    @Override
                    public Double getResult(Tuple2<Integer, Integer> integerIntegerTuple2) {
                        return null;
                    }

                    @Override
                    public Tuple2<Integer, Integer> merge(Tuple2<Integer, Integer> integerIntegerTuple2, Tuple2<Integer, Integer> acc1) {
                        return null;
                    }
                })
                .print();
        env.execute(SocketDemoIncrAgg.class.getSimpleName());

    }
}
